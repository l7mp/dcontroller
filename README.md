<!-- [![Go Report Card](https://goreportcard.com/badge/sigs.k8s.io/controller-runtime)](https://goreportcard.com/report/sigs.k8s.io/controller-runtime) -->
<!-- [![godoc](https://pkg.go.dev/badge/sigs.k8s.io/controller-runtime)](https://pkg.go.dev/sigs.k8s.io/controller-runtime) -->

# Δ-controller: Declarative Kubernetes controller framework

Δ-controller is a framework to simplify the design, implementation and maintenance of Kubernetes
operators. The main goal is to reduce the mental overhead of writing Kubernetes operators by
providing simple automations to eliminate some of the repetitive code that must be written when
coding against the [Kubernetes
controller-runtime](https://github.com/kubernetes-sigs/controller-runtime) project. The final goal
is to let anyone with minimal Go skills to write complex Kubernetes operators in the NoCode/LowCode
style.

## Description

The main design philosophy behind Δ-controller is to view the Kubernetes API as a gigantic NoSQL
database and implement a custom query language that can be used to process Kubernetes objects into
either a simplified internal representation that we call a *view*, or back into actual native
Kubernetes resources. This is much akin to MongoDB, so users familiar with document-oriented
databases and declarative queries will feel at home when using Δ-controller.

The Δ in the name stands for **declarative** and **delta**.

First, Δ-controller implements a *declarative query language* (inspired by [MongoDB
aggregators](https://www.mongodb.com/docs/manual/core/aggregation-pipeline)) that can be used to
combine, filter and transform Kubernetes API resources into a representation that better fits the
operators' internal logic, and from there back to native Kubernetes resources.  This is done by
registering a processing pipeline to map the Kubernetes API resources into a view of interest to
the controller. Views are dynamically maintained by Δ-controller by running the aggregation
pipeline on the watch events automatically installed for the source Kubernetes API resources of the
aggregation pipeline.

The literal delta in the name connotes that operators in Δ-controller use *incremental
reconciliation* style control loops. This means that the controllers watch the incremental changes
to Kubernetes API resources and internal views and act only on the deltas. This simplifies writing
[edge-triggered
conrollers](https://hackernoon.com/level-triggering-and-reconciliation-in-kubernetes-1f17fe30333d).

## Installation

Deploy Δ-controller with Helm:

``` console
helm repo add dcontroller https://hsnlab.github.io/dcontroller/
helm repo update
helm install dcontroller dcontroller/dcontroller
```

This will deploy the Δ-controller operator, a Kubernetes operator that can take custom resources
describing a declarative controller and implement the logics on the Kubernetes API.

## Getting started

We will create a simple operator below that will write into each pod the number containers of the
pod as a custom annotation. To simplify the operations, we will restrict the operator to the
default namespace. Whenever the container number is updated, the annotation will be updated too.

For [here](examples/) for more complex examples.

### Operator

First we create the operator that will implement the reconcile logic:

```console
kubectl apply -f - <<EOF
apiVersion: dcontroller.io/v1alpha1
kind: Operator
metadata:
  name: pod-container-num-annotator
spec:
  controllers:
    - name: pod-container-num-annotator
      sources:
        - apiGroup: ""
          kind: Pod
          namespace: default
      pipeline:
        "@aggregate":
          - "@project":
              metadata:
                name: "$.metadata.name"
                namespace: "$.metadata.namespace"
                annotations:
                  "dcontroller.io/container-num":
                    '@string':
                      '@len': ["$.spec.containers"]
      target:
        apiGroup: ""
        kind: Pod
        type: Patcher
EOF
```

The operator definition includes a single controller. The name specifies a unique controller name
that is used to refer to individual controllers inside the operator. The `sources` field specifies
the Kubernetes API resource(s) the operator watches, and the target spec describes the API resource
the operator will write. This time, both the source and target will be Pods. The target type is
`Patcher`, which means that the controller will use the processing pipeline output to patch the
target. (In contrast, `Updater` targets will simply overwrite the target.)

The most important field is the `pipeline`, which specifies a declarative pipeline to process the
source API resource(s) into the target resource. The fields of the source and the target resource
can be specified using standard [JSONPath notation](https://datatracker.ietf.org/doc/html/rfc9535).

```yaml
"@aggregate":
  - "@project":
      metadata:
        name: "$.metadata.name"
        namespace: "$.metadata.namespace"
        annotations:
          "dcontroller.io/container-num":
            '@string':
              '@len': ["$.spec.containers"]
```

The pipeline comprises a single aggregation operation, namely a projection. This `@project` op will
create a patch by copying the name and namespace from the pod's metadata and adding the length of
the `containers` list in the Pod spec as an annotation to it. The `@string` op is included to
explicitly cast the numeric length value into a string.

Ant that's the whole idea: you specify one or more Kubernetes API resources to watch, a declarative
pipeline that will process the input resources into a patch that is then automatically applied to
the target resource. The Δ-controller operators are completely dynamic so you can add, delete and
modify them anytime, they are fully described in a single custom resource with the entire logics,
there can be any number of operators running in parallel (but make sure they do not create infinite
update cycles!), and the entire framework involves zero line of code.

## Test

We will test the operator with a sample pod:

```console
kubectl run net-debug --image=docker.io/l7mp/net-debug:latest
```

Check that the operator status indicates no errors:

```console
kubectl get operators.dcontroller.io pod-container-num-annotator -o yaml
apiVersion: dcontroller.io/v1alpha1
kind: Operator
metadata:
  name: pod-container-num-annotator
spec:
...
status:
  controllers:
  - name: pod-container-num-annotator
    conditions:
    - lastTransitionTime: "2024-10-03T13:30:11Z"
      message: Controller is up and running
      observedGeneration: 1
      reason: Ready
      status: "True"
      type: Ready
```

Then check whether the container number actually appears as an annotation on the pod:

```console
kubectl get pod net-debug -o jsonpath='{.metadata.annotations}'
{"dcontroller.io/container-num":"1"}
```

So far so good. Now create another pod with 3 containers:

```console
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: net-debug-2
spec:
  containers:
  - name: net-debug-1
    image: docker.io/l7mp/net-debug:latest
  - name: net-debug-2
    image: docker.io/l7mp/net-debug:latest
  - name: net-debug-3
    image: docker.io/l7mp/net-debug:latest
EOF
```

The annotation should no indicate 3 containers:

```console
kubectl get pod net-debug-2 -o jsonpath='{.metadata.annotations}'
{"dcontroller.io/container-num":"3"}
```

Note that pods in other namespaces will not have the container-num annotation:

```console
kubectl -n kube-system get pod etcd -o jsonpath='{.metadata.annotations}'
...
```

## Cleanup

Remove all resources we have created:

```console
kubectl delete pod net-debug
kubectl delete pod net-debug-2
kubectl delete operators.dcontroller.io pod-container-num-annotator
```

<!-- ```console -->
<!-- kubectl run net-debug --image=docker.io/l7mp/net-debug:latest -->
<!-- ``` -->

<!-- ```console -->
<!-- kubectl apply -f - <<EOF -->
<!-- apiVersion: dcontroller.io/v1alpha1 -->
<!-- kind: Operator -->
<!-- metadata: -->
<!--   name: pod-cpu-usage-annotator -->
<!-- spec: -->
<!--   controllers: -->
<!--     - name: pod-cpu-usage-controller -->
<!--       sources:  -->
<!--         - apiGroup: "" -->
<!--           kind: Pod -->
<!--           namespace: default -->
<!--         - apiGroup: "metrics.k8s.io" -->
<!--           kind: PodMetrics -->
<!--           namespace: default -->
<!--       pipeline: -->
<!--         "@join": -->
<!--           '@eq': [ $.Pod.metadata.name, $.PodMetrics.metadata.name ] -->
<!--         "@aggregate": -->
<!--           - "@project": -->
<!--               metadata: -->
<!--                 name: $.Pod.metadata.name -->
<!--                 namespace: $.Pod.metadata.namespace -->
<!--                 annotations: -->
<!--                   "dcontroller.io/cpu-usage": "$.PodMetrics.containers[0].usage.cpu" -->
<!--       target: -->
<!--         apiGroup: "" -->
<!--         kind: Pod -->
<!--         type: Patcher -->
<!-- EOF -->
<!-- ``` -->

<!-- ### Expressions -->

<!-- - Aggregations work on objects that are indexed on (.metadata.namespace, .metadata.name): all -->
<!--   objects at every stage of the aggregation must have valid .metadata. -->
<!-- - Operator arguments go into lists, optional for single-argument ops (like @len and @not).  -->
<!-- - No multi-dimensional lists: arrays our unpacked to the top level. -->

## Caveats

- The operator runs with full RBAC capabilities. This is required at this point to make sure that
  the declarative operators loaded at runtime can access any Kubernetes API resource they intend to
  watch or modify. Later we may implement a dynamic RBAC scheme that would minimize the required
  permissions to the minimal set of APi resources the running operators need.
- The strategic merge patch implementation does not handle lists. Since Kubernetes does not
  implement native strategic merge patching for schemaless unstructured resources, currently
  patches must be implemented a simplified local JSON patch code that does not handle lists. Use
  `Patcher` targets carefullly.

## License

Copyright 2024 by its authors. Some rights reserved. See [AUTHORS](AUTHORS).

Apache License - see [LICENSE](LICENSE) for full text.
