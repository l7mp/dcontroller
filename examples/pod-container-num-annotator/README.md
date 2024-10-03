# Pod container number annotator

In this example we are going to create a simple operator that will write into each pod the number
containers of the pod as a custom annotation. To simplify the operations, we will restrict the
operator to the default namespace.

## Description

This example will specify a simple "self-referential" Kubernetes operator that watches and writes
pods. The operator will implemented in a completely declarative form using the Δ-controller
framework. The functionality will be extremely simple: the operator will add a custom annotation
into each pod in the default namespace that will specify the number of containers of the pod, and
whenever the container number is updated the annotation will be updated too.

## Setup

Make sure Δ-controller is up and running.

## Operator

Insert the operator that will implement the reconcile logic:

```console
kubectl apply -f examples/pod-container-num-annotator/pod-container-num-annotator.yaml
```

The operator definition includes a single controller, which in turn consists of 4 parts:

The name specifies a unique controller name that is used to refer to individual controllers inside
the operator. The `sources` field contains the Kubernetes API resource the operator watches and the
target spec describes the API resource the operator writes. This time, both the source and target
will be pods. The target type is `Patcher`, which means that the controller will use the processing
pipeline output to patch the target. (In contrast, `Updater` targets will simply overwrite the
target.)

The most important field is the `pipeline`, which specifies a declarative pipeline to process the
source API resource(s) into the target resource. The fields of the source and the target resource
can be specified using standard [JSONPath notation](https://datatracker.ietf.org/doc/html/rfc9535).

The pipeline consists of two aggregation ops. The first `@select` op will narrow the processed pods
to the ones in the `default` namespace. The second `@project` op in turn creates the patch, by
copying the name and namespace in the metadata and adding the length of the `$.spec.containers`
list as an annotation to the pod's metadata. The `@string` op is included to explicitly cast the
numeric length into a string.

```yaml
"@aggregate":
  - "@select":
      '@eq': ["$.metadata.namespace", default]
  - "@project":
      metadata:
        name: "$.metadata.name"
        namespace: "$.metadata.namespace"
        annotations:
          "dcontroller.io/container-num":
            '@string':
              '@len': ["$.spec.containers"]
```

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

Let's check whether the container number actually appears as an annotation on the pod:

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

The annotation should no show 3 containers:
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

## License

Copyright 2024 by its authors. Some rights reserved. See [AUTHORS](AUTHORS).

Apache License - see [LICENSE](LICENSE) for full text.
