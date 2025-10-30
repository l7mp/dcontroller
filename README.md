[![Go Report Card](https://goreportcard.com/badge/github.com/l7mp/dcontroller)](https://goreportcard.com/report/github.com/l7mp/dcontroller)
[![godoc](https://pkg.go.dev/badge/github.com/l7mp/dcontroller)](https://pkg.go.dev/github.com/l7mp/dcontroller)

# Δ-controller: A NoCode/LowCode Kubernetes controller framework

Δ-controller is a framework to simplify the design, implementation and maintenance of Kubernetes
operators. The main goal is to reduce the mental overhead of writing Kubernetes operators by
providing simple automations to eliminate the repetitive imperative code required when building
operators on the [Kubernetes
controller-runtime](https://github.com/kubernetes-sigs/controller-runtime) project. The final goal
is to let anyone with minimal Go skills to write complex Kubernetes operators in a NoCode/LowCode
style.

See the full documentation [here](doc/README.md).

## Description

The philosophy of Δ-controller is thinking of Kubernetes as a MongoDB database. Instead of writing imperative Go code, you query and transform Kubernetes resources using declarative pipelines, just like how [MongoDB aggregation](https://www.mongodb.com/docs/manual/core/aggregation-pipeline) pipelines are used to query JSON documents. If you've used document databases, you'll feel right at home.

The Δ (delta) in the name represents two core concepts:

**Declarative pipelines:** Write operators using a query language inspired by MongoDB aggregators. Combine, filter, and transform Kubernetes resources into simplified *views*, then project them back as native Kubernetes objects. No imperative reconciliation loops; just specify which Kubernetes resources your operator wants to watch and the declarative transformation rules to process them, and Δ-controller does the rest! Put your operator spec into a YAML file, and one kubectl-apply will deploy it right away.

**Delta processing:** Traditional controllers re-process the entire Kubernetes API state on every change. Δ-controller tracks only the deltas (changes) and propagates them incrementally through your pipeline. This means your operators react instantly to changes without re-computing everything from scratch. Δ-controller's incremental view maintenance engine is powered by [DBSP](https://mihaibudiu.github.io/work/dbsp-spec.pdf), providing a solid theoretical foundation for incremental computation.

## Installation

### Operator deployment

Deploy Δ-controller with Helm:

``` console
helm repo add dcontroller https://l7mp.github.io/dcontroller/
helm repo update
helm install dcontroller dcontroller/dcontroller
```

This will deploy the Δ-controller operator, a Kubernetes operator that can take custom resources
describing a declarative controller and implement the logics on the Kubernetes API. 

The `dctl` CLI tool is used for managing credentials, generating kubeconfigs, and accessing the
embedded API server. Download the latest release for your platform from the [releases
page](https://github.com/l7mp/dcontroller/releases):

```console
# Linux amd64
curl -LO https://github.com/l7mp/dcontroller/releases/download/v0.1.0/dcontroller_v0.1.0_Linux_x86_64.tar.gz
tar -xzf dcontroller_v0.1.0_Linux_x86_64.tar.gz
sudo mv dctl /usr/local/bin/
```

## Getting started

We will create a simple operator below that will write into each pod the number of containers in
the pod as a custom annotation. To simplify the operations, we will restrict the operator to the
default namespace. Whenever the container number is updated, the annotation will be updated too.

Navigate [here](examples/) for more complex examples.

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

The operator defines a single controller. The controller `name` indicates a unique identifier that
is used to refer to individual controllers inside the operator. The `sources` field specifies the
Kubernetes API resource(s) the operator watches, and the target spec describes the API resource the
operator will write. This time, both the source and target will be Pods. The target type is
`Patcher`, which means that the controller will use the processing pipeline output to patch the
target. (In contrast, `Updater` targets will simply overwrite the target.)

The most important field is the `pipeline`, a declarative pipeline describing how to process the
source API resource(s) into the target resource. The pipeline operates on the fields of the source
and the target resources using standard [JSONPath
notation](https://datatracker.ietf.org/doc/html/rfc9535).

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

Our sample pipeline comprises a single aggregation operation, namely a projection. This `@project`
op will create a patch by copying the pod's name and namespace and adding the length of the
`containers` list in the Pod spec as an annotation to the metadata. The `@string` op is included to
explicitly cast the numeric length value into a string.

Suppose the following pod resource:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: my-pod
  namespace: default
spec:
  containers:
  - name: container-1
    image: my-container-1:latest
    ...
  - name: container-2
    image: my-container-2:latest
    ...
  - name: container-3
    image: my-container-3:latest
    ...
```

Then, our pipeline will generate the below patch, which the controller will then readily apply to
patch the target pod.

```yaml
metadata:
  name: my-pod
  namespaace: default
  annotations:
    "dcontroller.io/container-num": "3"
```

And that's the whole idea: you specify one or more Kubernetes API resources to watch, a declarative
pipeline that will process the input resources into a patch that is then automatically applied to
the target resource. The Δ-controller operators are completely dynamic so you can add, delete and
modify them anytime, they are fully described in a single custom resource with the entire logics,
there can be any number of operators running in parallel (but make sure they do not create infinite
update cycles!), and the entire framework involves zero line of code. This is NoCode/LowCode at its
best!

### Test

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

The annotation should now indicate 3 containers:

```console
kubectl get pod net-debug-2 -o jsonpath='{.metadata.annotations}'
{"dcontroller.io/container-num":"3"}
```

Note that pods in other namespaces will not have the container-num annotation:

```console
kubectl -n kube-system get pod etcd -o jsonpath='{.metadata.annotations}'
...
```

### Cleanup

Remove all resources we have created:

```console
kubectl delete pod net-debug
kubectl delete pod net-debug-2
kubectl delete operators.dcontroller.io pod-container-num-annotator
```

## The API server

Δ-controller includes an embedded Kubernetes API server that allows standard kubectl clients to
inspect, modify or watch operators' view resources. The API server is deployed automatically with
the Helm chart and runs inside the dcontroller-manager pod.

### Accessing the API server (development)

The default Helm installation runs the API server in HTTP mode without authentication for easy
local development:

1. Port-forward to the API server:

```console
kubectl -n dcontroller-system port-forward deployment/dcontroller-manager 8443:8443 &
```

2. Generate a development kubeconfig:

```console
dctl generate-config --http --insecure --user=dev --namespaces="*" \
  --server-address=localhost:8443 > dev.config
export KUBECONFIG=./dev.config
```

3. Use kubectl to access view resources:

```console
kubectl api-resources | grep view.dcontroller.io
kubectl get <kind>.<operator>.view.dcontroller.io
```

### Production deployment with authentication

For production, redeploy with HTTPS and JWT authentication:

1. Upgrade Helm release to production mode (temporarily without TLS):
```console
helm upgrade dcontroller dcontroller/dcontroller \
  --set apiServer.mode=development \
  --set apiServer.service.type=LoadBalancer
```

2. Wait for LoadBalancer IP and generate TLS certificate with IP SAN:
```console
# Wait for external IP to be assigned
kubectl -n dcontroller-system wait --for=jsonpath='{.status.loadBalancer.ingress[0].ip}' \
  service/dcontroller-apiserver --timeout=60s

export EXTERNAL_IP=$(kubectl -n dcontroller-system get service dcontroller-apiserver \
  -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

# Generate certificate valid for both localhost and the external IP
dctl generate-keys --hostname=localhost --hostname=${EXTERNAL_IP}

kubectl create secret tls dcontroller-tls \
  --cert=apiserver.crt --key=apiserver.key \
  -n dcontroller-system
```

3. Upgrade to production mode with TLS:
```console
helm upgrade dcontroller dcontroller/dcontroller \
  --set apiServer.mode=production \
  --set apiServer.service.type=LoadBalancer
```

4. Create RBAC rules file (Kubernetes PolicyRule format):
```console
cat > alice-rules.json <<EOF
[
  {
    "verbs": ["get", "list", "watch"],
    "apiGroups": ["*.view.dcontroller.io"],
    "resources": ["*"]
  }
]
EOF
```

5. Generate user kubeconfig with restricted access:

```console
dctl generate-config --user=alice --namespaces=team-a \
  --rules-file=alice-rules.json \
  --tls-key-file=apiserver.key \
  --server-address=${EXTERNAL_IP}:8443 > alice.config
```

6. Verify and use the kubeconfig:

```console
export KUBECONFIG=alice.config
dctl get-config --tls-cert-file=apiserver.crt

kubectl get <kind>.<operator>.view.dcontroller.io -n team-a  # allowed
kubectl get <kind>.<operator>.view.dcontroller.io --all-namespaces  # denied
```

For admin access with no restrictions:

```console
dctl generate-config --user=admin --namespaces="*" \
  --tls-key-file=apiserver.key \
  --server-address=<EXTERNAL-IP>:8443 > admin.config
```

See the [Helm chart documentation](chart/helm/README.md) for more deployment options including
NodePort, Gateway API, and advanced configuration.

## Caveats

- The operator runs with full RBAC permissions to operate on the entire Kubernetes API. This is
  required at this point to make sure that the declarative operators loaded at runtime can access
  any Kubernetes API resource they intend to watch or modify. Later we may implement a dynamic RBAC
  scheme that would minimize the required permissions to the minimal set of API resources the
  running operators may need.
- The strategic merge patch implementation does not handle lists. Since Kubernetes does not
  implement native strategic merge patching for schemaless unstructured resources that Δ-controller
  uses internally, currently patches are implemented via a simplified local JSON patch engine that
  does not handle strategic merges on lists. Use `Patcher` targets carefully or, whenever possible,
  opt for `Updater` targets.

## License

Copyright 2024-2025 by its authors. Some rights reserved. See [AUTHORS](AUTHORS).

Apache License - see [LICENSE](LICENSE) for full text.
