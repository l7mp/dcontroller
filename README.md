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

Deploy Δ-controller with Helm:

``` console
helm repo add dcontroller https://l7mp.github.io/dcontroller/
helm repo update
helm install dcontroller dcontroller/dcontroller
```

This will deploy the Δ-controller operator, a Kubernetes operator that can take custom resources
describing a declarative controller and implement the logics on the Kubernetes API.

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

Δ-controller includes a custom embedded Kubernetes API server that allows standard kubectl
clients to inspect, modify or watch operators' view resources.

### Quick start (HTTP mode - development only)

For development and testing, you can run the API server in insecure HTTP mode without authentication:

1. Start the operator in HTTP mode with authentication disabled:

```console
dctl start --http --disable-authentication
```

This starts the API server on `http://localhost:8443` with unrestricted access.

2. Generate a kubeconfig pointing to the local HTTP server (outputs to stdout):

```console
dctl generate-config --http --user=dev --namespaces="*" --server-address=localhost:8443 > dev.config
export KUBECONFIG=./dev.config
```

3. Use kubectl to access view resources:

```console
kubectl get myoperator.view.dcontroller.io
```

### Production deployment (HTTPS with authentication)

For production deployments, use HTTPS with JWT-based authentication and RBAC authorization:

1. Generate TLS certificate and private key:

```console
dctl generate-keys
```

This creates `apiserver.key` (private key) and `apiserver.crt` (certificate). The certificate is used both for TLS encryption and for JWT token validation (the public key is extracted from it).

2. Start the API server with authentication enabled:

```console
dctl start --tls-cert-file=apiserver.crt --tls-key-file=apiserver.key
```

The API server will:
- Serve over HTTPS using the TLS certificate
- Require valid JWT tokens for all requests
- Validate tokens using the public key from the certificate

3. Create a rules file defining RBAC permissions (using Kubernetes PolicyRule format):

```console
cat > alice-rules.json <<EOF
[
  {
    "verbs": ["get", "list", "watch"],
    "apiGroups": [""],
    "resources": ["pods", "services"]
  },
  {
    "verbs": ["get", "list", "create", "update", "delete"],
    "apiGroups": ["myoperator.view.dcontroller.io"],
    "resources": ["*"]
  }
]
EOF
```

4. Generate a kubeconfig for a user (outputs to stdout, redirect to file):

```console
dctl generate-config --user=alice --namespaces=team-a --rules-file=alice-rules.json \
  --tls-key-file=apiserver.key --server-address=localhost:8443 > alice.config
```

Alternatively, use `--output` to write directly to a file:

```console
dctl generate-config --user=alice --namespaces=team-a --rules-file=alice-rules.json \
  --tls-key-file=apiserver.key --server-address=localhost:8443 --output=alice.config
```

5. Verify the config contents:

```console
export KUBECONFIG=alice.config
dctl get-config --tls-cert-file=apiserver.crt
```

6. Use the generated kubeconfig to access the API server:

```console
kubectl get myoperator.view.dcontroller.io
```

The user `alice` will be restricted to:
- Namespaces: `team-a`
- Read-only access to pods and services in core API group
- Full access to resources in `myoperator.view.dcontroller.io` API group

For admin access with no restrictions, omit the `--rules-file` flag:

```console
dctl generate-config --user=admin --namespaces="*" \
  --tls-key-file=apiserver.key --server-address=localhost:8443 > admin.config
export KUBECONFIG=admin.config
```

### Key differences between HTTP and HTTPS modes

- **HTTP mode (`--http`)**: No TLS encryption, typically used with `--disable-authentication` for local development
- **HTTPS mode (default)**: TLS encryption required, authentication recommended for production
- The same TLS certificate is used for both HTTPS encryption and JWT token validation
- Use `--insecure` flag in `generate-config` to skip TLS verification (for self-signed certificates)

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
