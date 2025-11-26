# Service Health Status Aggregator

This example demonstrates an operator that automatically monitors pod health and updates Service annotations with health metrics. The operator creates custom HealthView resources as intermediate representations and showcases multi-controller pipelines.

## Description

Many production environments need visibility into service health beyond basic Kubernetes metrics. This operator automates the collection of pod health information, making it available as Service annotations that can be consumed by monitoring systems, dashboards, or other automation.

The operator uses a two-stage pipeline:

1. **Pod Health Monitor**: Watches Pods with the `dcontroller.io/health-monitor: enabled` label and creates HealthView objects containing pod readiness status for all the pods of a Service.

2. **Service Health Monitor**: Watches pairs of HealthView objects and Services, counts healthy vs. all pods, and patches Services with pod health annotations.

This architecture demonstrates how Δ-controller can create data processing pipelines using custom views and multiple controllers working in sequence.

## Installation

Deploy Δ-controller with Helm (if not already installed):

```console
helm repo add dcontroller https://l7mp.github.io/dcontroller/
helm repo update
helm install dcontroller dcontroller/dcontroller
```

## Operator

Create the Service Health Status operator:

```console
kubectl apply -f svc-health-operator.yaml
```

The operator defines two controllers that work in sequence, the second watching the resources created by the first. You can watch take a look at the pipeline using `kubectl get operators.dcontroller.io svc-health-operator -o yaml`.

### Pod Health Monitor

This controller watches Pods that have explicitly opted into health monitoring:

```yaml
sources:
  - apiGroup: ""
    kind: Pod
    labelSelector:
      matchLabels:
        "dcontroller.io/health-monitor": "enabled"
```

The pipeline first extracts per-pod health metrics and creates HealthView objects with just a shape designed for easy consumption by the subsequent pipeline step:

```yaml
"@project":
  metadata:
    name: "$.metadata.labels['app']"
    namespace: "$.metadata.namespace"
  pods:
    podName: "$.metadata.name"
    ready: "$.status.conditions[?(@.type=='Ready')].status"
```

Note that the form `$.<query>` can be used to query internal fields of a resource using standard JSONpath expressions.

In the next step the pipeline collapses all the per-pod statuses into a single HealthView resource that contains the per-pod statuses as a single list.

```yaml
"@gather":
  # Key: group by service (name + namespace)
  - "@concat":
      - "$.metadata.name"
      - "--"
      - "$.metadata.namespace"
  # Value: collapse the pods fields into a list (contains ready status)
  - "$.pods"
```

Here, the `@concat` op will concatenate the subsequent list of strings.

Finally, the controller writes the results into a so called **view**.

```yaml
target:
  kind: HealthView
  type: Updater
```

Views in Δ-controller are internal resource pools containing resources that behave just like standard Kubernetes objects: they have a metadata with unique names and namespace, labels, annotations, and an arbitrary content, like in this case the `pods` list. Note that views are never uploaded to the Kubernetes API server, they exist only internally in Δ-controller, but you can use the extension API server to interact with them via a standard Kubernetes client like kubectl (see below).

### Service Health Monitor

This controller watches the HealthView resources the pod health monitor maintains, plus the Service objects from the Kubernetes API server:

```yaml
sources:
  - kind: HealthView
  - apiGroup: ""
    kind: Service
```

Note that an empty API group defaults to an internal view so it is enough to specify the view kind (HealthView). For standard Kubernetes API objects you have to specify the API group (`""` for the core API), and sometimes even the version (when you want to watch different versions of the same API group).

The pipeline first joins HealthView objects with the corresponding Service by name and namespace:

```yaml
"@join":
  "@and":
    - "@eq": ["$.HealthView.spec.serviceName", "$.Service.metadata.name"]
    - "@eq": ["$.HealthView.metadata.namespace", "$.Service.metadata.namespace"]
```

Use the `@eq` operator to check string and numeric equality. The `@and` op is just what the name says: boolean AND. 

This will create a big compound object that contains the HealthView-Service pairs with identical object key (name ane namespace).

Then the next pipeline stage projects the compound object into a shape that can be used to patch Service objects with the required annotation:

```yaml
"@project":
  metadata:
    name: "$.Service.metadata.name"
    namespace: "$.Service.metadata.namespace"
    annotations:
      "dcontroller.io/pod-ready":
        "@concat":
          - "@string":
              "@len":
                "@filter":
                  - "@eq": ["$$.ready", "True"]
                  - "$.HealthView.pods"
          - "/"
          - "@string":
              "@len": "$.HealthView.pods"
```

Here, `@string` converts numbers to strings, `@len` returns the length of a list, and `@filter` greps lists by a boolean expression evaluated on each element of the list (use `$$` to refer to the entry). For a particular Service named `my-service` with one healthy and one unhealthy pod we will get a patch:

``` yaml
name: "$.Service.metadata.name"
namespace: "$.Service.metadata.namespace"
annotations:
  "dcontroller.io/pod-ready": "1/2"
```

This patch is then applied back to the corresponding Service object by the target spec:

``` yaml
target:
  apiGroup: ""
  kind: Service
  type: Patcher
```

The target type (Patcher) in this case matters: we don't want to recreate the Service (that's what the default Updater type of targets do, and in this case this would effectively wipe out the entire Service spec), just modify it so that it contains our annotation.

And that's all: a fairly complex controller pipeline in about 30 lines of pure YAML! That's the main idea in Δ-controller.

## Test

Deploy a test application with health monitoring enabled:

```console
kubectl apply -f web-app.yaml
```

The deployment creates pods with the `dcontroller.io/health-monitor: enabled` label. The pods run a simple shell script that traps SIGUSR1 signals for graceful termination, allowing for controlled demonstration of health status changes. Initially, all pods should be healthy. The below will watch the current health status (should show "2/2"):

```console
watch 'kubectl get service web-app -o jsonpath="{.metadata.annotations}" | jq'
```

To demonstrate health status transitions, you can selectively terminate pods. In another terminal, terminate the first pod by sending it a termination signal:

```console
POD1=$(kubectl get pods -l app=web-app -o jsonpath='{.items[0].metadata.name}')
kubectl exec $POD1 -- kill -USR1 1
```

Watch the status change to `"1/2"`. Terminate the second pod:

```console
POD2=$(kubectl get pods -l app=web-app -o jsonpath='{.items[1].metadata.name}')
kubectl exec $POD2 -- kill -USR1 1
```

Watch the status change to `"0/2"`, then back to `"2/2"` as the pods restart automatically.

## Verification

Check that the operator status indicates no errors:

```console
kubectl get operators.dcontroller.io svc-health-operator -o yaml
```

You should see both controllers with `Ready` status:

```yaml
status:
  controllers:
  - name: health-monitor-controller
    conditions:
    - status: "True"
      type: Ready
  - name: service-health-controller
    conditions:
    - status: "True"
      type: Ready
```

### Extension API Server

Δ-controller offers an **extension API server** that can be used to query and modify view objects. Currently the extension API server uses unsafe unauthenticated HTTP and hence it is not exposed from the Kubernetes cluster. You can reach the API server on a port-forward connection via the main API server, using a specially configured kubectl client:

``` console
kubectl -n dcontroller-system port-forward deployment/dcontroller-manager 8443:8443 >/dev/null 2>&1 &
export KUBECONFIG=deploy/dcontroller-config
```

List the custom HealthView resources available through the extension API server:

```console
kubectl api-resources
```

You should see:
```
NAME         SHORTNAMES   APIVERSION                                         NAMESPACED   KIND
healthview                svc-health-operator.view.dcontroller.io/v1alpha1   true         HealthView
```

The API version for the view resources takes the form `<operator-name>.view.dcontroller.io/v1alpha1`, where the operator name is the name of operator that created the view (in this case `svc-health-operator`).

List the HealthView objects in YAML format:

```Console
kubectl get healthview.svc-health-operator.view.dcontroller.io -o yaml
apiVersion: v1
kind: List
items:
- apiVersion: svc-health-operator.view.dcontroller.io/v1alpha1
  kind: HealthView
  metadata:
    name: web-app
    namespace: default
  pods:
  - podName: web-app-7bfdf6588c-nvb84
    ready: "True"
  - podName: web-app-7bfdf6588c-vkptf
    ready: "True"
```

## Cleanup

Revert to the default Kubernetes config and remove all resources:

```console
unset KUBECONFIG
kubectl delete deployment web-app
kubectl delete service web-app
kubectl delete operators.dcontroller.io svc-health-operator
```

## License

Copyright 2024 by its authors. Some rights reserved. See [AUTHORS](AUTHORS).

Apache License - see [LICENSE](LICENSE) for full text.
