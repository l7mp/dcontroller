# Concepts: The Extension API Server

While Views provide a powerful way to manage intermediate state within Δ-controller, they are in-memory and not stored in Kubernetes' main `etcd` database. So how do you inspect, debug, or interact with them? The answer is the **Extension API Server**.

Δ-controller includes an optional, embedded Kubernetes-style API server that runs within the manager. Its sole purpose is to expose your in-memory Views over a standard, Kubernetes-compatible REST API. This makes the internal, "magic" state of your operators transparent and accessible using the tools you already know, like `kubectl` and `curl`.

Some cautionary remarks apply:

*   **Views Only**: The extension API server *only* serves internal view resources. You cannot use it to query native Kubernetes resources like `Pods` or `Services`; those requests will fail. Use a `kubectl` CLI client configured for reaching the main API server to do that.
*   **Read-Write Access**: While primarily used for inspection, the API server supports full CRUD (Create, Read, Update, Delete) operations. This can be a powerful debugging tool, allowing you to manually create or modify a view object to trigger a downstream controller and observe its behavior.
*   **Ephemeral State**: Remember that views are in-memory. If the Δ-controller manager pod restarts, all views will be cleared and then repopulated as the controllers re-reconcile their sources.

## Connecting to the Extension API Server

By default, the extension API server is not exposed outside the cluster for security reasons, as it runs with an unauthenticated HTTP endpoint. The recommended way to access it is via `kubectl port-forward`.

1.  **Start the Port-Forward:**
    Open a new terminal and run the following command. This will forward traffic from your local machine's port `8443` to the Δ-controller manager's API server port.

    ```bash
    kubectl -n dcontroller-system port-forward deployment/dcontroller-manager 8443:8443 &
    ```

2.  **Use the Correct Kubeconfig:**
    To direct `kubectl` commands to this local endpoint instead of your main cluster's API server, you need to use a specific kubeconfig file. The below assumes you are in the root of the dcontroller project checkout:

    ```bash
    export KUBECONFIG=deploy/dcontroller-config
    ```

You are now connected. All subsequent `kubectl` commands in this terminal will be sent to the Δ-controller's extension API server. When finishing the debug session, use `unset KUBECONFIG` to restore the default Kubernetes context and shoot down the port-forward.

## Discovering and Inspecting Views

Once connected, you can interact with your views as if they were standard Custom Resources.

### Discovering View API Groups

The first step is to see what view types are available. The API groups for views are generated dynamically from your `Operator` names. You can discover them using `kubectl api-resources`.

```bash
kubectl api-resources
```
The output will show the registered view kinds. Notice the special naming convention for the `APIVERSION`:

```
NAME         SHORTNAMES   APIVERSION                                         NAMESPACED   KIND
healthview                svc-health-operator.view.dcontroller.io/v1alpha1   true         HealthView
```

The API group for a view is always **`<operator-name>.view.dcontroller.io`** and the version is `v1alpha1`. This unique naming prevents conflicts between views defined in different operators.

### Getting and Listing Views

You can use standard `kubectl get` and `kubectl list` commands to inspect your view objects. Because views are schemaless and can have any structure, it's best to use the `-o yaml` or `-o json` output format to see their full content.

Get a specific `HealthView` object from the `svc-health-operator`:

```bash
kubectl get healthview.svc-health-operator.view.dcontroller.io web-app -o yaml
apiVersion: svc-health-operator.view.dcontroller.io/v1alpha1
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

> **Pro Tip:** Your shell's tab-completion for `kubectl` will work with these long resource names! Type `kubectl get health` and press `Tab` to have the shell complete the full resource name for you.

## Watching Views for Changes

For advanced debugging, you can `watch` a view to see real-time changes as they are processed by your controller pipelines.

The below will set up a watch for all changes to `HealthView` objects:

```bash
kubectl get healthview.svc-health-operator.view.dcontroller.io --watch
```
