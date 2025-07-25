# Getting Started

This guide will walk you through installing Δ-controller and deploying your first, completely declarative "NoCode" operator. This simple operator will watch for new Pods in the `default` namespace and add a `dcontroller.io/managed: "true"` annotation to them, providing a clear and immediate way to verify that the system is working.

## Prerequisites

*   A running Kubernetes cluster.
*   `kubectl` and `helm` installed on your local machine.

## Install Δ-controller

First, add the Δ-controller Helm repository and install the controller into your cluster.

```bash
helm repo add dcontroller https://l7mp.github.io/dcontroller/
helm repo update
helm install dcontroller dcontroller/dcontroller
```

## Deploy Your First Operator

Create the `Operator` custom resource. This single YAML file contains the entire logic for our Pod annotator.

```bash
kubectl apply -f - <<EOF
apiVersion: dcontroller.io/v1alpha1
kind: Operator
metadata:
  name: pod-annotator
spec:
  controllers:
    - name: pod-annotator-controller
      # SOURCE: Watch for Pods in the default namespace
      sources:
        - apiGroup: ""
          kind: Pod
          namespace: default
      # PIPELINE: Create a patch for the watched Pod
      pipeline:
        "@aggregate":
          - "@project":
              metadata:
                name: "$.metadata.name"
                namespace: "$.metadata.namespace"
                annotations:
                  "dcontroller.io/managed": "true"
      # TARGET: Apply the patch back to the Pod
      target:
        apiGroup: ""
        kind: Pod
        type: Patcher
EOF
```

## Test the Operator

Create a simple Pod in the `default` namespace to trigger the operator.

```bash
kubectl run nginx --image=nginx:latest
```

Wait a few moments for the controller to reconcile. You can check that the operator itself is ready:

```bash
kubectl get operator pod-annotator -o yaml
```

Look for a `Ready` condition with a status of `"True"`.

Finally, verify that the annotation was added to your `nginx` pod:

```bash
kubectl get pod nginx -o jsonpath='{.metadata.annotations.dcontroller\.io/managed}'
```

You should see the output: `true`.

Congratulations, you have just deployed and verified your first NoCode operator with Δ-controller!

## Accessing the Extension API Server

Δ-controller runs an embedded API server to let you inspect internal "views" using `kubectl`. To access it securely, you'll use a port-forward.

1.  **Start the Port-Forward:**
    Open a new terminal and run the following command. It will run in the background.

    ```bash
    kubectl -n dcontroller-system port-forward deployment/dcontroller-manager 8443:8443 &
    ```

2.  **Set Your Kubeconfig:** 
    Point your `KUBECONFIG` environment variable to the special configuration file that connects to the local port-forward. The below assumes you are in the root of the dcontroller project checkout:

    ```bash
    export KUBECONFIG=deploy/dcontroller-config
    ```

3.  **Query the API Server:**
    You can now list the available API resources, just like with a standard Kubernetes cluster. If you have deployed operators that create views (like the ones in the tutorials), you will see them here.

    ```bash
    kubectl api-resources
    ```

Note that you will see no API resources yet: the extension API server only handles Δ-controller's internal resources called "views", and currently the test operator has not created any so the above command will produce an empty list.

## Cleanup

To remove the resources created in this guide, run the following commands:

```bash
# Stop the port-forward and restore your original Kubeconfig
fg
# (press Ctrl+C to stop the port-forward)
unset KUBECONFIG
kubectl delete pod nginx
kubectl delete operator pod-annotator
```
