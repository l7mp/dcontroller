## Concepts: Views

**Views** are a cornerstone of what makes Δ-controller powerful and flexible. Views allow you to create your own custom, temporary Kubernetes resources on the fly to simplify your controller's business logic.

A View is a schemaless, in-memory Kubernetes-like object that serves as an intermediate data store. Views are created and populated by the pipeline of one controller and can be used as a `source` for another. They are the glue that lets you to break down complex automation into a chain of simple, manageable data transformation steps.

## The goal: Simplify operators

Views solve a common problem in operator development: the state of the world as represented by native Kubernetes resources is often not in the ideal shape for your business logic. You might need to combine information from multiple resources, filter out irrelevant data, or pre-calculate values before making a decision.

Views allow you to do this pre-processing declaratively. They serve three main purposes:

**Simplifying Data**: A controller can watch complex resources like `Deployments` or `Services` and project only the essential fields into a lightweight, purpose-built View.

**Chaining Controllers**: Views create a clean, observable boundary between controllers. Controller A can produce a `FooView`, and Controller B can consume it, allowing you to build multi-stage data processing pipelines without the controllers needing direct knowledge of each other.

**State Aggregation**: A controller can watch many individual resources (e.g., all `Pods` in a namespace) and use a `@gather` operation to aggregate their state into a single, summary View object.

Note that views are not shared with Kubernetes: views are completely internal to an operator and Kubernetes does not have any meaning of a view. Trying to access a view via a standard Kubernetes client like `kubectl` will produce an error. Δ-controller comes with an embedded extension API server that can be used to inspect and modify view resources (see later).

## The Anatomy of a View: API Promise

Under the hood, a View is a namespaced, unstructured Kubernetes resource. While you have complete freedom over its structure, it must adhere to the below fundamental rules. It is the responsibility of the controller's pipeline to construct a view that fulfills this API promise:

*   **Ephemeral**: Views are not stored in `etcd` or persisted by the Kubernetes API server. In fact, the Kubernetes API does not even know about your views. They exist only within the memory of the running Δ-controller manager. If the manager restarts, all views are lost and will be recreated as the source resources are reconciled.
*   **Namespaced**: Every view object **must** be scoped into a namespace. There is no such thing as a "cluster-scoped view". Like everything in Δ-controller, view namespaces are an internal concept within an operator and do not exist in Kubernetes.
*   **Standard Metadata**: Every view object **must** have `metadata.name` and `metadata.namespace` that uniquely identify it.
*   **Schemaless Body**: Beyond the required metadata, the structure is entirely up to you. You can add a `spec`, a `status`, or any other top-level fields and embedded structures and lists. The content is a flexible key-value struct, just like any other Kubernetes object.
*   **Labels and Annotations**: Views fully support labels and annotations, which can be used for filtering and metadata.

The GVK for a view is determined dynamically based on the name of the `Operator` that defines it and the `kind` specified in the controller's target. The structure of the API group is always: **`<operator-name>.view.dcontroller.io/v1alpha1`**

*   **`apiGroup`**: `my-operator.view.dcontroller.io` (where `my-operator` is the `metadata.name` of your `Operator` CR).
*   **`version`**: `v1alpha1`.
*   **`kind`**: The `kind` you specify in the controller's `target` (e.g., `DeploymentSummaryView`). Note that kinds usually follow the same API style as Kubernetes (i.e., use CamelCase if possible).

Note that you don't have to specify the API group, the version and the kind in the pipeline: Δ-controller is smart enough to deduce that from the context. However, for a controller to create or update a view object its pipeline **must** produce an object that adheres to the above rules.

## Example: A Two-Stage Controller Chain

Let's build an operator that creates a `ConfigMap` alert whenever a `Deployment` is scaled above 3 replicas. We'll use a view to decouple the logic for monitoring replica counts from the logic for creating the alert.

**Controller 1: The Deployment Summarizer**
This controller watches `Deployments` and creates a `DeploymentSummaryView` for each one, containing only the replica count.

```yaml
# In an Operator named 'replica-alerter'
- name: deployment-summarizer-controller
  sources:
    - apiGroup: "apps"
      kind: Deployment
  pipeline:
    "@project":
      metadata:
        # The view will have the same name and namespace as the Deployment
        name: "$.metadata.name"
        namespace: "$.metadata.namespace"
      spec:
        # The view's spec contains only the replica count
        replicas: "$.spec.replicas"
  target:
    # This controller writes to our custom view
    kind: DeploymentSummaryView
    type: Updater
```

Note that you can omit `type: Updater` in the target, since that is the default.

The output of this controller for a `Deployment` named `my-app` would be a `DeploymentSummaryView` object that looks like this:

```yaml
# In-memory object, not in etcd
apiVersion: replica-alerter.view.dcontroller.io/v1alpha1
kind: DeploymentSummaryView
metadata:
  name: my-app
  namespace: default
spec:
  replicas: 5
```

**Controller 2: The High-Replica Alerter**
This second controller is triggered by changes to the `DeploymentSummaryView`. It filters for high replica counts and creates a native `ConfigMap` as an "alert".

```yaml
- name: high-replica-alert-controller
  sources:
    # Source is the view created by the first controller
    - kind: DeploymentSummaryView
  pipeline:
    - "@select":
        "@gt": ["$.spec.replicas", 3]
    # Create a ConfigMap with alert details
    - "@project":
        metadata:
          name:
            "@concat": ["alert-", "$.metadata.name"] # e.g., "alert-my-app"
          namespace: "$.metadata.namespace"
        data:
          message: "High replica count detected!"
          replicas:
            "@string": "$.spec.replicas" # Convert number to string for ConfigMap data
  target:
    # The target is a native Kubernetes ConfigMap
    apiGroup: ""
    kind: ConfigMap
    type: Updater
```

By chaining these two controllers within a single `Operator`, we've built a sophisticated workflow where the first controller creates a simplified, abstract state (the view), and the second controller reacts to that state.

