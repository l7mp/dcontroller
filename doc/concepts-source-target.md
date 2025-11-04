# Concepts: Sources and Targets

Every controller in Δ-controller defines a data pipeline. **Sources** are the inputs to this pipeline, and the **Target** is the single output. Sources and targets define what a controller reads from and writes to, forming the I/O contract for your automation logic.

## Sources: Where the Data Comes From

An operator's `source` tells a controller which Kubernetes resources to watch for changes. A controller can have one or more sources. Each change (creation, update, deletion) to a source object triggers an incremental execution of the controller's pipeline.

A source definition consists of a resource identifier and optional filters. You identify a resource using its `apiGroup`, `kind`, and optionally, its `version`.

*   **`kind` (Required)**: The kind of the resource, like `Pod`, `Service`, or a custom view name like `HealthView`.
*   **`apiGroup` (Optional)**: The API group of the resource.
    *   For native Kubernetes resources in the core group, specify `""` (an empty string).
    *   For other native resources, specify their group (e.g., `apps`, `discovery.k8s.io`).
    *   For internal **views**, this field can be omitted: Δ-controller interprets a missing `apiGroup` as a reference to a view created by a controller within the same Operator.
*   **`version` (Optional)**: The API version. If omitted for native resources Δ-controller will attempt to discover the preferred version from the API server, and it uses a sensible default for views (`v1alpha1` for now).

The below example sets up a source for watching native Kubernetes resources:
```yaml
sources:
  # Watching Pods from the core API group
  - apiGroup: ""
    kind: Pod

  # Watching Deployments from the "apps" API group
  - apiGroup: "apps"
    kind: Deployment

  # Watching EndpointSlices, pinning to v1
  - apiGroup: "discovery.k8s.io"
    version: "v1"
    kind: EndpointSlice
```

The next example creates a source for an internal View resource:
```yaml
sources:
  # Watching a 'HealthView' created by another controller in this Operator. Note the absence of apiGroup.
  - kind: HealthView

  # The same source, with all defaults specified.
  - apiGroup: "my-operator.view.dcontroller.io"
    kind: HealthView
    version: v1alpha1
```

### Virtual Sources

In addition to watching Kubernetes resources, Δ-controller supports **virtual sources** that generate synthetic events based on timers. 

#### OneShot Source

A `OneShot` source triggers the pipeline exactly once when the controller starts with an empty object. This is useful for initializing resources or performing one-time setup operations.

*   OneShot sources generate a single empty object on controller startup, which can be transformed modified via the pipeline.
*   The empty resource flows through the incremental pipeline just like regular Kubernetes resource watches.
*   Use it when you need to bootstrap initial state or create resources that don't depend on other sources.

```yaml
sources:
  # Trigger once at startup
  - type: OneShot
    kind: InitialSetup
```

#### Periodic Source (Experimental)

The `Periodic` source triggers state-of-the-world reconciliation at regular intervals. Unlike incremental sources, periodic sources do not flow through the pipeline as deltas. Instead, they trigger a full reconciliation that:

1. Computes the required target state from all current source objects (using a snapshot of the pipeline).
2. Compares it against the actual target state.
3. Applies only the differences (adds missing objects, removes stale objects).

This reconciliation pattern is useful for:
*   Lease/TTL scenarios: Periodically refreshing resources that have expiration semantics.
*   External system polling: Regularly checking views reflecting external state and synchronizing it to Kubernetes.
*   Garbage collection: Removing stale objects that shouldn't exist based on current sources.
*   Drift correction: Ensuring target state stays synchronized even if manually modified.

Configure the period using the `parameters` field with a Go duration string (e.g., `"5m"`, `"30s"`). The default period is 5 minutes.

**Important**: Periodic sources are experimental. Reconciliation is based on the current state of **all other sources** (Watcher and OneShot), not the periodic trigger itself. The periodic trigger object is not visible in the pipeline.

```yaml
sources:
  # Regular watcher source - flows through incremental pipeline
  - kind: ConfigMap
    namespace: default
  # Periodic reconciliation every 30 seconds
  # This will reconcile based on the current ConfigMap state
  - kind: Sync
    type: Periodic
    parameters:
      period: "30s"
```

### Predicates

You can refine what a source watches by applying filters. This is crucial for performance and for targeting your controller's logic to specific resources. Filters are combined with a logical AND. 

*   **`namespace`**: Restricts the watch to a single Kubernetes namespace.

*   **`labelSelector`**: A standard Kubernetes [label selector](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors) to filter objects based on their labels.

*   **`predicate`**: For more advanced filtering, you can use a declarative form of `controller-runtime`'s [predicates](https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/predicate). This allows you to filter events based on changes to specific object fields, preventing unnecessary reconciliations. For instance, `GenerationChanged` only triggers the pipeline when the `metadata.generation` of an object changes, ignoring mere status updates or annotation tweaks.

The below source will only trigger the pipeline for `Pods` in the `production` namespace that have the `app: webserver` label applied and only when their specification (`.spec`) has actually changed.

```yaml
sources:
  - apiGroup: ""
    kind: Pod
    namespace: production
    labelSelector:
      matchLabels:
        app: webserver
    # Only reconcile on meaningful spec changes, not status updates.
    predicate: GenerationChanged
```

## Targets: Where the Data Goes

The operator `target` specifies the single resource type where the output of a controller's pipeline is written. The pipeline's output is a set of deltas (additions, updates, deletions), and the target's configuration determines how these deltas are applied.

The resource is identified using `apiGroup` and `kind`, following the same rules as sources. The most important field is `type`, which defines the write strategy.

There are two types of targets: `Updater` and `Patcher`, with `Updater` being the default.

### **`Updater`**

An `Updater` target performs a **full replacement** of the target object's content. When the pipeline outputs an object, the `Updater` will:
1.  Read the current version of the target object (identified by the key `medatata.namespace` and `medatata.name`) from the cluster.
2.  Merge the annotations and labels od the target object with those of the result object generated by the pipeline but leaves the rest of the metadata fields untouched.
3.  Replace the complete content, including the complete `spec` and even the `status` but excluding the metadata, with the content generated by the pipeline.
4.  Write the modified object back to the API server.

Use `Updater` when:
*   You are managing the *entire lifecycle* of a simple resource, like a `ConfigMap` or a custom view.
*   Your pipeline is responsible for generating the *complete* desired state of the object's `spec`.

**Warning:** Do use the `Updater` target to remove labels or annotations since these are always merged (this is to prevent the accidental removal of an important metadata field), only the `Patcher` target can do that.

**Warning:** Using an `Updater` on complex native resources like `Services` or `Deployments` is dangerous. It will wipe out important fields managed by Kubernetes itself (e.g., `spec.clusterIP` on a `Service`). For such cases, always use a `Patcher`.

The below example shows how to create a ConfigMap with an Updater, generating the entire `metadata` and `data` section.

```yaml
target:
  apiGroup: ""
  kind: ConfigMap
  type: Updater
```

### **`Patcher`**

A `Patcher` target performs a **merge patch** operation. The output of the pipeline is treated as a patch that is merged onto the existing object in the cluster. This is the safest and most common way to modify existing resources.

*   **For add/update deltas:** The pipeline output is merged into the target object. New fields are added, and existing fields are overwritten if the same fields exists in the result object or preserved otherwise.
*   **For delete deltas:** The fields present in the pipeline's output object are removed from the target object by setting their values to `null` in the patch.

Use `Patcher` when:
*   You want to add, modify or remove annotations or labels on an existing resource.
*   You need to update a specific field within a complex object (like a `Deployment`) without affecting other fields.
*   You are building a controller that collaborates with other controllers managing the same resource.

**Warning:** You cannot add or delete a complete resource using a `Patcher` target. Use an `Updater` for that.

**Warning:** Since Δ-controller is schemaless, it cannot implement strategic merge patches, only plain JSON patches are supported at the moment.

The below example shows how to annotate a Pod with using a Patcher target. Suppose that the pipeline generates a small snippet of YAML containing just the `metadata.annotations` to be added. Then, the below `Patcher` target can be used to merge this patch into the target `Pod`.

```yaml
target:
  apiGroup: ""
  kind: Pod
  type: Patcher
```
