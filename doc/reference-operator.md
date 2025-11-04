# Reference: The Operator Custom Resource

This is the formal specification for the `dcontroller.io/v1alpha1.Operator` Custom Resource (CR). Use this guide to understand the structure, fields, and available options when defining your own declarative operators.

## Operator

An `Operator` is the top-level, cluster-scoped resource that defines a complete unit of automation within Δ-controller. Operators are identified by a unique name that is used in the API group scope for the operator's Views so it is best to keep the operator name short and simple.

An Operator serves as a container for one or more controllers that work together. A complete `Operator` resource has the following structure:

```yaml
apiVersion: dcontroller.io/v1alpha1
kind: Operator
metadata:
  name: <string>
spec:
  controllers:
    - # --> See Controller specification below
status:
  # --> See OperatorStatus specification below
```

The `spec` defines the desired state of the Operator, which consists of a list of controllers.

| Field         | Type                         | Required | Description                                                                                                                                        |
|---------------|------------------------------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| `controllers` | list of `Controller` objects | **Yes**  | A list of one or more [Controller](#controller) objects that collectively implement the operator's logic. A maximum of 255 controllers is allowed. |

## Controller

A `Controller` defines a single, directional data-processing pipeline.

| Field      | Type                     | Required | Description                                                                                            |
|------------|--------------------------|----------|--------------------------------------------------------------------------------------------------------|
| `name`     | `string`                 | **Yes**  | A unique name for the controller within the Operator. Used for status reporting and logging.           |
| `sources`  | list of `Source` objects | **Yes**  | A list of one or more [Source](#source) objects that the controller will watch for changes.            |
| `pipeline` | `object`                 | **Yes**  | The declarative pipeline that transforms data from the sources. See the [Pipeline Reference `[TODO]`]. |
| `target`   | `object`                 | **Yes**  | The [Target](#target) resource where the output of the pipeline is written.                            |

The following example provides the general controller schema structure.

```yaml
- name: pod-annotator-controller
  sources:
    - apiGroup: ""
      kind: Pod
  pipeline:
    # ... pipeline definition ...
  target:
    apiGroup: ""
    kind: Pod
    type: Patcher
```

### Source

A `Source` defines a resource to watch for events and feed into the pipeline.

| Field           | Type                   | Required | Description                                                                                                                                                                                                                                                                                  |
|-----------------|------------------------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `type`          | `string`               | No       | The source type. One of: `Watcher` (default, watches Kubernetes resources), `OneShot` (triggers once at startup), or `Periodic` (triggers state-of-the-world reconciliation periodically). For `Watcher` sources, omit this field or set to `Watcher`.                                       |
| `apiGroup`      | `string`               | No       | The API group of the resource. Default: An internal operator view. For the core Kubernetes group, use `""`. Example: `apps`.                                                                                                                                                                 |
| `version`       | `string`               | No       | The API version of the resource. If omitted for native resources, Δ-controller will discover the preferred version.                                                                                                                                                                          |
| `kind`          | `string`               | Yes      | The kind of the resource. Example: `Pod`, `Service`, a custom view name like `HealthView`, or user-defined identifier for virtual sources (e.g., `InitialTrigger`, `PeriodicSync`).                                                                                                          |
| `namespace`     | `string`               | No       | If specified, restricts the watch to this namespace only. Only applicable to `Watcher` sources.                                                                                                                                                                                              |
| `labelSelector` | `metav1.LabelSelector` | No       | A standard Kubernetes [label selector](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors) to filter source objects. Only applicable to `Watcher` sources.                                                                                            |
| `predicate`     | `object`               | No       | A declarative [predicate](https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/predicate) to filter events and prevent unnecessary reconciliations. Can be one of `GenerationChanged`, `ResourceVersionChanged`, `LabelChanged`, `AnnotationChanged`. Only applicable to `Watcher` sources. |
| `parameters`    | `object`               | No       | Source-specific parameters. For `Periodic` sources, use `{"period": "<duration>"}` (e.g., `"30s"`, `"5m"`). Default: `5m` for `Periodic`.                                                                                                                                                    |

The below example shows a Source with filters that trigger the execution of the controller's pipeline when there is an update (add, delete, modify, etc.) for Pods with label `app: webserver`, and only when the Pod's annotations or labels *and* the resource version change.

```yaml
- apiGroup: ""
  kind: Pod
  namespace: production
  labelSelector:
    matchLabels:
      app: webserver
  predicate:
    and:
      - ResourceVersionChanged
      - or:
          - AnnotationChanged
          - LabelsChanged
```

### Target

A `Target` defines the destination resource for the pipeline's output.

| Field      | Type     | Required | Description                                                                                                         |
|------------|----------|----------|---------------------------------------------------------------------------------------------------------------------|
| `apiGroup` | `string` | No       | The API group of the target resource. **Default**: An internal view. For the core Kubernetes group, use `""`.       |
| `version`  | `string` | No       | The API version of the resource. If omitted for native resources, Δ-controller will discover the preferred version. |
| `kind`     | `string` | **Yes**  | The kind of the resource. Example: `Pod`, `Service`, or a custom view name like `HealthView`.                       |
| `type`     | `string` | No       | The write strategy for the target. **Default**: `"Updater"`. Valid values are `"Updater"` or `"Patcher"`.           |

A Target can be of one of two types:

*   `Updater`: The output of the pipeline **fully replaces** the target object's `spec` and `status`. Essential metadata is preserved, but labels and annotations are merged. This is suitable for creating or managing the entire state of simple resources or views.

*   `Patcher`: The output of the pipeline is applied as a **merge patch** to the existing target object. This is the safest and most common type for modifying existing Kubernetes resources, such as adding an annotation or updating a specific field in the `spec`.

## OperatorStatus

The `status` field is a read-only, system-managed subresource that provides the observed state of the Operator and its controllers.

| Field         | Type                               | Description                                          |
|---------------|------------------------------------|------------------------------------------------------|
| `controllers` | list of `ControllerStatus` objects | The status of each controller defined in the `spec`. |

### ControllerStatus

`ControllerStatus` provides detailed status information for a single controller within the Operator.

| Field        | Type                               | Description                                                                               |
|--------------|------------------------------------|-------------------------------------------------------------------------------------------|
| `name`       | `string`                           | The name of the controller, matching the one in the `spec`.                               |
| `conditions` | list of `metav1.Condition` objects | A list of conditions describing the controller's state. The primary condition is `Ready`. |
| `lastErrors` | list of `string` messages          | A rolling buffer of the last 10 error messages encountered during reconciliation, if any. |

#### Controller Conditions

| Type    | Status      | Reason                   | Description                                                                                                                           |
|---------|-------------|--------------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| `Ready` | `"True"`    | `"Ready"`                | The controller has started successfully, its configuration is valid, and it is actively processing events without any errors.         |
| `Ready` | `"False"`   | `"NotReady"`             | The controller failed to initialize due to a critical error, such as an invalid pipeline configuration. See `lastErrors` for details. |
| `Ready` | `"Unknown"` | `"ReconciliationFailed"` | The controller is running but has encountered one or more transient errors during reconciliation. See `lastErrors` for details.       |
