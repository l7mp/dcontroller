# Concepts: Pipelines

The **pipeline** is the core of every controller in Δ-controller. It's a declarative, multi-stage data processing workflow that defines how to transform incoming data from `sources` into the desired state for the `target`. Think of it as a sophisticated, Kubernetes-aware version of a Unix shell pipe or a database aggregation query.

A pipeline can be defined as either a single operation or as an array of operations. If multiple sources are used, the first operation must be `@join`.

```yaml
# Single operation example
pipeline:
  "@project": { ... }

# Multiple operations example
pipeline:
  - "@join": { ... }    # Required when multiple sources
  - "@select": { ... }  # Stage 1: filtering
  - "@project": { ... } # Stage 2: transformation
  # ... and so on
```

## The Workflow: From Delta to Delta

Understanding the flow of data through the pipeline is key to mastering Δ-controller. The entire process is triggered by a single change event, emitted by a controller `source` detecting a change as an object is added, updated, or deleted. This change is represented as a single **delta**, containing a delta type (like `Added` or `Upserted`) that Δ-controller manages internally, and an object that the pipeline will process.

**Join Stage (if present)**: If the pipeline has a `@join` operation, the object from the trigger delta is joined against the *current cached state* of all other source objects. This can result in zero, one, or many **compound objects**, containing the matching objects from each source, keyed by their `Kind`. For example:
```json
{
"Service": { "metadata": { "name": "my-svc" }, ... },
"EndpointSlice": { "metadata": { "name": "my-svc-xyz" }, ... }
}
```
If there is no `@join`, the source object from the delta is passed directly to the next pipeline stage; in this case the controller must have only a single source. Otherwise, a controller has as many sources are there are resources referenced in the join expression.

**Pipeline Stages**: The object (or set of objects) from the previous stage is fed into the first transformation stage of the pipeline. Each stage processes the data and passes its output as the input to the next stage. This sequential execution allows you to build up complex transformations step-by-step.

The result of the final pipeline stage is a set of deltas (adds, updates, or deletes, along with the processed object) that are then sent to the controller's `target` to be written to the Kubernetes API server or an internal view. 

Note that *inside* the pipeline transient result and subject objects passed by one pipeline stage to the other do not need to adhere to the Kubernetes naming conventions (i.e., have a valid namespace/name in the metadata) or satisfy any schemata. That is, pipelines are completely free to produce and consume any key-value struct internally, depending the business logic. However, the final result emitted by the pipeline *must* be a valid object: if the target is a View then the [view naming rules](concepts-views.md#the-anatomy-of-a-view-api-promise) apply, and it is a native Kubernetes resource (e.g., a `Pod` or a `Deployment`) then it also must be accepted by the corresponding Kubernetes API schema.

## The Join Operation: `@join`

The `@join` operation is the first step in any multi-source pipeline. It defines an **inner join** that combines objects from different sources into a single, compound object for further processing.

The join is defined by a boolean expression that is evaluated against a Cartesian product of all source objects. If the expression evaluates to `true`, the combined object is passed to the subsequent pipeline operations.

The below example joins a `UDPRoute` with its parent `Gateway` (see the [Gateway API spec](https://gateway-api.sigs.k8s.io/)) if they are in the same namespace and the route's `spec.parentRefs` refers to the gateway's name.

```yaml
sources:
  - apiGroup: "gateway.networking.k8s.io"
    kind: Gateway
  - apiGroup: "gateway.networking.k8s.io"
    kind: UDPRoute
pipeline:
  "@join":
    "@and":
      - "@eq": ["$.Gateway.metadata.namespace", "$.UDPRoute.metadata.namespace"]
      - "@in": ["$.Gateway.metadata.name", "@map": ["$$.name", "$.UDPRoute.spec.parentRefs"]]
```

## Pipeline Operations

This is where the main data transformation happens. The pipeline is a sequence of processing stages, executed in order. Each stage receives the output of the previous one (this is called the "subject") and starts with an empty object that it will gradually populate from the subject.

### `@select`: Filtering Objects

The `@select` operator acts like a filter, or a `WHERE` clause in SQL speak. It takes a boolean expression and, for each object it receives, passes the object through *unchanged* if the expression is true, or drops it if it's false.

The below stage only allows objects with more than 3 replicas to continue down the pipeline.

```yaml
- "@select":
    "@gt": ["$.spec.replicas", 3]
```

### `@project`: Reshaping Objects

The `@project` operator is the most versatile tool for transforming the structure of an object. It works like a `SELECT` clause in SQL, allowing you to define the exact shape of the output object. It can be used in several ways:

**As a single map expression to define a new shape:**
You define the output structure and use JSONPath expressions to populate it with values from the input object.

```yaml
# Input object has: { "metadata": { "name": "x" }, "spec": { "version": "v1" } }
- "@project":
    # The output object will have this exact structure:
    metadata:
      name: "$.metadata.name"   # Copies the name
      namespace: default        # Namespace statically set to "default"
    versionInfo:
      version: "$.spec.version" # Moves version into a new field
```

**As a list of sequential transformations:**
When `@project` contains a list, each item is a transformation applied sequentially to the result object. This is powerful for building up a complex object step-by-step, letting subsequent operations in the list to overwrite or modify the result of previous operations.

```yaml
- "@project":
    # Start by copying the entire metadata from the input object
    - metadata: "$.metadata"
    # Now, add a new 'spec' field to the *result* of the previous step
    - spec:
        replicas: "$.dep.spec.replicas"
        image: "$.pod.spec.image"
    # Finally, use a JSONPath setter to add a new annotation
    - "$.metadata.annotations.managed-by": "dcontroller"
```

**As a list of JSONPath setters:**
If a map key within `@project` is a JSONPath string, it acts as a "setter," modifying the input object at that path.

```yaml
# Input object: { "metadata": { "name": "my-pod" } }
- "@project":
    # This list applies two patches to the input object
    - "$.metadata.namespace": "production" # Sets the namespace
    - "$.spec.restartPolicy": "Always"     # Adds a spec field
```

### `@unwind`: Demultiplexing Lists

The `@unwind` operator (also aliased as `@demux`) is used to create multiple objects from a single object that contains an embedded list field. For each element in the specified list, `@unwind` creates a copy of the parent object and replaces the original list field with that single element. To ensure uniqueness, it appends the list index to the object's name.

As an example, given a `Service` with two ports the below `@unwind` stage will produce two separate output objects. 

```yaml
- "@unwind": "$.spec.ports"
```

Suppose the below subject:

```yaml
metadata:
  name: my-svc
spec:
  ports:
    - { name: http, port: 80 }
    - { name: https, port: 443 }
```

Then the output of the `@unwind` operation is 2 objects:

```yaml
metadata: { name: my-svc-0 }, spec: { ports: { name: http, port: 80 } }
metadata: { name: my-svc-1 }, spec: { ports: { name: https, port: 443 } }
```

### `@gather`: Multiplexing Objects

The `@gather` operator (also aliased as `@mux`) is the inverse of `@unwind`. It groups multiple objects into a single summary object based on a key. The `@gather` operator takes two arguments:
1.  **Grouping Key Expression**: An expression that evaluates to the key for grouping (e.g., `$.metadata.namespace`).
2.  **Value Expression**: An expression that extracts the value to be collected from each object in the group.

The output is one summary object per unique group key. The original object that first created the group is used as a template, and the collected values are written into it at the path specified by the value expression.

Note that `@unwind` followed by a `@gather` should yield the same object unchanged, but the list order is not guaranteed to be preserved (this is by nature of the underlying incremental processing engine, DBSP).

The below pipeline gathers all `Pods`, groups them by `namespace`, and collects their names back into the `$.metadata.name` field, which will now become a list:

```yaml
- "@gather":
    # Argument 1: Group by namespace
    - "$.metadata.namespace"
    # Argument 2: Collect the pod names and write them to `spec.podNames`
    - "$.metadata.name"
```

Consider the below input of 3 Pod objects:

```yaml
- { metadata: { name: pod-a, namespace: ns-1 } }
- { metadata: { name: pod-b, namespace: ns-2 } }
- { metadata: { name: pod-c, namespace: ns-1 } }
```

The output is 2 summary objects:

```yaml
{ metadata: { name: ["pod-a", "pod-c"], namespace: ns-1 } }
{ metadata: { name: ["pod-b"], namespace: ns-2 } }
```

It is very common for a gathered object to end up with an incorrect shape. In the below example, the result would be an invalid object since the `metadata.name` field must be a single `string`, and not be a list. Therefore it is a common pattern to follow up a `@gather` operation with a subsequent `@project` that will get the shape right. In the below example we will copy the names out into a `podNames` list and add a sensible name, which will be just the name of the namespace followed by `-summary`.

```yaml
pipeline:
  - "@gather":
      - "$.metadata.namespace"
      - "$.metadata.name"
  - "@project":
      metadata:
        name:
          "@concat": ["$.metadata.namespace", "-summary"]
        namespace: $.metadata.namespace
      podNames: "$.metadata.name"
```

This will result the following 2 result summaries:

```yaml
{ metadata: { name: ns-1-summary, namespace: ns-1 }, podNames: [pod-a, pod-c] }
{ metadata: { name: n2-2-summary, namespace: ns-2 }, podNames: [pod-b] }
```
