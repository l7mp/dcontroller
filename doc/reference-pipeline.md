# Reference: Pipeline

The `pipeline` field within a `Controller` defines the data transformation logic that converts objects from the `sources` into a desired state for the `target`. This document provides the technical specification for all pipeline stages and operators.

For a conceptual overview of how pipelines work, please see the [Concepts: Pipelines](./concepts-pipelines.md) guide.

A pipeline consists of a sequence of operations. If multiple sources are used, the first operation
must be `@join`. The pipeline can be specified as:
- A single operation: `pipeline: {"@project": ...}`
- An array of operations: `pipeline: [{"@join": ...}, {"@select": ...}, {"@project": ...}]`

```yaml
# Single operation example
pipeline:
  "@project": <projection_expression>

# Multiple operations example
pipeline:
  - "@join": <join_expression>
  - "@select": <filter_expression>
  - "@project": <projection_expression>
  # ...
```

## Combining multiple objects: `@join`

The `@join` operation is used to perform an inner join on objects from multiple `sources`. If present, this must be the first operation in a pipeline.

| Field   | Type               | Presence                                    |
|---------|--------------------|---------------------------------------------|
| `@join` | [Expression][expr] | Required when multiple sources, otherwise optional |

Behavior:
*   If a controller has more than one `source` definition, the `@join` operation is required and must be first.
*   The value must be an [Expression][expr] that evaluates to a boolean.
*   The expression is evaluated against a compound object formed by taking the Cartesian product of all source objects. This compound object contains each source object keyed by its `Kind`.
*   If the expression evaluates to `true`, the resulting compound object is passed to subsequent pipeline operations.

This join combines `Gateway` and `UDPRoute` objects that are in the same namespace and where the route references the gateway.

```yaml
# Compound object subject for the expression:
# {
#   "Gateway": { ... gateway object ... },
#   "UDPRoute": { ... route object ... }
# }

"@join":
  "@and":
    - "@eq": ["$.Gateway.metadata.namespace", "$.UDPRoute.metadata.namespace"]
    - "@in": ["$.Gateway.metadata.name", "@map": ["$$.name", "$.UDPRoute.spec.parentRefs"]]
```

## Pipeline Operations

Pipeline operations are executed sequentially. The output of each operation serves as the input for the next.

The following sections describe the valid operations that can be used in a pipeline.

### Selection: `@select`

Filters the stream of objects, passing through only those that satisfy a condition.

**Syntax:**
```yaml
"@select": <boolean_expression>
```

Or in an array:
```yaml
- "@select": <boolean_expression>
```

Here, `<boolean_expression>` is an [Expression][expr] that must evaluate to `true` or `false`.

**Behavior:**
*   For each object in the input stream, the expression is evaluated.
*   If the expression is `true`, the object is passed to the next stage **unchanged**.
*   If the expression is `false`, the object is dropped from the pipeline.

The following example selects only objects where the `spec.replicas` field is greater than 3.
```yaml
"@select":
  "@gt": ["$.spec.replicas", 3]
```

### Projection: `@project`

The `@project` operator transforms the structure of an object. This is the primary operator for reshaping data.

**Syntax:**
```yaml
- "@project": <projection_expression>
```

Here, `<projection_expression>` is an [Expression][expr] that can be a map (for object construction) or a list (for sequential transformation). Processing for the projection operator is initiated with the result from the previous pipeline or the delta object emitted by a Source as the global subject (i.e., the object that can be referenced by `$` in the projection), and a completely empty result object. It is the **responsibility of the projection op to copy all the required fields** from the subject to the result (especially the `metadata`); there is no way to recover fields lost in an earlier projection phase.

**Behavior:**

*  **Mode 1: Object Construction**
   When the expression is a map, it defines the entire structure of the **new output object**, replacing the input object completely.
   
   ```yaml
   # Input:  { "metadata": { "name": "pod-a" }, "spec": { "nodeName": "node-1" } }
   - "@project":
       metadata:
         name: "$.metadata.name"
       node: "$.spec.nodeName"
   # Output: { "metadata": { "name": "pod-a" }, "node": "node-1" }
   ```
   
*  **Mode 2: Sequential Transformation**
   When the expression is a list, each item in the list is a transformation that is applied sequentially. Each transformation modifies the result of the previous one. This is powerful for building up a result or applying targeted modifications, where subsequent modifications may overwrite the results from the previous steps or merge it with new content.
   
   ```yaml
   - "@project":
       # Start by copying the metadata from the input object.
       - metadata: "$.metadata"
       # Now add a 'spec' field to the result of the previous step.
       - "$.spec.replicas": 3
       # Finally, add an annotation to the metadata modifying the result of step 1.
       - "$.metadata.annotations.new-annotation": "true"
   ```

### Demultiplexing embedded lists: `@unwind` (or `@demux`)

The `@unwind` operator creates multiple objects from a single object by "unwinding" an embedded list field.

**Syntax:**
```yaml
- "@unwind": <jsonpath_to_list>
```

Here, `<jsonpath_to_list>` is a JSONPath string pointing to a list within the input object.

**Behavior:**
*   For each element in the source list, a new object is created.
*   In each new object, the original list field is replaced by the single element from that iteration.
*   To ensure uniqueness, the output object's name is modified to `<original-name>-<index>`.

The below example demultiplexes the `my-svc` object into two objects by the `$.spec.ports` list:
```yaml
# Input object:
# metadata:
#   name: my-svc
# spec:
#   ports:
#     - { name: http, port: 80 }
#     - { name: https, port: 443 }

- "@unwind": "$.spec.ports"

# Output (two objects):
# 1. { metadata: { name: my-svc-0 }, spec: { ports: { name: http, port: 80 } } }
# 2. { metadata: { name: my-svc-1 }, spec: { ports: { name: https, port: 443 } } }
```

### Multiplexing objects: `@gather` (or `@mux`)

The `@gather` operator groups multiple objects into a single summary object based on a key. It is the inverse of `@unwind`. Since this is the most complex operator in any Δ-controller pipeline, make sure you have a good understanding of the semantics before using it.

**Syntax:**
```yaml
- "@gather":
    - <key_expression>
    - <value_expression>
```

Here, `<key_expression>` is an [Expression][expr] that evaluates to a value to be used as the grouping key, and `<value_expression>` is a JSONPath "getter/setter" expression defining where and what to aggregate. In particular, this expression gives the path to the value to collect from each input object, which will also become the path in the output object where the list of collected values will be stored as a list.

**Behavior:**
*   The first object encountered for a new group key becomes the template for that group's output object.
*   For each subsequent object matching the key, the value specified by `<value_expression>` is extracted and appended to a list in the template object.

The following example demonstrates how to process a stream of unwound endpoint objects and group them back into a summary object for each service port.

**Scenario:** Imagine a preceding `@unwind` stage has produced a stream of objects, where each object represents a single endpoint address for a specific service port.

Consider the input Objects, a stream of 3 objects):
```yaml
{ "metadata": { "name": "my-svc-http-ep0" }, "spec": { "service": "my-svc", "port": 80, "address": "10.1.1.1" } }
{ "metadata": { "name": "my-svc-https-ep0" }, "spec": { "service": "my-svc", "port": 443, "address": "10.1.1.2" } }
{ "metadata": { "name": "my-svc-http-ep1" }, "spec": { "service": "my-svc", "port": 80, "address": "10.1.1.3" } }
```

We want to group these by `port` and collect all the `address` values for each port.

```yaml
- "@gather":
    # Argument 1: Group by port number.
    - "$.spec.port"
    # Argument 2: Collect 'spec.address' values and write the resulting list back into 'spec.address'.
    - "$.spec.address"
```

The flow of execution is as follows:
1.  **Object 1 (`port: 80`)**: A new group for key `80` is created. This object becomes the template. The value `"10.1.1.1"` is extracted from `spec.address`. The collected values list is `["10.1.1.1"]`.
2.  **Object 2 (`port: 443`)**: A new group for key `443` is created. This object becomes its template. The value `"10.1.1.2"` is extracted. The collected values list for this group is `["10.1.1.2"]`.
3.  **Object 3 (`port: 80`)**: This matches the existing group for key `80`. The value `"10.1.1.3"` is extracted from `spec.address` and appended to the group's list, which becomes `["10.1.1.1", "10.1.1.3"]`.
4.  **Finalization**: The pipeline creates one output object per group key.
    *   For the `80` group, it takes the template (from Object 1) and writes the final list `["10.1.1.1", "10.1.1.3"]` to the path `spec.address`.
    *   For the `443` group, it takes the template (from Object 2) and writes its list `["10.1.1.2"]` to `spec.address`.

This yields the final output, two summary objects):

```yaml
# Output Object 1
{
  "metadata": { "name": "my-svc-http-ep0" },
  "spec": {
    "service": "my-svc",
    "port": 80,
    "address": ["10.1.1.1", "10.1.1.3"] # Original 'address' field is overwritten with the list.
  }
}
# Output Object 2
{
  "metadata": { "name": "my-svc-https-ep0" },
  "spec": {
    "service": "my-svc",
    "port": 443,
    "address": ["10.1.1.2"]
  }
}
```
This demonstrates how `@gather` can effectively reverse an `@unwind` operation, summarizing detailed, per-item objects back into a consolidated group view. You may want to add a subsequent `@project` phase to tweak the object shape: e.g., it may be a good idea to change the key `address` to `addresses` in order to stress that the content is a list, or remove the `-ep0` suffix from the object summary names.
