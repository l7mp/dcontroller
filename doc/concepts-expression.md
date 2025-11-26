# Concepts: Expressions

In the previous sections, you've seen snippets of logic like `$.metadata.name` and operators like `@eq`. These are all part of Δ-controller's **expression language**, the low-level logic for Δ-controller pipelines.  An expression is a declarative, JSON/YAML-based structure that is evaluated at runtime to produce a value. Expressions are the fundamental building blocks used within a controller's `pipeline` to filter, transform, and construct Kubernetes objects.

## The Subject: What an Expression Operates On

Every expression is evaluated against a **subject** object, which provides the context for data access. This is similar to how `.` works in a shell script or `this` in many programming languages. In Δ-controller, the subject is the unstructured object emitted by a source after a watched resource has changed.

When a pipeline starts, the subject is the **compound object** produced by the `@join` stage (or the single source object if there is no join). As data flows through the pipeline stages, the output of one stage becomes the subject for the next.

## Accessing Data: JSONPath

The primary way to interact with the subject is through **JSONPath**, a standard query language for JSON documents. Δ-controller uses JSONPath to let you navigate the structure of an object and extract or modify values.

Expressions recognize strings that start with `$` as JSONPath queries.

*   `$.spec.replicas`: Navigates to the `replicas` field inside the `spec`.
*   `$.spec.ports[2]`: Navigates to the 2nd service-port of a `Service` or any other object that has such a field.
*   `$['metadata']['annotations']['my.co/annotation']`: Accesses an annotation with special characters in its key.

## Global vs. Local Subject: `$` vs. `$$`

This is one of the most important concepts for list manipulation. Δ-controller defines two special symbols for JSONPath:

*   **`$` (Global Subject)**: Refers to the root of the **single object** currently flowing through the pipeline stage.
*   **`$$` (Local Subject)**: Used *only inside list operators* like `@map` and `@filter`. It refers to the **individual item** of the list currently being processed.

This distinction allows you to compare or combine a specific list item with data from the parent object that contains the list.

The below example shows how to filter Service ports based on a Service annotation. Suppose a `Service` object is the global subject (`$`). 

```yaml
apiVersion: v1
kind: Service
metadata:
  name: kubernetes
  namespace: default
  annotations:
    "primary-port": "https-main"
spec:
  ports:
    - { name: http, port: 80 }
    - { name: https-main, port: 443 } # <-- We want to find this one.
    - { name: https-alt, port: 8443 }
```

We want to filter its `spec.ports` list to find the port whose name is specified in an annotation.

Consider the below @filter expression. Here, the condition compares each list item's name (accessed as the local list subject `$$`) with the global annotation (accessed as the global subject `$`). The second argument is the list we want to filter, in particular, the ports list from the Service (our global subject, accessed with `$`).

```yaml
@filter
  - "@eq": ["$$.name", "$.metadata.annotations.primary-port"]
  - "$.spec.ports"
```

The `@filter` operator iterates through the list provided by the `$.spec.ports` JSONPath expression. For each port object in the list, `$$` becomes the local subject (e.g., `{ name: http, port: 80 }`), `$$.name` extracts the name of the current port being checked, `$.metadata.annotations.primary-port` goes back to the global subject (the Service) to get the value `"https-main"`, and the `@eq` expression returns `true` only for the port where the names match.

## A Tour of Expression Operators

Expressions are composed of operators, which are always denoted by keys starting with `@`. Below is a conceptual overview of the most common types. A full list is available in the Reference guide.

### Conversion Operators

These operators are used to convert values between types, which is often necessary to satisfy the schema of a target resource (e.g., all data in a `ConfigMap` must be strings). Note that Δ-controller does not care about schemas at all, but Kubernetes does and will err if you try to re-inject a `ConfigMap` violating the API schema.

For instance, `@string` converts its argument to a string.

```yaml
# Creates an annotation with the string value "3"
"dcontroller.io/container-num":
  "@string":
    "@len": "$.spec.containers"
```

### Conditional and Logical Operators

These operators allow you to build decision-making logic into your pipeline.

*   **`@and`, `@or`, `@not`**: Standard boolean logic that takes a list of boolean arguments. They perform short-circuiting, meaning they stop evaluating as soon as the outcome is certain.
*   **`@eq`, `@gt`, `@lt`, etc.**: Comparison operators that take a list of two arguments and return `true` or `false`.
*   **`@cond`**: An `if/then/else` construct. It takes an list of three arguments: `[<condition>, <value_if_true>, <value_if_false>]`.

The below example sets the 'environment' field to "prod" if the namespace is "production", otherwise sets it to "dev".

```yaml
"@cond":
  - "@eq": ["$.metadata.namespace", "production"]
  - "prod"
  - "dev"
```

#### List Operators

These operators are the heart of data transformation, used for iterating and manipulating lists within your objects. They always introduce the `$$` local subject.

*   **`@map`**: Applies an expression to every item in a list and returns a new list with the transformed items. It takes two arguments: `[<expression_to_apply_to_$$>, <list>]`.

    The below expression transforms each port number into an object.
    ```yaml
    # Input: { "spec": { "ports": [80, 443, 8080] } }
    containerPorts:
      "@map":
        - containerPort: "$$"  # $$ is the current port number (e.g., 80)
        - "$.spec.ports"
    # Output: "containerPorts": [ { "containerPort": 80 }, { "containerPort": 443 }, ... ]
    ```

*   **`@filter`**: Selects items from a list that satisfy a condition. It takes two arguments: `[<condition_on_$$>, <list>]`.

    ```yaml
    # Input: { "spec": { "scores": [50, 95, 80, 20] } }
    passingScores:
      "@filter":
        - "@gte": ["$$", 80] # $$ is the current score
        - "$.spec.scores"

    # Output: "passingScores": [95, 80]
    ```

*   **`@len`**: Returns the number of items in a list.

### Miscellaneous Operators

*   **`@concat`**: Joins a list of strings into a single string.
*   **`@sum`**: Calculates the sum of a list of numbers.
*   **`@hash`**: Computes a short, stable hash of any value. This is extremely useful for generating unique but deterministic names for objects.
*   **`@new`**: Returns the current date in ISO format. Useful to set status fields like `lastTransitionTime` or `startTime`.
