# Reference: Expression Language

The Δ-controller expression language is a declarative, JSON/YAML-based syntax used within a controller's `pipeline` to query, transform, and construct data. This is the formal technical reference for the expression language syntax and operators.

Please see the [Concepts: Expressions](./concepts-expressions.md) guide for a conceptual introduction and the specifics of working with [JSONPath expressions](concepts-expressions.md#accessing-data-jsonpath) and [global vs local subjects](concepts-expressions.md#global-vs-local-subject--vs-).

## Literals

Every valid JSON/YAML value is a potential expression. The framework interprets these values based on their structure. Standard JSON/YAML types are treated as literal values:
*   **String**: `"hello"`
*   **Integer** (64-bit int): `42`
*   **Float** (64-bit floating point): `3.14`
*   **Boolean**: `true`
*   **Null**: `null`
*   **List**: `[1, "two", false]`
*   **Map/Object**: `{"key-1": "value-1", "key-2": "value-2"}`

## Operators

Expression operators perform data transformations and computations. They are always represented as a JSON object with a single key that begins with the `@` symbol. The value associated with the key contains the operator's arguments. 

This example shows an equality check with an argument list:
```yaml
{"@eq": ["$.spec.replicas", 3]}
```
The following example shows a single-argument operator:
```yaml
{"@not": true}
```
### Logical and Conditional Operators

#### `@and`
Performs a logical AND on a list of boolean expressions. It short-circuits, stopping evaluation at the first `false` argument.

*   **Signature**: `{"@and": [<arg1>, <arg2>, ...]}`
*   **Arguments**: An list of two or more boolean expressions.
*   **Returns**: A `boolean`.
*   **Example**:
    ```yaml
    "@and":
      - "@gt": ["$.spec.replicas", 1]
      - "@eq": ["$.metadata.namespace", "production"]
    ```

#### `@or`
Performs a logical OR on a list of boolean expressions. It short-circuits, stopping evaluation at the first `true` argument.

*   **Signature**: `{"@or": [<arg1>, <arg2>, ...]}`
*   **Arguments**: An list of two or more boolean expressions.
*   **Returns**: A `boolean`.
*   **Example**:
    ```yaml
    "@or":
      - "@eq": ["$.spec.priorityClassName", "high-priority"]
      - "@exists": "$.metadata.annotations.priority-override"
    ```

#### `@not`
Negates a single boolean expression.

*   **Signature**: `{"@not": <expression>}`
*   **Arguments**: A single boolean expression.
*   **Returns**: A `boolean`.
*   **Example**:
    ```yaml
    "@not":
      "@eq": ["$.status.phase", "Running"]
    ```

#### `@cond`
An `if-then-else` conditional.

*   **Signature**: `{"@cond": [<condition>, <if_true_expr>, <if_false_expr>]}`
*   **Arguments**: An list of three expressions. The first must evaluate to a boolean.
*   **Returns**: The result of `<if_true_expr>` if the condition is true, otherwise the result of `<if_false_expr>`.
*   **Example**:
    ```yaml
    logLevel:
      "@cond":
        - "@eq": ["$.metadata.labels.env", "prod"]
        - "INFO"
        - "DEBUG"
    ```

#### `@definedOr`
Returns the result of the first expression if it is not `null`, otherwise returns the result of the second expression. Useful for setting default values.

*   **Signature**: `{"@definedOr": [<expr1>, <default_expr>]}`
*   **Arguments**: An list of two expressions.
*   **Returns**: The result of `<expr1>` or `<default_expr>`.
*   **Example**:
    ```yaml
    # Use the annotation if it exists, otherwise default to port 8080.
    targetPort:
      "@definedOr":
        - "$.metadata.annotations.targetPort"
        - 8080
    ```

### Comparison Operators

These operators all take an list of two arguments and return a `boolean`.

*   **`@eq`**: Returns `true` if the two arguments are deeply equal.
*   **`@gt`**: Returns `true` if the first numeric argument is greater than the second.
*   **`@gte`**: Returns `true` if the first numeric argument is greater than or equal to the second.
*   **`@lt`**: Returns `true` if the first numeric argument is less than the second.
*   **`@lte`**: Returns `true` if the first numeric argument is less than or equal to the second.

**Example**:
```yaml
"@gte": ["$.status.readyReplicas", "$.spec.replicas"]
```

### List Operators

#### `@map`
Applies an expression to each item in a list and returns a new list containing the results.

*   **Signature**: `{"@map": [<transform_expression>, <list_expression>]}`
*   **Arguments**:
    1.  An expression to apply to each item. Use `$$` to refer to the item.
    2.  An expression that evaluates to a list.
*   **Returns**: A `list`.
*   **Example**:
    ```yaml
    # Creates a list of container names from a Pod spec.
    "@map":
      - "$$.name"
      - "$.spec.containers"
    ```

#### `@filter`
Selects items from a list that satisfy a boolean condition.

*   **Signature**: `{"@filter": [<condition_expression>, <list_expression>]}`
*   **Arguments**:
    1.  A boolean expression. Use `$$` to refer to the item being tested.
    2.  An expression that evaluates to a list.
*   **Returns**: A `list` containing only the items that passed the condition.
*   **Example**:
    ```yaml
    # Returns only the ports with protocol TCP.
    "@filter":
      - "@eq": ["$$.protocol", "TCP"]
      - "$.spec.ports"
    ```

#### `@len`
Returns the number of items in a list.

*   **Signature**: `{"@len": <list_expression>}`
*   **Arguments**: An expression that evaluates to a list.
*   **Returns**: An `integer`.
*   **Example**:
    ```yaml
    containerCount:
      "@len": "$.spec.containers"
    ```

#### `@in`
Checks if an element exists within a list.

*   **Signature**: `{"@in": [<element_expression>, <list_expression>]}`
*   **Arguments**: An element and a list to search within.
*   **Returns**: A `boolean`.
*   **Example**:
    ```yaml
    # Checks if "nginx" is one of the container images.
    "@in":
      - "nginx"
      - "@map": ["$$.image", "$.spec.containers"]
    ```

### String Operators

#### `@concat`
Concatenates a list of strings. Non-string arguments are converted to their string representation.

*   **Signature**: `{"@concat": [<arg1>, <arg2>, ...]}`
*   **Arguments**: An list of expressions.
*   **Returns**: A `string`.
*   **Example**:
    ```yaml
    # Creates a name like "my-pod-prod"
    name:
      "@concat":
        - "$.metadata.name"
        - "-"
        - "$.metadata.labels.env"
    ```

### Type Conversion Operators

*   **`@string`**: Converts the argument to a `string`.
*   **`@int`**: Converts the argument to an `integer`. Fails if the conversion is not possible.
*   **`@float`**: Converts the argument to a `float`.
*   **`@bool`**: Converts the argument to a `boolean`.

**Example**:
```yaml
# ConfigMap data values must be strings.
data:
  replicas:
    "@string": "$.spec.replicas"
```

### Utility Operators

#### `@hash`
Computes a short, stable, base36-encoded MD5 hash of its argument. Useful for creating deterministic names.

*   **Signature**: `{"@hash": <expression>}`
*   **Arguments**: Any expression.
*   **Returns**: A 6-character `string`.
*   **Example**:
    ```yaml
    # Create a unique but stable name for a ConfigMap based on a Service's spec
    name:
      "@concat":
        - "config-"
        - "$.metadata.name"
        - "-"
        - "@hash": "$.spec"
    ```

#### `@exists`
Returns `true` if the JSONPath expression successfully resolves to a value (i.e., not `null`).

*   **Signature**: `{"@exists": <jsonpath_string>}`
*   **Arguments**: A JSONPath `string`.
*   **Returns**: A `boolean`.

#### `@isnil`
Returns `true` if the expression evaluates to `null`.

*   **Signature**: `{"@isnil": <expression>}`
*   **Arguments**: Any expression.
*   **Returns**: A `boolean`.

#### `@now`
A special string literal that evaluates to the current time in RFC3339 format.

*   **Syntax**: Must be used as a literal string `"@now"`.
*   **Returns**: A `string` representing the current timestamp, e.g., `"2025-07-25T12:00:00Z"`.
*   **Example**:
    ```yaml
    metadata:
      annotations:
        "reconciled-at": "@now"
    ```
