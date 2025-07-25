# What is Δ-controller?

Δ-controller is a Kubernetes operator framework designed to simplify the creation and maintenance of custom controllers. It addresses the complexity of writing operators with standard tools like `controller-runtime` by introducing a NoCode/LowCode approach. This allows developers and platform engineers to implement powerful automation with minimal Go programming, and in many cases, with no code at all.

The core philosophy of Δ-controller is to treat the Kubernetes API as a vast, queryable JSON database. Instead of writing complex imperative Go code to watch, fetch, and reconcile resources, you define declarative pipelines that transform Kubernetes objects in a style similar to MongoDB's aggregation framework. This is similar to how MongoDB aggregation pipelines are used to process JSON databases, just applying the NoSQL concept to the Kubernetes API.

The **Δ** (Delta) in the name signifies two core principles:

1.  **Declarative**: You declare a *transformation pipeline* that you want to apply to Kubernetes objects in a YAML manifest, and Δ-controller's engine handles the implementation. This is in contrast to imperative methods where you specify the exact steps to achieve a result.
2.  **Delta (Incremental)**: The framework is built on a high-performance incremental computation engine inspired by Differential Dataflow (DBSP). This means controllers react only to the *changes* (deltas) in the system state, rather than performing full state-of-the-world reconciliations on every event. This makes Δ-controller operators highly efficient and scalable.

Δ-controller is not just another operator framework; it's a fundamental rethinking of how we should interact with the Kubernetes API for automation. That being said, as every fundamental rethinking of an old concept, Δ-controller also comes with certain quirks: below is a rundown of the Good, the Bad and the Ugly of Δ-controller.

## Δ-controller: The Good

**Kubernetes automation simplified: Declarative controllers.** Anyone who has written a non-trivial Kubernetes operator using the standard tools knows the pain. You spend 20% of your time on the actual business logic and 80% on boilerplate: setting up watches, managing caches, handling events, performing complex state reconciliation, and writing endless `if err != nil{...}` blocks. Δ-controller's declarative pipeline model directly attacks this 80%, abstracting away the most tedious and error-prone parts of operator development.

**The "Kubernetes as a Database" Abstraction.** Platform engineers and SREs are already used to thinking in terms of data transformation and queries. The mental model of Δ-controller builds on the same idea of framing Kubernetes object manipulation as a database aggregation pipeline (`@join` this with that, `@select` these, `@project` into a new shape). This makes the use of Δ-controller intuitive and lowers the barrier to entry dramatically. You don't need to be a Go expert to build a useful controller; you just need to understand your data.

**The Incremental Engine (DBSP).**  The embedded DBSP-inspired incremental computation engine allows Δ-controller to scale to more objects and more frequent updates while still remaining efficient. Δ-controller does not re-compute the entire world on every change; it intelligently calculates and propagates only the *deltas*. This is a huge performance and scalability advantage in large-scale clusters. Note that at the moment the DBSP evaluator is not yet optimized for maximum performance so expect slowdowns every now and then.

**Composable Automation: The "View" Concept.** The main philosophy of Δ-controller is to allow controllers to be chained together into essentially any datatlow graph, with one controller's output (a view) serving as another's input. This allows for building complex workflows from simple, reusable components, and maximizes composability at the cost of a slight increase of (declarative) complexity. Δ-controller includes an embedded Kubernetes API server that exposes internal views. This allows you to inspect, query, and even watch the state of your views using standard tools like `kubectl`, making debugging and introspection seamless.

## Δ-controller: The Bad

**Debugging Can Be Opaque.** The declarative nature that makes it so easy to write controllers can make it hard to debug them. When a pipeline doesn't produce the expected output, there is no way to set a breakpoint or add a `printf` or a log message. The extension API server can be used for inspecting the final state of views, but the framework currently lacks more advanced debugging tools. A "dry-run" mode, pipeline tracing, and metrics are on the TODO list to improve the debuggability.

**Permissive RBAC model.** In its current form, the central Δ-controller manager needs cluster-wide permissions to watch and modify *any* resource type, because it can't know in advance what declarative operators users will submit. This is a non-starter for some production security postures. Eventually Δ-controller will gain a mechanism to dynamically generate and apply scoped RBAC rules from the deployed operators; for now consider this limitation when using Δ-controller.

**No schemata.** Δ-controller uses a completely the schemaless, unstructured API object model. This allows it to process *any* resource, even CRDs it does not even know and view resources not registered with the Kubernetes API server, flexibly. At the same time this approach can be risky, since there is no guarantee that the final shape and content of a processed resource will adhere to *any* API promise. Your pipeline is your schema, use it accordingly.

**The "NoCode" Promise.** In truly complex real-world scenarios, you will still drop down into Go. Perhaps it is best to describe Δ-controller as a powerful "LowCode" framework that lets you solve 80% of your problems declaratively and provides clean hooks for the remaining 20% that requires imperative Go logic.

## Δ-controller: The Ugly

**Another YAML DSL.** Working with Kubernetes a lot it is easy to grow a certain level of "YAML fatigue", and Δ-controller will require learning yet another way to express logic in structured text. And while it eliminates the need to learn the depths of `controller-runtime`, Δ-controller introduces its own domain-specific language (DSL) for expressions. A new user has to learn the syntax and semantics of `@project`, `@unwind`, `$.` vs `$$`, and the nuances of the JSONPath, which can make for a steeper learning curve for new users, especially for those without former familiarity with MongoDB. 

Δ-controller has the potential to fundamentally change how a large class of operators are built, but the code is still in an initial phase. It works surprisingly reliably in many complex use cases, but the usual cautionary approach doubly applies here: **use Δ-controller at your own risk**.
