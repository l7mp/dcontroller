<!-- [![Go Report Card](https://goreportcard.com/badge/sigs.k8s.io/controller-runtime)](https://goreportcard.com/report/sigs.k8s.io/controller-runtime) -->
<!-- [![godoc](https://pkg.go.dev/badge/sigs.k8s.io/controller-runtime)](https://pkg.go.dev/sigs.k8s.io/controller-runtime) -->

# Δ-controller: Declarative Kubernetes controller framework

Δ-controller is a framework to simplify the design, implementation and maintenance of Kubernetes
operators. The main goal is to reduce the mental overhead of writing Kubernetes operators by
providing simple automations to eliminate some of the repetitive code that must be written when
coding against the [Kubernetes
controller-runtime](https://github.com/kubernetes-sigs/controller-runtime) project. The final goal
is to let anyone with minimal Go skills to write complex Kubernetes operators in the NoCode/LowCode
style.

## Description

The main design philosophy behind Δ-controller is to view the Kubernetes API as a gigantic NoSQL
database and implement a custom query language that can be used to process Kubernetes objects into
either a simplified internal representation that we call a *view*, or back into actual native
Kubernetes resources. This is much akin to MongoDB, so users familiar with document-oriented
databases and declarative queries will feel at home when using Δ-controller.

The Δ in the name stands for **declarative** and **delta**. 

First, Δ-controller implements a *declarative query language* (inspired by [MongoDB
aggregators](https://www.mongodb.com/docs/manual/core/aggregation-pipeline)) that can be used to
combine, filter and transform Kubernetes API resources into a representation that better fits the
operators' internal logic, and from there back to native Kubernetes resources.  This is done by
registering a processing pipeline to map the Kubernetes API resources into a view of interest to
the controller. Views are dynamically maintained by Δ-controller by running the aggregation
pipeline on the watch events automatically installed for the source Kubernetes API resources of the
aggregation pipeline. 

The literal delta in the name connotes that operators in Δ-controller use *incremental
reconciliation* style control loops. This means that the controllers watch the incremental changes
to Kubernetes API resources and internal views and act only on the deltas. This simplifies writing
[edge-triggered
conrollers](https://hackernoon.com/level-triggering-and-reconciliation-in-kubernetes-1f17fe30333d).

## Getting started

<!-- ### Expressions -->

<!-- - Aggregations work on objects that are indexed on (.metadata.namespace, .metadata.name): all -->
<!--   objects at every stage of the aggregation must have valid .metadata. -->
<!-- - Operator arguments go into lists, optional for single-argument ops (like @len and @not).  -->
<!-- - No multi-dimensional lists: arrays our unpacked to the top level. -->

## Caveats

- The operator runs with full RBAC capabilities. This is required at this point to make sure that
  the declarative operators loaded at runtime can access any Kubernetes API resource they intend to
  watch or modify. Later we may implement a dynamic RBAC scheme that would minimize the required
  permissions to the minimal set of APi resources the running operators need.
- The strategic merge patch implementation does not handle lists. Since Kubernetes does not
  implement native strategic merge patching for schemaless unstructured resources, currently
  patches must be implemented a simplified local JSON patch code that does not handle lists. Use
  `Patcher` targets carefullly.

## License

Copyright 2024 by its authors. Some rights reserved. See [AUTHORS](AUTHORS).

Apache License - see [LICENSE](LICENSE) for full text.

