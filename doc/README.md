# Δ-controller: Documentation

Welcome to the official documentation for Δ-controller, a NoCode/LowCode framework for building powerful and efficient Kubernetes operators. This guide will take you from the core concepts to advanced tutorials.

## Table of Contents

*   [What is Δ-controller?](./what-is-delta-controller.md)
*   [Getting Started](./getting-started.md)
*   **Concepts**
    *   [Operators, Controllers, and Objects](./concepts-operators-objects.md)
    *   [Sources and Targets](./concepts-sources-targets.md)
    *   [Views](./concepts-views.md)
    *   [Pipelines](./concepts-pipelines.md)
    *   [Expressions](./concepts-expressions.md)
    *   [Extension API Server](./concepts-API-server.md)
*   **Reference**
    *   The Operator CR `[TODO]`
    *   Pipeline Reference `[TODO]`
    *   Expression Language Reference `[TODO]`
*   **Tutorials**
    *   [ConfigMap-Deployment Controller](./examples/configmap-deployment-controller/README.md): A purely declarative controller that automatically restarts Deployments when a referenced ConfigMap changes.
    *   [Service Health Monitor](./examples/service-health-monitor/README.md): A two-stage pipeline using a custom View to monitor pod health and annotate Services with the results.
    *   [EndpointSlice Controller](./examples/endpointslice-controller/README.md): A hybrid controller demonstrating how to integrate declarative pipelines with imperative Go-based reconcilers.
