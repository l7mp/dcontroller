# Δ-controller: Documentation

Welcome to the official documentation for Δ-controller, a NoCode/LowCode framework for building powerful and efficient Kubernetes operators. This guide will take you from the core concepts to advanced tutorials.

## Table of Contents

*   [What is Δ-controller?](./what-is-delta-controller.md)
*   [Getting Started](./getting-started.md)
*   **Concepts**
    *   [Operator, Controller, and Object](./concepts-operator-controller-object.md)
    *   [Source and Target](./concepts-source-target.md)
    *   [View](./concepts-view.md)
    *   [Pipeline](./concepts-pipeline.md)
    *   [Expression](./concepts-expression.md)
    *   [Extension API Server](./concepts-API-server.md)
*   **Reference**
    *   [Operator CR](./reference-operator.md)
    *   [Pipeline](./reference-pipeline.md)
    *   [Expression Language](./reference-expression.md)
*   **Tutorials**
    *   [ConfigMap-Deployment Controller](./examples/configmap-deployment-controller/README.md): A purely declarative controller that automatically restarts Deployments when a referenced ConfigMap changes.
    *   [Service Health Monitor](./examples/service-health-monitor/README.md): A two-stage pipeline using a custom View to monitor pod health and annotate Services with the results.
    *   [EndpointSlice Controller](./examples/endpointslice-controller/README.md): A hybrid controller demonstrating how to integrate declarative pipelines with imperative Go-based reconcilers.
*   [Further reading](./further-reading.md)
