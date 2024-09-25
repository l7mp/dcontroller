<!-- [![Go Report Card](https://goreportcard.com/badge/sigs.k8s.io/controller-runtime)](https://goreportcard.com/report/sigs.k8s.io/controller-runtime) -->
<!-- [![godoc](https://pkg.go.dev/badge/sigs.k8s.io/controller-runtime)](https://pkg.go.dev/sigs.k8s.io/controller-runtime) -->

# Declarative Kubernetes controller runtime

This is a preliminary implementation for the declarative Kubernetes controller runtime. The main
goal is to reduce the mental overhead of writing Kubernetes controllers, by providing simple
automations to eliminate some of the repetitive code that must be written when coding against the
upstream [controller-runtime](https://github.com/kubernetes-sigs/controller-runtime) project. The
final goal is to let anyone with minimal Go skills to write NoCode or LowCode style.

The main tools here are *declarative Kubernetes API translation* and *incremental
reconciliation*. Declarative Kubernetes API translation allows to view the Kubernetes API as a
NoSQL database and run simple declarative queries on it. This can be done by registering an
aggregation pipeline (inspired by [MongoDB
aggregators](https://www.mongodb.com/docs/manual/core/aggregation-pipeline)) to map the Kubernetes
API resources to a *view* of interest to the controller. These views are dynamically maintained by
the declarative controller runtime, by running the aggregation pipeline on the watch events
automatically installed for the base Kubernetes API resources on top of which the view is defined
on. Incremental reconciliation then means that the controller can watch the incremental changes to
the views, instead of the raw ("materialized") views, which simplifies writing [edge-triggered
conrollers](https://hackernoon.com/level-triggering-and-reconciliation-in-kubernetes-1f17fe30333d).

### Expressions

- Aggregations work on objects that are indexed on (.metadata.namespace, .metadata.name): all
  objects at every stage of the aggregation must have valid .metadata.
- Operator arguments go into lists, optional for single-argument ops (like @len and @not). 
- No multi-dimensional lists: arrays our unpacked to the top level.

## Caveats

- Full RBAC.
- The strategic merge patch implementation does not handle lists.

<!-- The Kubernetes controller-runtime Project is a set of go libraries for building -->
<!-- Controllers. It is leveraged by [Kubebuilder](https://book.kubebuilder.io/) and -->
<!-- [Operator SDK](https://github.com/operator-framework/operator-sdk). Both are -->
<!-- a great place to start for new projects. See -->
<!-- [Kubebuilder's Quick Start](https://book.kubebuilder.io/quick-start.html) to -->
<!-- see how it can be used. -->

<!-- Documentation: -->

<!-- - [Package overview](https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg) -->
<!-- - [Basic controller using builder](https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/builder#example-Builder) -->
<!-- - [Creating a manager](https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/manager#example-New) -->
<!-- - [Creating a controller](https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/controller#example-New) -->
<!-- - [Examples](https://github.com/kubernetes-sigs/controller-runtime/blob/main/examples) -->
<!-- - [Designs](https://github.com/kubernetes-sigs/controller-runtime/blob/main/designs) -->

<!-- # Versioning, Maintenance, and Compatibility -->

<!-- The full documentation can be found at [VERSIONING.md](VERSIONING.md), but TL;DR: -->

<!-- Users: -->

<!-- - We follow [Semantic Versioning (semver)](https://semver.org) -->
<!-- - Use releases with your dependency management to ensure that you get compatible code -->
<!-- - The main branch contains all the latest code, some of which may break compatibility (so "normal" `go get` is not recommended) -->

<!-- Contributors: -->

<!-- - All code PR must be labeled with :bug: (patch fixes), :sparkles: (backwards-compatible features), or :warning: (breaking changes) -->
<!-- - Breaking changes will find their way into the next major release, other changes will go into an semi-immediate patch or minor release -->
<!-- - For a quick PR template suggesting the right information, use one of these PR templates: -->
<!--   * [Breaking Changes/Features](/.github/PULL_REQUEST_TEMPLATE/breaking_change.md) -->
<!--   * [Backwards-Compatible Features](/.github/PULL_REQUEST_TEMPLATE/compat_feature.md) -->
<!--   * [Bug fixes](/.github/PULL_REQUEST_TEMPLATE/bug_fix.md) -->
<!--   * [Documentation Changes](/.github/PULL_REQUEST_TEMPLATE/docs.md) -->
<!--   * [Test/Build/Other Changes](/.github/PULL_REQUEST_TEMPLATE/other.md) -->

<!-- ## FAQ -->

<!-- See [FAQ.md](FAQ.md) -->

<!-- ## Community, discussion, contribution, and support -->

<!-- Learn how to engage with the Kubernetes community on the [community page](http://kubernetes.io/community/). -->

<!-- controller-runtime is a subproject of the [kubebuilder](https://github.com/kubernetes-sigs/kubebuilder) project -->
<!-- in sig apimachinery. -->

<!-- You can reach the maintainers of this project at: -->

<!-- - Slack channel: [#controller-runtime](https://kubernetes.slack.com/archives/C02MRBMN00Z) -->
<!-- - Google Group: [kubebuilder@googlegroups.com](https://groups.google.com/forum/#!forum/kubebuilder) -->

<!-- ## Contributing -->
<!-- Contributions are greatly appreciated. The maintainers actively manage the issues list, and try to highlight issues suitable for newcomers. -->
<!-- The project follows the typical GitHub pull request model. See [CONTRIBUTING.md](CONTRIBUTING.md) for more details. -->
<!-- Before starting any work, please either comment on an existing issue, or file a new one. -->

<!-- ## Code of conduct -->

<!-- Participation in the Kubernetes community is governed by the [Kubernetes Code of Conduct](code-of-conduct.md). -->
