# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Δ-controller (Delta-controller) is a NoCode/LowCode Kubernetes operator framework that simplifies writing Kubernetes operators by treating the Kubernetes API as a queryable JSON database. It uses a declarative pipeline model inspired by MongoDB aggregation pipelines and DBSP (Database Stream Processing) for incremental computation.

The framework allows creating Kubernetes operators using YAML manifests rather than Go code, abstracting away the boilerplate of controller-runtime while maintaining efficiency through incremental reconciliation.

## Build Commands

```bash
# Build the binary
make build

# Build binary only (without code generation)
make build-bin

# Run tests
make test

# Format code
make fmt

# Run linter
make vet

# Generate CRDs and Go code from kubebuilder markers
make generate

# Generate CRD manifests
make manifests

# Run locally (without building binary)
make run

# Build Docker image (runs tests first)
make docker-build

# Build with Podman
make podman-build
```

## Testing

```bash
# Run all tests with coverage
make test

# Run tests for a specific package
go test ./pkg/dbsp/...

# Run with verbose output
VERBOSE=1 make test

# View coverage report
go tool cover -html=cover.out
```

## Deployment

```bash
# Install CRDs only
make install

# Deploy the operator with kustomize
make deploy

# Uninstall CRDs
make uninstall

# Undeploy the operator
make undeploy

# Build and publish Helm chart
make chart
```

## Architecture

### Core Concepts

**Operator**: A cluster-scoped custom resource that acts as a container for one or more controllers. It defines the complete automation logic in a single YAML manifest.

**Controller**: A self-contained data processing pipeline within an operator. Each controller watches source resources (Kubernetes objects or views), processes them through a declarative pipeline, and writes output to a target.

**View**: An ephemeral, in-memory, schemaless Kubernetes-like object used as intermediate storage between controllers. Views are namespaced and follow the naming convention `<operator-name>.view.dcontroller.io/v1alpha1`. They are not persisted in etcd and exist only in the Δ-controller manager's memory.

**Pipeline**: The declarative transformation logic that processes source data into target data. Pipelines consist of:
- Optional `@join` operation to combine multiple sources
- Required `@aggregate` operation with stages like `@select`, `@project`, `@unwind`, `@gather`

**DBSP Engine**: The incremental computation engine based on Database Stream Processing theory. It processes only deltas (changes) rather than full state reconciliation, using Z-sets (multisets with integer multiplicities) to represent data.

### Package Structure

```
pkg/
├── api/                  # CRD definitions (Operator, View)
├── apiserver/            # Embedded Kubernetes API server for view introspection
├── composite/            # Composite object handling for multi-source joins
├── controller/           # Controller implementation (watches sources, runs pipelines)
├── dbsp/                 # DBSP incremental computation engine (Z-sets, operators, executor)
├── expression/           # Expression language parser and evaluator (JSONPath, operators)
├── kubernetes/           # Kubernetes client wrappers and controller-runtime integration
├── manager/              # Top-level manager that orchestrates operator lifecycle
├── object/               # Object model and unstructured object utilities
├── operator/             # Operator abstraction (creates/manages controllers)
├── pipeline/             # Pipeline processing (join, aggregate stages)
├── predicate/            # Event predicates for filtering watch events
├── reconciler/           # Reconciler that applies pipeline outputs to targets
└── util/                 # Common utilities
```

### Data Flow

1. **Source Events**: Controller watches Kubernetes resources or views for changes (add/update/delete)
2. **Join Stage**: If multiple sources, combine objects based on join expression
3. **Aggregation Pipeline**: Process data through sequential stages (@select, @project, @unwind, @gather)
4. **DBSP Processing**: Each stage is compiled into DBSP operators that process deltas incrementally
5. **Target Reconciliation**: Apply results to target (Patcher or Updater mode)

### Key Components

**OpController** (`pkg/kubernetes/controllers/`): The main Kubernetes controller that watches Operator CRDs and instantiates declarative operators.

**Operator** (`pkg/operator/`): Manages the lifecycle of controllers defined in an Operator CR. Creates controller instances and manages their view GVRs with the API server.

**Controller** (`pkg/controller/`): Implements a single declarative controller. Sets up watches on sources, compiles pipelines into DBSP chains, and reconciles targets.

**Pipeline** (`pkg/pipeline/`): Compiles declarative pipeline specs into DBSP operator chains. Handles join expressions and aggregation stages.

**DBSP Package** (`pkg/dbsp/`):
- `DocumentZSet`: Core Z-set implementation with document collections and multiplicities
- `Operator`: Interface for linear, bilinear, and nonlinear operators
- `Executor`: Orchestrates operator chains for incremental computation
- `ChainGraph`: Represents computation DAGs with optimization support
- `RewriteEngine`: Fuses operators for performance

**Expression Language** (`pkg/expression/`): Implements the declarative expression DSL with JSONPath support and operators like `@and`, `@or`, `@eq`, `@concat`, `@len`, etc.

**API Server** (`pkg/apiserver/`): Embedded Kubernetes API server that exposes views over standard Kubernetes client interface. Allows `kubectl` to inspect and modify view resources. Listens on port 8443 by default.

## Expression Language

The declarative pipeline uses a JSON/YAML-based expression language:

- **JSONPath**: `$.metadata.name` (current object), `$$.field` (nested iteration context)
- **Logical**: `@and`, `@or`, `@not`, `@cond`, `@definedOr`
- **Comparison**: `@eq`, `@ne`, `@gt`, `@gte`, `@lt`, `@lte`, `@in`
- **String**: `@concat`, `@lower`, `@upper`, `@split`, `@replace`, `@regex`
- **Arithmetic**: `@add`, `@sub`, `@mul`, `@div`, `@mod`
- **List**: `@len`, `@map`, `@filter`, `@contains`, `@first`, `@last`
- **Type**: `@string`, `@int`, `@float`, `@bool`, `@exists`

## Important Constraints

1. **Schemaless Objects**: All objects are unstructured (`map[string]any`). No compile-time type safety.
2. **View Naming**: Views must have valid `metadata.name` and `metadata.namespace`.
3. **RBAC**: The operator requires cluster-wide permissions to watch/modify any resource type.
4. **Strategic Merge Limitations**: The local patch engine doesn't fully handle list merging. Prefer `Updater` targets over `Patcher` when working with complex list fields.
5. **Ephemeral Views**: Views are not persisted. Manager restarts will lose all view state until sources are reconciled.

## Development Workflow

When modifying CRD types in `pkg/api/`:
```bash
make generate  # Generate deepcopy methods
make manifests # Generate CRD YAML files
make test      # Ensure tests pass
```

When adding new pipeline operators or expressions, update both:
- Implementation in `pkg/expression/` or `pkg/pipeline/`
- Tests in corresponding `*_test.go` files
- Documentation would be in `doc/reference-expression.md` (though not required for code changes)

## Testing Philosophy

- Use Ginkgo/Gomega for BDD-style tests
- Test files colocated with implementation (`*_test.go`)
- DBSP operators have comprehensive unit tests for linearity properties
- Integration tests use envtest for Kubernetes API simulation

## Common Patterns

**Creating a controller programmatically** (hybrid operators):
```go
import dcontroller "github.com/l7mp/dcontroller/pkg/controller"

ctrl, err := dcontroller.NewController(opName, ctrlName, sources,
    pipeline, target, client, logger)
```

**Accessing views via kubectl**:
```bash
kubectl -n dcontroller-system port-forward deployment/dcontroller-manager 8443:8443 &
export KUBECONFIG=deploy/dcontroller-config
kubectl get <operator-name>.<kind>.view.dcontroller.io
```

**Pipeline debugging**: Use the extension API server to inspect view contents at intermediate stages. Split complex pipelines into multiple chained controllers with intermediate views for observability.
