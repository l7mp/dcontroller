// Package dbsp implements Database Stream Processing (DBSP) operators for incremental computation
// on Z-sets (multisets with integer multiplicities). See detailed documentation in
// https://mihaibudiu.github.io/work/dbsp-spec.pdf.
//
// DBSP provides the theoretical foundation for Δ-controller's incremental view maintenance.
// It represents data as Z-sets where each document has an associated multiplicity,
// enabling efficient incremental processing of insertions, updates, and deletions.
//
// Key components:
//   - DocumentZSet: Core Z-set implementation for document collections.
//   - Operator: Interface for DBSP operators (linear, bilinear, nonlinear ops).
//   - Executor: Orchestrates operator chains and manages incremental computation.
//   - ChainGraph: Represents computation graphs with optimization support.
//   - RewriteEngine: Performs operator fusion and optimization.
//
// Operator types:
//   - Linear: Selection, projection, aggregation (preserve zero, commute with addition).
//   - Bilinear: Join operations (multiplication-like semantics).
//   - Nonlinear: Complex transformations that don't preserve linearity.
//
// The DBSP implementation supports:
//   - Incremental view maintenance with O(|changes|) complexity.
//   - Operator fusion for performance optimization.
//   - Mathematical correctness based on DBSP theory.
//   - Support for complex relational operations.
//
// Example usage:
//
//	zset := dbsp.NewDocumentZSet()
//	zset.AddDocument(doc, 1) // Insert with multiplicity 1
//	op := dbsp.NewProjection(projector)
//	result, err := op.Process(zset)
package dbsp
