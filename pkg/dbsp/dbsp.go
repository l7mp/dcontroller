// Package dbsp (partially) implements Database Stream Processing (DBSP) operators for incremental
// computation on Z-sets (multisets with integer multiplicities). See detailed documentation in
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
//   - Linear: Selection, projection, etc (preserve zero, commute with addition).
//   - Bilinear: Join operations (multiplication-like semantics).
//   - Nonlinear: Complex transformations that don't preserve linearity.
//
// The DBSP implementation supports incremental view maintenance (IVM) with O(|changes|) complexity
// and operator fusion for performance optimization.
//
// Example usage:
//
//	zset := dbsp.NewDocumentZSet()
//	zset.AddDocument(doc, 1) // Insert with multiplicity 1
//	op := dbsp.NewProjection(projector)
//	result, err := op.Process(zset)
package dbsp

// SortGatherValues controls whether @gather sorts lists for deterministic output.  When true,
// values in each group are sorted before being passed to the aggregator.  This ensures consistent
// array ordering across runs, which can prevent unnecessary reconciliation churn in downstream
// systems that compare arrays element-by-element.
//
// Default is false for backward compatibility.
var SortGatherValues = true
