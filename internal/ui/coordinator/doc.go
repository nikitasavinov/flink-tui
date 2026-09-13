// Package coordinator coordinates the Bubble Tea event loop, screen registry, and
// cross-feature lifecycle for Flink operations.
//
// Self-contained capabilities live in child packages. Job selection and
// exploration, graph interaction, checkpoints, exceptions, custom metrics,
// accumulators, infrastructure, process diagnostics, profiling, flame graphs,
// job operations, SQL, and shell chrome privately own their state. Generic
// terminal primitives belong in internal/ui/shared rather than here.
package coordinator
