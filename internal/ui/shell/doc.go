// Package shell owns application-wide terminal chrome: responsive navigation,
// the command palette, and connection-status presentation. It deliberately
// knows nothing about Flink clients or screen implementations; the root UI
// coordinator supplies rows, commands, and already-derived context.
package shell
