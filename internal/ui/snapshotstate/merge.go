// Package snapshotstate owns the UI's last-good-data policy for partially
// successful Flink snapshot refreshes.
package snapshotstate

import (
	"slices"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

// Merge retains prior subresources explicitly reported as unavailable in the
// next snapshot. A job identity change always starts with a clean snapshot.
func Merge(previous, next flink.Snapshot) flink.Snapshot {
	if previous.JobID == "" || previous.JobID != next.JobID || len(next.Issues) == 0 {
		return next
	}
	previousNodes := make(map[string]flink.Node, len(previous.Nodes))
	for _, node := range previous.Nodes {
		previousNodes[node.ID] = node
	}
	failedMetrics := make(map[string]bool)
	for _, issue := range next.Issues {
		switch issue.Kind {
		case flink.SnapshotIssueMetrics:
			failedMetrics[issue.VertexID] = true
		case flink.SnapshotIssueCheckpoints:
			next.Checkpoints = previous.Checkpoints
		case flink.SnapshotIssueExceptions:
			next.Exceptions = previous.Exceptions
		}
	}
	if len(failedMetrics) > 0 {
		// The input snapshots may still be held by another view or message.
		// Clone the node slice before writing, then merge each vertex once even
		// when an outage reports failures for every vertex in a large job.
		next.Nodes = slices.Clone(next.Nodes)
		for index := range next.Nodes {
			id := next.Nodes[index].ID
			if previousNode, ok := previousNodes[id]; ok && failedMetrics[id] {
				next.Nodes[index].Metrics = previousNode.Metrics
			}
		}
	}
	return next
}
