package snapshotstate

import (
	"syscall"
	"testing"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestMergeRetainsUnavailableSubresources(t *testing.T) {
	previous := flink.Snapshot{
		JobID:       "job",
		Checkpoints: flink.CheckpointSummary{Total: 8, LatestID: 8},
		Exceptions:  flink.ExceptionSummary{Count: 1, Latest: "boom"},
		Nodes:       []flink.Node{{ID: "source", Metrics: flink.Metrics{RecordsOutPerSecond: 123}}},
	}
	next := flink.Snapshot{
		JobID: "job",
		Nodes: []flink.Node{{ID: "source"}},
		Issues: []flink.SnapshotIssue{
			{Kind: flink.SnapshotIssueMetrics, VertexID: "source", Err: syscall.ECONNRESET},
			{Kind: flink.SnapshotIssueCheckpoints, Err: syscall.ECONNRESET},
			{Kind: flink.SnapshotIssueExceptions, Err: syscall.ECONNRESET},
		},
	}
	merged := Merge(previous, next)
	if merged.Nodes[0].Metrics.RecordsOutPerSecond != 123 || merged.Checkpoints.LatestID != 8 || merged.Exceptions.Latest != "boom" {
		t.Fatalf("merged snapshot = %#v", merged)
	}
	if next.Nodes[0].Metrics.RecordsOutPerSecond != 0 {
		t.Fatal("merge modified the original refresh snapshot through its node slice")
	}
	merged.Nodes[0].Metrics.RecordsOutPerSecond = 456
	if previous.Nodes[0].Metrics.RecordsOutPerSecond != 123 || next.Nodes[0].Metrics.RecordsOutPerSecond != 0 {
		t.Fatal("merged metrics share mutable storage with an input snapshot")
	}
}
