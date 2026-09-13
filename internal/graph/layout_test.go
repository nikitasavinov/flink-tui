package graph

import (
	"testing"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestBuildRanksForkAndJoin(t *testing.T) {
	nodes := []flink.Node{
		{ID: "join", Name: "Join", Inputs: []flink.Input{{ID: "a"}, {ID: "b"}}},
		{ID: "source", Name: "Source"},
		{ID: "a", Name: "A", Inputs: []flink.Input{{ID: "source"}}},
		{ID: "b", Name: "B", Inputs: []flink.Input{{ID: "source"}}},
	}

	layout := Build(nodes, 24, 5)
	if len(layout.Layers) != 3 {
		t.Fatalf("layers = %d, want 3", len(layout.Layers))
	}
	if layout.Rects["source"].X >= layout.Rects["a"].X || layout.Rects["a"].X >= layout.Rects["join"].X {
		t.Fatalf("expected left-to-right ranks, got %#v", layout.Rects)
	}
	if len(layout.Children["source"]) != 2 {
		t.Fatalf("source children = %v, want two", layout.Children["source"])
	}
}

func TestBuildWithSpacingSupportsTopologyDensity(t *testing.T) {
	nodes := []flink.Node{
		{ID: "source", Name: "Source"},
		{ID: "sink", Name: "Sink", Inputs: []flink.Input{{ID: "source"}}},
	}

	layout := BuildWithSpacing(nodes, 5, 1, 4, 0)
	if got := layout.Rects["source"]; got.W != 5 || got.H != 1 {
		t.Fatalf("source rect = %#v, want 5x1", got)
	}
	if gap := layout.Rects["sink"].X - (layout.Rects["source"].X + 5); gap != 4 {
		t.Fatalf("horizontal gap = %d, want 4", gap)
	}
	if layout.Height != 3 {
		t.Fatalf("topology height = %d, want 3", layout.Height)
	}
}
