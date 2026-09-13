package flamegraph

import (
	"context"
	"testing"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestPollingCoalescesAndRejectsRepliesAfterScopeRoundTrip(t *testing.T) {
	model := flameGraphTestModel(t)
	client, err := flink.NewClient("http://flink.test")
	if err != nil {
		t.Fatal(err)
	}
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	model.client, model.parent = client, parent

	first := model.Poll()
	if first == nil || model.Poll() != nil || model.Refresh() != nil {
		t.Fatal("poll and refresh did not coalesce the current sample request")
	}
	subtask := model.HandleKey("s").Command
	current := model.HandleKey("S").Command
	if subtask == nil || current == nil {
		t.Fatal("scope changes did not start immediate requests")
	}
	currentReply := current().(sampleMsg)
	currentReply.err = nil
	currentReply.graph = flink.FlameGraph{Root: flink.FlameGraphNode{Name: "current", Value: 200}}
	model.Apply(currentReply)
	oldReply := first().(sampleMsg)
	oldReply.err = nil
	oldReply.graph = flink.FlameGraph{Root: flink.FlameGraphNode{Name: "old", Value: 100}}
	model.Apply(oldReply)
	model.Apply(subtask().(Message))
	if model.State().LiveGraph.Root.Name != "current" || model.Error() != nil {
		t.Fatalf("late reply replaced current sample: %#v", model.State())
	}
	retry := model.Poll()
	if retry == nil {
		t.Fatal("successful reply did not release polling")
	}
	model.Apply(retry().(Message))
	if model.Error() == nil || model.Poll() == nil {
		t.Fatal("failed reply did not release polling")
	}
}
