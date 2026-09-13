package profiler

import (
	"context"
	"testing"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestPollingCoalescesAndRejectsRepliesAfterProcessRoundTrip(t *testing.T) {
	client, err := flink.NewClient("http://flink.test")
	if err != nil {
		t.Fatal(err)
	}
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	model := New(client, parent)
	process := flink.JobManagerProcess()
	first := model.Open(process, 0)
	if first == nil || model.Poll() != nil || model.Refresh() != nil {
		t.Fatal("poll and refresh did not coalesce the current history request")
	}
	peer := model.Retarget(flink.TaskManagerProcess("tm-1"))
	current := model.Retarget(process)
	if peer == nil || current == nil {
		t.Fatal("process changes did not start immediate requests")
	}
	currentReply := current().(listMsg)
	currentReply.err = nil
	currentReply.entries = []flink.Profiling{{OutputFile: "current.html"}}
	model.Apply(currentReply, true)
	oldReply := first().(listMsg)
	oldReply.err = nil
	oldReply.entries = []flink.Profiling{{OutputFile: "old.html"}}
	model.Apply(oldReply, true)
	model.Apply(peer().(Message), true)
	if entries := model.State().Entries; len(entries) != 1 || entries[0].OutputFile != "current.html" || model.Error() != nil {
		t.Fatalf("late reply replaced current history: %#v", model.State())
	}
	retry := model.Poll()
	if retry == nil {
		t.Fatal("successful reply did not release polling")
	}
	model.Apply(retry().(Message), true)
	if model.Error() == nil || model.Poll() == nil {
		t.Fatal("failed reply did not release polling")
	}
	model.Reset()
	if model.Open(process, 0) == nil {
		t.Fatal("reset did not release polling")
	}
	model.Apply(startMsg{generation: oldReply.generation, process: process, entry: flink.Profiling{Message: "old capture"}}, true)
	if model.State().Message != "" {
		t.Fatal("reset reused a generation and accepted an old capture reply")
	}
}
