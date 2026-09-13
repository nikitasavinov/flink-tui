package jobdetail

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestSubtaskPollingCoalescesAndRejectsRepliesAfterRevisitingVertex(t *testing.T) {
	var count atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `{"name":"snapshot-%d","subtasks":[]}`, count.Add(1))
	}))
	defer server.Close()
	client, err := flink.NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	model := New(client, nil)
	context := Context{Snapshot: flink.Snapshot{JobID: "job"}, Selected: "a", Generation: 1}
	first := model.OpenSubtasks(context)
	if first == nil || model.Poll() != nil || model.Refresh() != nil {
		t.Fatal("subtask refreshes were not coalesced")
	}
	oldReply := first().(diagnosticsMsg)
	context.Selected = "b"
	if model.OpenSubtasks(context) == nil {
		t.Fatal("pending request blocked opening another vertex")
	}
	context.Selected = "a"
	latest := model.OpenSubtasks(context)
	if latest == nil {
		t.Fatal("pending request blocked revisiting the original vertex")
	}
	newReply := latest().(diagnosticsMsg)
	if newReply.err != nil {
		t.Fatal(newReply.err)
	}
	model.Apply(newReply)
	model.Apply(oldReply)
	if state := model.State(); state.DiagnosticsBusy || state.Diagnostics.Name != newReply.diagnostics.Name {
		t.Fatalf("old request replaced revisited vertex: %#v", state)
	}
	command := model.Poll()
	if command == nil {
		t.Fatal("completed request blocked polling")
	}
	failed := command().(diagnosticsMsg)
	failed.err = errors.New("offline")
	model.Apply(failed)
	if model.Poll() == nil {
		t.Fatal("failed request blocked retry")
	}
}
