package accumulators

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestAccumulatorPollingCoalescesAndRejectsRepliesAfterRevisitingVertex(t *testing.T) {
	var count atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `{"user-accumulators":[{"name":"counter","type":"LongCounter","value":"%d"}]}`, count.Add(1))
	}))
	defer server.Close()
	client, err := flink.NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	model := New(client, nil)
	context := Context{Snapshot: flink.Snapshot{JobID: "job"}, Selected: "a", Generation: 1}
	first := model.Open(context)
	if first == nil || model.Poll() != nil || model.Refresh() != nil {
		t.Fatal("accumulator refreshes were not coalesced")
	}
	oldReply := first().(reply)
	context.Selected = "b"
	if model.Open(context) == nil {
		t.Fatal("pending request blocked opening another vertex")
	}
	context.Selected = "a"
	latest := model.Open(context)
	if latest == nil {
		t.Fatal("pending request blocked revisiting the original vertex")
	}
	newReply := latest().(reply)
	if newReply.err != nil || len(newReply.values.Vertex) != 1 {
		t.Fatalf("fixture reply = %#v", newReply)
	}
	model.Apply(newReply)
	model.Apply(oldReply)
	if state := model.State(); state.Busy || state.Values.Vertex[0].Value != newReply.values.Vertex[0].Value {
		t.Fatalf("old request replaced revisited vertex: %#v", state)
	}
	command := model.Poll()
	if command == nil {
		t.Fatal("completed request blocked polling")
	}
	failed := command().(reply)
	failed.err = errors.New("offline")
	model.Apply(failed)
	if model.Poll() == nil {
		t.Fatal("failed request blocked retry")
	}
}
