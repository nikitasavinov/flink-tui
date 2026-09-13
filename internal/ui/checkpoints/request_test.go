package checkpoints

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestCheckpointRequestsCoalesceAndRejectRevisitedTargetReplies(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client, err := flink.NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	model := New(client, nil)
	model.Sync(Context{JobID: "job"})
	model.checkpointDetailID = 42
	first := model.RefreshOperators()
	if first == nil || model.RefreshOperators() != nil {
		t.Fatal("repeated checkpoint refreshes did not coalesce")
	}
	model.checkpointDetailID = 41
	if model.RefreshOperators() == nil {
		t.Fatal("in-flight checkpoint delayed a peer comparison")
	}
	model.checkpointDetailID = 42
	current := model.RefreshOperators()
	if current == nil {
		t.Fatal("revisiting a checkpoint did not start its current request")
	}
	model.Apply(first().(Message))
	if !model.State().DetailBusy || model.State().DetailErr != nil || model.RefreshOperators() != nil {
		t.Fatal("obsolete reply changed or released the revisited checkpoint's request")
	}
	model.Apply(current().(Message))
	if model.State().DetailErr == nil || model.RefreshOperators() == nil {
		t.Fatal("current failure did not surface and allow retry")
	}
}

func TestCheckpointSubtaskRequestsKeepTargetIdentity(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client, err := flink.NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	model := New(client, nil)
	model.Sync(Context{JobID: "job"})
	model.checkpointDetailID = 42
	model.checkpointSubtaskVertex = "risk"
	first := model.RefreshSubtasks()
	if first == nil || model.RefreshSubtasks() != nil {
		t.Fatal("repeated subtask refreshes did not coalesce")
	}
	if model.RefreshOperators() == nil {
		t.Fatal("subtask request blocked independent operator details")
	}
	model.checkpointSubtaskVertex = "source"
	if model.RefreshSubtasks() == nil {
		t.Fatal("in-flight subtask request delayed a new vertex")
	}
	model.checkpointSubtaskVertex = "risk"
	current := model.RefreshSubtasks()
	if current == nil {
		t.Fatal("revisited vertex did not start a current request")
	}
	model.Apply(first().(Message))
	if !model.State().SubtasksBusy || model.State().SubtasksErr != nil || model.RefreshSubtasks() != nil {
		t.Fatal("obsolete reply changed the revisited vertex's request")
	}
	model.Apply(current().(Message))
	if model.State().SubtasksErr == nil || model.RefreshSubtasks() == nil {
		t.Fatal("current failure did not surface and allow retry")
	}
}
