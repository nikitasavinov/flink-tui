package coordinator

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestInitialSnapshotRemainsSingleFlightUntilReply(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client, err := flink.NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	model := NewModel(client, "job", time.Second)
	batch := model.Init()().(tea.BatchMsg)
	if command := model.fetchSnapshot(); command != nil {
		t.Fatal("refresh started a second request before Init completed")
	}
	message := batch[0]().(snapshotMsg)
	if message.err == nil {
		t.Fatal("fixture did not produce an initial snapshot error")
	}
	updated, _ := model.Update(message)
	model = updated.(Model)
	if model.loading || model.err == nil {
		t.Fatalf("initial reply was lost: loading=%t error=%v", model.loading, model.err)
	}
	if command := model.fetchSnapshot(); command == nil {
		t.Fatal("failed request prevented retry")
	}
}

func TestSnapshotRefreshesCoalesceAndJobSwitchStartsImmediately(t *testing.T) {
	model := interactionTestModel(t)
	if command := model.fetchSnapshot(); command == nil {
		t.Fatal("first refresh did not start")
	}
	if command := refreshGraph(&model); command != nil {
		t.Fatal("manual refresh overlapped an in-flight snapshot")
	}
	if command := refreshSnapshot(&model); command != nil {
		t.Fatal("automatic refresh overlapped an in-flight snapshot")
	}
	oldGeneration := model.generation
	if command := model.switchToJob("next-job"); command == nil {
		t.Fatal("old job's in-flight request delayed opening the next job")
	}
	model.applySnapshot(snapshotMsg{generation: oldGeneration, snapshot: flink.Snapshot{JobID: "old-job"}})
	if !model.requests.snapshotPending || model.snapshot.JobID != "" {
		t.Fatal("stale reply changed the new job's in-flight state")
	}
	model.applySnapshot(snapshotMsg{generation: model.generation, snapshot: flink.Snapshot{JobID: "next-job"}})
	if model.snapshot.JobID != "next-job" || model.fetchSnapshot() == nil {
		t.Fatal("completed snapshot did not allow the next refresh")
	}
	model.applySnapshot(snapshotMsg{generation: model.generation, err: errors.New("offline")})
	if model.fetchSnapshot() == nil {
		t.Fatal("failed snapshot did not allow the next refresh")
	}
}

func TestCheckpointReplyDoesNotStealNavigation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client, err := flink.NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	for _, destination := range []screenMode{modeJobs, modeGraph, modeSQL, modeTaskManagers} {
		model := NewModel(client, "", time.Second)
		model.snapshot = flink.Snapshot{
			JobID: "job", Checkpoints: flink.CheckpointSummary{History: []flink.Checkpoint{{ID: 42}}},
		}
		model.openCheckpoints()
		command := model.handleCheckpointKey("enter")
		if command == nil || model.mode != modeCheckpointOperators {
			t.Fatal("checkpoint detail request did not start")
		}
		model.mode = destination
		updated, _ := model.Update(command())
		result := updated.(Model)
		if result.mode != destination {
			t.Errorf("late checkpoint reply moved screen %v to %v", destination, result.mode)
		}
		if result.checkpoints.State().DetailErr == nil {
			t.Fatal("background checkpoint reply did not update cached result")
		}
	}
}

func TestLoadingMoreExceptionsFollowsAnOlderSnapshotWhilePaused(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client, err := flink.NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	for _, earlierFailed := range []bool{false, true} {
		model := interactionTestModel(t)
		model.client = client
		model.paused = true
		model.snapshot.Exceptions.Truncated = true
		model.configureExceptions()
		model.openExceptions()
		if model.fetchSnapshot() == nil {
			t.Fatal("initial snapshot did not start")
		}
		if model.handleExceptionKey("L") != nil || !model.exceptions.State().LoadingMore {
			t.Fatal("Load more did not wait for the older request")
		}
		message := snapshotMsg{generation: model.generation, snapshot: model.snapshot, exceptionLimit: flink.DefaultExceptionLimit}
		if earlierFailed {
			message.err = errors.New("older request failed")
		}
		command := model.applySnapshot(message)
		if command == nil || !model.requests.snapshotPending || !model.exceptions.State().LoadingMore {
			t.Fatalf("older reply lost the requested exception history: failed=%t pending=%t loading=%t", earlierFailed, model.requests.snapshotPending, model.exceptions.State().LoadingMore)
		}
		if model.fetchSnapshot() != nil {
			t.Fatal("follow-up allowed another overlapping snapshot")
		}
		followup := command().(snapshotMsg)
		if followup.exceptionLimit != 2*flink.DefaultExceptionLimit {
			t.Fatalf("follow-up limit = %d", followup.exceptionLimit)
		}
		if followup.err == nil {
			t.Fatal("fixture did not fail the requested snapshot")
		}
		if model.applySnapshot(followup) != nil || model.exceptions.State().LoadingMore || model.requests.snapshotPending {
			t.Fatal("completed expanded request did not clear loading state")
		}
	}
}
