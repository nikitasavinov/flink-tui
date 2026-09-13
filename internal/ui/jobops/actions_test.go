package jobops

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/charmbracelet/x/ansi"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestPerformJobActionRoutesEveryActionToFlinkClient(t *testing.T) {
	tests := []struct {
		name           string
		kind           actionKind
		wantCall       string
		wantCheckpoint string
		wantDrain      bool
		wantTrigger    string
	}{
		{name: "configured checkpoint", kind: actionConfiguredCheckpoint, wantCall: "checkpoint", wantCheckpoint: "CONFIGURED", wantTrigger: "trigger"},
		{name: "full checkpoint", kind: actionFullCheckpoint, wantCall: "checkpoint", wantCheckpoint: "FULL", wantTrigger: "trigger"},
		{name: "savepoint", kind: actionSavepoint, wantCall: "savepoint", wantTrigger: "trigger"},
		{name: "stop with savepoint", kind: actionStopSavepoint, wantCall: "stop", wantTrigger: "trigger"},
		{name: "stop and drain", kind: actionStopDrain, wantCall: "stop", wantDrain: true, wantTrigger: "trigger"},
		{name: "cancel", kind: actionCancel, wantCall: "cancel"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &recordingJobActionClient{}
			command := performJobActionCommand(client, "job", 17, test.kind, testActionContext)
			message, ok := command().(actionMsg)
			if !ok {
				t.Fatalf("action command returned %T", message)
			}
			if client.call != test.wantCall || client.jobID != "job" || client.checkpointType != test.wantCheckpoint || client.drain != test.wantDrain {
				t.Fatalf("client call = %#v", client)
			}
			if message.err != nil || message.triggerID != test.wantTrigger || message.kind != test.kind || message.jobID != "job" || message.generation != 17 {
				t.Fatalf("action message = %#v", message)
			}
		})
	}
}

func TestPerformJobActionRejectsUnknownKindWithoutClientCall(t *testing.T) {
	client := &recordingJobActionClient{}
	message := performJobActionCommand(client, "job", 0, actionKind(255), testActionContext)().(actionMsg)
	if message.err == nil || client.call != "" {
		t.Fatalf("unknown action = message:%#v client:%#v", message, client)
	}
}

func TestActionConfirmationOwnsTheWholeScreenAndQCancels(t *testing.T) {
	model := Model{
		view:    ViewActions,
		context: Context{JobID: "0123456789abcdef0123456789abcdef", JobState: "RUNNING", JobType: "STREAMING", BodyHeight: 20},
	}
	model.actionSelection.Set(5, len(model.actionRows()))
	model.actionConfirm = true
	rendered := ansi.Strip(model.Render(100, 20))
	for _, expected := range []string{"DESTRUCTIVE JOB ACTION", "INPUT IS LOCKED", "Cancel job immediately", model.context.JobID, "[ y ]"} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("confirmation missing %q:\n%s", expected, rendered)
		}
	}
	if strings.Contains(rendered, "Trigger configured checkpoint") {
		t.Fatalf("confirmation left the action list visible:\n%s", rendered)
	}
	model.HandleKey("q")
	if model.actionConfirm {
		t.Fatal("q did not cancel action confirmation")
	}
}

func TestActionWheelPreservesConfirmedAndInFlightAction(t *testing.T) {
	for _, state := range []string{"confirming", "sending"} {
		t.Run(state, func(t *testing.T) {
			model := Model{
				view:          ViewActions,
				context:       Context{JobID: "job", JobState: "RUNNING", JobType: "STREAMING"},
				actionConfirm: state == "confirming",
				actionBusy:    state == "sending",
			}
			model.actionSelection.Set(2, len(model.actionRows()))
			model.HandleWheel(3)
			if got := model.actionSelection.Index(); got != 2 {
				t.Fatalf("wheel changed %s action from savepoint to index %d", state, got)
			}
		})
	}
}

func TestOperationPollWaitsForReplyAndRetriesFailures(t *testing.T) {
	model := Model{
		context:         Context{JobID: "job", Generation: 1},
		actionTriggerID: "trigger", actionStatus: "IN_PROGRESS",
	}
	if command := model.Poll(); command == nil {
		t.Fatal("first poll was suppressed")
	}
	if command := model.Poll(); command != nil {
		t.Fatal("duplicate in-flight poll was started")
	}
	model.Apply(actionStatusMsg{jobID: "other-job", generation: 1, triggerID: "trigger"})
	if command := model.Poll(); command != nil {
		t.Fatal("stale reply released the current poll")
	}
	model.Apply(actionStatusMsg{jobID: "job", generation: 1, triggerID: "trigger", err: errors.New("timed out")})
	if command := model.Poll(); command == nil {
		t.Fatal("failed poll prevented a retry")
	}
	model.Apply(actionStatusMsg{jobID: "job", generation: 1, triggerID: "trigger", operation: flink.AsyncOperation{Status: "COMPLETED"}})
	if command := model.Poll(); command != nil {
		t.Fatal("completed operation kept polling")
	}
}

type recordingJobActionClient struct {
	call           string
	jobID          string
	checkpointType string
	drain          bool
}

func (client *recordingJobActionClient) TriggerCheckpoint(_ context.Context, jobID, checkpointType string) (string, error) {
	client.call, client.jobID, client.checkpointType = "checkpoint", jobID, checkpointType
	return "trigger", nil
}

func (client *recordingJobActionClient) TriggerSavepoint(_ context.Context, jobID, _ string) (string, error) {
	client.call, client.jobID = "savepoint", jobID
	return "trigger", nil
}

func (client *recordingJobActionClient) StopWithSavepoint(_ context.Context, jobID string, drain bool, _ string) (string, error) {
	client.call, client.jobID, client.drain = "stop", jobID, drain
	return "trigger", nil
}

func (client *recordingJobActionClient) CancelJob(_ context.Context, jobID string) error {
	client.call, client.jobID = "cancel", jobID
	return nil
}

func testActionContext() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}
