package exceptions

import (
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func exceptionIncidentTestModel(t *testing.T) Model {
	t.Helper()
	model := New()
	now := time.Now()
	model.snapshot = flink.Snapshot{JobID: "job"}
	model.snapshot.Nodes = []flink.Node{
		{ID: "risk", Name: "Risk Features", Parallelism: 2},
		{ID: "sink", Name: "Fraud Sink", Parallelism: 2},
	}
	model.snapshot.Checkpoints.History = []flink.Checkpoint{{
		ID: 42, Status: "COMPLETED", TriggeredAt: now.Add(-5 * time.Second),
	}}
	model.snapshot.Exceptions = flink.ExceptionSummary{
		Count:     2,
		Truncated: true,
		Entries: []flink.JobException{
			{
				Name:          "java.util.concurrent.TimeoutException",
				Stacktrace:    "java.util.concurrent.TimeoutException: heartbeat\n\tat JobMaster.run",
				At:            now,
				TaskName:      "Risk Features (2/2) - execution #0",
				Endpoint:      "tm-b:41399",
				TaskManagerID: "tm-b",
				FailureLabels: map[string]string{"region": "eu-west", "severity": "critical"},
				ConcurrentExceptions: []flink.JobException{{
					Name:          "java.io.IOException",
					Stacktrace:    "java.io.IOException: disk full\n\tat Writer.flush",
					At:            now.Add(-time.Millisecond),
					TaskName:      "Risk Features (1/2) - execution #0",
					Endpoint:      "tm-a:41399",
					TaskManagerID: "tm-a",
					FailureLabels: map[string]string{"disk": "full"},
				}},
			},
			{
				Name:          "java.util.concurrent.TimeoutException",
				Stacktrace:    "java.util.concurrent.TimeoutException: earlier",
				At:            now.Add(-time.Minute),
				TaskName:      "Risk Features (1/2) - execution #0",
				Endpoint:      "tm-a:41399",
				TaskManagerID: "tm-a",
			},
		},
	}
	model.exceptionLimit = flink.DefaultExceptionLimit
	model.context = Context{Snapshot: model.snapshot, BodyHeight: 28}
	model.syncExceptionSelection()
	return model
}

func TestExceptionIncidentViewGroupsContextAndCorrelatesCheckpoint(t *testing.T) {
	model := exceptionIncidentTestModel(t)
	rendered := model.renderExceptions(140, 28)
	for _, expected := range []string{
		"EXCEPTION INCIDENTS",
		"3 failures",
		"TimeoutException",
		"Risk Features (2/2)",
		"x2",
		"failure 1/2",
		"tm-b @ tm-b:41399",
		"region=eu-west",
		"severity=critical",
		"checkpoint #42 completed",
		"web.exception-history-size",
		"more available: L",
		"[d thread dump]",
	} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("incident view missing %q:\n%s", expected, rendered)
		}
	}

	model.moveExceptionVariant(1)
	selected, ok := model.selectedException()
	if !ok || selected.Name != "java.io.IOException" || selected.TaskManagerID != "tm-a" {
		t.Fatalf("concurrent failure selection = %#v, %t", selected, ok)
	}
	rendered = model.renderExceptions(140, 28)
	for _, expected := range []string{"failure 2/2", "disk=full", "tm-a @ tm-a:41399", "IOException"} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("concurrent failure detail missing %q:\n%s", expected, rendered)
		}
	}
}

func TestExceptionIncidentFilteringSortingAndIncrementalHistory(t *testing.T) {
	model := exceptionIncidentTestModel(t)
	state := model.State()
	state.Query = "disk full"
	model.RestoreState(state)
	if incidents := model.filteredExceptionIncidents(); len(incidents) != 1 || len(incidents[0].ConcurrentExceptions) != 1 {
		t.Fatalf("filtered incidents = %#v", incidents)
	}

	state = model.State()
	state.Query = ""
	model.RestoreState(state)
	model.exceptionSort = exceptionSortRecurrence
	incidents := model.filteredExceptionIncidents()
	if len(incidents) != 2 || model.exceptionRecurrenceCount(incidents[0]) != 2 {
		t.Fatalf("recurrence sort = %#v", incidents)
	}

	result := model.HandleKey("L")
	if result.Intent != IntentReloadSnapshot || model.exceptionLimit != 40 || !model.exceptionLoadingMore {
		t.Fatalf("load more = limit %d loading %t intent %d", model.exceptionLimit, model.exceptionLoadingMore, result.Intent)
	}
	model.RefreshFinished(20)
	model.SnapshotApplied(model.context)
	if !model.exceptionLoadingMore {
		t.Fatal("older snapshot cleared the outstanding larger history request")
	}
	model.RefreshFinished(model.Limit())
	if model.exceptionLoadingMore {
		t.Fatal("failed snapshot completion would leave load-more stuck")
	}

	model.HandleKey("/")
	_ = model.HandleKey("q")
	if model.State().Query != "q" {
		t.Fatalf("exception search swallowed q as global quit: state=%#v", model.State())
	}
}

func TestExceptionIncidentActionsJumpIntoDiagnosticsAndReturn(t *testing.T) {
	model := exceptionIncidentTestModel(t)

	result := model.HandleKey("enter")
	if result.Intent != IntentGraph || result.Selected != "risk" {
		t.Fatalf("graph jump = intent %d selected %q", result.Intent, result.Selected)
	}

	result = model.HandleKey("d")
	if result.Intent != IntentThreadDump || result.Process != flink.TaskManagerProcess("tm-b") || result.Focus.Prefix != "Risk Features (2/" {
		t.Fatalf("thread jump = intent %d process %#v focus %#v", result.Intent, result.Process, result.Focus)
	}

	result = model.HandleKey("l")
	if result.Intent != IntentLogs || result.Process.TaskManagerID != "tm-b" {
		t.Fatalf("log jump = intent %d process %#v", result.Intent, result.Process)
	}

	result = model.HandleKey("T")
	if result.Intent != IntentTaskManager || result.TaskManagerID != "tm-b" {
		t.Fatalf("TaskManager jump = intent %d target %q", result.Intent, result.TaskManagerID)
	}
}

func TestExceptionIncidentTaskManagerTargetResolvesAfterRefreshAndMouseActions(t *testing.T) {
	model := exceptionIncidentTestModel(t)
	result := model.HandleKey("T")
	if result.Intent != IntentTaskManager || result.TaskManagerID != "tm-b" {
		t.Fatalf("pending TaskManager target = intent %d target %q", result.Intent, result.TaskManagerID)
	}

	model = exceptionIncidentTestModel(t)
	dumpX := strings.Index(exceptionActionLine(), "[d thread dump]") + 2
	dumpY := headerHeight + exceptionBodyRowStart + model.exceptionRowsAvailable() + 5
	result = model.HandleClick(tea.Mouse{X: dumpX, Y: dumpY, Button: tea.MouseLeft})
	if result.Intent != IntentThreadDump {
		t.Fatalf("mouse thread action = intent %d", result.Intent)
	}
}
