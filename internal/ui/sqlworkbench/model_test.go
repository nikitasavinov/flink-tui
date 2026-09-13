package sqlworkbench

import (
	"context"
	"errors"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestEditorAndResultIntents(t *testing.T) {
	model := testModel(t)
	before := model.text
	if model.CapturesKeys() {
		t.Fatal("workbench opened in insert mode")
	}
	model.HandleKey(tea.KeyPressMsg(tea.Key{Code: 'q', Text: "q"}), 20)
	if model.text != before {
		t.Fatalf("normal-mode q changed SQL to %q", model.text)
	}
	model.HandleKey(tea.KeyPressMsg(tea.Key{Code: 'i', Text: "i"}), 20)
	model.HandleKey(tea.KeyPressMsg(tea.Key{Code: 'q', Text: "q"}), 20)
	if !model.CapturesKeys() || model.text != before+"q" {
		t.Fatalf("insert-mode q produced editing=%t SQL=%q", model.CapturesKeys(), model.text)
	}
	model.HandleKey(tea.KeyPressMsg(tea.Key{Code: tea.KeyEscape}), 20)

	model.focus = FocusResults
	if result := model.HandleKey(tea.KeyPressMsg(tea.Key{Code: ':'}), 20); result.Intent != IntentPalette {
		t.Fatalf("results : intent = %d", result.Intent)
	}
	if result := model.HandleKey(tea.KeyPressMsg(tea.Key{Code: 'o', Mod: tea.ModCtrl}), 20); result.Intent != IntentOverview {
		t.Fatalf("results ctrl+o intent = %d", result.Intent)
	}
}

func TestExecutionContinuesAfterLazySessionOpen(t *testing.T) {
	model := testModel(t)
	model.session = ""
	model.sessionBusy = true
	model.sessionGeneration = 7

	if command := model.executeStatement(); command != nil {
		t.Fatalf("execution while session opens returned command %v", command)
	}
	if model.pendingStatement == "" || !model.busy || model.resultType != "CONNECTING" {
		t.Fatalf("statement not queued: pending=%q busy=%t status=%q", model.pendingStatement, model.busy, model.resultType)
	}
	execute := model.Apply(sessionMsg{handle: "opened", generation: 7})
	if execute == nil || model.session != "opened" || model.pendingStatement != "" || model.resultType != "SUBMITTING" {
		t.Fatalf("session reply did not execute: command=%v session=%q pending=%q status=%q",
			execute, model.session, model.pendingStatement, model.resultType)
	}
}

func TestResultsPollAndAccumulateChangelogRows(t *testing.T) {
	model := testModel(t)
	model.operation = "operation"
	model.busy = true
	model.resultType = "RUNNING"

	command := model.Apply(resultMsg{
		session: model.session, operation: model.operation,
		result: flink.SQLResult{
			ResultType: "PAYLOAD", ResultKind: "SUCCESS_WITH_CONTENT", JobID: "job-id",
			Columns: []flink.SQLColumn{{Name: "answer", Type: "INTEGER"}},
			Rows:    []flink.SQLRow{{Kind: "INSERT", Fields: []string{"1"}}},
			NextURI: "/v1/sessions/session/operations/operation/result/1",
		},
	})
	if command == nil || model.busy || model.jobID != "job-id" || len(model.rows) != 1 || model.nextURI == "" {
		t.Fatalf("payload produced command=%v status=%q rows=%#v next=%q", command, model.resultType, model.rows, model.nextURI)
	}

	command = model.Apply(resultMsg{
		session: model.session, operation: model.operation,
		result: flink.SQLResult{ResultType: "EOS"},
	})
	if command != nil || model.operationActive() || model.resultType != "EOS" || len(model.rows) != 1 {
		t.Fatalf("EOS produced command=%v status=%q active=%t rows=%d", command, model.resultType, model.operationActive(), len(model.rows))
	}
}

func TestNotReadyWithoutNextURIKeepsPollingInitialPage(t *testing.T) {
	model := testModel(t)
	model.operation = "operation"
	model.resultType = "RUNNING"
	command := model.Apply(resultMsg{
		session: model.session, operation: model.operation,
		result: flink.SQLResult{ResultType: "NOT_READY"},
	})
	if command == nil || !strings.HasSuffix(model.nextURI, "/result/0") {
		t.Fatalf("NOT_READY produced command=%v next=%q", command, model.nextURI)
	}
}

func TestPollingIsIndependentOfParentScreen(t *testing.T) {
	model := testModel(t)
	model.operation = "operation"
	model.resultType = "RUNNING"
	next := "/v1/sessions/session/operations/operation/result/1"
	if command := model.Apply(pollMsg{session: model.session, operation: model.operation, nextURI: next}); command == nil {
		t.Fatal("active result poll was discarded")
	}
}

func TestLateResultCannotRestartCanceledOperation(t *testing.T) {
	model := testModel(t)
	model.operation = "operation"
	model.resultType = "RUNNING"
	if command := model.cancelOperation(); command == nil {
		t.Fatal("active operation did not start cancellation")
	}
	if command := model.cancelOperation(); command != nil {
		t.Fatal("duplicate cancellation request was started")
	}
	model.Apply(cancelMsg{session: model.session, operation: model.operation, status: "CANCELED"})
	command := model.Apply(resultMsg{
		session: model.session, operation: model.operation,
		result: flink.SQLResult{ResultType: "PAYLOAD", NextURI: "/next", Rows: []flink.SQLRow{{Fields: []string{"late"}}}},
	})
	if command != nil || model.operationActive() || model.resultType != "CANCELED" || len(model.rows) != 0 || model.busy {
		t.Fatalf("late result revived canceled operation: %#v", model.State())
	}
}

func TestResultFailureLeavesRemoteOperationCancelable(t *testing.T) {
	model := testModel(t)
	model.operation = "operation"
	model.resultType = "RUNNING"
	model.Apply(resultMsg{session: model.session, operation: model.operation, err: errors.New("request timed out")})
	if !model.operationActive() {
		t.Fatal("failed result fetch marked the remote operation as terminated")
	}
	if command := model.executeStatement(); command != nil {
		t.Fatal("new statement abandoned the previous remote operation")
	}
	if command := model.cancelOperation(); command == nil {
		t.Fatal("failed result fetch made operation cancellation unavailable")
	}
}

func TestResultDuringCancellationRetainsBusyState(t *testing.T) {
	model := testModel(t)
	model.operation = "operation"
	model.resultType = "RUNNING"
	model.cancelOperation()
	model.Apply(resultMsg{session: model.session, operation: model.operation, result: flink.SQLResult{ResultType: "EOS"}})
	if !model.busy {
		t.Fatal("result reply unlocked a still-pending cancellation")
	}
	if command := model.executeStatement(); command != nil {
		t.Fatal("new statement submitted before cancellation completed")
	}
	model.Apply(cancelMsg{session: model.session, operation: model.operation, status: "CANCELED"})
	if model.busy || model.canceling {
		t.Fatal("cancellation reply left the editor busy")
	}
}

func TestStateIsDetachedFromOwnedRows(t *testing.T) {
	model := testModel(t)
	model.rows = []flink.SQLRow{{Kind: "INSERT", Fields: []string{"one"}}}
	state := model.State()
	state.Rows[0].Fields[0] = "changed"
	if model.rows[0].Fields[0] != "one" {
		t.Fatal("State exposed mutable workbench rows")
	}
}

func TestRenderStaysInsideRectangle(t *testing.T) {
	model := testModel(t)
	model.columns = []flink.SQLColumn{{Name: "answer", Type: "INTEGER"}}
	model.rows = []flink.SQLRow{{Kind: "INSERT", Fields: []string{"1"}}}
	model.resultType = "EOS"
	for _, width := range []int{60, 80, 140} {
		rendered := model.Render(width, 20)
		if !strings.Contains(rendered, "SQL WORKBENCH") || !strings.Contains(rendered, "+I") {
			t.Fatalf("width %d missing content", width)
		}
		for index, line := range strings.Split(rendered, "\n") {
			if got := lipgloss.Width(line); got > width {
				t.Fatalf("width %d line %d occupies %d cells", width, index, got)
			}
		}
	}
}

func TestStaleSessionReplyIsIgnored(t *testing.T) {
	model := testModel(t)
	model.sessionGeneration = 2
	model.sessionBusy = true
	if command := model.Apply(sessionMsg{handle: "stale", generation: 1}); command != nil || model.session == "stale" || !model.sessionBusy {
		t.Fatalf("stale session applied: command=%v session=%q busy=%t", command, model.session, model.sessionBusy)
	}
}

func TestShutdownContextDoesNotInheritParentCancellation(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	cancelParent()
	model := New(nil, parent, nil)
	requestContext, cancel := model.withTimeout(requestTimeout)
	defer cancel()
	if !errors.Is(requestContext.Err(), context.Canceled) {
		t.Fatalf("request context error = %v", requestContext.Err())
	}

	cleanupContext, cancelCleanup := shutdownContext()
	defer cancelCleanup()
	if err := cleanupContext.Err(); err != nil {
		t.Fatalf("shutdown context inherited cancellation: %v", err)
	}
}

func testModel(t *testing.T) Model {
	t.Helper()
	client, err := flink.NewSQLGatewayClient("http://localhost:8083")
	if err != nil {
		t.Fatal(err)
	}
	model := New(client, context.Background(), func(err error) string { return err.Error() })
	model.session = "session"
	return model
}
