package processdiag

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	shellmodule "github.com/nikitasavinov/flink-tui/internal/ui/shell"
)

func TestCurrentLogOpensAtTailAndRemainsSearchable(t *testing.T) {
	model := New(nil, nil)
	process := flink.TaskManagerProcess("tm-1")
	if command := model.OpenCurrentLog(process, 13, 10); command == nil {
		t.Fatal("current log returned no fetch command")
	}
	state := model.State()
	if state.View != ViewDocument || state.DocumentSource != SourceCurrentLog || !state.DocumentTailOnLoad {
		t.Fatalf("current log initial state = %#v", state)
	}
	lines := make([]string, 30)
	for index := range lines {
		lines[index] = fmt.Sprintf("line %02d", index+1)
	}
	model.Apply(documentMsg{
		title: state.DocumentTitle, generation: state.Generation, content: strings.Join(lines, "\n"),
	})
	state = model.State()
	wantOffset := len(lines) - model.documentRowsAvailable()
	if state.DocumentLoading || state.DocumentTailOnLoad || state.DocumentOffsetY != wantOffset {
		t.Fatalf("loaded current log = offset:%d tail:%t loading:%t, want offset %d",
			state.DocumentOffsetY, state.DocumentTailOnLoad, state.DocumentLoading, wantOffset)
	}
	model.HandleKey("/", 10)
	if !model.State().DocumentSearch {
		t.Fatal("/ did not open current-log search")
	}
}

func TestProcessRetargetPreservesSearchTermsAndResetsPosition(t *testing.T) {
	first := flink.TaskManagerProcess("tm-1")
	second := flink.TaskManagerProcess("tm-2")
	model := New(nil, nil)
	model.OpenLog(first, flink.LogFile{Name: "taskmanager.log"}, 7)
	model.document.filter.Restore(shellmodule.QueryState{Value: "checkpoint timeout", Open: model.document.filter.Active()})
	model.document.filter.Restore(shellmodule.QueryState{Value: model.document.filter.Value(), Open: true})
	model.document.offsetY = 41
	model.document.offsetX = 12

	command, ok := model.RetargetDocument(second)
	state := model.State()
	if !ok || command == nil || state.Process != second || state.DocumentProcess != second ||
		state.DocumentFilename != "taskmanager.log" || state.DocumentQuery != "checkpoint timeout" ||
		state.DocumentSearch || state.DocumentOffsetY != 0 || state.DocumentOffsetX != 0 {
		t.Fatalf("retargeted document = ok:%t command nil:%t state:%#v", ok, command == nil, state)
	}

	model.OpenThreadDump(first, 9, Focus{})
	model.threadFilter.Restore(shellmodule.QueryState{Value: "BLOCKED", Open: model.threadFilter.Active()})
	model.threadFilter.Restore(shellmodule.QueryState{Value: model.threadFilter.Value(), Open: true})
	model.threadCursor = 14
	model.threadStackOffset = 8
	if command = model.RetargetThreadDump(second); command == nil {
		t.Fatal("retargeted thread dump returned no command")
	}
	state = model.State()
	if state.Process != second || state.ThreadQuery != "BLOCKED" || state.ThreadSearch ||
		state.ThreadCursor != 0 || state.ThreadStackOffset != 0 {
		t.Fatalf("retargeted thread dump = %#v", state)
	}
}

func TestStaticDocumentHasNoProcessPeer(t *testing.T) {
	model := New(nil, nil)
	model.OpenStatic("Accumulator", "value", 2)
	if command, ok := model.RetargetDocument(flink.JobManagerProcess()); ok || command != nil {
		t.Fatalf("static retarget = ok:%t command:%v", ok, command)
	}
}

func TestProcessRepliesReduceInsideOwningModule(t *testing.T) {
	model := New(nil, nil)
	process := flink.TaskManagerProcess("tm-1")
	model.OpenLogs(process, 13)
	generation := model.State().Generation
	model.Apply(processLogsMsg{
		process: process, generation: generation,
		logs: []flink.LogFile{{Name: "task.log"}},
	})
	state := model.State()
	if state.LogsBusy || state.LogsErr != nil || len(state.Logs) != 1 || state.Logs[0].Name != "task.log" {
		t.Fatalf("log reply state = %#v", state)
	}

	model.OpenLog(process, state.Logs[0], 17)
	generation = model.State().Generation
	model.Apply(documentMsg{
		title: model.State().DocumentTitle, generation: generation,
		content: "first\r\n\x1b[31msecond\x1b[0m\tvalue",
	})
	state = model.State()
	if state.DocumentLoading || state.DocumentErr != nil || len(state.DocumentLines) != 2 ||
		state.DocumentLines[1] != "second    value" {
		t.Fatalf("document reply state = %#v", state)
	}

	focus := NewFocus("Risk Map (2/", "subtask #1")
	model.OpenThreadDump(process, 3, focus)
	generation = model.State().Generation
	model.Apply(threadDumpMsg{
		process: process, generation: generation,
		threads: []flink.ThreadInfo{
			{Name: "main", Stack: "\"main\" WAITING"},
			{Name: "Legacy Source Thread - Risk Map (2/4)#1", Stack: "\"Legacy Source Thread - Risk Map (2/4)#1\" RUNNABLE"},
		},
	})
	state = model.State()
	thread, ok := model.SelectedThread()
	if state.ThreadBusy || state.ThreadErr != nil || !state.ThreadFocus.Matched || !ok ||
		thread.Name != "Legacy Source Thread - Risk Map (2/4)#1" {
		t.Fatalf("thread reply state = %#v selected=%#v ok=%t", state, thread, ok)
	}
}

func TestThreadDumpDefaultsToDiagnosticOrder(t *testing.T) {
	model := New(nil, nil)
	process := flink.TaskManagerProcess("tm-1")
	model.OpenThreadDump(process, 3, Focus{})
	generation := model.State().Generation
	model.Apply(threadDumpMsg{
		process: process, generation: generation,
		threads: []flink.ThreadInfo{
			{Name: "main", Stack: `"main" WAITING`},
			{Name: "generic runnable", Stack: `"generic runnable" RUNNABLE`},
			{Name: "lock owner", Stack: `"lock owner" BLOCKED`},
			{Name: "Risk Map (2/4)#1", Stack: `"Risk Map (2/4)#1" WAITING`},
		},
	})
	state := model.State()
	got := make([]string, 0, len(state.Threads))
	for _, thread := range state.Threads {
		got = append(got, thread.Name)
	}
	want := []string{"Risk Map (2/4)#1", "lock owner", "generic runnable", "main"}
	if !slices.Equal(got, want) {
		t.Fatalf("thread order = %#v, want %#v", got, want)
	}
	selected, ok := model.SelectedThread()
	if !ok || selected.Name != want[0] {
		t.Fatalf("default thread = %#v, ok=%t", selected, ok)
	}
}

func TestStaleProcessRepliesCannotOverwriteNewerView(t *testing.T) {
	model := New(nil, nil)
	first := flink.TaskManagerProcess("tm-old")
	second := flink.TaskManagerProcess("tm-new")
	model.OpenLogs(first, 1)
	oldGeneration := model.State().Generation
	model.OpenLogs(second, 1)
	model.Apply(processLogsMsg{
		process: first, generation: oldGeneration,
		logs: []flink.LogFile{{Name: "stale.log"}},
	})
	state := model.State()
	if state.Process != second || len(state.Logs) != 0 || !state.LogsBusy {
		t.Fatalf("stale reply changed state: %#v", state)
	}
}

func TestFailedProcessRefreshPreservesLastGoodRows(t *testing.T) {
	model := New(nil, nil)
	state := model.State()
	state.Process = flink.JobManagerProcess()
	state.Logs = []flink.LogFile{{Name: "jobmanager.log"}}
	model.RestoreState(state)
	model.Refresh()
	wantErr := errors.New("unavailable")
	model.Apply(processLogsMsg{
		process: state.Process, generation: model.State().Generation, err: wantErr,
	})
	state = model.State()
	if !errors.Is(state.LogsErr, wantErr) || state.LogsBusy || len(state.Logs) != 1 {
		t.Fatalf("failed refresh state = %#v", state)
	}
}
