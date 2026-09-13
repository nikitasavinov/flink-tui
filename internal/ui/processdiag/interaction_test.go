package processdiag

import (
	"errors"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	shellmodule "github.com/nikitasavinov/flink-tui/internal/ui/shell"
)

func TestProcessLogKeyboardMouseAndDocumentOpening(t *testing.T) {
	tests := []struct {
		name        string
		key         string
		prepare     func(*Model)
		assert      func(*testing.T, Model)
		wantCommand bool
	}{
		{name: "back", key: "esc", assert: func(t *testing.T, m Model) {
			if m.mode != modeTaskManagerDetail {
				t.Fatalf("mode = %d, want TaskManager detail", m.mode)
			}
		}},
		{name: "up", key: "up", prepare: func(m *Model) { m.processLogSelection.Set(1, 2) }, assert: assertProcessLogCursor(0)},
		{name: "down", key: "down", assert: assertProcessLogCursor(1)},
		{name: "page up", key: "pgup", prepare: func(m *Model) { m.processLogSelection.Set(3, 4) }, assert: assertProcessLogCursor(0)},
		{name: "page down", key: "pgdown", assert: assertProcessLogCursor(3)},
		{name: "home", key: "home", prepare: func(m *Model) { m.processLogSelection.Set(3, 4) }, assert: assertProcessLogCursor(0)},
		{name: "end", key: "end", assert: assertProcessLogCursor(3)},
		{name: "log document", key: "enter", wantCommand: true, assert: func(t *testing.T, m Model) {
			if m.mode != modeDocument || m.document.source != documentLog || m.document.filename != "task.log" {
				t.Fatalf("log document = mode:%d document:%#v", m.mode, m.document)
			}
		}},
		{name: "stdout", key: "x", wantCommand: true, assert: func(t *testing.T, m Model) {
			if m.mode != modeDocument || m.document.source != documentStdout {
				t.Fatalf("stdout document = mode:%d document:%#v", m.mode, m.document)
			}
		}},
		{name: "threads", key: "d", wantCommand: true, assert: func(t *testing.T, m Model) {
			if m.mode != modeThreadDump || !m.threadBusy {
				t.Fatalf("thread dump = mode:%d busy:%t", m.mode, m.threadBusy)
			}
		}},
		{name: "profiler", key: "p", wantCommand: true, assert: func(t *testing.T, m Model) {
			if m.mode != modeProfiler || m.pendingIntent != IntentProfiler {
				t.Fatalf("profiler = mode:%d intent:%d", m.mode, m.pendingIntent)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			model := processInteractionTestModel(t)
			if test.prepare != nil {
				test.prepare(&model)
			}
			command := model.handleProcessLogKey(test.key)
			if (command != nil) != test.wantCommand {
				t.Fatalf("command presence = %t, want %t", command != nil, test.wantCommand)
			}
			test.assert(t, model)
		})
	}

	model := processInteractionTestModel(t)
	model.moveProcessLogSelection(0)
	model.handleProcessLogMouseClick(tea.Mouse{Y: headerHeight + processLogBodyRowStart + 2, Button: tea.MouseRight})
	model.handleProcessLogMouseClick(tea.Mouse{Y: 0, Button: tea.MouseLeft})
	if model.processLogSelection.Index() != 0 {
		t.Fatal("invalid log click changed selection")
	}
	command := model.handleProcessLogMouseClick(tea.Mouse{Y: headerHeight + processLogBodyRowStart + 2, Button: tea.MouseLeft})
	if command == nil || model.processLogSelection.Index() != 2 || model.mode != modeDocument {
		t.Fatalf("log click = cursor:%d mode:%d command:%v", model.processLogSelection.Index(), model.mode, command)
	}

	empty := processInteractionTestModel(t)
	empty.processLogs = nil
	empty.moveProcessLogSelection(1)
	if empty.processLogSelection.Index() != 0 || empty.openSelectedProcessLog() != nil {
		t.Fatal("empty process log list moved or opened")
	}
}

func TestJobManagerLogAndStdoutDocuments(t *testing.T) {
	model := processInteractionTestModel(t)
	if command := model.openLogDocument(flink.JobManagerProcess(), flink.LogFile{Name: "jobmanager.log", Size: 42}, modeJobManager); command == nil || model.mode != modeDocument ||
		model.document.process.Kind != flink.ProcessJobManager || model.document.filename != "jobmanager.log" {
		t.Fatalf("JobManager log document = mode:%d document:%#v command:%v", model.mode, model.document, command)
	}

	model = processInteractionTestModel(t)
	if command := model.openStdout(flink.JobManagerProcess(), modeJobManager); command == nil ||
		model.mode != modeDocument || model.document.title != "STDOUT  JobManager" || model.document.backMode != modeJobManager {
		t.Fatalf("stdout document = mode:%d document:%#v command:%v", model.mode, model.document, command)
	}
}

func TestProcessLogRenderingCoversEmptyErrorAndMetadata(t *testing.T) {
	model := processInteractionTestModel(t)
	first, second := model.renderProcessLogDetail(120)
	if !strings.Contains(first, "task.log") || !strings.Contains(second, "2026-") {
		t.Fatalf("log detail = %q / %q", first, second)
	}
	model.processLogSelection.Set(1, 2)
	_, second = model.renderProcessLogDetail(120)
	if !strings.Contains(second, "modified unknown") {
		t.Fatalf("zero timestamp log detail = %q", second)
	}
	model.processLogSelection.Set(99, 100)
	if first, _ := model.renderProcessLogDetail(80); !strings.Contains(first, "No log selected") {
		t.Fatalf("invalid log detail = %q", first)
	}

	model.processLogs = nil
	if rendered := model.renderProcessLogs(100, 20); !strings.Contains(rendered, "No log files returned") {
		t.Fatalf("empty log rendering:\n%s", rendered)
	}
	model.processErr = errors.New("logs unavailable")
	if rendered := model.renderProcessLogs(100, 20); !strings.Contains(rendered, "Could not load logs") {
		t.Fatalf("log error rendering:\n%s", rendered)
	}
}

func TestDocumentNavigationSearchAndRemoteRefresh(t *testing.T) {
	tests := []struct {
		name        string
		key         string
		prepare     func(*Model)
		assert      func(*testing.T, Model)
		wantCommand bool
	}{
		{name: "back", key: "esc", assert: func(t *testing.T, m Model) {
			if m.mode != modeProcessLogs {
				t.Fatalf("mode = %d, want process logs", m.mode)
			}
		}},
		{name: "up", key: "up", prepare: func(m *Model) { m.document.offsetY = 2 }, assert: assertDocumentOffset(1, 0)},
		{name: "down", key: "down", assert: assertDocumentOffset(1, 0)},
		{name: "page up", key: "pgup", prepare: func(m *Model) { m.document.offsetY = 4 }, assert: assertDocumentOffset(0, 0)},
		{name: "page down", key: "pgdown", assert: func(t *testing.T, m Model) {
			if m.document.offsetY <= 0 {
				t.Fatal("page down did not move")
			}
		}},
		{name: "home", key: "home", prepare: func(m *Model) { m.document.offsetY = 5 }, assert: assertDocumentOffset(0, 0)},
		{name: "end", key: "end", assert: func(t *testing.T, m Model) {
			if m.document.offsetY == 0 {
				t.Fatal("end did not move to the document tail")
			}
		}},
		{name: "left", key: "h", prepare: func(m *Model) { m.document.offsetX = 8 }, assert: assertDocumentOffset(0, 4)},
		{name: "right", key: "l", assert: assertDocumentOffset(0, 4)},
		{name: "column zero", key: "0", prepare: func(m *Model) { m.document.offsetX = 8 }, assert: assertDocumentOffset(0, 0)},
		{name: "search", key: "/", assert: func(t *testing.T, m Model) {
			if !m.document.filter.Active() {
				t.Fatal("document search did not open")
			}
		}},
		{name: "next match", key: "n", prepare: func(m *Model) {
			m.document.filter.Restore(shellmodule.QueryState{Value: "target", Open: m.document.filter.Active()})
		}, assert: func(t *testing.T, m Model) {
			if m.document.offsetY == 0 {
				t.Fatal("next match did not move")
			}
		}},
		{name: "previous match", key: "N", prepare: func(m *Model) {
			m.document.filter.Restore(shellmodule.QueryState{Value: "target", Open: m.document.filter.Active()})
			m.document.offsetY = 6
		}, assert: func(t *testing.T, m Model) {
			if m.document.offsetY >= 6 {
				t.Fatalf("previous match offset = %d", m.document.offsetY)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			model := documentInteractionTestModel(t)
			if test.prepare != nil {
				test.prepare(&model)
			}
			command := model.handleDocumentKey(test.key)
			if (command != nil) != test.wantCommand {
				t.Fatalf("command presence = %t, want %t", command != nil, test.wantCommand)
			}
			test.assert(t, model)
		})
	}

	model := documentInteractionTestModel(t)
	model.handleDocumentKey("/")
	model.document.offsetY = 0
	model.handleDocumentKey("t")
	model.handleDocumentKey("space")
	model.handleDocumentKey("x")
	if model.document.filter.Value() != "t x" {
		t.Fatalf("document search query = %q", model.document.filter.Value())
	}
	model.handleDocumentKey("backspace")
	if model.document.filter.Value() != "t " {
		t.Fatalf("document search backspace = %q", model.document.filter.Value())
	}
	model.document.filter.Restore(shellmodule.QueryState{Value: "target", Open: model.document.filter.Active()})
	model.handleDocumentKey("enter")
	if model.document.filter.Active() || model.document.offsetY == 0 {
		t.Fatalf("document search enter = open:%t offset:%d", model.document.filter.Active(), model.document.offsetY)
	}
	model.handleDocumentKey("/")
	model.handleDocumentKey("s")
	model.handleDocumentKey("ctrl+w")
	if model.document.filter.Value() != "targets" {
		t.Fatalf("reopened document search = %q", model.document.filter.Value())
	}
	model.handleDocumentKey("esc")
	if model.document.filter.Active() {
		t.Fatal("escape did not close document search")
	}

	model.document.filter.Restore(shellmodule.QueryState{Value: "", Open: model.document.filter.Active()})
	model.findDocumentMatch(1)
	model.document.lines = nil
	model.document.filter.Restore(shellmodule.QueryState{Value: "target", Open: model.document.filter.Active()})
	model.findDocumentMatch(1)
}

func TestDocumentRenderingCoversSearchErrorsAndEmptyContent(t *testing.T) {
	model := documentInteractionTestModel(t)
	model.document.filter.Restore(shellmodule.QueryState{Value: "target", Open: model.document.filter.Active()})
	if rendered := model.renderDocument(90, 15); !strings.Contains(rendered, "search /target/") || !strings.Contains(rendered, "target") {
		t.Fatalf("document search rendering:\n%s", rendered)
	}
	model.document.filter.Restore(shellmodule.QueryState{Value: model.document.filter.Value(), Open: true})
	if rendered := model.renderDocument(90, 15); !strings.Contains(rendered, "/target|") {
		t.Fatalf("open document search rendering:\n%s", rendered)
	}
	model.document.filter.Restore(shellmodule.QueryState{Value: model.document.filter.Value(), Open: false})
	model.document.err = errors.New("document unavailable")
	if rendered := model.renderDocument(90, 15); !strings.Contains(rendered, "Could not load document") {
		t.Fatalf("document error rendering:\n%s", rendered)
	}
	model.document.err = nil
	model.document.lines = nil
	if rendered := model.renderDocument(90, 15); !strings.Contains(rendered, "Document is empty") {
		t.Fatalf("empty document rendering:\n%s", rendered)
	}
}

func TestThreadDumpFilteringKeyboardAndMouse(t *testing.T) {
	tests := []struct {
		name        string
		key         string
		prepare     func(*Model)
		assert      func(*testing.T, Model)
		wantCommand bool
	}{
		{name: "back", key: "esc", assert: func(t *testing.T, m Model) {
			if m.mode != modeTaskManagerDetail {
				t.Fatalf("mode = %d, want TaskManager detail", m.mode)
			}
		}},
		{name: "up", key: "up", prepare: func(m *Model) { m.threadCursor = 1 }, assert: assertThreadCursor(0)},
		{name: "down", key: "down", assert: assertThreadCursor(1)},
		{name: "page up", key: "pgup", prepare: func(m *Model) { m.threadCursor = 3 }, assert: assertThreadCursor(0)},
		{name: "page down", key: "pgdown", assert: assertThreadCursor(3)},
		{name: "home", key: "home", prepare: func(m *Model) { m.threadCursor = 3; m.threadStackOffset = 9 }, assert: assertThreadPosition(0, 0)},
		{name: "end", key: "end", assert: assertThreadPosition(3, 0)},
		{name: "stack up", key: "ctrl+u", prepare: func(m *Model) { m.threadStackOffset = 9 }, assert: func(t *testing.T, m Model) {
			if m.threadStackOffset >= 9 {
				t.Fatal("stack did not scroll up")
			}
		}},
		{name: "stack down", key: "ctrl+d", assert: func(t *testing.T, m Model) {
			if m.threadStackOffset == 0 {
				t.Fatal("stack did not scroll down")
			}
		}},
		{name: "document", key: "enter", assert: func(t *testing.T, m Model) {
			if m.mode != modeDocument || m.document.backMode != modeThreadDump || !strings.HasPrefix(m.document.title, "THREAD") {
				t.Fatalf("thread document = mode:%d document:%#v", m.mode, m.document)
			}
		}},
		{name: "search", key: "/", prepare: func(m *Model) { m.threadFocus = threadFocusState{Prefix: "task", Matched: true} }, assert: func(t *testing.T, m Model) {
			if !m.threadFilter.Active() || m.threadFocus.Prefix != "" {
				t.Fatalf("thread search = open:%t focus:%#v", m.threadFilter.Active(), m.threadFocus)
			}
		}},
		{name: "profiler", key: "p", wantCommand: true, assert: func(t *testing.T, m Model) {
			if m.mode != modeProfiler {
				t.Fatalf("mode = %d, want profiler", m.mode)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			model := threadInteractionTestModel(t)
			if test.prepare != nil {
				test.prepare(&model)
			}
			command := model.handleThreadDumpKey(test.key)
			if (command != nil) != test.wantCommand {
				t.Fatalf("command presence = %t, want %t", command != nil, test.wantCommand)
			}
			test.assert(t, model)
		})
	}

	model := threadInteractionTestModel(t)
	model.threadFilter.Restore(shellmodule.QueryState{Value: "worker", Open: model.threadFilter.Active()})
	if threads := model.filteredThreads(); len(threads) != 1 || threads[0].Name != "worker" {
		t.Fatalf("name-filtered threads = %#v", threads)
	}
	model.threadFilter.Restore(shellmodule.QueryState{Value: "database.call", Open: model.threadFilter.Active()})
	if threads := model.filteredThreads(); len(threads) != 1 || threads[0].Name != "blocked" {
		t.Fatalf("stack-filtered threads = %#v", threads)
	}
	model.threadFilter.Restore(shellmodule.QueryState{Value: "", Open: model.threadFilter.Active()})
	model.moveThreadSelection(0)
	model.handleThreadMouseClick(tea.Mouse{Y: headerHeight + 5, Button: tea.MouseRight})
	model.handleThreadMouseClick(tea.Mouse{Y: 0, Button: tea.MouseLeft})
	if model.threadCursor != 0 {
		t.Fatal("invalid thread click changed selection")
	}
	model.handleThreadMouseClick(tea.Mouse{Y: headerHeight + 3 + 2, Button: tea.MouseLeft})
	if model.threadCursor != 2 {
		t.Fatalf("thread click cursor = %d, want 2", model.threadCursor)
	}

	model.threadDump = nil
	model.moveThreadSelection(1)
	if _, ok := model.selectedThread(); ok {
		t.Fatal("empty thread list returned a selection")
	}
}

func TestThreadSearchFocusAndStates(t *testing.T) {
	model := threadInteractionTestModel(t)
	model.handleThreadDumpKey("/")
	model.threadCursor = 2
	model.threadStackOffset = 5
	model.handleThreadDumpKey("w")
	model.handleThreadDumpKey("space")
	model.handleThreadDumpKey("x")
	if model.threadFilter.Value() != "w x" || model.threadCursor != 0 || model.threadStackOffset != 0 {
		t.Fatalf("thread search input = query:%q cursor:%d stack:%d", model.threadFilter.Value(), model.threadCursor, model.threadStackOffset)
	}
	model.handleThreadDumpKey("backspace")
	if model.threadFilter.Value() != "w " {
		t.Fatalf("thread search backspace = %q", model.threadFilter.Value())
	}
	model.handleThreadDumpKey("enter")
	if model.threadFilter.Active() {
		t.Fatal("enter did not close thread search")
	}
	model.handleThreadDumpKey("/")
	model.handleThreadDumpKey("s")
	model.handleThreadDumpKey("ctrl+w")
	if model.threadFilter.Value() != "w s" {
		t.Fatalf("reopened thread search = %q", model.threadFilter.Value())
	}
	model.handleThreadDumpKey("esc")
	if model.threadFilter.Active() {
		t.Fatal("escape did not close thread search")
	}

	model = threadInteractionTestModel(t)
	model.threadFocus = threadFocusState{Prefix: "worker", Label: "subtask"}
	model.threadDump = append([]flink.ThreadInfo{{Name: "Legacy Source Thread - worker", Stack: `"Legacy Source Thread - worker" RUNNABLE`}}, model.threadDump...)
	model.focusThreadDump()
	if !model.threadFocus.Matched || model.threadCursor != 0 {
		t.Fatalf("legacy focus = matched:%t cursor:%d", model.threadFocus.Matched, model.threadCursor)
	}
	model.threadFocus = threadFocusState{Prefix: "missing"}
	model.focusThreadDump()
	if model.threadFocus.Matched || model.threadCursor != 0 {
		t.Fatalf("missing focus = matched:%t cursor:%d", model.threadFocus.Matched, model.threadCursor)
	}

	states := map[string]string{
		`"a" TIMED_WAITING`: "TIMED_WAITING",
		`"a" RUNNABLE`:      "RUNNABLE",
		`"a" BLOCKED`:       "BLOCKED",
		`"a" WAITING`:       "WAITING",
		`"a" TERMINATED`:    "TERMINATED",
		`"a" NEW`:           "NEW",
		`"a" mystery`:       "UNKNOWN",
	}
	for stack, want := range states {
		if got := threadState(stack + "\nframe"); got != want {
			t.Errorf("thread state %q = %q, want %q", stack, got, want)
		}
	}
}

func assertProcessLogCursor(want int) func(*testing.T, Model) {
	return func(t *testing.T, model Model) {
		if model.processLogSelection.Index() != want {
			t.Fatalf("process log cursor = %d, want %d", model.processLogSelection.Index(), want)
		}
	}
}

func assertDocumentOffset(wantY, wantX int) func(*testing.T, Model) {
	return func(t *testing.T, model Model) {
		if model.document.offsetY != wantY || model.document.offsetX != wantX {
			t.Fatalf("document offset = (%d,%d), want (%d,%d)", model.document.offsetY, model.document.offsetX, wantY, wantX)
		}
	}
}

func assertThreadCursor(want int) func(*testing.T, Model) {
	return func(t *testing.T, model Model) {
		if model.threadCursor != want {
			t.Fatalf("thread cursor = %d, want %d", model.threadCursor, want)
		}
	}
}

func assertThreadPosition(wantCursor, wantStack int) func(*testing.T, Model) {
	return func(t *testing.T, model Model) {
		if model.threadCursor != wantCursor || model.threadStackOffset != wantStack {
			t.Fatalf("thread position = cursor:%d stack:%d, want cursor:%d stack:%d", model.threadCursor, model.threadStackOffset, wantCursor, wantStack)
		}
	}
}

func processInteractionTestModel(t *testing.T) Model {
	t.Helper()
	client, err := flink.NewClient("http://localhost:8081")
	if err != nil {
		t.Fatal(err)
	}
	model := New(client, nil)
	model.mode = modeProcessLogs
	model.height = 20
	model.process = flink.TaskManagerProcess("tm-1")
	model.processLogBackMode = modeTaskManagerDetail
	model.processLogs = []flink.LogFile{
		{Name: "task.log", Size: 100, ModifiedAt: time.Date(2026, time.August, 28, 12, 0, 0, 0, time.UTC)},
		{Name: "task.out", Size: 200},
		{Name: "gc.log", Size: 300},
		{Name: "audit.log", Size: 400},
	}
	return model
}

func documentInteractionTestModel(t *testing.T) Model {
	t.Helper()
	model := processInteractionTestModel(t)
	model.mode = modeDocument
	model.height = 12
	model.document = documentState{
		title: "LOG", backMode: modeProcessLogs, source: documentStatic,
		lines: []string{"zero", "one", "target first", "three", "four", "five", "target second", "seven", "eight", "nine"},
	}
	return model
}

func threadInteractionTestModel(t *testing.T) Model {
	t.Helper()
	model := processInteractionTestModel(t)
	model.mode = modeThreadDump
	model.threadBackMode = modeTaskManagerDetail
	model.threadDump = []flink.ThreadInfo{
		{Name: "main", Stack: `"main" RUNNABLE` + "\nmain.run\nframe2\nframe3\nframe4\nframe5"},
		{Name: "worker", Stack: `"worker" WAITING` + "\nworker.run"},
		{Name: "blocked", Stack: `"blocked" BLOCKED` + "\ndatabase.call"},
		{Name: "timer", Stack: `"timer" TIMED_WAITING` + "\ntimer.run"},
	}
	return model
}
