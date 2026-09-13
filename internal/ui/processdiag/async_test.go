package processdiag

import (
	"testing"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestDocumentRefreshDoesNotDiscardPairedLogCatalog(t *testing.T) {
	model := New(nil, nil)
	model.OpenCurrentLog(flink.JobManagerProcess(), 1, 20)
	process := flink.TaskManagerProcess("tm-2")
	if command, ok := model.RetargetDocument(process); !ok || command == nil {
		t.Fatal("process retarget did not request document and catalog")
	}
	catalogGeneration := model.State().Generation
	model.Refresh()
	current := model.State()
	model.Apply(documentMsg{generation: current.Generation, title: current.DocumentTitle, content: "current log"})
	model.Apply(processLogsMsg{
		generation: catalogGeneration, process: process,
		logs: []flink.LogFile{{Name: "taskmanager.log"}},
	})
	state := model.State()
	if state.LogsBusy || len(state.Logs) != 1 || state.DocumentLoading || len(state.DocumentLines) != 1 {
		t.Fatalf("document refresh discarded the independent log catalog: %#v", state)
	}
}

func TestReturningToDocumentCompletesItsPendingRequest(t *testing.T) {
	model := New(nil, nil)
	process := flink.JobManagerProcess()
	model.OpenCurrentLog(process, 1, 20)
	document := model.State()
	model.OpenThreadDump(process, 1, Focus{})
	threadGeneration := model.State().Generation
	model.Apply(documentMsg{generation: document.Generation, title: document.DocumentTitle, content: "loaded in background"})
	model.Apply(threadDumpMsg{generation: threadGeneration, process: process, threads: []flink.ThreadInfo{{Name: "main"}}})
	model.Activate(ViewDocument)
	state := model.State()
	if state.DocumentLoading || len(state.DocumentLines) != 1 || state.DocumentLines[0] != "loaded in background" || state.ThreadBusy {
		t.Fatalf("restored document remained stuck behind another view's request: %#v", state)
	}
}

func TestStaticDocumentRejectsEarlierRemoteContentWithSameTitle(t *testing.T) {
	model := New(nil, nil)
	model.OpenCurrentLog(flink.JobManagerProcess(), 1, 20)
	remote := model.State()
	model.OpenStatic(remote.DocumentTitle, "captured content", 1)
	model.Apply(documentMsg{generation: remote.Generation, title: remote.DocumentTitle, content: "late remote content"})
	if lines := model.State().DocumentLines; len(lines) != 1 || lines[0] != "captured content" {
		t.Fatalf("late remote response changed static content: %v", lines)
	}
}
