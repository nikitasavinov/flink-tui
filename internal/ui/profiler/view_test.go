package profiler

import (
	"errors"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestProfilerHistoryCaptureAndReportLifecycle(t *testing.T) {
	process := flink.TaskManagerProcess("tm-1")
	model := New(nil, nil)
	model.SetViewport(120, 24)
	model.process = process
	model.observabilityGeneration = 3
	model.profilerBusy = true

	entry := flink.Profiling{
		Status: "FINISHED", Mode: flink.ProfilerITimer,
		TriggeredAt: time.UnixMilli(1787500000000), FinishedAt: time.UnixMilli(1787500005000),
		Duration: 5 * time.Second, Message: "Profiling Successful", OutputFile: "report.html",
	}
	model.Apply(listMsg{entries: []flink.Profiling{entry}, process: process, generation: 3}, true)
	if model.profilerBusy || len(model.profilerEntries) != 1 {
		t.Fatalf("history reply = busy:%t entries:%#v", model.profilerBusy, model.profilerEntries)
	}
	for _, expected := range []string{"PROCESS PROFILER", "Wall-Clock", "Allocation", "ITIMER", "FINISHED", "report.html"} {
		if rendered := model.Render(120, 24); !strings.Contains(rendered, expected) {
			t.Fatalf("profiler missing %q:\n%s", expected, rendered)
		}
	}

	model.profilerAction = "loading-report"
	graph := flink.FlameGraph{EndTimestampMillis: time.Now().UnixMilli(), Root: flink.FlameGraphNode{Name: "all", Value: 10}}
	result := model.Apply(downloadMsg{filename: "report.html", graph: graph, process: process, generation: 3}, true)
	if result.Intent != IntentReport || result.Command == nil || result.ReportName != "report.html" || !result.Graph.Ready() {
		t.Fatalf("report result = %#v", result)
	}

	model.profilerAction = "loading-report"
	late := model.Apply(downloadMsg{filename: "late.html", graph: graph, process: process, generation: 3}, false)
	if late.Intent != IntentNone || !strings.Contains(late.Notice, "ready") {
		t.Fatalf("late report result = %#v", late)
	}
}

func TestProfilerStartReplyAndMissingTimestampBaseline(t *testing.T) {
	process := flink.JobManagerProcess()
	model := New(nil, nil)
	model.process = process
	model.observabilityGeneration = 7
	oldRun := flink.Profiling{Status: "FINISHED", Mode: flink.ProfilerCPU, Duration: 5 * time.Second, Message: "old", OutputFile: "old.html"}
	model.profilerMode = flink.ProfilerCPU
	model.profilerAction = "waiting"
	model.profilerPendingRuns = profilerRunCounts([]flink.Profiling{oldRun})

	model.completePendingProfiler([]flink.Profiling{oldRun})
	if model.profilerAction != "waiting" {
		t.Fatal("older zero-timestamp run completed the pending capture")
	}
	newRun := oldRun
	newRun.Message, newRun.OutputFile = "new", "new.html"
	model.completePendingProfiler([]flink.Profiling{newRun, oldRun})
	if model.profilerAction != "" || model.profilerMessageStatus != "FINISHED" {
		t.Fatalf("new run did not complete capture: action=%q status=%q", model.profilerAction, model.profilerMessageStatus)
	}

	pendingAt := time.UnixMilli(1787500010000)
	result := model.Apply(startMsg{process: process, generation: 7, entry: flink.Profiling{
		Status: "RUNNING", Mode: flink.ProfilerCPU, TriggeredAt: pendingAt, Duration: 5 * time.Second, Message: "Profiling Started",
	}}, true)
	if result.Command == nil || model.profilerAction != "waiting" || !model.profilerPendingAt.Equal(pendingAt) {
		t.Fatalf("start reply = result:%#v action:%q pending:%s", result, model.profilerAction, model.profilerPendingAt)
	}
}

func TestProfilerControlsAndCrossFeatureIntents(t *testing.T) {
	process := flink.TaskManagerProcess("tm-1")
	model := New(nil, nil)
	model.SetViewport(120, 24)
	model.process = process
	model.profilerBackMode = 12
	model.profilerEntries = []flink.Profiling{{Status: "FAILED", Mode: flink.ProfilerCPU, Message: "perf denied"}}

	model.HandleKey("[")
	if model.profilerMode != flink.ProfilerAlloc {
		t.Fatalf("previous mode = %q, want ALLOC", model.profilerMode)
	}
	model.HandleKey("+")
	if model.profilerDuration != 10*time.Second {
		t.Fatalf("next duration = %s, want 10s", model.profilerDuration)
	}
	if result := model.HandleKey("l"); result.Intent != IntentLogs || result.Process != process {
		t.Fatalf("logs intent = %#v", result)
	}
	if result := model.HandleKey("esc"); result.Intent != IntentBack || result.BackToken != 12 {
		t.Fatalf("back intent = %#v", result)
	}

	model.profilerErr = errors.New("history failed")
	model.profilerBusy = false
	if command := model.startProfiler(); command != nil || model.pendingNotice == "" {
		t.Fatalf("failed-history start = command:%v notice:%q", command, model.pendingNotice)
	}

	model.profilerErr = nil
	model.profilerEntries = []flink.Profiling{{Status: "FINISHED", Mode: flink.ProfilerCPU, OutputFile: "report.html"}}
	command := model.handleProfilerMouseClick(tea.Mouse{X: 3, Y: headerHeight + profilerBodyRowStart, Button: tea.MouseLeft})
	if command == nil || model.profilerAction != "loading-report" {
		t.Fatalf("finished-row click = command:%v action:%q", command, model.profilerAction)
	}
}

func TestProfilerIgnoresStaleReplies(t *testing.T) {
	model := New(nil, nil)
	model.process = flink.TaskManagerProcess("new")
	model.observabilityGeneration = 4
	want := errors.New("must survive")
	model.profilerErr = want
	model.Apply(listMsg{process: flink.TaskManagerProcess("old"), generation: 3}, true)
	if !errors.Is(model.profilerErr, want) {
		t.Fatalf("stale reply changed error to %v", model.profilerErr)
	}
}
