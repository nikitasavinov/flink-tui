package flamegraph

import (
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestFlameGraphRendersProportionalFramesAndSelectedDetails(t *testing.T) {
	model := flameGraphTestModel(t)
	rendered := model.renderFlameGraph(100, 20)
	for _, expected := range []string{
		"VERTEX FLAME GRAPH", "Source", "On-CPU", "Off-CPU", "Mixed", "subtask", "all",
		"Task.run:579", "Unsafe.park:-2", "selected", "org.apache.flink.runtime.taskmanager.Task.run:579",
	} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("flame graph missing %q:\n%s", expected, rendered)
		}
	}
	lines := strings.Split(rendered, "\n")
	if len(lines) != 20 {
		t.Fatalf("rendered %d lines, want 20", len(lines))
	}
	for index, line := range lines {
		if got := lipgloss.Width(line); got > 100 {
			t.Fatalf("line %d is %d cells wide", index, got)
		}
	}
}

func TestProcessProfileTitleCannotBeConfusedWithVertexSampling(t *testing.T) {
	model := flameGraphTestModel(t)
	model.LoadProfilerReport(flink.TaskManagerProcess("tm-1"), "cpu.html", flameGraphFixture())
	model.Activate(ViewProfilerReport)
	rendered := model.renderFlameGraph(100, 20)
	if !strings.Contains(rendered, "PROCESS PROFILE FLAME GRAPH") || strings.Contains(rendered, "VERTEX FLAME GRAPH") {
		t.Fatalf("process profile title is ambiguous:\n%s", rendered)
	}
}

func TestFlameGraphKeyboardNavigatesFramesAndSamplingControls(t *testing.T) {
	model := flameGraphTestModel(t)
	model.flameGraphSelected = "0"

	model.handleFlameGraphKey("down")
	if model.flameGraphSelected != "0/0" {
		t.Fatalf("down selected %q, want largest child", model.flameGraphSelected)
	}
	model.handleFlameGraphKey("right")
	if model.flameGraphSelected != "0/1" {
		t.Fatalf("right selected %q, want sibling", model.flameGraphSelected)
	}
	model.handleFlameGraphKey("enter")
	if model.flameGraphFocus != "0/1" {
		t.Fatalf("enter focused %q, want selected frame", model.flameGraphFocus)
	}
	model.handleFlameGraphKey("backspace")
	if model.flameGraphFocus != "0" || model.flameGraphSelected != "0" {
		t.Fatalf("backspace produced focus=%q selected=%q", model.flameGraphFocus, model.flameGraphSelected)
	}

	if command := model.handleFlameGraphKey("]"); command == nil || model.flameGraphType != flink.FlameGraphOnCPU {
		t.Fatalf("] produced command=%v type=%q", command, model.flameGraphType)
	}
	model.flameGraph = flameGraphFixture()
	if command := model.handleFlameGraphKey("s"); command == nil || model.flameGraphSubtask != 0 {
		t.Fatalf("s produced command=%v subtask=%d", command, model.flameGraphSubtask)
	}
}

func TestFlameGraphMouseSelectsFramesAndChangesType(t *testing.T) {
	model := flameGraphTestModel(t)
	model.width = 100
	model.height = 24
	model.flameGraphSelected = "0"

	view := model.flameGraphView(model.width, model.height-headerHeight-footerHeight)
	left, right := flameFrameColumns(view.frames[1], view.total, view.graphWidth)
	command := model.handleFlameGraphMouseClick(tea.Mouse{
		X:      view.graphX + (left+right)/2,
		Y:      headerHeight + flameGraphFirstFrameRow + view.frames[1].Depth,
		Button: tea.MouseLeft,
	})
	if command != nil || model.flameGraphSelected != "0/0" {
		t.Fatalf("frame click produced command=%v selected=%q", command, model.flameGraphSelected)
	}

	_, hits := model.flameGraphControls(model.width)
	var offCPU flameControlHit
	for _, hit := range hits {
		if hit.kind == "type:"+string(flink.FlameGraphOffCPU) {
			offCPU = hit
			break
		}
	}
	command = model.handleFlameGraphMouseClick(tea.Mouse{
		X:      offCPU.start,
		Y:      headerHeight + flameGraphControlRow,
		Button: tea.MouseLeft,
	})
	if command == nil || model.flameGraphType != flink.FlameGraphOffCPU {
		t.Fatalf("type click produced command=%v type=%q", command, model.flameGraphType)
	}
}

func TestFlameGraphExplainsSamplingAndDisabledStates(t *testing.T) {
	model := flameGraphTestModel(t)
	model.flameGraph = flink.FlameGraph{EndTimestampMillis: -3}
	if rendered := model.renderFlameGraphStatus(100); !strings.Contains(rendered, "Collecting the first samples") {
		t.Fatalf("sampling status = %q", rendered)
	}
	model.flameGraph = flink.FlameGraph{EndTimestampMillis: -2}
	if rendered := model.renderFlameGraphStatus(100); !strings.Contains(rendered, "rest.flamegraph.enabled") {
		t.Fatalf("disabled status = %q", rendered)
	}
}

func flameGraphTestModel(t *testing.T) Model {
	t.Helper()
	model := New(nil, nil)
	model.Sync(Context{
		JobID: "job", Selected: "source", Width: 100, Height: 24, ContentWidth: 100,
		Nodes: []flink.Node{{ID: "source", Name: "Source", Parallelism: 2}},
	})
	model.Activate(ViewVertex)
	model.flameGraphVertex = "source"
	model.flameGraphType = flink.FlameGraphFull
	model.flameGraphSubtask = -1
	model.flameGraph = flameGraphFixture()
	model.flameGraphFocus = "0"
	model.flameGraphSelected = "0/0"
	return model
}

func flameGraphFixture() flink.FlameGraph {
	return flink.FlameGraph{
		Type:               flink.FlameGraphFull,
		Subtask:            -1,
		EndTimestampMillis: 1710000000123,
		EndTimestamp:       time.UnixMilli(1710000000123),
		Root: flink.FlameGraphNode{
			Name: "root", Value: 100,
			Children: []flink.FlameGraphNode{
				{
					Name: "org.apache.flink.runtime.taskmanager.Task.run:579", Value: 70,
					Children: []flink.FlameGraphNode{{Name: "jdk.internal.misc.Unsafe.park:-2", Value: 50}},
				},
				{Name: "java.util.concurrent.ThreadPoolExecutor.runWorker:1136", Value: 30},
			},
		},
	}
}
