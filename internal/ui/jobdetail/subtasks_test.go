package jobdetail

import (
	"strings"
	"testing"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestSubtaskSortCanOrderRecordsSentDescending(t *testing.T) {
	model := New(nil, nil)
	model.diagnostics = flink.VertexDiagnostics{Subtasks: []flink.Subtask{
		{Index: 0, State: "RUNNING", Metrics: flink.Metrics{RecordsOutPerSecond: 12, RecordsOut: 120}},
		{Index: 1, State: "RUNNING", Metrics: flink.Metrics{RecordsOutPerSecond: 90, RecordsOut: 900}},
		{Index: 2, State: "RUNNING", Metrics: flink.Metrics{RecordsOutPerSecond: 30, RecordsOut: 300}},
	}}
	model.selectSubtaskSort(sortSubtaskOutput)
	if !model.subtaskSortDescending {
		t.Fatal("numeric output column did not default to descending")
	}
	rows := model.sortedSubtasks()
	if rows[0].Index != 1 || rows[1].Index != 2 || rows[2].Index != 0 {
		t.Fatalf("records sent rate order = %#v", rows)
	}
	model.subtaskTotals = true
	rows = model.sortedSubtasks()
	if rows[0].Metrics.RecordsOut != 900 {
		t.Fatalf("records sent total order = %#v", rows)
	}
	if label := model.subtaskSortLabel(); !strings.Contains(label, "OUTPUT") || !strings.Contains(label, "↓") {
		t.Fatalf("output sort label = %q", label)
	}
}

func TestSubtaskHeaderClicksMapToRenderedColumns(t *testing.T) {
	tests := []struct {
		name   string
		x      int
		width  int
		totals bool
		want   subtaskSort
	}{
		{name: "narrow input rate", x: 37, width: 80, want: sortSubtaskInput},
		{name: "narrow output rate", x: 46, width: 80, want: sortSubtaskOutput},
		{name: "wide idle", x: 38, width: 120, want: sortSubtaskIdle},
		{name: "wide watermark", x: 66, width: 120, want: sortSubtaskWatermark},
		{name: "wide task manager", x: 79, width: 120, want: sortSubtaskTaskManager},
		{name: "total records out", x: 36, width: 120, totals: true, want: sortSubtaskOutput},
		{name: "total bytes in", x: 54, width: 120, totals: true, want: sortSubtaskBytesInput},
		{name: "total bytes out", x: 68, width: 120, totals: true, want: sortSubtaskBytesOutput},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := subtaskSortColumnAt(test.x, test.width, test.totals)
			if !ok || got != test.want {
				t.Fatalf("column at x=%d width=%d totals=%t = (%d, %t), want %d", test.x, test.width, test.totals, got, ok, test.want)
			}
		})
	}
}
