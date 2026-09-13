package infrastructure

import (
	"strings"
	"testing"

	"github.com/charmbracelet/x/ansi"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestTaskManagerViewsDistinguishUnknownAndZeroAssignedTasks(t *testing.T) {
	for _, test := range []struct {
		name  string
		count int
		known bool
		want  string
	}{
		{name: "Flink 1.20 missing count", want: "unknown"},
		{name: "explicit zero count", known: true, want: "0"},
		{name: "reported nonzero count", count: 7, known: true, want: "7"},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager := flink.TaskManager{
				ID: "tm-1", Slots: 4, FreeSlots: 1,
				AssignedTasks: test.count, AssignedTasksKnown: test.known,
				Allocations: []flink.TaskManagerAllocation{{
					JobID: "job", AssignedTasks: test.count, AssignedTasksKnown: test.known,
				}},
			}
			for _, width := range []int{80, 140} {
				row := strings.Fields(ansi.Strip(renderTaskManagerRow(manager, false, width)))
				if len(row) < 3 || row[2] != test.want {
					t.Errorf("TaskManager table at width %d: %q; want task count %q", width, row, test.want)
				}
			}

			model := New(nil, nil, nil)
			model.Restore(flink.Infrastructure{TaskManagers: []flink.TaskManager{manager}})
			summary := strings.Join(model.renderSelectedTaskManager(140), "\n")
			if !strings.Contains(summary, "assigned tasks "+test.want+"  |") || !strings.Contains(summary, "slots 3/4 used") {
				t.Errorf("TaskManager summary lost count or slots:\n%s", summary)
			}
			detail := ansi.Strip(strings.Join(taskManagerDetailContent(manager, 140), "\n"))
			if !strings.Contains(detail, "slots 3/4 used  |  assigned tasks "+test.want) ||
				!strings.Contains(detail, "job job  |  assigned tasks "+test.want) {
				t.Errorf("TaskManager detail lost worker or allocation count:\n%s", detail)
			}
		})
	}
}
