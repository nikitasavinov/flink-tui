package infrastructure

import (
	"errors"
	"strings"
	"testing"

	"github.com/charmbracelet/x/ansi"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestLateReplyUpdatesCacheWithoutStealingNavigation(t *testing.T) {
	model := New(nil, nil, nil)
	result := model.OpenTaskManagerByID("tm-target", 17, "Exception Incidents")
	if result.Command == nil || model.View() != ViewTaskManagers {
		t.Fatalf("open target produced command=%v view=%d", result.Command, model.View())
	}
	generation := model.State().Generation

	result = model.Apply(refreshMsg{
		generation: generation,
		infrastructure: flink.Infrastructure{
			TaskManagers: []flink.TaskManager{{ID: "tm-target"}},
		},
	}, false)
	if result.Intent != IntentNone || model.View() != ViewTaskManagers || model.State().PendingTargetID != "" {
		t.Fatalf("late reply produced result=%#v view=%d target=%q", result, model.View(), model.State().PendingTargetID)
	}
	if managers := model.State().Infrastructure.TaskManagers; len(managers) != 1 || managers[0].ID != "tm-target" {
		t.Fatalf("accepted reply did not update cache: %#v", managers)
	}

	model.Poll()
	model.Apply(refreshMsg{
		generation: generation,
		infrastructure: flink.Infrastructure{
			TaskManagers: []flink.TaskManager{{ID: "stale"}},
		},
	}, true)
	if got := model.State().Infrastructure.TaskManagers[0].ID; got != "tm-target" {
		t.Fatalf("older request overwrote cache with %q", got)
	}
}

func TestSlowInfrastructureRefreshIsNotStarvedByTicks(t *testing.T) {
	model := New(nil, nil, nil)
	command := model.Poll()
	if command == nil {
		t.Fatal("initial poll did not start")
	}
	generation := model.State().Generation
	for range 10 {
		if model.Poll() != nil || model.Refresh() != nil {
			t.Fatal("a slow infrastructure refresh allowed an overlapping request")
		}
	}
	if model.State().Generation != generation {
		t.Fatal("ticks superseded the only in-flight reply")
	}
	model.Apply(command().(Message), true)
	if model.State().Err == nil || model.State().Busy {
		t.Fatal("current failure was dropped instead of completing the request")
	}
	if model.Poll() == nil {
		t.Fatal("failure prevented the next retry")
	}
	model.Apply(refreshMsg{
		generation:     model.State().Generation,
		infrastructure: flink.Infrastructure{TaskManagers: []flink.TaskManager{{ID: "tm"}}},
	}, true)
	if model.State().Err != nil || len(model.State().Infrastructure.TaskManagers) != 1 || model.Poll() == nil {
		t.Fatal("successful reply did not restore data and allow later refresh")
	}
}

func TestJobManagerConfigurationIsFilterableTypedAndComparedWithLiveSlots(t *testing.T) {
	model := New(nil, nil, nil)
	model.Restore(flink.Infrastructure{
		TaskManagers: []flink.TaskManager{{ID: "tm-1", Slots: 16}},
		JobManager: flink.JobManager{Configuration: []flink.ConfigurationEntry{
			{Key: "jobmanager.memory.heap.size", Value: "1073741824b"},
			{Key: "taskmanager.numberOfTaskSlots", Value: "1"},
		}},
	})
	model.Activate(ViewJobManager)
	rendered := ansi.Strip(model.Render(120, 24))
	for _, expected := range []string{"CONFIG / LIVE MISMATCH", "configured; registered TaskManagers report 16 each", "1.0GiB (1073741824b)"} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("JobManager config missing %q:\n%s", expected, rendered)
		}
	}

	model.HandleKey("/", 20, 120)
	for _, key := range []string{"h", "e", "a", "p"} {
		model.HandleKey(key, 20, 120)
	}
	if !model.CapturesKeys() || model.State().JobManagerConfigQuery != "heap" || len(model.filteredJobManagerConfiguration()) != 1 {
		t.Fatalf("config filter state = %#v rows=%#v", model.State(), model.filteredJobManagerConfiguration())
	}
	model.HandleKey("enter", 20, 120)
	if model.CapturesKeys() {
		t.Fatal("enter did not apply JobManager config filter")
	}
	model.HandleKey("/", 20, 120)
	model.HandleKey("s", 20, 120)
	if model.State().JobManagerConfigQuery != "heaps" {
		t.Fatalf("reopened config search = %q", model.State().JobManagerConfigQuery)
	}
	model.HandleKey("ctrl+w", 20, 120)
	model.HandleKey("esc", 20, 120)
	if model.CapturesKeys() || model.State().JobManagerConfigQuery != "" || len(model.filteredJobManagerConfiguration()) != 2 {
		t.Fatalf("cleared config search = %#v", model.State())
	}
}

func TestPendingTaskManagerTargetResolvesOnlyWhileActive(t *testing.T) {
	model := New(nil, nil, nil)
	model.OpenTaskManagerByID("tm-b", 9, "Exceptions")
	generation := model.State().Generation
	result := model.Apply(refreshMsg{
		generation: generation,
		infrastructure: flink.Infrastructure{TaskManagers: []flink.TaskManager{
			{ID: "tm-a"}, {ID: "tm-b"},
		}},
	}, true)
	manager, ok := model.SelectedTaskManager()
	if result.Intent != IntentNone || model.View() != ViewTaskManagerDetail || !ok || manager.ID != "tm-b" {
		t.Fatalf("resolved target = result:%#v view:%d manager:%#v ok:%t", result, model.View(), manager, ok)
	}
}

func TestMissingPendingTargetReturnsToItsCaller(t *testing.T) {
	model := New(nil, nil, nil)
	model.OpenTaskManagerByID("tm-gone", 13, "Exception Incidents")
	result := model.Apply(refreshMsg{
		generation:     model.State().Generation,
		infrastructure: flink.Infrastructure{TaskManagers: []flink.TaskManager{{ID: "tm-other"}}},
	}, true)
	if result.Intent != IntentBack || result.BackToken != 13 || result.Notice == "" {
		t.Fatalf("missing-target result = %#v", result)
	}
}

func TestRefreshFailureKeepsLastGoodInfrastructure(t *testing.T) {
	model := New(nil, nil, nil)
	model.Restore(flink.Infrastructure{TaskManagers: []flink.TaskManager{{ID: "last-good"}}})
	model.Refresh()
	wantErr := errors.New("unavailable")
	model.Apply(refreshMsg{generation: model.State().Generation, err: wantErr}, true)
	state := model.State()
	if !errors.Is(state.Err, wantErr) || state.Busy || len(state.Infrastructure.TaskManagers) != 1 || state.Infrastructure.TaskManagers[0].ID != "last-good" {
		t.Fatalf("failed refresh state = %#v", state)
	}
}

func TestStateIsDetachedFromModel(t *testing.T) {
	model := New(nil, nil, nil)
	model.Restore(flink.Infrastructure{
		TaskManagers: []flink.TaskManager{{ID: "tm-a"}},
		JobManager:   flink.JobManager{Logs: []flink.LogFile{{Name: "jobmanager.log"}}},
	})
	state := model.State()
	state.Infrastructure.TaskManagers[0].ID = "mutated"
	state.Infrastructure.JobManager.Logs[0].Name = "mutated.log"
	actual := model.State().Infrastructure
	if actual.TaskManagers[0].ID != "tm-a" || actual.JobManager.Logs[0].Name != "jobmanager.log" {
		t.Fatalf("state mutation leaked into model: %#v", actual)
	}
}
