package joblist

import (
	"errors"
	"testing"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestSlowOverviewRefreshIsNotSupersededByTicks(t *testing.T) {
	model := New(nil, nil)
	if model.Poll() == nil {
		t.Fatal("initial poll did not start")
	}
	generation := model.State().Generation
	for range 5 {
		if model.Poll() != nil || model.Refresh() != nil {
			t.Fatal("pending overview refresh was superseded")
		}
	}
	model.Apply(refreshMsg{generation: generation, jobs: []flink.JobSummary{{ID: "job", State: "RUNNING"}}})
	if state := model.State(); state.Loading || len(state.Jobs) != 1 || state.Jobs[0].ID != "job" {
		t.Fatalf("slow refresh did not reach overview: %#v", state)
	}
	if model.Poll() == nil {
		t.Fatal("successful refresh blocked the next poll")
	}
	model.Apply(refreshMsg{generation: model.State().Generation, err: errors.New("offline")})
	if model.Poll() == nil {
		t.Fatal("failed refresh blocked retry")
	}
}

func TestInitialOverviewReservationSurvivesTicks(t *testing.T) {
	model := New(nil, nil)
	model.ReserveInitialPoll()
	if model.Poll() != nil {
		t.Fatal("tick superseded the initial refresh")
	}
	model.Apply(refreshMsg{generation: model.State().Generation})
	if model.Poll() == nil {
		t.Fatal("initial reply did not release reservation")
	}
}
