package jobops

import (
	"context"
	"testing"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestConfigurationRequestsCoalesceAndRejectRevisitedJob(t *testing.T) {
	client, err := flink.NewClient("http://flink.test")
	if err != nil {
		t.Fatal(err)
	}
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	model := New(client, parent)
	first := model.OpenConfiguration(Context{JobID: "first"})
	if first == nil || model.Refresh().Command != nil {
		t.Fatal("configuration refresh did not coalesce")
	}
	if model.OpenConfiguration(Context{JobID: "second"}) == nil {
		t.Fatal("new job did not start an immediate request")
	}
	current := model.OpenConfiguration(Context{JobID: "first"})
	if current == nil {
		t.Fatal("revisited job did not start a current request")
	}
	model.Apply(first().(Message))
	if !model.State().ConfigurationBusy || model.State().ConfigurationErr != nil || model.Refresh().Command != nil {
		t.Fatal("old reply changed or released current configuration request")
	}
	model.Apply(current().(Message))
	if model.State().ConfigurationErr == nil || model.Refresh().Command == nil {
		t.Fatal("configuration error did not allow retry")
	}
}
