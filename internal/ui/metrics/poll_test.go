package metrics

import (
	"context"
	"testing"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestMetricPollingCoalescesEachRequestKindAndRejectsOldScope(t *testing.T) {
	model := metricInteractionTestModel(t)
	client, err := flink.NewClient("http://flink.test")
	if err != nil {
		t.Fatal(err)
	}
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	model.client, model.parent = client, parent
	first := model.Poll()
	catalog := model.fetchMetricCatalog()
	if first == nil || catalog == nil || model.Poll() != nil || model.fetchMetricCatalog() != nil {
		t.Fatal("catalog and value requests were not independently coalesced")
	}
	// A scope round trip has the same final identity as the first request,
	// but its newer values must win even if the old response arrives last.
	model.metricScope = 0
	peer := model.RefreshWindow()
	model.metricScope = -1
	current := model.RefreshWindow()
	if peer == nil || current == nil {
		t.Fatal("scope changes did not start immediate requests")
	}
	currentReply := current().(metricValuesMsg)
	currentReply.err = nil
	currentReply.values = map[string]float64{"alpha": 200}
	model.Apply(currentReply)
	oldReply := first().(metricValuesMsg)
	oldReply.err = nil
	oldReply.values = map[string]float64{"alpha": 100}
	model.Apply(oldReply)
	model.Apply(peer().(Message))
	if model.State().Values["alpha"] != 200 || model.Error() != nil {
		t.Fatalf("late reply replaced current values: %#v", model.State())
	}
	model.Apply(catalog().(Message))
	if model.fetchMetricCatalog() == nil {
		t.Fatal("failed catalog request did not release discovery")
	}
	retry := model.Poll()
	if retry == nil {
		t.Fatal("successful value reply did not release polling")
	}
	model.Apply(retry().(Message))
	if model.Error() == nil || model.Poll() == nil {
		t.Fatal("failed value request did not release polling")
	}
}
