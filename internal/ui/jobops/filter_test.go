package jobops

import (
	"strings"
	"testing"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestConfigurationFilterMatchesKeyAndValueAndPersists(t *testing.T) {
	model := New(nil, nil)
	model.Sync(Context{JobID: "job", JobType: "STREAMING", Scheduler: "Adaptive", BodyHeight: 20})
	model.RestoreState(State{
		View: ViewConfiguration,
		Configuration: flink.JobConfiguration{
			JobID: "job", RestartStrategy: "fixed-delay", Parallelism: 4,
			User: []flink.ConfigurationEntry{
				{Key: "state.backend", Value: "rocksdb"},
				{Key: "execution.checkpointing.interval", Value: "3 s"},
			},
		},
	})

	model.HandleKey("/")
	for _, key := range []string{"r", "o", "c", "k", "s"} {
		model.HandleKey(key)
	}
	if rows := model.configurationRows(); len(rows) != 1 || rows[0].key != "state.backend" {
		t.Fatalf("configuration value filter = %#v", rows)
	}
	model.HandleKey("esc")
	if rendered := model.Render(100, 20); !strings.Contains(rendered, "filter /rocks/") {
		t.Fatalf("configuration filter chip missing:\n%s", rendered)
	}

	model.HandleKey("/")
	for _, key := range []string{"n", "o", "n", "e"} {
		model.HandleKey(key)
	}
	model.HandleKey("enter")
	if rendered := model.Render(100, 20); !strings.Contains(rendered, "No configuration entries match filter /none/") {
		t.Fatalf("configuration zero match unexplained:\n%s", rendered)
	}
}
