package flink

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestCheckpointDiagnosticEndpoints(t *testing.T) {
	client := testClient(t, func(request *http.Request) (string, int) {
		switch request.URL.Path {
		case "/jobs/job/checkpoints/details/42":
			return `{
				"id":42,"status":"COMPLETED","checkpoint_type":"CHECKPOINT",
				"trigger_timestamp":10000,"latest_ack_timestamp":10020,"end_to_end_duration":20,
				"checkpointed_size":3000,"state_size":2500,"num_subtasks":4,"num_acknowledged_subtasks":4,
				"tasks":{
					"slow":{"status":"COMPLETED","latest_ack_timestamp":10020,"end_to_end_duration":20,"checkpointed_size":2200,"state_size":2000,"processed_data":512,"num_subtasks":2,"num_acknowledged_subtasks":2},
					"fast":{"status":"COMPLETED","latest_ack_timestamp":10010,"end_to_end_duration":10,"checkpointed_size":800,"state_size":500,"num_subtasks":2,"num_acknowledged_subtasks":2}
				}
			}`, http.StatusOK
		case "/jobs/job/checkpoints/config":
			return `{
				"mode":"exactly_once","interval":3000,"timeout":20000,"min_pause":1000,"max_concurrent":1,
				"externalization":{"enabled":true,"delete_on_cancellation":false},
				"state_backend":"HashMapStateBackend","checkpoint_storage":"FileSystemCheckpointStorage",
				"unaligned_checkpoints":true,"tolerable_failed_checkpoints":2,"aligned_checkpoint_timeout":500,
				"checkpoints_after_tasks_finish":true,"state_changelog_enabled":true,
				"changelog_periodic_materialization_interval":600000,"changelog_storage":"filesystem"
			}`, http.StatusOK
		case "/jobs/job/checkpoints/details/42/subtasks/slow":
			return `{
				"status":"COMPLETED","latest_ack_timestamp":10020,"end_to_end_duration":20,
				"checkpointed_size":2200,"state_size":2000,"processed_data":512,
				"num_subtasks":2,"num_acknowledged_subtasks":2,
				"summary":{
					"checkpointed_size":{"min":1000,"max":1200,"avg":1100,"p50":"NaN"},
					"state_size":{"min":900,"max":1100,"avg":1000},
					"end_to_end_duration":{"min":10,"max":20,"avg":15},
					"checkpoint_duration":{"sync":{"min":1,"max":2,"avg":1.5},"async":{"min":3,"max":6,"avg":4.5}},
					"alignment":{"buffered":{"min":0,"max":10,"avg":5},"processed":{"min":20,"max":40,"avg":30},"persisted":{"min":0,"max":0,"avg":0},"duration":{"min":2,"max":4,"avg":3}},
					"start_delay":{"min":4,"max":8,"avg":6}
				},
				"subtasks":[
					{"index":1,"status":"completed","ack_timestamp":10020,"end_to_end_duration":20,"checkpointed_size":1200,"state_size":1100,"checkpoint":{"sync":2,"async":6},"alignment":{"buffered":10,"processed":40,"persisted":0,"duration":4},"start_delay":8,"unaligned_checkpoint":true,"aborted":false},
					{"index":0,"status":"completed","ack_timestamp":10010,"end_to_end_duration":10,"checkpointed_size":1000,"state_size":900,"checkpoint":{"sync":1,"async":3},"alignment":{"buffered":0,"processed":20,"persisted":0,"duration":2},"start_delay":4,"unaligned_checkpoint":false,"aborted":false}
				]
			}`, http.StatusOK
		default:
			return "", http.StatusNotFound
		}
	})

	details, err := client.CheckpointDetails(context.Background(), "job", 42)
	if err != nil {
		t.Fatal(err)
	}
	if details.Checkpoint.ID != 42 || len(details.Operators) != 2 || details.UpdatedAt.IsZero() {
		t.Fatalf("checkpoint details = %#v", details)
	}
	var slow CheckpointOperator
	for _, operator := range details.Operators {
		if operator.VertexID == "slow" {
			slow = operator
		}
	}
	if slow.Duration != 20*time.Millisecond || slow.StateSize != 2000 || slow.ProcessedData != 512 {
		t.Fatalf("slow operator = %#v", slow)
	}

	config, err := client.CheckpointConfig(context.Background(), "job")
	if err != nil {
		t.Fatal(err)
	}
	if config.Interval != 3*time.Second || config.MinimumPause != time.Second ||
		!config.UnalignedCheckpoints || !config.StateChangelogEnabled || !config.ExternalizationEnabled {
		t.Fatalf("checkpoint config = %#v", config)
	}

	subtasks, err := client.CheckpointSubtasks(context.Background(), "job", 42, "slow")
	if err != nil {
		t.Fatal(err)
	}
	if len(subtasks.Subtasks) != 2 || subtasks.Subtasks[0].Index != 0 || subtasks.Subtasks[1].Index != 1 {
		t.Fatalf("checkpoint subtasks = %#v", subtasks.Subtasks)
	}
	if got := subtasks.Subtasks[1]; got.Duration != 20*time.Millisecond || got.AsyncDuration != 6*time.Millisecond ||
		got.AlignmentProcessed != 40 || !got.Unaligned {
		t.Fatalf("slow subtask = %#v", got)
	}
	if subtasks.Summary.EndToEndDuration.Average != 15 || subtasks.Summary.CheckpointedSize.P50 != 0 {
		t.Fatalf("subtask summary = %#v", subtasks.Summary)
	}
}
