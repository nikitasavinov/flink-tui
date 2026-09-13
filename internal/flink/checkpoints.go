package flink

import (
	"cmp"
	"context"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"time"
)

// CheckpointDetails is the job-level checkpoint record plus the contribution
// and acknowledgement state of every execution vertex.
type CheckpointDetails struct {
	JobID      string
	Checkpoint Checkpoint
	Operators  []CheckpointOperator
	UpdatedAt  time.Time
}

type CheckpointOperator struct {
	VertexID             string
	Status               string
	LatestAcknowledgedAt time.Time
	Duration             time.Duration
	CheckpointedSize     int64
	StateSize            int64
	AlignmentBuffered    int64
	ProcessedData        int64
	PersistedData        int64
	Subtasks             int
	AcknowledgedSubtasks int
}

type CheckpointSubtaskDetails struct {
	JobID        string
	CheckpointID int64
	VertexID     string
	Operator     CheckpointOperator
	Summary      CheckpointSubtaskSummary
	Subtasks     []CheckpointSubtask
	UpdatedAt    time.Time
}

type CheckpointSubtask struct {
	Index              int
	Status             string
	AcknowledgedAt     time.Time
	Duration           time.Duration
	CheckpointedSize   int64
	StateSize          int64
	SyncDuration       time.Duration
	AsyncDuration      time.Duration
	AlignmentBuffered  int64
	AlignmentProcessed int64
	AlignmentPersisted int64
	AlignmentDuration  time.Duration
	StartDelay         time.Duration
	Unaligned          bool
	Aborted            bool
}

// CheckpointDistribution preserves the min/average/max and percentile values
// returned by Flink. Values are milliseconds for durations and bytes for sizes.
type CheckpointDistribution struct {
	Min     float64
	Average float64
	Max     float64
	P50     float64
	P90     float64
	P95     float64
	P99     float64
	P999    float64
}

// CheckpointStatistics is Flink's job-wide summary over the complete
// checkpoint population. The REST endpoint returns this independently from
// the short recent-history window.
type CheckpointStatistics struct {
	EndToEndDuration  CheckpointDistribution
	CheckpointedSize  CheckpointDistribution
	StateSize         CheckpointDistribution
	AlignmentBuffered CheckpointDistribution
	ProcessedData     CheckpointDistribution
	PersistedData     CheckpointDistribution
}

type CheckpointSubtaskSummary struct {
	EndToEndDuration   CheckpointDistribution
	CheckpointedSize   CheckpointDistribution
	StateSize          CheckpointDistribution
	SyncDuration       CheckpointDistribution
	AsyncDuration      CheckpointDistribution
	AlignmentBuffered  CheckpointDistribution
	AlignmentProcessed CheckpointDistribution
	AlignmentPersisted CheckpointDistribution
	AlignmentDuration  CheckpointDistribution
	StartDelay         CheckpointDistribution
}

type CheckpointConfig struct {
	Mode                             string
	Interval                         time.Duration
	Timeout                          time.Duration
	MinimumPause                     time.Duration
	MaximumConcurrent                int
	ExternalizationEnabled           bool
	DeleteExternalizedOnCancellation bool
	StateBackend                     string
	CheckpointStorage                string
	UnalignedCheckpoints             bool
	TolerableFailedCheckpoints       int
	AlignedCheckpointTimeout         time.Duration
	CheckpointsAfterTasksFinish      bool
	StateChangelogEnabled            bool
	ChangelogMaterializationInterval time.Duration
	ChangelogStorage                 string
}

// CheckpointDetails loads one checkpoint's per-operator diagnostics.
func (c *Client) CheckpointDetails(ctx context.Context, jobID string, checkpointID int64) (CheckpointDetails, error) {
	path := fmt.Sprintf(
		"/jobs/%s/checkpoints/details/%s",
		url.PathEscape(jobID),
		strconv.FormatInt(checkpointID, 10),
	)
	var response checkpointItem
	if err := c.get(ctx, path, &response); err != nil {
		return CheckpointDetails{}, err
	}
	details := CheckpointDetails{
		JobID:      jobID,
		Checkpoint: checkpointFromItem(response),
		Operators:  make([]CheckpointOperator, 0, len(response.Tasks)),
		UpdatedAt:  time.Now(),
	}
	for vertexID, task := range response.Tasks {
		details.Operators = append(details.Operators, checkpointOperatorFromTask(vertexID, task))
	}
	slices.SortFunc(details.Operators, func(left, right CheckpointOperator) int {
		return cmp.Compare(left.VertexID, right.VertexID)
	})
	return details, nil
}

// CheckpointConfig loads the checkpoint settings effective for a job.
func (c *Client) CheckpointConfig(ctx context.Context, jobID string) (CheckpointConfig, error) {
	path := "/jobs/" + url.PathEscape(jobID) + "/checkpoints/config"
	var response checkpointConfigResponse
	if err := c.get(ctx, path, &response); err != nil {
		return CheckpointConfig{}, err
	}
	return CheckpointConfig{
		Mode:                             response.Mode,
		Interval:                         milliseconds(response.Interval),
		Timeout:                          milliseconds(response.Timeout),
		MinimumPause:                     milliseconds(response.MinimumPause),
		MaximumConcurrent:                response.MaximumConcurrent,
		ExternalizationEnabled:           response.Externalization.Enabled,
		DeleteExternalizedOnCancellation: response.Externalization.DeleteOnCancellation,
		StateBackend:                     response.StateBackend,
		CheckpointStorage:                response.CheckpointStorage,
		UnalignedCheckpoints:             response.UnalignedCheckpoints,
		TolerableFailedCheckpoints:       response.TolerableFailedCheckpoints,
		AlignedCheckpointTimeout:         milliseconds(response.AlignedCheckpointTimeout),
		CheckpointsAfterTasksFinish:      response.CheckpointsAfterTasksFinish,
		StateChangelogEnabled:            response.StateChangelogEnabled,
		ChangelogMaterializationInterval: milliseconds(response.ChangelogMaterializationInterval),
		ChangelogStorage:                 response.ChangelogStorage,
	}, nil
}

// CheckpointSubtasks loads the checkpoint phases for each subtask of one vertex.
func (c *Client) CheckpointSubtasks(ctx context.Context, jobID string, checkpointID int64, vertexID string) (CheckpointSubtaskDetails, error) {
	path := fmt.Sprintf(
		"/jobs/%s/checkpoints/details/%s/subtasks/%s",
		url.PathEscape(jobID),
		strconv.FormatInt(checkpointID, 10),
		url.PathEscape(vertexID),
	)
	var response checkpointSubtaskResponse
	if err := c.get(ctx, path, &response); err != nil {
		return CheckpointSubtaskDetails{}, err
	}
	details := CheckpointSubtaskDetails{
		JobID:        jobID,
		CheckpointID: checkpointID,
		VertexID:     vertexID,
		Operator: checkpointOperatorFromTask(vertexID, checkpointTask{
			Status:               response.Status,
			LatestAckTimestamp:   response.LatestAckTimestamp,
			EndToEndDuration:     response.EndToEndDuration,
			CheckpointedSize:     response.CheckpointedSize,
			StateSize:            response.StateSize,
			AlignmentBuffered:    response.AlignmentBuffered,
			ProcessedData:        response.ProcessedData,
			PersistedData:        response.PersistedData,
			NumSubtasks:          response.NumSubtasks,
			AcknowledgedSubtasks: response.AcknowledgedSubtasks,
		}),
		Summary:   normalizeCheckpointSubtaskSummary(response.Summary),
		Subtasks:  make([]CheckpointSubtask, len(response.Subtasks)),
		UpdatedAt: time.Now(),
	}
	for index, subtask := range response.Subtasks {
		details.Subtasks[index] = CheckpointSubtask{
			Index:              subtask.Index,
			Status:             subtask.Status,
			AcknowledgedAt:     unixMillis(subtask.AcknowledgedTimestamp),
			Duration:           milliseconds(subtask.EndToEndDuration),
			CheckpointedSize:   subtask.CheckpointedSize,
			StateSize:          subtask.StateSize,
			SyncDuration:       milliseconds(subtask.Checkpoint.Sync),
			AsyncDuration:      milliseconds(subtask.Checkpoint.Async),
			AlignmentBuffered:  subtask.Alignment.Buffered,
			AlignmentProcessed: subtask.Alignment.Processed,
			AlignmentPersisted: subtask.Alignment.Persisted,
			AlignmentDuration:  milliseconds(subtask.Alignment.Duration),
			StartDelay:         milliseconds(subtask.StartDelay),
			Unaligned:          subtask.Unaligned,
			Aborted:            subtask.Aborted,
		}
	}
	slices.SortFunc(details.Subtasks, func(left, right CheckpointSubtask) int {
		return cmp.Compare(left.Index, right.Index)
	})
	return details, nil
}

func checkpointOperatorFromTask(vertexID string, task checkpointTask) CheckpointOperator {
	return CheckpointOperator{
		VertexID:             vertexID,
		Status:               task.Status,
		LatestAcknowledgedAt: unixMillis(task.LatestAckTimestamp),
		Duration:             milliseconds(task.EndToEndDuration),
		CheckpointedSize:     task.CheckpointedSize,
		StateSize:            task.StateSize,
		AlignmentBuffered:    task.AlignmentBuffered,
		ProcessedData:        task.ProcessedData,
		PersistedData:        task.PersistedData,
		Subtasks:             task.NumSubtasks,
		AcknowledgedSubtasks: task.AcknowledgedSubtasks,
	}
}

func milliseconds(value int64) time.Duration {
	return time.Duration(value) * time.Millisecond
}

type checkpointConfigResponse struct {
	Mode              string `json:"mode"`
	Interval          int64  `json:"interval"`
	Timeout           int64  `json:"timeout"`
	MinimumPause      int64  `json:"min_pause"`
	MaximumConcurrent int    `json:"max_concurrent"`
	Externalization   struct {
		Enabled              bool `json:"enabled"`
		DeleteOnCancellation bool `json:"delete_on_cancellation"`
	} `json:"externalization"`
	StateBackend                     string `json:"state_backend"`
	CheckpointStorage                string `json:"checkpoint_storage"`
	UnalignedCheckpoints             bool   `json:"unaligned_checkpoints"`
	TolerableFailedCheckpoints       int    `json:"tolerable_failed_checkpoints"`
	AlignedCheckpointTimeout         int64  `json:"aligned_checkpoint_timeout"`
	CheckpointsAfterTasksFinish      bool   `json:"checkpoints_after_tasks_finish"`
	StateChangelogEnabled            bool   `json:"state_changelog_enabled"`
	ChangelogMaterializationInterval int64  `json:"changelog_periodic_materialization_interval"`
	ChangelogStorage                 string `json:"changelog_storage"`
}

type checkpointDistributionResponse struct {
	Min     flexibleNumber `json:"min"`
	Max     flexibleNumber `json:"max"`
	Average flexibleNumber `json:"avg"`
	P50     flexibleNumber `json:"p50"`
	P90     flexibleNumber `json:"p90"`
	P95     flexibleNumber `json:"p95"`
	P99     flexibleNumber `json:"p99"`
	P999    flexibleNumber `json:"p999"`
}

type checkpointSubtaskSummaryResponse struct {
	CheckpointedSize checkpointDistributionResponse `json:"checkpointed_size"`
	StateSize        checkpointDistributionResponse `json:"state_size"`
	EndToEndDuration checkpointDistributionResponse `json:"end_to_end_duration"`
	Checkpoint       struct {
		Sync  checkpointDistributionResponse `json:"sync"`
		Async checkpointDistributionResponse `json:"async"`
	} `json:"checkpoint_duration"`
	Alignment struct {
		Buffered  checkpointDistributionResponse `json:"buffered"`
		Processed checkpointDistributionResponse `json:"processed"`
		Persisted checkpointDistributionResponse `json:"persisted"`
		Duration  checkpointDistributionResponse `json:"duration"`
	} `json:"alignment"`
	StartDelay checkpointDistributionResponse `json:"start_delay"`
}

type checkpointSubtaskResponse struct {
	Status               string                           `json:"status"`
	LatestAckTimestamp   int64                            `json:"latest_ack_timestamp"`
	EndToEndDuration     int64                            `json:"end_to_end_duration"`
	CheckpointedSize     int64                            `json:"checkpointed_size"`
	StateSize            int64                            `json:"state_size"`
	AlignmentBuffered    int64                            `json:"alignment_buffered"`
	ProcessedData        int64                            `json:"processed_data"`
	PersistedData        int64                            `json:"persisted_data"`
	NumSubtasks          int                              `json:"num_subtasks"`
	AcknowledgedSubtasks int                              `json:"num_acknowledged_subtasks"`
	Summary              checkpointSubtaskSummaryResponse `json:"summary"`
	Subtasks             []struct {
		Index                 int    `json:"index"`
		Status                string `json:"status"`
		AcknowledgedTimestamp int64  `json:"ack_timestamp"`
		EndToEndDuration      int64  `json:"end_to_end_duration"`
		CheckpointedSize      int64  `json:"checkpointed_size"`
		StateSize             int64  `json:"state_size"`
		Checkpoint            struct {
			Sync  int64 `json:"sync"`
			Async int64 `json:"async"`
		} `json:"checkpoint"`
		Alignment struct {
			Buffered  int64 `json:"buffered"`
			Processed int64 `json:"processed"`
			Persisted int64 `json:"persisted"`
			Duration  int64 `json:"duration"`
		} `json:"alignment"`
		StartDelay int64 `json:"start_delay"`
		Unaligned  bool  `json:"unaligned_checkpoint"`
		Aborted    bool  `json:"aborted"`
	} `json:"subtasks"`
}

func normalizeCheckpointSubtaskSummary(response checkpointSubtaskSummaryResponse) CheckpointSubtaskSummary {
	return CheckpointSubtaskSummary{
		EndToEndDuration:   normalizeCheckpointDistribution(response.EndToEndDuration),
		CheckpointedSize:   normalizeCheckpointDistribution(response.CheckpointedSize),
		StateSize:          normalizeCheckpointDistribution(response.StateSize),
		SyncDuration:       normalizeCheckpointDistribution(response.Checkpoint.Sync),
		AsyncDuration:      normalizeCheckpointDistribution(response.Checkpoint.Async),
		AlignmentBuffered:  normalizeCheckpointDistribution(response.Alignment.Buffered),
		AlignmentProcessed: normalizeCheckpointDistribution(response.Alignment.Processed),
		AlignmentPersisted: normalizeCheckpointDistribution(response.Alignment.Persisted),
		AlignmentDuration:  normalizeCheckpointDistribution(response.Alignment.Duration),
		StartDelay:         normalizeCheckpointDistribution(response.StartDelay),
	}
}

func normalizeCheckpointDistribution(response checkpointDistributionResponse) CheckpointDistribution {
	return CheckpointDistribution{
		Min:     response.Min.Float64(),
		Average: response.Average.Float64(),
		Max:     response.Max.Float64(),
		P50:     response.P50.Float64(),
		P90:     response.P90.Float64(),
		P95:     response.P95.Float64(),
		P99:     response.P99.Float64(),
		P999:    response.P999.Float64(),
	}
}
