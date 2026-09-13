package flink

import (
	"cmp"
	"context"
	"fmt"
	"html"
	"math"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"
)

const (
	planCacheTTL        = time.Minute
	planCacheMaxEntries = 64
)

type cachedPlan struct {
	plan      jobPlanResponse
	fetchedAt time.Time
}

// JobSummary is the small, stable job record used by the cluster overview.
type JobSummary struct {
	ID           string
	Name         string
	State        string
	Type         string
	StartedAt    time.Time
	Duration     time.Duration
	TotalTasks   int
	RunningTasks int
}

// ClusterOverview is the small set of cluster-wide signals shown on the home screen.
type ClusterOverview struct {
	TaskManagers   int
	SlotsTotal     int
	SlotsAvailable int
	JobsRunning    int
	JobsFinished   int
	JobsCanceled   int
	JobsFailed     int
	FlinkVersion   string
	FlinkCommit    string
	UpdatedAt      time.Time
}

type VertexDiagnostics struct {
	JobID          string
	VertexID       string
	Name           string
	ExecutionName  string
	Parallelism    int
	UpdatedAt      time.Time
	Subtasks       []Subtask
	MetricWarnings int
}

type Subtask struct {
	Index         int
	Attempt       int
	State         string
	Endpoint      string
	TaskManagerID string
	StartedAt     time.Time
	Duration      time.Duration
	Metrics       Metrics
}

// Jobs returns every running and completed job, with active jobs first.
func (c *Client) Jobs(ctx context.Context) ([]JobSummary, error) {
	var response jobsOverviewResponse
	if err := c.get(ctx, "/jobs/overview", &response); err != nil {
		return nil, err
	}
	jobs := make([]JobSummary, 0, len(response.Jobs))
	for _, job := range response.Jobs {
		jobs = append(jobs, JobSummary{
			ID:           job.ID,
			Name:         job.Name,
			State:        job.State,
			Type:         job.JobType,
			StartedAt:    unixMillis(job.StartTime),
			Duration:     time.Duration(job.Duration) * time.Millisecond,
			TotalTasks:   taskCount(job.Tasks, "total"),
			RunningTasks: taskCount(job.Tasks, "running"),
		})
	}
	slices.SortStableFunc(jobs, func(left, right JobSummary) int {
		if comparison := cmp.Compare(jobStateRank(left.State), jobStateRank(right.State)); comparison != 0 {
			return comparison
		}
		return right.StartedAt.Compare(left.StartedAt)
	})
	return jobs, nil
}

// ClusterOverview returns cluster capacity and aggregate job health.
func (c *Client) ClusterOverview(ctx context.Context) (ClusterOverview, error) {
	var response clusterOverviewResponse
	if err := c.get(ctx, "/overview", &response); err != nil {
		return ClusterOverview{}, err
	}
	return ClusterOverview{
		TaskManagers:   response.TaskManagers,
		SlotsTotal:     response.SlotsTotal,
		SlotsAvailable: response.SlotsAvailable,
		JobsRunning:    response.JobsRunning,
		JobsFinished:   response.JobsFinished,
		JobsCanceled:   response.JobsCanceled,
		JobsFailed:     response.JobsFailed,
		FlinkVersion:   response.FlinkVersion,
		FlinkCommit:    response.FlinkCommit,
		UpdatedAt:      time.Now(),
	}, nil
}

func (c *Client) loadPlan(ctx context.Context, jobID string, force bool) (jobPlanResponse, error) {
	if !force {
		c.mu.RLock()
		cached, ok := c.plans[jobID]
		c.mu.RUnlock()
		if ok && time.Since(cached.fetchedAt) < planCacheTTL {
			return cached.plan, nil
		}
	}

	var plan jobPlanResponse
	if err := c.get(ctx, "/jobs/"+url.PathEscape(jobID)+"/plan", &plan); err != nil {
		return jobPlanResponse{}, err
	}
	c.mu.Lock()
	now := time.Now()
	for id, cached := range c.plans {
		if now.Sub(cached.fetchedAt) >= planCacheTTL {
			delete(c.plans, id)
		}
	}
	if _, replacing := c.plans[jobID]; !replacing && len(c.plans) >= planCacheMaxEntries {
		var oldestID string
		var oldestAt time.Time
		for id, cached := range c.plans {
			if oldestAt.IsZero() || cached.fetchedAt.Before(oldestAt) {
				oldestID, oldestAt = id, cached.fetchedAt
			}
		}
		delete(c.plans, oldestID)
	}
	c.plans[jobID] = cachedPlan{plan: plan, fetchedAt: now}
	c.mu.Unlock()
	return plan, nil
}

// VertexDiagnostics loads subtask placement and live metrics only for one open vertex.
func (c *Client) VertexDiagnostics(ctx context.Context, jobID, vertexID string) (VertexDiagnostics, error) {
	path := fmt.Sprintf("/jobs/%s/vertices/%s", url.PathEscape(jobID), url.PathEscape(vertexID))
	var response vertexDiagnosticsResponse
	if err := c.get(ctx, path, &response); err != nil {
		return VertexDiagnostics{}, err
	}

	diagnostics := VertexDiagnostics{
		JobID:    jobID,
		VertexID: vertexID,
		Name:     cleanName(response.Name),
		ExecutionName: strings.Join(strings.Fields(
			html.UnescapeString(htmlTag.ReplaceAllString(response.Name, "")),
		), " "),
		Parallelism: response.Parallelism,
		UpdatedAt:   time.Now(),
		Subtasks:    make([]Subtask, len(response.Subtasks)),
	}
	for index, subtask := range response.Subtasks {
		diagnostics.Subtasks[index] = Subtask{
			Index:         subtask.Index,
			Attempt:       subtask.Attempt,
			State:         subtask.Status,
			Endpoint:      subtask.Endpoint,
			TaskManagerID: subtask.TaskManagerID,
			StartedAt:     unixMillis(subtask.StartTime),
			Duration:      time.Duration(subtask.Duration) * time.Millisecond,
		}
	}

	type result struct {
		position int
		metrics  Metrics
		err      error
	}
	results := make(chan result, len(diagnostics.Subtasks))
	semaphore := make(chan struct{}, 8)
	var wg sync.WaitGroup
	for position := range diagnostics.Subtasks {
		wg.Add(1)
		go func(position int) {
			defer wg.Done()
			semaphore <- struct{}{}
			metrics, err := c.subtaskMetrics(ctx, jobID, vertexID, diagnostics.Subtasks[position].Index)
			<-semaphore
			results <- result{position: position, metrics: metrics, err: err}
		}(position)
	}
	wg.Wait()
	close(results)
	for result := range results {
		if result.err != nil {
			diagnostics.MetricWarnings++
			continue
		}
		diagnostics.Subtasks[result.position].Metrics = result.metrics
	}
	slices.SortFunc(diagnostics.Subtasks, func(left, right Subtask) int {
		return cmp.Compare(left.Index, right.Index)
	})
	return diagnostics, nil
}

func (c *Client) subtaskMetrics(ctx context.Context, jobID, vertexID string, subtask int) (Metrics, error) {
	query := url.Values{}
	query.Set("get", strings.Join([]string{
		"numRecordsInPerSecond",
		"numRecordsOutPerSecond",
		"numRecordsIn",
		"numRecordsOut",
		"numBytesIn",
		"numBytesOut",
		"busyTimeMsPerSecond",
		"backPressuredTimeMsPerSecond",
		"idleTimeMsPerSecond",
		"currentInputWatermark",
	}, ","))
	path := fmt.Sprintf(
		"/jobs/%s/vertices/%s/subtasks/%d/metrics?%s",
		url.PathEscape(jobID),
		url.PathEscape(vertexID),
		subtask,
		query.Encode(),
	)
	var response []metricValue
	if err := c.get(ctx, path, &response); err != nil {
		return Metrics{}, err
	}
	byID := make(map[string]flexibleNumber, len(response))
	for _, metric := range response {
		byID[metric.ID] = metric.Value
	}
	backpressure := percent(byID["backPressuredTimeMsPerSecond"].Float64())
	metrics := Metrics{
		RecordsInPerSecond:  finite(byID["numRecordsInPerSecond"].Float64()),
		RecordsOutPerSecond: finite(byID["numRecordsOutPerSecond"].Float64()),
		RecordsIn:           finite(byID["numRecordsIn"].Float64()),
		RecordsOut:          finite(byID["numRecordsOut"].Float64()),
		BytesIn:             finite(byID["numBytesIn"].Float64()),
		BytesOut:            finite(byID["numBytesOut"].Float64()),
		BusyPercent:         percent(byID["busyTimeMsPerSecond"].Float64()),
		BackpressurePercent: backpressure,
		BackpressureLevel:   ClassifyBackpressure(backpressure),
		IdlePercent:         percent(byID["idleTimeMsPerSecond"].Float64()),
	}
	if watermark, ok := byID["currentInputWatermark"]; ok {
		metrics.LowWatermark, metrics.WatermarkKnown = parseWatermark(watermark.Float64())
	}
	return metrics, nil
}

type vertexDiagnosticsResponse struct {
	Name        string `json:"name"`
	Parallelism int    `json:"parallelism"`
	Subtasks    []struct {
		Index         int    `json:"subtask"`
		Status        string `json:"status"`
		Attempt       int    `json:"attempt"`
		Endpoint      string `json:"endpoint"`
		TaskManagerID string `json:"taskmanager-id"`
		StartTime     int64  `json:"start-time"`
		Duration      int64  `json:"duration"`
	} `json:"subtasks"`
}

type metricValue struct {
	ID    string         `json:"id"`
	Value flexibleNumber `json:"value"`
}

type checkpointStatsResponse struct {
	Counts struct {
		Restored   int `json:"restored"`
		Total      int `json:"total"`
		InProgress int `json:"in_progress"`
		Completed  int `json:"completed"`
		Failed     int `json:"failed"`
	} `json:"counts"`
	Summary checkpointStatsSummaryResponse `json:"summary"`
	Latest  struct {
		Completed *checkpointItem `json:"completed"`
		Failed    *checkpointItem `json:"failed"`
	} `json:"latest"`
	History []checkpointItem `json:"history"`
}

type checkpointStatsSummaryResponse struct {
	CheckpointedSize  checkpointDistributionResponse `json:"checkpointed_size"`
	StateSize         checkpointDistributionResponse `json:"state_size"`
	EndToEndDuration  checkpointDistributionResponse `json:"end_to_end_duration"`
	AlignmentBuffered checkpointDistributionResponse `json:"alignment_buffered"`
	ProcessedData     checkpointDistributionResponse `json:"processed_data"`
	PersistedData     checkpointDistributionResponse `json:"persisted_data"`
}

type checkpointItem struct {
	ID                   int64                     `json:"id"`
	Status               string                    `json:"status"`
	Type                 string                    `json:"checkpoint_type"`
	TriggerTimestamp     int64                     `json:"trigger_timestamp"`
	LatestAckTimestamp   int64                     `json:"latest_ack_timestamp"`
	EndToEndDuration     int64                     `json:"end_to_end_duration"`
	CheckpointedSize     int64                     `json:"checkpointed_size"`
	StateSize            int64                     `json:"state_size"`
	AlignmentBuffered    int64                     `json:"alignment_buffered"`
	ProcessedData        int64                     `json:"processed_data"`
	PersistedData        int64                     `json:"persisted_data"`
	NumSubtasks          int                       `json:"num_subtasks"`
	AcknowledgedSubtasks int                       `json:"num_acknowledged_subtasks"`
	ExternalPath         string                    `json:"external_path"`
	FailureMessage       string                    `json:"failure_message"`
	IsSavepoint          bool                      `json:"is_savepoint"`
	Discarded            bool                      `json:"discarded"`
	Tasks                map[string]checkpointTask `json:"tasks"`
}

type checkpointTask struct {
	Status               string `json:"status"`
	LatestAckTimestamp   int64  `json:"latest_ack_timestamp"`
	EndToEndDuration     int64  `json:"end_to_end_duration"`
	CheckpointedSize     int64  `json:"checkpointed_size"`
	StateSize            int64  `json:"state_size"`
	AlignmentBuffered    int64  `json:"alignment_buffered"`
	ProcessedData        int64  `json:"processed_data"`
	PersistedData        int64  `json:"persisted_data"`
	NumSubtasks          int    `json:"num_subtasks"`
	AcknowledgedSubtasks int    `json:"num_acknowledged_subtasks"`
}

func summarizeCheckpoints(response checkpointStatsResponse) CheckpointSummary {
	summary := CheckpointSummary{
		Total:      response.Counts.Total,
		Completed:  response.Counts.Completed,
		Failed:     response.Counts.Failed,
		InProgress: response.Counts.InProgress,
		Restored:   response.Counts.Restored,
		Statistics: CheckpointStatistics{
			EndToEndDuration:  normalizeCheckpointDistribution(response.Summary.EndToEndDuration),
			CheckpointedSize:  normalizeCheckpointDistribution(response.Summary.CheckpointedSize),
			StateSize:         normalizeCheckpointDistribution(response.Summary.StateSize),
			AlignmentBuffered: normalizeCheckpointDistribution(response.Summary.AlignmentBuffered),
			ProcessedData:     normalizeCheckpointDistribution(response.Summary.ProcessedData),
			PersistedData:     normalizeCheckpointDistribution(response.Summary.PersistedData),
		},
	}
	if latest := response.Latest.Completed; latest != nil {
		summary.LatestID = latest.ID
		summary.LatestDuration = time.Duration(latest.EndToEndDuration) * time.Millisecond
		summary.LatestSize = latest.CheckpointedSize
		if summary.LatestSize == 0 {
			summary.LatestSize = latest.StateSize
		}
		completedAt := latest.LatestAckTimestamp
		if completedAt <= 0 {
			completedAt = latest.TriggerTimestamp + latest.EndToEndDuration
		}
		summary.LatestCompletedAt = unixMillis(completedAt)
	}
	if latest := response.Latest.Failed; latest != nil {
		failed := checkpointFromItem(*latest)
		if failed.Status == "" {
			failed.Status = "FAILED"
		}
		summary.LatestFailed = &failed
	}
	summary.History = make([]Checkpoint, 0, len(response.History))
	for _, item := range response.History {
		summary.History = append(summary.History, checkpointFromItem(item))
	}
	slices.SortStableFunc(summary.History, func(left, right Checkpoint) int {
		return cmp.Compare(right.ID, left.ID)
	})
	if summary.LatestFailed == nil {
		for index := range summary.History {
			if summary.History[index].Status == "FAILED" {
				failed := summary.History[index]
				summary.LatestFailed = &failed
				break
			}
		}
	}
	return summary
}

func checkpointFromItem(item checkpointItem) Checkpoint {
	completedAt := item.LatestAckTimestamp
	if completedAt <= 0 && item.EndToEndDuration > 0 {
		completedAt = item.TriggerTimestamp + item.EndToEndDuration
	}
	return Checkpoint{
		ID:                   item.ID,
		Status:               item.Status,
		Type:                 item.Type,
		TriggeredAt:          unixMillis(item.TriggerTimestamp),
		CompletedAt:          unixMillis(completedAt),
		Duration:             time.Duration(item.EndToEndDuration) * time.Millisecond,
		CheckpointedSize:     item.CheckpointedSize,
		StateSize:            item.StateSize,
		AlignmentBuffered:    item.AlignmentBuffered,
		ProcessedData:        item.ProcessedData,
		PersistedData:        item.PersistedData,
		Subtasks:             item.NumSubtasks,
		AcknowledgedSubtasks: item.AcknowledgedSubtasks,
		ExternalPath:         item.ExternalPath,
		Failure:              cleanCheckpointFailure(item.FailureMessage),
		IsSavepoint:          item.IsSavepoint,
		Discarded:            item.Discarded,
	}
}

func cleanCheckpointFailure(value string) string {
	value = strings.TrimSpace(value)
	if line, _, ok := strings.Cut(value, "\n"); ok {
		value = line
	}
	return value
}

type jobExceptionsResponse struct {
	ExceptionHistory struct {
		Entries   []exceptionEntry `json:"entries"`
		Truncated bool             `json:"truncated"`
	} `json:"exceptionHistory"`
	RootException string `json:"root-exception"`
	Timestamp     int64  `json:"timestamp"`
	AllExceptions []struct {
		Exception string `json:"exception"`
		Timestamp int64  `json:"timestamp"`
	} `json:"all-exceptions"`
}

type exceptionEntry struct {
	ExceptionName        string            `json:"exceptionName"`
	Stacktrace           string            `json:"stacktrace"`
	Timestamp            int64             `json:"timestamp"`
	TaskName             string            `json:"taskName"`
	Endpoint             string            `json:"endpoint"`
	TaskManagerID        string            `json:"taskManagerId"`
	FailureLabels        map[string]string `json:"failureLabels"`
	ConcurrentExceptions []exceptionEntry  `json:"concurrentExceptions"`
}

func jobExceptionFromEntry(entry exceptionEntry) JobException {
	name := firstLine(entry.ExceptionName)
	if name == "" {
		name = firstLine(entry.Stacktrace)
	}
	result := JobException{
		Name:          name,
		Stacktrace:    strings.TrimSpace(entry.Stacktrace),
		At:            unixMillis(entry.Timestamp),
		TaskName:      strings.TrimSpace(entry.TaskName),
		Endpoint:      strings.TrimSpace(entry.Endpoint),
		TaskManagerID: strings.TrimSpace(entry.TaskManagerID),
	}
	if len(entry.FailureLabels) > 0 {
		result.FailureLabels = make(map[string]string, len(entry.FailureLabels))
		for key, value := range entry.FailureLabels {
			result.FailureLabels[key] = value
		}
	}
	if len(entry.ConcurrentExceptions) > 0 {
		result.ConcurrentExceptions = make([]JobException, len(entry.ConcurrentExceptions))
		for index, concurrent := range entry.ConcurrentExceptions {
			result.ConcurrentExceptions[index] = jobExceptionFromEntry(concurrent)
		}
	}
	return result
}

func summarizeExceptions(response jobExceptionsResponse) ExceptionSummary {
	summary := ExceptionSummary{
		Count:     len(response.ExceptionHistory.Entries),
		Truncated: response.ExceptionHistory.Truncated,
		Entries:   make([]JobException, 0, len(response.ExceptionHistory.Entries)),
	}
	var latestTimestamp int64
	for _, entry := range response.ExceptionHistory.Entries {
		exception := jobExceptionFromEntry(entry)
		summary.Entries = append(summary.Entries, exception)
		if summary.Latest == "" || entry.Timestamp >= latestTimestamp {
			latestTimestamp = entry.Timestamp
			summary.Latest = exception.Name
		}
	}
	if summary.Count == 0 && response.RootException != "" {
		summary.Count = 1 + len(response.AllExceptions)
		summary.Latest = firstLine(response.RootException)
		latestTimestamp = response.Timestamp
		summary.Entries = append(summary.Entries, JobException{
			Name:       summary.Latest,
			Stacktrace: strings.TrimSpace(response.RootException),
			At:         unixMillis(response.Timestamp),
		})
		for _, entry := range response.AllExceptions {
			name := firstLine(entry.Exception)
			summary.Entries = append(summary.Entries, JobException{
				Name:       name,
				Stacktrace: strings.TrimSpace(entry.Exception),
				At:         unixMillis(entry.Timestamp),
			})
			if entry.Timestamp >= latestTimestamp {
				latestTimestamp = entry.Timestamp
				summary.Latest = name
			}
		}
	}
	slices.SortStableFunc(summary.Entries, func(left, right JobException) int {
		return right.At.Compare(left.At)
	})
	summary.LatestAt = unixMillis(latestTimestamp)
	return summary
}

func firstLine(value string) string {
	value = strings.TrimSpace(value)
	if index := strings.IndexByte(value, '\n'); index >= 0 {
		value = value[:index]
	}
	return strings.TrimSpace(value)
}

func taskCount(tasks map[string]int, name string) int {
	for key, value := range tasks {
		if strings.EqualFold(key, name) {
			return value
		}
	}
	return 0
}

func jobStateRank(state string) int {
	switch state {
	case "RUNNING", "RESTARTING", "INITIALIZING", "CREATED":
		return 0
	case "FAILING", "CANCELLING", "RECONCILING":
		return 1
	default:
		return 2
	}
}

func unixMillis(value int64) time.Time {
	if value <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(value)
}

func parseWatermark(value float64) (int64, bool) {
	// float64 rounds Long.MAX_VALUE up to 2^63, which cannot be converted
	// back to int64. Treat that terminal watermark as unknown rather than
	// wrapping it into a negative timestamp.
	if math.IsNaN(value) || math.IsInf(value, 0) || value <= -9e18 || value >= float64(math.MaxInt64) {
		return 0, false
	}
	return int64(value), true
}
