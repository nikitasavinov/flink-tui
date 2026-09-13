package flink

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"
)

// MetricAggregation is the reduction Flink applies across a vertex's
// subtasks when loading an arbitrary metric.
type MetricAggregation string

const (
	MetricSum MetricAggregation = "sum"
	MetricAvg MetricAggregation = "avg"
	MetricMax MetricAggregation = "max"
	MetricMin MetricAggregation = "min"
)

// MetricNames returns every metric exposed by the selected execution vertex.
// Flink returns the union of the subtask metric identifiers from this route.
func (c *Client) MetricNames(ctx context.Context, jobID, vertexID string) ([]string, error) {
	path := fmt.Sprintf(
		"/jobs/%s/vertices/%s/subtasks/metrics",
		url.PathEscape(jobID),
		url.PathEscape(vertexID),
	)
	var response []struct {
		ID string `json:"id"`
	}
	if err := c.get(ctx, path, &response); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(response))
	seen := make(map[string]struct{}, len(response))
	for _, metric := range response {
		name := strings.TrimSpace(metric.ID)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	slices.SortStableFunc(names, func(left, right string) int {
		return cmp.Compare(strings.ToLower(left), strings.ToLower(right))
	})
	return names, nil
}

// MetricValues loads arbitrary metrics either aggregated across the vertex or
// from one subtask. A negative subtask selects the aggregate route.
func (c *Client) MetricValues(
	ctx context.Context,
	jobID, vertexID string,
	subtask int,
	aggregation MetricAggregation,
	names []string,
) (map[string]float64, error) {
	names = normalizedMetricNames(names)
	values := make(map[string]float64, len(names))
	if len(names) == 0 {
		return values, nil
	}

	query := url.Values{}
	query.Set("get", strings.Join(names, ","))
	base := fmt.Sprintf(
		"/jobs/%s/vertices/%s/subtasks",
		url.PathEscape(jobID),
		url.PathEscape(vertexID),
	)
	if subtask >= 0 {
		var response []metricValue
		path := fmt.Sprintf("%s/%d/metrics?%s", base, subtask, query.Encode())
		if err := c.get(ctx, path, &response); err != nil {
			return nil, err
		}
		for _, metric := range response {
			values[metric.ID] = float64(metric.Value)
		}
		return values, nil
	}

	aggregation = normalizeMetricAggregation(aggregation)
	query.Set("agg", string(aggregation))
	var response []aggregatedMetric
	if err := c.get(ctx, base+"/metrics?"+query.Encode(), &response); err != nil {
		return nil, err
	}
	for _, metric := range response {
		switch aggregation {
		case MetricAvg:
			values[metric.ID] = float64(metric.Avg)
		case MetricMax:
			values[metric.ID] = float64(metric.Max)
		case MetricMin:
			values[metric.ID] = float64(metric.Min)
		default:
			values[metric.ID] = float64(metric.Sum)
		}
	}
	return values, nil
}

func normalizedMetricNames(names []string) []string {
	result := make([]string, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		result = append(result, name)
	}
	slices.Sort(result)
	return result
}

func normalizeMetricAggregation(aggregation MetricAggregation) MetricAggregation {
	switch aggregation {
	case MetricAvg, MetricMax, MetricMin, MetricSum:
		return aggregation
	default:
		return MetricSum
	}
}

// UserAccumulator is one user-defined accumulator reported by Flink.
type UserAccumulator struct {
	Name  string
	Type  string
	Value string
}

type SubtaskAccumulators struct {
	Subtask      int
	Attempt      int
	Endpoint     string
	Accumulators []UserAccumulator
}

type VertexAccumulators struct {
	JobID     string
	VertexID  string
	Vertex    []UserAccumulator
	Subtasks  []SubtaskAccumulators
	UpdatedAt time.Time
}

// Accumulators combines Flink's vertex accumulator response with its all-
// subtasks response. Flink 2.3 does not expose an individual-subtask
// accumulator route.
func (c *Client) Accumulators(ctx context.Context, jobID, vertexID string) (VertexAccumulators, error) {
	base := fmt.Sprintf(
		"/jobs/%s/vertices/%s",
		url.PathEscape(jobID),
		url.PathEscape(vertexID),
	)
	var vertexResponse accumulatorResponse
	var subtaskResponse struct {
		ID          string `json:"id"`
		Parallelism int    `json:"parallelism"`
		Subtasks    []struct {
			Subtask      int               `json:"subtask"`
			Attempt      int               `json:"attempt"`
			Endpoint     string            `json:"endpoint"`
			Accumulators []accumulatorItem `json:"user-accumulators"`
		} `json:"subtasks"`
	}
	var vertexErr, subtaskErr error
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		vertexErr = c.get(ctx, base+"/accumulators", &vertexResponse)
	}()
	go func() {
		defer wait.Done()
		subtaskErr = c.get(ctx, base+"/subtasks/accumulators", &subtaskResponse)
	}()
	wait.Wait()
	if vertexErr != nil {
		return VertexAccumulators{}, vertexErr
	}
	if subtaskErr != nil {
		return VertexAccumulators{}, subtaskErr
	}

	result := VertexAccumulators{
		JobID:     jobID,
		VertexID:  vertexID,
		Vertex:    accumulatorsFromItems(vertexResponse.Accumulators),
		Subtasks:  make([]SubtaskAccumulators, len(subtaskResponse.Subtasks)),
		UpdatedAt: time.Now(),
	}
	for index, subtask := range subtaskResponse.Subtasks {
		result.Subtasks[index] = SubtaskAccumulators{
			Subtask:      subtask.Subtask,
			Attempt:      subtask.Attempt,
			Endpoint:     subtask.Endpoint,
			Accumulators: accumulatorsFromItems(subtask.Accumulators),
		}
	}
	slices.SortStableFunc(result.Subtasks, func(left, right SubtaskAccumulators) int {
		return cmp.Compare(left.Subtask, right.Subtask)
	})
	return result, nil
}

type accumulatorResponse struct {
	ID           string            `json:"id"`
	Accumulators []accumulatorItem `json:"user-accumulators"`
}

type accumulatorItem struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Value string `json:"value"`
}

func accumulatorsFromItems(items []accumulatorItem) []UserAccumulator {
	result := make([]UserAccumulator, len(items))
	for index, item := range items {
		result[index] = UserAccumulator(item)
	}
	slices.SortStableFunc(result, func(left, right UserAccumulator) int {
		return cmp.Compare(strings.ToLower(left.Name), strings.ToLower(right.Name))
	})
	return result
}

type ProcessKind uint8

const (
	ProcessJobManager ProcessKind = iota
	ProcessTaskManager
)

// ProcessRef identifies the Flink JVM whose diagnostics should be loaded.
type ProcessRef struct {
	Kind          ProcessKind
	TaskManagerID string
}

func JobManagerProcess() ProcessRef { return ProcessRef{Kind: ProcessJobManager} }

func TaskManagerProcess(taskManagerID string) ProcessRef {
	return ProcessRef{Kind: ProcessTaskManager, TaskManagerID: taskManagerID}
}

func (process ProcessRef) Label() string {
	if process.Kind == ProcessTaskManager {
		return "TaskManager " + process.TaskManagerID
	}
	return "JobManager"
}

func (process ProcessRef) basePath() (string, error) {
	if process.Kind == ProcessTaskManager {
		if strings.TrimSpace(process.TaskManagerID) == "" {
			return "", errors.New("TaskManager ID is required")
		}
		return "/taskmanagers/" + url.PathEscape(process.TaskManagerID), nil
	}
	return "/jobmanager", nil
}

func (c *Client) LogFiles(ctx context.Context, process ProcessRef) ([]LogFile, error) {
	base, err := process.basePath()
	if err != nil {
		return nil, err
	}
	var response struct {
		Logs []struct {
			Name  string `json:"name"`
			Size  int64  `json:"size"`
			MTime int64  `json:"mtime"`
		} `json:"logs"`
	}
	if err := c.get(ctx, base+"/logs", &response); err != nil {
		return nil, err
	}
	logs := make([]LogFile, len(response.Logs))
	for index, item := range response.Logs {
		logs[index] = LogFile{Name: item.Name, Size: item.Size, ModifiedAt: unixMillis(item.MTime)}
	}
	slices.SortStableFunc(logs, func(left, right LogFile) int {
		return right.ModifiedAt.Compare(left.ModifiedAt)
	})
	return logs, nil
}

func (c *Client) LogContent(ctx context.Context, process ProcessRef, name string) (string, error) {
	base, err := process.basePath()
	if err != nil {
		return "", err
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return "", errors.New("log filename is required")
	}
	return c.getText(ctx, base+"/logs/"+url.PathEscape(name))
}

// CurrentLog returns the process's active log through Flink's dedicated
// endpoint. Unlike LogContent, it does not require discovering a filename and
// therefore cannot accidentally open an old rotated file.
func (c *Client) CurrentLog(ctx context.Context, process ProcessRef) (string, error) {
	base, err := process.basePath()
	if err != nil {
		return "", err
	}
	return c.getText(ctx, base+"/log")
}

func (c *Client) Stdout(ctx context.Context, process ProcessRef) (string, error) {
	base, err := process.basePath()
	if err != nil {
		return "", err
	}
	return c.getText(ctx, base+"/stdout")
}

type ThreadInfo struct {
	Name  string
	Stack string
}

func (c *Client) ThreadDump(ctx context.Context, process ProcessRef) ([]ThreadInfo, error) {
	base, err := process.basePath()
	if err != nil {
		return nil, err
	}
	var response struct {
		Threads []struct {
			Name  string `json:"threadName"`
			Stack string `json:"stringifiedThreadInfo"`
		} `json:"threadInfos"`
	}
	if err := c.get(ctx, base+"/thread-dump", &response); err != nil {
		return nil, err
	}
	threads := make([]ThreadInfo, len(response.Threads))
	for index, thread := range response.Threads {
		threads[index] = ThreadInfo{Name: thread.Name, Stack: thread.Stack}
	}
	return threads, nil
}

const maxTextResponseBytes = 16 << 20

func (c *Client) getText(ctx context.Context, path string) (string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil) // #nosec G704 -- The CLI intentionally connects to the operator-selected endpoint validated by normalizeEndpoint.
	if err != nil {
		return "", &RequestError{Service: "Flink", Method: http.MethodGet, Path: path, Err: err}
	}
	request.Header.Set("Accept", "text/plain")
	response, err := c.http.Do(request) // #nosec G704 -- Requests target the configured cluster; newHTTPClient blocks redirects to other origins.
	if err != nil {
		return "", &RequestError{Service: "Flink", Method: http.MethodGet, Path: path, Err: err}
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", newResponseError(response, "Flink", http.MethodGet, path, 1024)
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, maxTextResponseBytes+1))
	if readErr != nil {
		return "", &RequestError{Service: "Flink", Method: http.MethodGet, Path: path, Err: fmt.Errorf("read response: %w", readErr)}
	}
	if len(body) > maxTextResponseBytes {
		return "", &RequestError{Service: "Flink", Method: http.MethodGet, Path: path,
			Err: fmt.Errorf("response exceeds %d MiB; download the file directly from Flink", maxTextResponseBytes>>20)}
	}
	return string(body), nil
}
