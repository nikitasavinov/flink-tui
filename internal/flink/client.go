package flink

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	demoJobName           = "Flink TUI Demo"
	DefaultExceptionLimit = 20
)

var htmlTag = regexp.MustCompile(`<[^>]*>`)

type Client struct {
	baseURL string
	http    *http.Client
	mu      sync.RWMutex
	plans   map[string]cachedPlan
}

type Snapshot struct {
	Endpoint    string
	JobID       string
	JobName     string
	JobState    string
	JobType     string
	Scheduler   string
	IsStoppable bool
	StartedAt   time.Time
	Duration    time.Duration
	UpdatedAt   time.Time
	Transitions []StateTransition
	Nodes       []Node
	Checkpoints CheckpointSummary
	Exceptions  ExceptionSummary
	Warnings    int
	Issues      []SnapshotIssue
}

type SnapshotIssueKind string

const (
	SnapshotIssueMetrics     SnapshotIssueKind = "metrics"
	SnapshotIssueCheckpoints SnapshotIssueKind = "checkpoints"
	SnapshotIssueExceptions  SnapshotIssueKind = "exceptions"
)

// SnapshotIssue identifies a non-fatal subresource failure in an otherwise
// usable job snapshot.
type SnapshotIssue struct {
	Kind     SnapshotIssueKind
	VertexID string
	Err      error
}

// StateTransition is one non-zero job state timestamp reported by Flink.
type StateTransition struct {
	State string
	At    time.Time
}

type Node struct {
	ID             string
	Name           string
	State          string
	Parallelism    int
	MaxParallelism int
	StartedAt      time.Time
	Duration       time.Duration
	Inputs         []Input
	Metrics        Metrics
}

type Input struct {
	ID           string
	ShipStrategy string
	Exchange     string
}

type Metrics struct {
	RecordsInPerSecond  float64
	RecordsOutPerSecond float64
	RecordsIn           float64
	RecordsOut          float64
	BytesIn             float64
	BytesOut            float64
	BusyPercent         float64
	BackpressurePercent float64
	BackpressureLevel   string
	IdlePercent         float64
	DataSkewPercent     float64
	LowWatermark        int64
	WatermarkKnown      bool
}

type CheckpointSummary struct {
	Total             int
	Completed         int
	Failed            int
	InProgress        int
	Restored          int
	Statistics        CheckpointStatistics
	LatestID          int64
	LatestCompletedAt time.Time
	LatestDuration    time.Duration
	LatestSize        int64
	LatestFailed      *Checkpoint
	History           []Checkpoint
}

type Checkpoint struct {
	ID                   int64
	Status               string
	Type                 string
	TriggeredAt          time.Time
	CompletedAt          time.Time
	Duration             time.Duration
	CheckpointedSize     int64
	StateSize            int64
	AlignmentBuffered    int64
	ProcessedData        int64
	PersistedData        int64
	Subtasks             int
	AcknowledgedSubtasks int
	ExternalPath         string
	Failure              string
	IsSavepoint          bool
	Discarded            bool
}

type ExceptionSummary struct {
	Count     int
	Truncated bool
	Latest    string
	LatestAt  time.Time
	Entries   []JobException
}

// JobException preserves enough of Flink's exception history for terminal
// list/detail navigation without re-fetching the job on every selection.
type JobException struct {
	Name                 string
	Stacktrace           string
	At                   time.Time
	TaskName             string
	Endpoint             string
	TaskManagerID        string
	FailureLabels        map[string]string
	ConcurrentExceptions []JobException
}

const (
	BackpressureOK   = "OK"
	BackpressureLow  = "LOW"
	BackpressureHigh = "HIGH"
)

func NewClient(endpoint string) (*Client, error) {
	return NewClientWithConfig(endpoint, HTTPClientConfig{})
}

// NewClientWithConfig builds a Flink REST client with optional authentication,
// custom certificate authorities, and mutual TLS.
func NewClientWithConfig(endpoint string, config HTTPClientConfig) (*Client, error) {
	baseURL, err := normalizeEndpoint(endpoint, "Flink")
	if err != nil {
		return nil, err
	}
	httpClient, err := newHTTPClient(15*time.Second, config)
	if err != nil {
		return nil, fmt.Errorf("configure Flink HTTP client: %w", err)
	}
	return &Client{
		baseURL: baseURL,
		// Snapshot callers use shorter per-request contexts. The longer client
		// ceiling leaves enough room for complete multi-megabyte log bodies and
		// large thread dumps on remote clusters.
		http:  httpClient,
		plans: make(map[string]cachedPlan),
	}, nil
}

func (c *Client) Endpoint() string { return c.baseURL }

func (c *Client) Snapshot(ctx context.Context, preferredJobID string) (Snapshot, error) {
	return c.SnapshotWithExceptionLimit(ctx, preferredJobID, DefaultExceptionLimit)
}

// SnapshotWithExceptionLimit returns a regular job snapshot while allowing
// callers to page farther into Flink's retained root-failure history.
func (c *Client) SnapshotWithExceptionLimit(ctx context.Context, preferredJobID string, exceptionLimit int) (Snapshot, error) {
	if exceptionLimit <= 0 {
		exceptionLimit = DefaultExceptionLimit
	}
	job, err := c.selectJob(ctx, preferredJobID)
	if err != nil {
		return Snapshot{}, err
	}

	var plan jobPlanResponse
	var details jobDetailsResponse
	var checkpoints checkpointStatsResponse
	var exceptions jobExceptionsResponse
	var planErr, detailsErr, checkpointsErr, exceptionsErr error
	var wg sync.WaitGroup
	wg.Add(4)
	go func() {
		defer wg.Done()
		plan, planErr = c.loadPlan(ctx, job.ID, false)
	}()
	go func() {
		defer wg.Done()
		detailsErr = c.get(ctx, "/jobs/"+url.PathEscape(job.ID), &details)
	}()
	go func() {
		defer wg.Done()
		checkpointsErr = c.get(ctx, "/jobs/"+url.PathEscape(job.ID)+"/checkpoints", &checkpoints)
	}()
	go func() {
		defer wg.Done()
		query := url.Values{}
		query.Set("maxExceptions", strconv.Itoa(exceptionLimit))
		exceptionsErr = c.get(ctx, "/jobs/"+url.PathEscape(job.ID)+"/exceptions?"+query.Encode(), &exceptions)
	}()
	wg.Wait()
	if planErr != nil {
		return Snapshot{}, planErr
	}
	if detailsErr != nil {
		return Snapshot{}, detailsErr
	}
	if len(details.Vertices) != len(plan.Plan.Nodes) {
		plan, err = c.loadPlan(ctx, job.ID, true)
		if err != nil {
			return Snapshot{}, err
		}
	}

	states := make(map[string]vertexDetails, len(details.Vertices))
	for _, vertex := range details.Vertices {
		states[vertex.ID] = vertex
	}

	nodes := make([]Node, len(plan.Plan.Nodes))
	for index, planNode := range plan.Plan.Nodes {
		name := cleanName(planNode.Description)
		state := "UNKNOWN"
		parallelism := planNode.Parallelism
		if detail, ok := states[planNode.ID]; ok {
			if clean := cleanName(detail.Name); clean != "" {
				name = clean
			}
			state = detail.Status
			if detail.Parallelism > 0 {
				parallelism = detail.Parallelism
			}
		}
		if name == "" {
			name = cleanName(planNode.Operator)
		}
		if name == "" {
			name = shortID(planNode.ID)
		}

		inputs := make([]Input, 0, len(planNode.Inputs))
		for _, input := range planNode.Inputs {
			inputs = append(inputs, Input{
				ID:           input.ID,
				ShipStrategy: input.ShipStrategy,
				Exchange:     input.Exchange,
			})
		}
		nodes[index] = Node{
			ID:             planNode.ID,
			Name:           name,
			State:          state,
			Parallelism:    parallelism,
			MaxParallelism: states[planNode.ID].MaxParallelism,
			StartedAt:      unixMillis(states[planNode.ID].StartTime),
			Duration:       time.Duration(states[planNode.ID].Duration) * time.Millisecond,
			Inputs:         inputs,
		}
	}

	issues := c.loadMetrics(ctx, job.ID, nodes)
	if checkpointsErr != nil {
		issues = append(issues, SnapshotIssue{Kind: SnapshotIssueCheckpoints, Err: checkpointsErr})
	}
	if exceptionsErr != nil {
		issues = append(issues, SnapshotIssue{Kind: SnapshotIssueExceptions, Err: exceptionsErr})
	}
	jobName := job.Name
	if details.Name != "" {
		jobName = details.Name
	}
	return Snapshot{
		Endpoint:    c.baseURL,
		JobID:       job.ID,
		JobName:     jobName,
		JobState:    details.State,
		JobType:     details.JobType,
		Scheduler:   details.Scheduler,
		IsStoppable: details.IsStoppable,
		StartedAt:   unixMillis(details.StartTime),
		Duration:    time.Duration(details.Duration) * time.Millisecond,
		UpdatedAt:   time.Now(),
		Transitions: stateTransitions(details.Timestamps),
		Nodes:       nodes,
		Checkpoints: summarizeCheckpoints(checkpoints),
		Exceptions:  summarizeExceptions(exceptions),
		Warnings:    len(issues),
		Issues:      issues,
	}, nil
}

func (c *Client) selectJob(ctx context.Context, preferredJobID string) (JobSummary, error) {
	if preferredJobID != "" {
		return JobSummary{ID: preferredJobID}, nil
	}
	jobs, err := c.Jobs(ctx)
	if err != nil {
		return JobSummary{}, err
	}
	if len(jobs) == 0 {
		return JobSummary{}, errors.New("flink has no jobs yet; the demo submitter may still be starting")
	}
	for _, job := range jobs {
		if job.State == "RUNNING" && job.Name == demoJobName {
			return job, nil
		}
	}
	for _, job := range jobs {
		if job.State == "RUNNING" {
			return job, nil
		}
	}
	return JobSummary{}, fmt.Errorf("flink has %d job(s), but none are running", len(jobs))
}

func (c *Client) loadMetrics(ctx context.Context, jobID string, nodes []Node) []SnapshotIssue {
	type result struct {
		index   int
		metrics Metrics
		err     error
	}
	results := make(chan result, len(nodes))
	semaphore := make(chan struct{}, 4)
	var wg sync.WaitGroup
	for index := range nodes {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			semaphore <- struct{}{}
			metrics, err := c.vertexMetrics(ctx, jobID, nodes[index].ID, len(nodes[index].Inputs) > 0)
			<-semaphore
			results <- result{index: index, metrics: metrics, err: err}
		}(index)
	}
	wg.Wait()
	close(results)

	issues := make([]SnapshotIssue, 0)
	for result := range results {
		if result.err != nil {
			issues = append(issues, SnapshotIssue{
				Kind:     SnapshotIssueMetrics,
				VertexID: nodes[result.index].ID,
				Err:      result.err,
			})
			continue
		}
		nodes[result.index].Metrics = result.metrics
	}
	return issues
}

func (c *Client) vertexMetrics(ctx context.Context, jobID, vertexID string, includeWatermark bool) (Metrics, error) {
	query := url.Values{}
	metricNames := []string{
		"numRecordsInPerSecond",
		"numRecordsOutPerSecond",
		"numRecordsIn",
		"numRecordsOut",
		"numBytesIn",
		"numBytesOut",
		"busyTimeMsPerSecond",
		"backPressuredTimeMsPerSecond",
		"idleTimeMsPerSecond",
	}
	if includeWatermark {
		metricNames = append(metricNames, "currentInputWatermark")
	}
	query.Set("get", strings.Join(metricNames, ","))
	query.Set("agg", "min,max,avg,sum,skew")
	path := fmt.Sprintf(
		"/jobs/%s/vertices/%s/subtasks/metrics?%s",
		url.PathEscape(jobID),
		url.PathEscape(vertexID),
		query.Encode(),
	)
	var response []aggregatedMetric
	if err := c.get(ctx, path, &response); err != nil {
		return Metrics{}, err
	}
	// Some vertices (notably sources and some batch operators) do not expose an
	// input watermark. Flink may return an empty aggregate result when a missing
	// metric is mixed into the request, so retry without it instead of losing all
	// busy/backpressure/rate data for that vertex.
	if len(response) == 0 && includeWatermark {
		return c.vertexMetrics(ctx, jobID, vertexID, false)
	}

	byID := make(map[string]aggregatedMetric, len(response))
	for _, metric := range response {
		byID[metric.ID] = metric
	}
	backpressure := percent(byID["backPressuredTimeMsPerSecond"].Max.Float64())
	metrics := Metrics{
		RecordsInPerSecond:  finite(byID["numRecordsInPerSecond"].Sum.Float64()),
		RecordsOutPerSecond: finite(byID["numRecordsOutPerSecond"].Sum.Float64()),
		RecordsIn:           finite(byID["numRecordsIn"].Sum.Float64()),
		RecordsOut:          finite(byID["numRecordsOut"].Sum.Float64()),
		BytesIn:             finite(byID["numBytesIn"].Sum.Float64()),
		BytesOut:            finite(byID["numBytesOut"].Sum.Float64()),
		BusyPercent:         percent(byID["busyTimeMsPerSecond"].Max.Float64()),
		BackpressurePercent: backpressure,
		BackpressureLevel:   ClassifyBackpressure(backpressure),
		IdlePercent:         percent(byID["idleTimeMsPerSecond"].Avg.Float64()),
		DataSkewPercent:     math.Min(100, finite(byID["numRecordsInPerSecond"].Skew.Float64())),
	}
	if watermark, ok := byID["currentInputWatermark"]; ok {
		metrics.LowWatermark, metrics.WatermarkKnown = parseWatermark(watermark.Min.Float64())
	}
	return metrics, nil
}

func (c *Client) get(ctx context.Context, path string, target any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil) // #nosec G704 -- The CLI intentionally connects to the operator-selected endpoint validated by normalizeEndpoint.
	if err != nil {
		return &RequestError{Service: "Flink", Method: http.MethodGet, Path: path, Err: err}
	}
	request.Header.Set("Accept", "application/json")
	response, err := c.http.Do(request) // #nosec G704 -- Requests target the configured cluster; newHTTPClient blocks redirects to other origins.
	if err != nil {
		return &RequestError{Service: "Flink", Method: http.MethodGet, Path: path, Err: err}
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return newResponseError(response, "Flink", http.MethodGet, path, 1024)
	}
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		return &RequestError{Service: "Flink", Method: http.MethodGet, Path: path, Err: fmt.Errorf("decode response: %w", err)}
	}
	return nil
}

type jobsOverviewResponse struct {
	Jobs []struct {
		ID        string         `json:"jid"`
		Name      string         `json:"name"`
		State     string         `json:"state"`
		JobType   string         `json:"jobType"`
		StartTime int64          `json:"start-time"`
		Duration  int64          `json:"duration"`
		Tasks     map[string]int `json:"tasks"`
	} `json:"jobs"`
}

type clusterOverviewResponse struct {
	TaskManagers   int    `json:"taskmanagers"`
	SlotsTotal     int    `json:"slots-total"`
	SlotsAvailable int    `json:"slots-available"`
	JobsRunning    int    `json:"jobs-running"`
	JobsFinished   int    `json:"jobs-finished"`
	JobsCanceled   int    `json:"jobs-cancelled"`
	JobsFailed     int    `json:"jobs-failed"`
	FlinkVersion   string `json:"flink-version"`
	FlinkCommit    string `json:"flink-commit"`
}

type jobPlanResponse struct {
	Plan struct {
		Nodes []struct {
			ID               string `json:"id"`
			Parallelism      int    `json:"parallelism"`
			Operator         string `json:"operator"`
			OperatorStrategy string `json:"operator_strategy"`
			Description      string `json:"description"`
			Inputs           []struct {
				ID           string `json:"id"`
				ShipStrategy string `json:"ship_strategy"`
				Exchange     string `json:"exchange"`
			} `json:"inputs"`
		} `json:"nodes"`
	} `json:"plan"`
}

type vertexDetails struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	Parallelism    int    `json:"parallelism"`
	MaxParallelism int    `json:"maxParallelism"`
	Status         string `json:"status"`
	StartTime      int64  `json:"start-time"`
	Duration       int64  `json:"duration"`
}

type jobDetailsResponse struct {
	Name        string           `json:"name"`
	State       string           `json:"state"`
	JobType     string           `json:"job-type"`
	Scheduler   string           `json:"schedulerType"`
	IsStoppable bool             `json:"isStoppable"`
	StartTime   int64            `json:"start-time"`
	Duration    int64            `json:"duration"`
	Timestamps  map[string]int64 `json:"timestamps"`
	Vertices    []vertexDetails  `json:"vertices"`
}

func stateTransitions(timestamps map[string]int64) []StateTransition {
	transitions := make([]StateTransition, 0, len(timestamps))
	for state, timestamp := range timestamps {
		if timestamp <= 0 {
			continue
		}
		transitions = append(transitions, StateTransition{State: state, At: unixMillis(timestamp)})
	}
	slices.SortStableFunc(transitions, func(left, right StateTransition) int {
		if comparison := left.At.Compare(right.At); comparison != 0 {
			return comparison
		}
		return cmp.Compare(left.State, right.State)
	})
	return transitions
}

type aggregatedMetric struct {
	ID   string         `json:"id"`
	Min  flexibleNumber `json:"min"`
	Max  flexibleNumber `json:"max"`
	Avg  flexibleNumber `json:"avg"`
	Sum  flexibleNumber `json:"sum"`
	Skew flexibleNumber `json:"skew"`
}

type flexibleNumber float64

func (number *flexibleNumber) UnmarshalJSON(data []byte) error {
	text := strings.Trim(string(data), `"`)
	if text == "" || strings.EqualFold(text, "nan") || text == "null" {
		*number = 0
		return nil
	}
	value, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return err
	}
	*number = flexibleNumber(value)
	return nil
}

func (number flexibleNumber) Float64() float64 { return float64(number) }

func cleanName(value string) string {
	value = strings.ReplaceAll(value, "<br>", " / ")
	value = strings.ReplaceAll(value, "<br/>", " / ")
	value = strings.ReplaceAll(value, "<br />", " / ")
	value = html.UnescapeString(htmlTag.ReplaceAllString(value, ""))
	value = strings.Join(strings.Fields(value), " ")
	value = strings.TrimPrefix(value, "Source: ")
	value = strings.TrimSuffix(value, ": Writer")
	return value
}

func shortID(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}

func finite(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	return math.Max(0, value)
}

func percent(millisecondsPerSecond float64) float64 {
	return math.Min(100, finite(millisecondsPerSecond)/10)
}

// ClassifyBackpressure mirrors the levels used by Flink's Web UI: up to 10%
// is OK, over 10% through 50% is LOW, and over 50% is HIGH.
func ClassifyBackpressure(value float64) string {
	switch {
	case value > 50:
		return BackpressureHigh
	case value > 10:
		return BackpressureLow
	default:
		return BackpressureOK
	}
}

// SortedInputs makes detail rendering deterministic.
func SortedInputs(inputs []Input) []Input {
	result := slices.Clone(inputs)
	slices.SortFunc(result, func(left, right Input) int {
		return cmp.Compare(left.ID, right.ID)
	})
	return result
}
