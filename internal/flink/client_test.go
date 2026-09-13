package flink

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestClassifyBackpressure(t *testing.T) {
	tests := []struct {
		name    string
		percent float64
		want    string
	}{
		{name: "zero", percent: 0, want: BackpressureOK},
		{name: "ok boundary", percent: 10, want: BackpressureOK},
		{name: "low", percent: 10.1, want: BackpressureLow},
		{name: "low boundary", percent: 50, want: BackpressureLow},
		{name: "high", percent: 50.1, want: BackpressureHigh},
		{name: "fully backpressured", percent: 100, want: BackpressureHigh},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := ClassifyBackpressure(test.percent); got != test.want {
				t.Fatalf("ClassifyBackpressure(%v) = %q, want %q", test.percent, got, test.want)
			}
		})
	}
}

func TestJobsSortsActiveJobsAndNormalizesOverview(t *testing.T) {
	client := testClient(t, func(request *http.Request) (string, int) {
		if request.URL.Path != "/jobs/overview" {
			return "", http.StatusNotFound
		}
		return `{"jobs":[
			{"jid":"done","name":"Done","jobType":"BATCH","start-time":1000,"duration":9000,"state":"FINISHED","tasks":{"total":3,"running":0}},
			{"jid":"live","name":"Live","jobType":"STREAMING","start-time":2000,"duration":5000,"state":"RUNNING","tasks":{"total":4,"running":4}}
		]}`, http.StatusOK
	})
	jobs, err := client.Jobs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 2 || jobs[0].ID != "live" || jobs[1].ID != "done" {
		t.Fatalf("jobs = %#v, want active job first", jobs)
	}
	if jobs[0].RunningTasks != 4 || jobs[0].TotalTasks != 4 || jobs[0].Type != "STREAMING" {
		t.Fatalf("live job summary = %#v", jobs[0])
	}
}

func TestClusterOverviewNormalizesCapacityAndJobHealth(t *testing.T) {
	client := testClient(t, func(request *http.Request) (string, int) {
		if request.URL.Path != "/overview" {
			return "", http.StatusNotFound
		}
		return `{
			"taskmanagers":2,
			"slots-total":16,
			"slots-available":5,
			"jobs-running":3,
			"jobs-finished":7,
			"jobs-cancelled":1,
			"jobs-failed":2,
			"flink-version":"2.3.0",
			"flink-commit":"abc1234"
		}`, http.StatusOK
	})
	overview, err := client.ClusterOverview(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if overview.TaskManagers != 2 || overview.SlotsTotal != 16 || overview.SlotsAvailable != 5 {
		t.Fatalf("cluster capacity = %#v", overview)
	}
	if overview.JobsRunning != 3 || overview.JobsFinished != 7 ||
		overview.JobsCanceled != 1 || overview.JobsFailed != 2 {
		t.Fatalf("cluster job health = %#v", overview)
	}
	if overview.FlinkVersion != "2.3.0" || overview.FlinkCommit != "abc1234" || overview.UpdatedAt.IsZero() {
		t.Fatalf("cluster version = %#v", overview)
	}
}

func TestSnapshotIncludesHealthSignalsAndCachesPlan(t *testing.T) {
	var planRequests atomic.Int32
	client := testClient(t, func(request *http.Request) (string, int) {
		switch request.URL.Path {
		case "/jobs/job/plan":
			planRequests.Add(1)
			return `{"plan":{"nodes":[{"id":"vertex","parallelism":2,"description":"Stage<br/>","inputs":[{"id":"upstream"}]}]}}`, http.StatusOK
		case "/jobs/job":
			return `{"name":"Test Job","state":"RUNNING","job-type":"STREAMING","schedulerType":"Default","isStoppable":true,"start-time":1000,"duration":5000,"timestamps":{"CREATED":1000,"RUNNING":1200,"FAILED":0},"vertices":[{"id":"vertex","name":"Stage","parallelism":2,"maxParallelism":128,"status":"RUNNING","start-time":1300,"duration":4700}]}`, http.StatusOK
		case "/jobs/job/checkpoints":
			return `{"counts":{"restored":0,"total":3,"in_progress":0,"completed":2,"failed":1},"summary":{"end_to_end_duration":{"min":10,"avg":20,"max":30,"p50":18,"p90":27,"p95":28,"p99":29,"p999":29.9},"checkpointed_size":{"min":100,"avg":200,"max":300,"p50":180,"p90":270,"p95":280,"p99":290,"p999":299},"state_size":{"min":90,"avg":190,"max":290,"p50":170,"p90":260,"p95":270,"p99":280,"p999":289},"processed_data":{"min":1,"avg":2,"max":3,"p50":1.8,"p90":2.7,"p95":2.8,"p99":2.9,"p999":2.99},"persisted_data":{"min":0,"avg":1,"max":2,"p50":0.5,"p90":1.5,"p95":1.7,"p99":1.9,"p999":1.99}},"latest":{"completed":{"id":7,"trigger_timestamp":10000,"latest_ack_timestamp":11200,"end_to_end_duration":1200,"checkpointed_size":2048},"failed":{"id":6,"status":"FAILED","trigger_timestamp":8000,"end_to_end_duration":300,"failure_message":"Checkpoint Coordinator is suspending.\nstack trace"}},"history":[{"id":7,"status":"COMPLETED","checkpoint_type":"CHECKPOINT","trigger_timestamp":10000,"latest_ack_timestamp":11200,"end_to_end_duration":1200,"checkpointed_size":2048,"state_size":1024,"processed_data":512,"num_subtasks":2,"num_acknowledged_subtasks":2,"external_path":"file:///cp-7"},{"id":6,"status":"FAILED","trigger_timestamp":8000,"end_to_end_duration":300,"failure_message":"storage unavailable\nstack trace"}]}`, http.StatusOK
		case "/jobs/job/exceptions":
			limit := request.URL.Query().Get("maxExceptions")
			if limit != "20" && limit != "60" {
				t.Errorf("maxExceptions = %q, want 20 or 60", limit)
			}
			return `{"exceptionHistory":{"entries":[{"exceptionName":"java.lang.IllegalStateException","stacktrace":"trace","timestamp":12000,"taskName":"Stage (2/2) - execution #0","endpoint":"tm-a:123","taskManagerId":"tm-a","failureLabels":{"region":"eu-west"},"concurrentExceptions":[{"exceptionName":"java.io.IOException","stacktrace":"concurrent trace","timestamp":11999,"taskName":"Stage (1/2) - execution #0","endpoint":"tm-b:123","taskManagerId":"tm-b","failureLabels":{"disk":"full"}}]}],"truncated":false}}`, http.StatusOK
		case "/jobs/job/vertices/vertex/subtasks/metrics":
			if !strings.Contains(request.URL.RawQuery, "currentInputWatermark") || !strings.Contains(request.URL.RawQuery, "skew") {
				t.Errorf("metric query = %q, want watermark and skew", request.URL.RawQuery)
			}
			return `[
				{"id":"numRecordsInPerSecond","min":4,"max":12,"avg":8,"sum":16,"skew":25},
				{"id":"numRecordsOutPerSecond","sum":15},
				{"id":"numRecordsIn","sum":12345},
				{"id":"numRecordsOut","sum":12000},
				{"id":"numBytesIn","sum":1048576},
				{"id":"numBytesOut","sum":524288},
				{"id":"busyTimeMsPerSecond","max":800},
				{"id":"backPressuredTimeMsPerSecond","max":600},
				{"id":"idleTimeMsPerSecond","avg":100},
				{"id":"currentInputWatermark","min":1710000000000}
			]`, http.StatusOK
		default:
			return "", http.StatusNotFound
		}
	})
	for iteration := 0; iteration < 2; iteration++ {
		var snapshot Snapshot
		var snapshotErr error
		if iteration == 0 {
			snapshot, snapshotErr = client.Snapshot(context.Background(), "job")
		} else {
			snapshot, snapshotErr = client.SnapshotWithExceptionLimit(context.Background(), "job", 60)
		}
		if snapshotErr != nil {
			t.Fatal(snapshotErr)
		}
		if snapshot.JobName != "Test Job" || snapshot.Checkpoints.LatestID != 7 || snapshot.Exceptions.Count != 1 {
			t.Fatalf("snapshot health = %#v", snapshot)
		}
		if snapshot.Scheduler != "Default" || !snapshot.IsStoppable || len(snapshot.Transitions) != 2 ||
			snapshot.Transitions[1].State != "RUNNING" || snapshot.Nodes[0].MaxParallelism != 128 ||
			snapshot.Nodes[0].Duration != 4700*time.Millisecond {
			t.Fatalf("snapshot timeline = %#v", snapshot)
		}
		if entries := snapshot.Exceptions.Entries; len(entries) != 1 || entries[0].Name != "java.lang.IllegalStateException" ||
			entries[0].Stacktrace != "trace" || entries[0].TaskName != "Stage (2/2) - execution #0" ||
			entries[0].TaskManagerID != "tm-a" || entries[0].Endpoint != "tm-a:123" || entries[0].FailureLabels["region"] != "eu-west" ||
			len(entries[0].ConcurrentExceptions) != 1 || entries[0].ConcurrentExceptions[0].TaskManagerID != "tm-b" ||
			entries[0].ConcurrentExceptions[0].FailureLabels["disk"] != "full" {
			t.Fatalf("exception entries = %#v", entries)
		}
		if history := snapshot.Checkpoints.History; len(history) != 2 || history[0].ID != 7 ||
			history[0].AcknowledgedSubtasks != 2 || history[1].Failure != "storage unavailable" {
			t.Fatalf("checkpoint history = %#v", history)
		}
		if latestFailed := snapshot.Checkpoints.LatestFailed; latestFailed == nil || latestFailed.ID != 6 ||
			latestFailed.Failure != "Checkpoint Coordinator is suspending." {
			t.Fatalf("latest failed checkpoint = %#v", latestFailed)
		}
		if statistics := snapshot.Checkpoints.Statistics; statistics.EndToEndDuration.P999 != 29.9 ||
			statistics.CheckpointedSize.P95 != 280 || statistics.ProcessedData.Average != 2 {
			t.Fatalf("checkpoint statistics = %#v", statistics)
		}
		metrics := snapshot.Nodes[0].Metrics
		if metrics.BusyPercent != 80 || metrics.BackpressurePercent != 60 || metrics.DataSkewPercent != 25 {
			t.Fatalf("metrics = %#v", metrics)
		}
		if metrics.RecordsIn != 12345 || metrics.RecordsOut != 12000 || metrics.BytesIn != 1048576 || metrics.BytesOut != 524288 {
			t.Fatalf("cumulative metrics = %#v", metrics)
		}
		if !metrics.WatermarkKnown || metrics.LowWatermark != 1710000000000 {
			t.Fatalf("watermark = (%d, %t)", metrics.LowWatermark, metrics.WatermarkKnown)
		}
	}
	if got := planRequests.Load(); got != 1 {
		t.Fatalf("plan requests = %d, want 1 cached request", got)
	}
}

func TestVertexDiagnosticsLoadsPerSubtaskMetrics(t *testing.T) {
	client := testClient(t, func(request *http.Request) (string, int) {
		switch request.URL.Path {
		case "/jobs/job/vertices/vertex":
			return `{"name":"Risk Score: Writer","parallelism":2,"subtasks":[
				{"subtask":0,"status":"RUNNING","attempt":1,"endpoint":"tm-a:123","taskmanager-id":"tm-a","start-time":1000,"duration":5000},
				{"subtask":1,"status":"RUNNING","attempt":0,"endpoint":"tm-b:123","taskmanager-id":"tm-b","start-time":2000,"duration":4000}
			]}`, http.StatusOK
		case "/jobs/job/vertices/vertex/subtasks/0/metrics":
			return metricValuesJSON("900", "100", "12.5", "1710000000000"), http.StatusOK
		case "/jobs/job/vertices/vertex/subtasks/1/metrics":
			return metricValuesJSON("100", "850", "8.5", "-9223372036854775808"), http.StatusOK
		default:
			return "", http.StatusNotFound
		}
	})
	diagnostics, err := client.VertexDiagnostics(context.Background(), "job", "vertex")
	if err != nil {
		t.Fatal(err)
	}
	if diagnostics.Name != "Risk Score" || diagnostics.ExecutionName != "Risk Score: Writer" || len(diagnostics.Subtasks) != 2 {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	if diagnostics.Subtasks[0].Metrics.BusyPercent != 90 || diagnostics.Subtasks[0].Metrics.BackpressurePercent != 10 {
		t.Fatalf("subtask 0 metrics = %#v", diagnostics.Subtasks[0].Metrics)
	}
	if diagnostics.Subtasks[0].Metrics.RecordsIn != 42 || diagnostics.Subtasks[0].Metrics.BytesOut != 2048 {
		t.Fatalf("subtask cumulative metrics = %#v", diagnostics.Subtasks[0].Metrics)
	}
	if !diagnostics.Subtasks[0].Metrics.WatermarkKnown || diagnostics.Subtasks[1].Metrics.WatermarkKnown {
		t.Fatalf("watermark flags = %t, %t", diagnostics.Subtasks[0].Metrics.WatermarkKnown, diagnostics.Subtasks[1].Metrics.WatermarkKnown)
	}
}

func TestVertexMetricsRetriesWhenWatermarkIsUnavailable(t *testing.T) {
	var requests atomic.Int32
	client := testClient(t, func(request *http.Request) (string, int) {
		requests.Add(1)
		if strings.Contains(request.URL.RawQuery, "currentInputWatermark") {
			return `[]`, http.StatusOK
		}
		return `[
			{"id":"numRecordsInPerSecond","sum":0},
			{"id":"numRecordsOutPerSecond","sum":12},
			{"id":"busyTimeMsPerSecond","max":"NaN"},
			{"id":"backPressuredTimeMsPerSecond","max":900},
			{"id":"idleTimeMsPerSecond","avg":0}
		]`, http.StatusOK
	})
	metrics, err := client.vertexMetrics(context.Background(), "job", "source", true)
	if err != nil {
		t.Fatal(err)
	}
	if metrics.BackpressurePercent != 90 || metrics.RecordsOutPerSecond != 12 || metrics.WatermarkKnown {
		t.Fatalf("fallback metrics = %#v", metrics)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("metric requests = %d, want combined request plus fallback", got)
	}
}

func metricValuesJSON(busy, pressure, input, watermark string) string {
	return `[
		{"id":"busyTimeMsPerSecond","value":"` + busy + `"},
		{"id":"backPressuredTimeMsPerSecond","value":"` + pressure + `"},
		{"id":"idleTimeMsPerSecond","value":"0"},
		{"id":"numRecordsInPerSecond","value":"` + input + `"},
		{"id":"numRecordsOutPerSecond","value":"` + input + `"},
		{"id":"numRecordsIn","value":"42"},
		{"id":"numRecordsOut","value":"40"},
		{"id":"numBytesIn","value":"4096"},
		{"id":"numBytesOut","value":"2048"},
		{"id":"currentInputWatermark","value":"` + watermark + `"}
	]`
}

func TestParseWatermarkRejectsFlinkSentinel(t *testing.T) {
	if _, ok := parseWatermark(-9.223372036854776e18); ok {
		t.Fatal("Flink Long.MIN_VALUE watermark should be unknown")
	}
	if value, ok := parseWatermark(float64(math.MaxInt64)); ok {
		t.Fatalf("terminal Long.MAX_VALUE watermark became (%d, true)", value)
	}
	value, ok := parseWatermark(float64(time.Unix(100, 0).UnixMilli()))
	if !ok || value != 100000 {
		t.Fatalf("watermark = (%d, %t), want (100000, true)", value, ok)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func testClient(t *testing.T, respond func(*http.Request) (string, int)) *Client {
	t.Helper()
	client, err := NewClient("http://flink.test")
	if err != nil {
		t.Fatal(err)
	}
	client.http.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body, status := respond(request)
		return &http.Response{
			StatusCode: status,
			Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(body)),
			Request:    request,
		}, nil
	})
	return client
}
