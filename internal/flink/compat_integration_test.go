package flink

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

// TestE2EReadOnlyCompatibility supplements the operator journeys with API
// capabilities those journeys do not exercise. It uses the prepared playground
// and never submits jobs, starts profiling, or changes running operations.
func TestE2EReadOnlyCompatibility(t *testing.T) {
	endpoint := os.Getenv("FLINK_TUI_E2E_ENDPOINT")
	if endpoint == "" {
		t.Skip("set FLINK_TUI_E2E_ENDPOINT or run make test-e2e")
	}
	client, err := NewClient(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	var overview ClusterOverview
	waitForLiveAPI(t, ctx, "registered workers and running checkpoint fixture", func() (bool, error) {
		var err error
		overview, err = client.ClusterOverview(ctx)
		if err != nil || overview.TaskManagers == 0 || overview.FlinkVersion == "" {
			return false, err
		}
		jobs, err := client.Jobs(ctx)
		if err != nil {
			return false, err
		}
		for _, job := range jobs {
			if job.Name == "Flink TUI Checkpoints" && job.State == "RUNNING" && job.RunningTasks > 0 && job.RunningTasks == job.TotalTasks {
				return true, nil
			}
		}
		return false, nil
	})
	if expected := os.Getenv("FLINK_TUI_EXPECT_VERSION"); expected != "" && overview.FlinkVersion != expected {
		t.Fatalf("connected to Flink %s, expected %s", overview.FlinkVersion, expected)
	}
	t.Logf("read-only compatibility against Flink %s", overview.FlinkVersion)

	t.Run("job-and-vertex", func(t *testing.T) {
		checkLiveJobCompatibility(t, ctx, client)
	})

	infrastructure, err := client.Infrastructure(ctx)
	if err != nil {
		t.Fatal(err)
	}
	manager := infrastructure.JobManager
	if len(manager.Configuration) == 0 || manager.JVMVersion == "" || manager.HeapMax <= 0 || manager.Threads <= 0 {
		t.Fatalf("incomplete JobManager configuration/environment/metrics: %#v", manager)
	}
	if len(infrastructure.TaskManagers) == 0 || infrastructure.TaskManagers[0].HeapMax <= 0 {
		t.Fatalf("missing TaskManager detail/metrics: %#v", infrastructure.TaskManagers)
	}
	for _, process := range []ProcessRef{JobManagerProcess(), TaskManagerProcess(infrastructure.TaskManagers[0].ID)} {
		t.Run(process.Label(), func(t *testing.T) {
			checkLiveProcessCompatibility(t, ctx, client, process)
		})
	}

	if endpoint := os.Getenv("FLINK_TUI_E2E_SQL_ENDPOINT"); endpoint != "" {
		t.Run("sql-gateway-version", func(t *testing.T) {
			gateway, err := NewSQLGatewayClient(endpoint)
			if err != nil {
				t.Fatal(err)
			}
			var info SQLGatewayInfo
			waitForLiveAPI(t, ctx, "SQL Gateway", func() (bool, error) {
				var err error
				info, err = gateway.Info(ctx)
				return err == nil, err
			})
			if info.ProductName == "" || info.Version != overview.FlinkVersion {
				t.Fatalf("SQL Gateway info = %#v, want Flink %s", info, overview.FlinkVersion)
			}
		})
	}
}

func waitForLiveAPI(t *testing.T, ctx context.Context, description string, probe func() (bool, error)) {
	t.Helper()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		ready, err := probe()
		if ready {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s: %v (last API error: %v)", description, ctx.Err(), err)
		case <-ticker.C:
		}
	}
}

func checkLiveJobCompatibility(t *testing.T, ctx context.Context, client *Client) {
	t.Helper()
	jobs, err := client.Jobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var job JobSummary
	for _, candidate := range jobs {
		if candidate.State == "RUNNING" {
			job = candidate
			if job.Name == "Flink TUI Checkpoints" {
				break
			}
		}
	}
	if job.ID == "" {
		t.Fatal("prepared playground has no running job")
	}
	configuration, err := client.JobConfiguration(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if configuration.JobID != job.ID || configuration.Name != job.Name || configuration.Parallelism <= 0 {
		t.Fatalf("job configuration did not retain identity or parallelism: %#v", configuration)
	}
	snapshot, err := client.Snapshot(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Nodes) == 0 {
		t.Fatal("running job has no execution vertices")
	}
	vertex := snapshot.Nodes[0]
	diagnostics, err := client.VertexDiagnostics(ctx, job.ID, vertex.ID)
	if err != nil {
		t.Fatal(err)
	}
	if diagnostics.VertexID != vertex.ID || diagnostics.Parallelism != vertex.Parallelism || len(diagnostics.Subtasks) != vertex.Parallelism {
		t.Fatalf("vertex diagnostics lost identity or subtasks: %#v", diagnostics)
	}
	if diagnostics.MetricWarnings != 0 {
		t.Fatalf("vertex diagnostics had %d metric endpoint failures", diagnostics.MetricWarnings)
	}
	accumulators, err := client.Accumulators(ctx, job.ID, vertex.ID)
	if err != nil {
		t.Fatal(err)
	}
	// A running fixture need not register user accumulators, but the combined
	// vertex and subtask response must still preserve every execution subtask.
	if len(accumulators.Subtasks) != vertex.Parallelism {
		t.Fatalf("accumulator response has %d subtasks, want %d", len(accumulators.Subtasks), vertex.Parallelism)
	}
	const metric = "numRecordsOut"
	metricContext, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	waitForLiveAPI(t, metricContext, "vertex metric registration", func() (bool, error) {
		names, err := client.MetricNames(metricContext, job.ID, vertex.ID)
		if err != nil {
			return false, err
		}
		if !slices.Contains(names, metric) {
			return false, fmt.Errorf("catalog does not expose %s", metric)
		}
		return true, nil
	})
	for _, aggregation := range []MetricAggregation{MetricSum, MetricAvg, MetricMin, MetricMax} {
		values, err := client.MetricValues(ctx, job.ID, vertex.ID, -1, aggregation, []string{metric})
		checkLiveMetric(t, values, err, metric, "aggregate "+string(aggregation))
	}
	values, err := client.MetricValues(ctx, job.ID, vertex.ID, diagnostics.Subtasks[0].Index, MetricSum, []string{metric})
	checkLiveMetric(t, values, err, metric, "individual subtask")
}

func checkLiveMetric(t *testing.T, values map[string]float64, err error, name, scope string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s metric request: %v", scope, err)
	}
	value, exists := values[name]
	if !exists || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		t.Fatalf("%s did not decode a finite nonnegative %s: %#v", scope, name, values)
	}
}

func checkLiveProcessCompatibility(t *testing.T, ctx context.Context, client *Client, process ProcessRef) {
	t.Helper()
	logs, err := client.LogFiles(ctx, process)
	if err != nil {
		t.Fatal(err)
	}
	filename := ""
	for _, log := range logs {
		if log.Size > 0 {
			filename = log.Name
			break
		}
	}
	if filename == "" {
		t.Fatal("running process has no nonempty log file")
	}
	content, err := client.LogContent(ctx, process, filename)
	if err != nil || strings.TrimSpace(content) == "" {
		t.Fatalf("named process log was unreadable: %v", err)
	}
	content, err = client.CurrentLog(ctx, process)
	if err != nil || strings.TrimSpace(content) == "" {
		t.Fatalf("current process log was unreadable: %v", err)
	}
	if _, err := client.Stdout(ctx, process); err != nil {
		// Foreground Docker processes may have no .out file. Match the missing
		// resource response specifically; an unregistered route still fails.
		var requestError *RequestError
		if !errors.As(err, &requestError) || requestError.StatusCode != http.StatusNotFound ||
			!strings.Contains(requestError.Body, "file does not exist") {
			t.Fatalf("stdout endpoint: %v", err)
		}
		t.Log("stdout endpoint is present; foreground process has no stdout file")
	}
	threads, err := client.ThreadDump(ctx, process)
	if err != nil {
		t.Fatal(err)
	}
	if len(threads) == 0 || threads[0].Name == "" || strings.TrimSpace(threads[0].Stack) == "" {
		t.Fatalf("thread dump did not decode named execution stacks: %#v", threads)
	}
	if _, err := client.ProfilingList(ctx, process); err != nil {
		t.Fatalf("profiler history endpoint (requires rest.profiling.enabled): %v", err)
	}
}
