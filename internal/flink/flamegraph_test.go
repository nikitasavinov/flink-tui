package flink

import (
	"context"
	"net/http"
	"testing"
)

func TestVertexFlameGraphParsesTreeAndScopesSubtask(t *testing.T) {
	client := testClient(t, func(request *http.Request) (string, int) {
		if request.URL.Path != "/jobs/job/vertices/vertex/flamegraph" {
			t.Fatalf("path = %q", request.URL.Path)
		}
		if got := request.URL.Query().Get("type"); got != "off_cpu" {
			t.Fatalf("type = %q, want off_cpu", got)
		}
		if got := request.URL.Query().Get("subtaskindex"); got != "2" {
			t.Fatalf("subtaskindex = %q, want 2", got)
		}
		return `{"endTimestamp":1710000000123,"data":{"name":"root","value":100,"children":[{"name":"Task.run:1","value":75,"children":[]}]}}`, http.StatusOK
	})

	graph, err := client.VertexFlameGraph(context.Background(), "job", "vertex", FlameGraphOffCPU, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !graph.Ready() || graph.Sampling() || graph.Disabled() {
		t.Fatalf("ready graph state = %#v", graph)
	}
	if graph.Type != FlameGraphOffCPU || graph.Subtask != 2 || graph.EndTimestamp.UnixMilli() != 1710000000123 {
		t.Fatalf("graph metadata = %#v", graph)
	}
	if graph.Root.Name != "root" || graph.Root.Value != 100 || len(graph.Root.Children) != 1 || graph.Root.Children[0].Value != 75 {
		t.Fatalf("graph tree = %#v", graph.Root)
	}
}

func TestVertexFlameGraphAggregateAndSentinelStates(t *testing.T) {
	requestCount := 0
	client := testClient(t, func(request *http.Request) (string, int) {
		requestCount++
		if _, present := request.URL.Query()["subtaskindex"]; present {
			t.Fatal("aggregate graph sent subtaskindex")
		}
		if requestCount == 1 {
			return `{"endTimestamp":-3}`, http.StatusOK
		}
		return `{"endTimestamp":-2}`, http.StatusOK
	})

	sampling, err := client.VertexFlameGraph(context.Background(), "job", "vertex", FlameGraphFull, -1)
	if err != nil || !sampling.Sampling() || sampling.Disabled() || sampling.Ready() {
		t.Fatalf("sampling graph = %#v, %v", sampling, err)
	}
	disabled, err := client.VertexFlameGraph(context.Background(), "job", "vertex", FlameGraphOnCPU, -1)
	if err != nil || !disabled.Disabled() || disabled.Sampling() || disabled.Ready() {
		t.Fatalf("disabled graph = %#v, %v", disabled, err)
	}
}

func TestVertexFlameGraphRejectsInvalidOptionsBeforeRequest(t *testing.T) {
	client := testClient(t, func(*http.Request) (string, int) {
		t.Fatal("invalid options reached HTTP server")
		return "", http.StatusInternalServerError
	})
	if _, err := client.VertexFlameGraph(context.Background(), "job", "vertex", FlameGraphType("mixed"), -1); err == nil {
		t.Fatal("invalid type returned nil error")
	}
	if _, err := client.VertexFlameGraph(context.Background(), "job", "vertex", FlameGraphFull, -2); err == nil {
		t.Fatal("invalid subtask returned nil error")
	}
}
