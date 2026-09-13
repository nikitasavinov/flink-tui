package flink

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func TestCustomMetricCatalogueAndValues(t *testing.T) {
	client := testClient(t, func(request *http.Request) (string, int) {
		switch request.URL.Path {
		case "/jobs/job/vertices/vertex/subtasks/metrics":
			if request.URL.Query().Get("get") == "" {
				return `[{"id":"z.metric"},{"id":"a.metric"},{"id":"a.metric"}]`, http.StatusOK
			}
			if request.URL.Query().Get("agg") != "avg" {
				t.Errorf("aggregation = %q, want avg", request.URL.Query().Get("agg"))
			}
			return `[{"id":"a.metric","avg":"12.5"},{"id":"z.metric","avg":7}]`, http.StatusOK
		case "/jobs/job/vertices/vertex/subtasks/1/metrics":
			return `[{"id":"a.metric","value":"9.25"}]`, http.StatusOK
		default:
			return "", http.StatusNotFound
		}
	})

	names, err := client.MetricNames(context.Background(), "job", "vertex")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(names, ",") != "a.metric,z.metric" {
		t.Fatalf("metric names = %#v", names)
	}
	values, err := client.MetricValues(context.Background(), "job", "vertex", -1, MetricAvg, []string{"z.metric", "a.metric"})
	if err != nil {
		t.Fatal(err)
	}
	if values["a.metric"] != 12.5 || values["z.metric"] != 7 {
		t.Fatalf("aggregate values = %#v", values)
	}
	values, err = client.MetricValues(context.Background(), "job", "vertex", 1, MetricSum, []string{"a.metric"})
	if err != nil || values["a.metric"] != 9.25 {
		t.Fatalf("subtask values = %#v, %v", values, err)
	}
}

func TestAccumulatorsCombinesVertexAndSubtaskEndpoints(t *testing.T) {
	client := testClient(t, func(request *http.Request) (string, int) {
		switch request.URL.Path {
		case "/jobs/job/vertices/vertex/accumulators":
			return `{"id":"vertex","user-accumulators":[{"name":"rows","type":"LongCounter","value":"12"}]}`, http.StatusOK
		case "/jobs/job/vertices/vertex/subtasks/accumulators":
			return `{"id":"vertex","parallelism":2,"subtasks":[
				{"subtask":1,"attempt":0,"endpoint":"tm-b","user-accumulators":[{"name":"errors","type":"LongCounter","value":"2"}]},
				{"subtask":0,"attempt":1,"endpoint":"tm-a","user-accumulators":[{"name":"errors","type":"LongCounter","value":"1"}]}
			]}`, http.StatusOK
		default:
			return "", http.StatusNotFound
		}
	})

	result, err := client.Accumulators(context.Background(), "job", "vertex")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Vertex) != 1 || result.Vertex[0].Name != "rows" || len(result.Subtasks) != 2 {
		t.Fatalf("accumulators = %#v", result)
	}
	if result.Subtasks[0].Subtask != 0 || result.Subtasks[0].Attempt != 1 || result.Subtasks[1].Accumulators[0].Value != "2" {
		t.Fatalf("subtask accumulators = %#v", result.Subtasks)
	}
}

func TestProcessLogsStdoutAndThreadDumps(t *testing.T) {
	client := testClient(t, func(request *http.Request) (string, int) {
		switch request.URL.Path {
		case "/jobmanager/logs":
			return `{"logs":[{"name":"older.log","size":10,"mtime":1000},{"name":"newer.log","size":20,"mtime":2000}]}`, http.StatusOK
		case "/jobmanager/logs/newer.log":
			return "line one\nline two", http.StatusOK
		case "/jobmanager/log":
			return "current jobmanager log", http.StatusOK
		case "/jobmanager/stdout":
			return "jobmanager stdout", http.StatusOK
		case "/taskmanagers/tm:1/logs":
			return `{"logs":[{"name":"task.log","size":30,"mtime":3000}]}`, http.StatusOK
		case "/taskmanagers/tm:1/logs/task.log":
			return "task log body", http.StatusOK
		case "/taskmanagers/tm:1/log":
			return "current task log", http.StatusOK
		case "/taskmanagers/tm:1/stdout":
			return "task stdout", http.StatusOK
		case "/jobmanager/thread-dump", "/taskmanagers/tm:1/thread-dump":
			return `{"threadInfos":[{"threadName":"main","stringifiedThreadInfo":"\"main\" Id=1 RUNNABLE\n\tat Main.run"}]}`, http.StatusOK
		default:
			return "missing", http.StatusNotFound
		}
	})

	logs, err := client.LogFiles(context.Background(), JobManagerProcess())
	if err != nil || len(logs) != 2 || logs[0].Name != "newer.log" {
		t.Fatalf("JobManager logs = %#v, %v", logs, err)
	}
	content, err := client.LogContent(context.Background(), JobManagerProcess(), "newer.log")
	if err != nil || content != "line one\nline two" {
		t.Fatalf("JobManager log content = %q, %v", content, err)
	}
	current, err := client.CurrentLog(context.Background(), JobManagerProcess())
	if err != nil || current != "current jobmanager log" {
		t.Fatalf("JobManager current log = %q, %v", current, err)
	}
	stdout, err := client.Stdout(context.Background(), JobManagerProcess())
	if err != nil || stdout != "jobmanager stdout" {
		t.Fatalf("JobManager stdout = %q, %v", stdout, err)
	}
	threads, err := client.ThreadDump(context.Background(), JobManagerProcess())
	if err != nil || len(threads) != 1 || threads[0].Name != "main" {
		t.Fatalf("JobManager thread dump = %#v, %v", threads, err)
	}

	taskManager := TaskManagerProcess("tm:1")
	logs, err = client.LogFiles(context.Background(), taskManager)
	if err != nil || len(logs) != 1 || logs[0].Name != "task.log" {
		t.Fatalf("TaskManager logs = %#v, %v", logs, err)
	}
	content, err = client.LogContent(context.Background(), taskManager, "task.log")
	if err != nil || content != "task log body" {
		t.Fatalf("TaskManager log content = %q, %v", content, err)
	}
	current, err = client.CurrentLog(context.Background(), taskManager)
	if err != nil || current != "current task log" {
		t.Fatalf("TaskManager current log = %q, %v", current, err)
	}
	stdout, err = client.Stdout(context.Background(), taskManager)
	if err != nil || stdout != "task stdout" {
		t.Fatalf("TaskManager stdout = %q, %v", stdout, err)
	}
	threads, err = client.ThreadDump(context.Background(), taskManager)
	if err != nil || len(threads) != 1 || threads[0].Name != "main" || !strings.Contains(threads[0].Stack, "RUNNABLE") {
		t.Fatalf("thread dump = %#v, %v", threads, err)
	}
}

func TestLogContentEscapesFilenameAndReportsHTTPFailure(t *testing.T) {
	client := testClient(t, func(request *http.Request) (string, int) {
		decoded, _ := url.PathUnescape(request.URL.EscapedPath())
		if decoded != "/jobmanager/logs/name with spaces.log" {
			t.Errorf("path = %q", decoded)
		}
		return "not available", http.StatusNotFound
	})
	_, err := client.LogContent(context.Background(), JobManagerProcess(), "name with spaces.log")
	if err == nil || !strings.Contains(err.Error(), "404") || !strings.Contains(err.Error(), "not available") {
		t.Fatalf("error = %v", err)
	}
}

func TestTextResponseSizeLimit(t *testing.T) {
	const limit = 16 << 20
	for _, size := range []int{limit, limit + 2} {
		body := strings.NewReader(strings.Repeat("x", size))
		client, err := NewClient("http://flink.test")
		if err != nil {
			t.Fatal(err)
		}
		client.http.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
			response := testHTTPResponse(request, http.StatusOK, "")
			response.ContentLength = -1
			response.Body = io.NopCloser(body)
			return response, nil
		})
		content, err := client.CurrentLog(context.Background(), JobManagerProcess())
		if size == limit {
			if err != nil || len(content) != size {
				t.Fatalf("response at limit: length=%d, error=%v", len(content), err)
			}
			continue
		}
		var requestError *RequestError
		if !errors.As(err, &requestError) || !strings.Contains(err.Error(), "exceeds 16 MiB") || content != "" {
			t.Fatalf("oversized response: length=%d, error=%v", len(content), err)
		}
		if body.Len() == 0 {
			t.Fatal("read beyond the size limit")
		}
	}
}

func TestLogHTTPFailureBoundsBodyAndPreservesReadFailure(t *testing.T) {
	readFailure := errors.New("connection reset")
	for _, test := range []struct {
		name string
		body io.ReadCloser
		want string
		err  error
	}{
		{name: "large error", body: io.NopCloser(strings.NewReader(strings.Repeat("x", 2048))), want: strings.Repeat("x", 1024)},
		{name: "interrupted error", body: &failingReadCloser{err: readFailure}, want: "partial response", err: readFailure},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, err := NewClient("http://flink.test")
			if err != nil {
				t.Fatal(err)
			}
			client.http.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
				response := testHTTPResponse(request, http.StatusBadGateway, "")
				response.Body = test.body
				return response, nil
			})
			_, err = client.CurrentLog(context.Background(), JobManagerProcess())
			var requestError *RequestError
			if !errors.As(err, &requestError) || requestError.StatusCode != http.StatusBadGateway || requestError.Body != test.want {
				t.Fatalf("log HTTP error = %#v, %v", requestError, err)
			}
			if test.err != nil && !errors.Is(err, test.err) {
				t.Fatalf("log HTTP error lost read failure: %v", err)
			}
		})
	}
}
