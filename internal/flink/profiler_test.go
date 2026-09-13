package flink

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestProfilerListStartAndReport(t *testing.T) {
	client := testClient(t, func(request *http.Request) (string, int) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/taskmanagers/tm:1/profiler":
			return `{"profilingList":[
				{"status":"FAILED","mode":"CPU","triggerTime":1000,"finishedTime":2000,"duration":3,"message":"perf denied","outputFile":null,"profilingMode":"CPU"},
				{"status":"FINISHED","mode":"ITIMER","triggerTime":3000,"finishedTime":8000,"duration":5,"message":"Profiling Successful","outputFile":"report.html","profilingMode":"ITIMER"}
			]}`, http.StatusOK
		case request.Method == http.MethodPost && request.URL.Path == "/taskmanagers/tm:1/profiler":
			var body struct {
				Mode     ProfilerMode `json:"mode"`
				Duration int64        `json:"duration"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Errorf("decode body: %v", err)
			}
			if body.Mode != ProfilerWall || body.Duration != 10 {
				t.Errorf("body = %#v", body)
			}
			return `{"status":"RUNNING","mode":"WALL","triggerTime":9000,"finishedTime":0,"duration":10,"message":"Profiling Started","outputFile":null,"profilingMode":"WALL"}`, http.StatusOK
		case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/taskmanagers/tm:1/profiler/"):
			decoded, _ := url.PathUnescape(request.URL.EscapedPath())
			if decoded != "/taskmanagers/tm:1/profiler/report with spaces.html" {
				t.Errorf("report path = %q", decoded)
			}
			return "<html>flame graph</html>", http.StatusOK
		default:
			return "missing", http.StatusNotFound
		}
	})

	process := TaskManagerProcess("tm:1")
	profiles, err := client.ProfilingList(context.Background(), process)
	if err != nil {
		t.Fatal(err)
	}
	if len(profiles) != 2 || profiles[0].Mode != ProfilerITimer || profiles[0].OutputFile != "report.html" || profiles[1].Message != "perf denied" {
		t.Fatalf("profiles = %#v", profiles)
	}
	profile, err := client.StartProfiling(context.Background(), process, ProfilerWall, 10*time.Second)
	if err != nil || profile.Status != "RUNNING" || profile.Duration != 10*time.Second {
		t.Fatalf("profile = %#v, %v", profile, err)
	}
	report, err := client.ProfilerReport(context.Background(), process, "report with spaces.html")
	if err != nil || report != "<html>flame graph</html>" {
		t.Fatalf("report = %q, %v", report, err)
	}
}

func TestProfilerValidatesInputs(t *testing.T) {
	client := testClient(t, func(*http.Request) (string, int) {
		t.Fatal("HTTP should not be called for invalid profiler input")
		return "", http.StatusInternalServerError
	})
	if _, err := client.StartProfiling(context.Background(), JobManagerProcess(), "NOPE", 5*time.Second); err == nil {
		t.Fatal("unsupported mode was accepted")
	}
	if _, err := client.StartProfiling(context.Background(), JobManagerProcess(), ProfilerCPU, 0); err == nil {
		t.Fatal("zero duration was accepted")
	}
	if _, err := client.ProfilerReport(context.Background(), JobManagerProcess(), " "); err == nil {
		t.Fatal("empty filename was accepted")
	}
}

func TestParseProfilerReportBuildsAsyncProfilerTree(t *testing.T) {
	report := strings.Join([]string{
		"<html><script>",
		"f(0,0,100,3,'all')",
		"f(1,0,60,1,'left, branch')",
		"f(2,10,20,1,'work\\u0020unit')",
		"f(1,60,40,1,'right branch')",
		"f(2,75,10,1,'quoted \\'frame\\'')",
		"</script></html>",
	}, "\n")
	root, err := ParseProfilerReport(report)
	if err != nil {
		t.Fatal(err)
	}
	if root.Name != "all" || root.Value != 100 || len(root.Children) != 2 {
		t.Fatalf("root = %#v", root)
	}
	if root.Children[0].Name != "left, branch" || len(root.Children[0].Children) != 1 ||
		root.Children[0].Children[0].Name != "work unit" {
		t.Fatalf("left tree = %#v", root.Children[0])
	}
	if root.Children[1].Name != "right branch" || len(root.Children[1].Children) != 1 ||
		root.Children[1].Children[0].Name != "quoted 'frame'" {
		t.Fatalf("right tree = %#v", root.Children[1])
	}
}

func TestParseProfilerReportRejectsUnsupportedDocument(t *testing.T) {
	if _, err := ParseProfilerReport("<html>no async-profiler frames</html>"); err == nil {
		t.Fatal("report without frames was accepted")
	}
}

func TestProfilerJavaScriptEscapes(t *testing.T) {
	for _, test := range []struct {
		input string
		want  string
	}{
		{`'\x41\u00e9\uD83D\uDE00'`, "Aé😀"},
		{`'\x4'`, ""},
		{`'\u123'`, ""},
		{`'\xGG'`, ""},
		{`'\u+041'`, ""},
	} {
		got, err := unquoteJavaScriptString(test.input)
		if got != test.want || (err != nil) != (test.want == "") {
			t.Errorf("decode %s = %q, %v; want %q", test.input, got, err, test.want)
		}
	}
}
