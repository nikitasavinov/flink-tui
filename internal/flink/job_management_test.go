package flink

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestJobConfigurationNormalizesAndSortsUserValues(t *testing.T) {
	client := testClient(t, func(request *http.Request) (string, int) {
		if request.URL.Path != "/jobs/job/config" {
			return "", http.StatusNotFound
		}
		return `{
			"jid":"job","name":"Config Job",
			"execution-config":{
				"restart-strategy":"fixed-delay","job-parallelism":4,"object-reuse-mode":true,
				"user-config":{"z.enabled":true,"a.endpoint":"kafka:9092","n.retries":3}
			}
		}`, http.StatusOK
	})
	configuration, err := client.JobConfiguration(context.Background(), "job")
	if err != nil {
		t.Fatal(err)
	}
	if configuration.RestartStrategy != "fixed-delay" || configuration.Parallelism != 4 || !configuration.ObjectReuse {
		t.Fatalf("execution config = %#v", configuration)
	}
	if len(configuration.User) != 3 || configuration.User[0].Key != "a.endpoint" ||
		configuration.User[0].Value != "kafka:9092" || configuration.User[2].Value != "true" {
		t.Fatalf("user config = %#v", configuration.User)
	}
}

func TestLifecycleRequestsUseFlink23EndpointsAndBodies(t *testing.T) {
	type seenRequest struct {
		method string
		path   string
		query  string
		body   map[string]any
	}
	seen := make(chan seenRequest, 8)
	client := testClient(t, func(request *http.Request) (string, int) {
		var body map[string]any
		if request.Body != nil {
			payload, _ := io.ReadAll(request.Body)
			if len(payload) > 0 {
				_ = json.Unmarshal(payload, &body)
			}
		}
		seen <- seenRequest{method: request.Method, path: request.URL.Path, query: request.URL.RawQuery, body: body}
		switch {
		case request.Method == http.MethodPatch:
			return `{}`, http.StatusAccepted
		case strings.HasSuffix(request.URL.Path, "/trigger"):
			return `{"status":{"id":"COMPLETED"},"operation":{"location":"file:///savepoint","failure-cause":null}}`, http.StatusOK
		default:
			return `{"request-id":"trigger"}`, http.StatusAccepted
		}
	})

	checkpointID, err := client.TriggerCheckpoint(context.Background(), "job", "full")
	if err != nil || checkpointID != "trigger" {
		t.Fatalf("checkpoint = %q, %v", checkpointID, err)
	}
	savepointID, err := client.TriggerSavepoint(context.Background(), "job", "file:///savepoints")
	if err != nil || savepointID != "trigger" {
		t.Fatalf("savepoint = %q, %v", savepointID, err)
	}
	stopID, err := client.StopWithSavepoint(context.Background(), "job", true, "")
	if err != nil || stopID != "trigger" {
		t.Fatalf("stop = %q, %v", stopID, err)
	}
	if err := client.CancelJob(context.Background(), "job"); err != nil {
		t.Fatal(err)
	}
	operation, err := client.SavepointOperation(context.Background(), "job", "trigger")
	if err != nil || operation.Status != "COMPLETED" || operation.Location != "file:///savepoint" {
		t.Fatalf("operation = %#v, %v", operation, err)
	}

	requests := make([]seenRequest, 0, 5)
	for range 5 {
		requests = append(requests, <-seen)
	}
	if requests[0].method != http.MethodPost || requests[0].path != "/jobs/job/checkpoints" || requests[0].body["checkpointType"] != "FULL" {
		t.Fatalf("checkpoint request = %#v", requests[0])
	}
	if requests[1].path != "/jobs/job/savepoints" || requests[1].body["target-directory"] != "file:///savepoints" || requests[1].body["formatType"] != "CANONICAL" {
		t.Fatalf("savepoint request = %#v", requests[1])
	}
	if requests[2].path != "/jobs/job/stop" || requests[2].body["drain"] != true {
		t.Fatalf("stop request = %#v", requests[2])
	}
	if requests[3].method != http.MethodPatch || requests[3].query != "mode=cancel" {
		t.Fatalf("cancel request = %#v", requests[3])
	}
}

func TestInfrastructureNormalizesTaskAndJobManagerDetails(t *testing.T) {
	client := testClient(t, func(request *http.Request) (string, int) {
		switch request.URL.Path {
		case "/taskmanagers":
			return `{"taskmanagers":[{"id":"tm:1"}]}`, http.StatusOK
		case "/taskmanagers/tm:1":
			return `{
				"id":"tm:1","path":"pekko://tm","dataPort":6122,"timeSinceLastHeartbeat":10000,
				"slotsNumber":4,"freeSlots":1,"assignedTasks":7,
				"hardware":{"cpuCores":8,"physicalMemory":8000,"freeMemory":2000,"managedMemory":1000},
				"memoryConfiguration":{"taskHeap":4000,"networkMemory":1000,"totalProcessMemory":8000},
				"metrics":{"heapUsed":3000,"heapCommitted":3500,"heapMax":4000,
					"nonHeapUsed":100,"nonHeapCommitted":200,"nonHeapMax":300,
					"directCount":4,"directUsed":138,"directMax":140,
					"mappedCount":0,"mappedUsed":0,"mappedMax":0,
					"nettyShuffleMemoryUsed":500,"nettyShuffleMemoryTotal":1000,
					"garbageCollectors":[{"name":"All","count":3,"time":20},{"name":"G1_Young_Generation","count":3,"time":20},{"name":"G1_Old_Generation","count":0,"time":0}]},
				"allocatedSlots":[{"jobId":"job","assignedTasks":7}]
			}`, http.StatusOK
		case "/taskmanagers/tm:1/metrics":
			if !strings.Contains(request.URL.Query().Get("get"), "Status.Flink.Memory.Managed.Used") ||
				!strings.Contains(request.URL.Query().Get("get"), "Status.JVM.Memory.Metaspace.Used") ||
				!strings.Contains(request.URL.Query().Get("get"), "Status.JVM.Memory.Direct.TotalCapacity") ||
				!strings.Contains(request.URL.Query().Get("get"), "Status.JVM.Memory.Mapped.Count") {
				t.Errorf("memory metric query = %q", request.URL.Query().Get("get"))
			}
			return `[
				{"id":"Status.JVM.Memory.Heap.Used","value":"3100"},{"id":"Status.JVM.Memory.Heap.Committed","value":"3600"},
				{"id":"Status.JVM.Memory.Heap.Max","value":"4100"},
				{"id":"Status.JVM.Memory.NonHeap.Used","value":"110"},{"id":"Status.JVM.Memory.NonHeap.Committed","value":"210"},{"id":"Status.JVM.Memory.NonHeap.Max","value":"310"},
				{"id":"Status.JVM.Memory.Direct.Count","value":"5"},{"id":"Status.JVM.Memory.Direct.MemoryUsed","value":"139"},{"id":"Status.JVM.Memory.Direct.TotalCapacity","value":"141"},
				{"id":"Status.JVM.Memory.Mapped.Count","value":"0"},{"id":"Status.JVM.Memory.Mapped.MemoryUsed","value":"0"},{"id":"Status.JVM.Memory.Mapped.TotalCapacity","value":"0"},
				{"id":"Status.Shuffle.Netty.UsedMemory","value":"510"},
				{"id":"Status.Shuffle.Netty.TotalMemory","value":"1010"},
				{"id":"Status.Flink.Memory.Managed.Used","value":"250"},
				{"id":"Status.Flink.Memory.Managed.Total","value":"1000"},
				{"id":"Status.JVM.Memory.Metaspace.Used","value":"120"},
				{"id":"Status.JVM.Memory.Metaspace.Max","value":"500"}
			]`, http.StatusOK
		case "/jobmanager/config":
			return `[{"key":"z.key","value":"z"},{"key":"a.key","value":"a"}]`, http.StatusOK
		case "/jobmanager/environment":
			return `{"jvm":{"version":"OpenJDK 17","arch":"aarch64"}}`, http.StatusOK
		case "/jobmanager/metrics":
			if !strings.Contains(request.URL.Query().Get("get"), "Status.JVM.Memory.NonHeap.Committed") ||
				!strings.Contains(request.URL.Query().Get("get"), "Status.JVM.Memory.Direct.TotalCapacity") ||
				!strings.Contains(request.URL.Query().Get("get"), "Status.JVM.Memory.Mapped.Count") {
				t.Errorf("JobManager metric query = %q", request.URL.Query().Get("get"))
			}
			return `[
				{"id":"Status.JVM.CPU.Load","value":"0.25"},{"id":"Status.JVM.Memory.Heap.Used","value":"1000"},
				{"id":"Status.JVM.Memory.Heap.Committed","value":"2000"},{"id":"Status.JVM.Memory.Heap.Max","value":"4000"},
				{"id":"Status.JVM.Memory.NonHeap.Used","value":"100"},{"id":"Status.JVM.Memory.NonHeap.Committed","value":"200"},{"id":"Status.JVM.Memory.NonHeap.Max","value":"700"},
				{"id":"Status.JVM.Memory.Direct.Count","value":"4"},{"id":"Status.JVM.Memory.Direct.MemoryUsed","value":"138"},{"id":"Status.JVM.Memory.Direct.TotalCapacity","value":"140"},
				{"id":"Status.JVM.Memory.Mapped.Count","value":"0"},{"id":"Status.JVM.Memory.Mapped.MemoryUsed","value":"0"},{"id":"Status.JVM.Memory.Mapped.TotalCapacity","value":"0"},
				{"id":"Status.JVM.Threads.Count","value":"42"},
				{"id":"numRegisteredTaskManagers","value":"1"},{"id":"numRunningJobs","value":"2"},
				{"id":"taskSlotsAvailable","value":"1"},{"id":"taskSlotsTotal","value":"4"}
			]`, http.StatusOK
		case "/jobmanager/logs":
			return `{"logs":[{"name":"jobmanager.log","size":1234,"mtime":20000}]}`, http.StatusOK
		default:
			return "", http.StatusNotFound
		}
	})

	infrastructure, err := client.Infrastructure(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(infrastructure.TaskManagers) != 1 {
		t.Fatalf("task managers = %#v", infrastructure.TaskManagers)
	}
	manager := infrastructure.TaskManagers[0]
	if manager.ID != "tm:1" || manager.Slots != 4 || manager.FreeSlots != 1 || manager.HeapUsed != 3100 ||
		manager.ManagedUsed != 250 || manager.ManagedTotal != 1000 || manager.MetaspaceUsed != 120 || manager.MetaspaceMax != 500 ||
		manager.NonHeapCommitted != 210 || manager.NonHeapMax != 310 || manager.DirectCount != 5 || manager.DirectMax != 141 ||
		manager.MappedCount != 0 || manager.MappedUsed != 0 || manager.MappedMax != 0 ||
		manager.GCCount != 3 || manager.GCTime != 20*time.Millisecond || len(manager.GarbageCollectors) != 3 ||
		manager.GarbageCollectors[2].Name != "G1_Old_Generation" || manager.GarbageCollectors[2].Count != 0 || len(manager.Allocations) != 1 {
		t.Fatalf("task manager = %#v", manager)
	}
	jobManager := infrastructure.JobManager
	if jobManager.CPUPercent != 25 || jobManager.Threads != 42 || jobManager.SlotsAvailable != 1 ||
		jobManager.HeapCommitted != 2000 || jobManager.NonHeapCommitted != 200 || jobManager.NonHeapMax != 700 ||
		jobManager.DirectCount != 4 || jobManager.DirectMax != 140 || jobManager.MappedCount != 0 ||
		jobManager.JVMVersion != "OpenJDK 17" || len(jobManager.Configuration) != 2 ||
		jobManager.Configuration[0].Key != "a.key" || len(jobManager.Logs) != 1 {
		t.Fatalf("job manager = %#v", jobManager)
	}
}
