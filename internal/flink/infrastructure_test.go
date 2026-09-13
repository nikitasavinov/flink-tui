package flink

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestTaskManagerAssignedTasksPreservesAbsentAndZeroCounts(t *testing.T) {
	for _, test := range []struct {
		name            string
		body            string
		managerCount    int
		managerKnown    bool
		allocationCount int
		allocationKnown bool
	}{
		{
			name: "Flink 1.20 allocated worker without task counts",
			body: `{"id":"tm-1","slotsNumber":4,"freeSlots":1,
				"allocatedSlots":[{"jobId":"job","resource":{"cpuCores":1.0,"taskHeapMemory":128}}]}`,
		},
		{
			name: "explicit null counts are unknown",
			body: `{"id":"tm-1","slotsNumber":4,"freeSlots":1,"assignedTasks":null,
				"allocatedSlots":[{"jobId":"job","assignedTasks":null}]}`,
		},
		{
			name: "reported zero counts remain known",
			body: `{"id":"tm-1","slotsNumber":4,"freeSlots":1,"assignedTasks":0,
				"allocatedSlots":[{"jobId":"job","assignedTasks":0}]}`,
			managerKnown: true, allocationKnown: true,
		},
		{
			name: "reported task counts differ from occupied slots",
			body: `{"id":"tm-1","slotsNumber":4,"freeSlots":1,"assignedTasks":7,
				"allocatedSlots":[{"jobId":"job","assignedTasks":5}]}`,
			managerCount: 7, managerKnown: true, allocationCount: 5, allocationKnown: true,
		},
		{
			name: "worker count does not invent allocation count",
			body: `{"id":"tm-1","slotsNumber":4,"freeSlots":1,"assignedTasks":7,
				"allocatedSlots":[{"jobId":"job"}]}`,
			managerCount: 7, managerKnown: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := testClient(t, func(request *http.Request) (string, int) {
				switch request.URL.Path {
				case "/taskmanagers":
					return `{"taskmanagers":[{"id":"tm-1"}]}`, http.StatusOK
				case "/taskmanagers/tm-1":
					return test.body, http.StatusOK
				case "/taskmanagers/tm-1/metrics":
					return `[]`, http.StatusOK
				default:
					t.Errorf("unexpected request: %s", request.URL.Path)
					return "", http.StatusNotFound
				}
			})
			managers, err := client.taskManagers(context.Background())
			if err != nil || len(managers) != 1 {
				t.Fatalf("TaskManager fetch = %#v, %v", managers, err)
			}
			manager := managers[0]
			if manager.AssignedTasks != test.managerCount || manager.AssignedTasksKnown != test.managerKnown {
				t.Errorf("worker tasks = %d known=%t; want %d known=%t", manager.AssignedTasks, manager.AssignedTasksKnown, test.managerCount, test.managerKnown)
			}
			if manager.Slots != 4 || manager.FreeSlots != 1 || len(manager.Allocations) != 1 {
				t.Fatalf("slot and allocation data was lost: %#v", manager)
			}
			allocation := manager.Allocations[0]
			if allocation.JobID != "job" || allocation.AssignedTasks != test.allocationCount || allocation.AssignedTasksKnown != test.allocationKnown {
				t.Errorf("allocation = %#v; want job tasks=%d known=%t", allocation, test.allocationCount, test.allocationKnown)
			}
		})
	}
}

func TestTaskManagersBoundsConcurrentFetchesAndLoadsEveryManager(t *testing.T) {
	const count = 25
	started := make(chan struct{}, count)
	release := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := taskManagerConcurrencyClient(t, count, started, release)
	type result struct {
		managers []TaskManager
		err      error
	}
	done := make(chan result, 1)
	go func() {
		managers, err := client.taskManagers(ctx)
		done <- result{managers, err}
	}()
	for range taskManagerFetchConcurrency {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("TaskManager requests did not fill the allowed concurrency")
		}
	}
	select {
	case <-started:
		t.Error("scheduled another TaskManager while every request slot was occupied")
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	select {
	case got := <-done:
		if got.err != nil || len(got.managers) != count {
			t.Fatalf("TaskManager fetch = %d managers, %v", len(got.managers), got.err)
		}
		for index, manager := range got.managers {
			if manager.ID != fmt.Sprintf("tm-%02d", index) || manager.HeapUsed != 123 {
				t.Errorf("TaskManager %d lost detail or metrics: %#v", index, manager)
			}
		}
	case <-ctx.Done():
		t.Fatal("bounded TaskManager requests did not finish")
	}
}

func TestTaskManagersCancellationStopsQueuedFetches(t *testing.T) {
	const count = 50
	started := make(chan struct{}, count)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := taskManagerConcurrencyClient(t, count, started, make(chan struct{}))
	done := make(chan error, 1)
	go func() {
		_, err := client.taskManagers(ctx)
		done <- err
	}()
	for range taskManagerFetchConcurrency {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("TaskManager requests did not start")
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled TaskManager fetch = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("TaskManager requests did not stop after cancellation")
	}
	if queued := len(started); queued != 0 {
		t.Fatalf("started %d queued TaskManager requests after cancellation", queued)
	}
}

func taskManagerConcurrencyClient(t *testing.T, count int, started chan<- struct{}, release <-chan struct{}) *Client {
	t.Helper()
	var summaries []string
	for index := range count {
		summaries = append(summaries, fmt.Sprintf(`{"id":"tm-%02d"}`, index))
	}
	return testClient(t, func(request *http.Request) (string, int) {
		if request.URL.Path == "/taskmanagers" {
			return `{"taskmanagers":[` + strings.Join(summaries, ",") + `]}`, http.StatusOK
		}
		if strings.HasSuffix(request.URL.Path, "/metrics") {
			return `[{"id":"Status.JVM.Memory.Heap.Used","value":"123"}]`, http.StatusOK
		}
		started <- struct{}{}
		select {
		case <-release:
			return fmt.Sprintf(`{"id":%q}`, strings.TrimPrefix(request.URL.Path, "/taskmanagers/")), http.StatusOK
		case <-request.Context().Done():
			return "canceled", http.StatusGatewayTimeout
		}
	})
}
