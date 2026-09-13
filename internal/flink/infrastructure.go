package flink

import (
	"cmp"
	"context"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"
)

type Infrastructure struct {
	TaskManagers []TaskManager
	JobManager   JobManager
	UpdatedAt    time.Time
}

type TaskManager struct {
	ID                   string
	Path                 string
	DataPort             int
	LastHeartbeat        time.Time
	Slots                int
	FreeSlots            int
	AssignedTasks        int
	AssignedTasksKnown   bool // Flink 1.20 does not report assignedTasks.
	CPUCores             int
	PhysicalMemory       int64
	FreeMemory           int64
	ManagedMemory        int64
	ManagedUsed          int64
	ManagedTotal         int64
	FrameworkHeap        int64
	TaskHeap             int64
	FrameworkOffHeap     int64
	TaskOffHeap          int64
	NetworkMemory        int64
	Metaspace            int64
	MetaspaceUsed        int64
	MetaspaceMax         int64
	JVMOverhead          int64
	TotalFlinkMemory     int64
	TotalProcessMemory   int64
	HeapUsed             int64
	HeapCommitted        int64
	HeapMax              int64
	NonHeapUsed          int64
	NonHeapCommitted     int64
	NonHeapMax           int64
	DirectCount          int64
	DirectUsed           int64
	DirectMax            int64
	MappedCount          int64
	MappedUsed           int64
	MappedMax            int64
	ShuffleUsed          int64
	ShuffleTotal         int64
	ShuffleSegmentsUsed  int64
	ShuffleSegmentsTotal int64
	GCCount              int64
	GCTime               time.Duration
	GarbageCollectors    []GarbageCollector
	Allocations          []TaskManagerAllocation
}

type GarbageCollector struct {
	Name  string
	Count int64
	Time  time.Duration
}

type TaskManagerAllocation struct {
	JobID              string
	AssignedTasks      int
	AssignedTasksKnown bool // Flink 1.20 does not report assignedTasks.
}

type JobManager struct {
	JVMVersion       string
	Architecture     string
	CPUPercent       float64
	HeapUsed         int64
	HeapCommitted    int64
	HeapMax          int64
	NonHeapUsed      int64
	NonHeapCommitted int64
	NonHeapMax       int64
	DirectCount      int64
	DirectUsed       int64
	DirectMax        int64
	MappedCount      int64
	MappedUsed       int64
	MappedMax        int64
	Threads          int64
	GCCount          int64
	GCTime           time.Duration
	OpenDescriptors  int64
	MaxDescriptors   int64
	TaskManagers     int
	RunningJobs      int
	SlotsAvailable   int
	SlotsTotal       int
	Configuration    []ConfigurationEntry
	Logs             []LogFile
}

type LogFile struct {
	Name       string
	Size       int64
	ModifiedAt time.Time
}

const taskManagerFetchConcurrency = 8

func (c *Client) Infrastructure(ctx context.Context) (Infrastructure, error) {
	var (
		taskManagers   []TaskManager
		jobManager     JobManager
		taskErr        error
		configErr      error
		environmentErr error
		metricsErr     error
		logsErr        error
		mutex          sync.Mutex
		wait           sync.WaitGroup
	)

	wait.Add(5)
	go func() {
		defer wait.Done()
		taskManagers, taskErr = c.taskManagers(ctx)
	}()
	go func() {
		defer wait.Done()
		jobManager.Configuration, configErr = c.configuration(ctx, "/jobmanager/config")
	}()
	go func() {
		defer wait.Done()
		var response struct {
			JVM struct {
				Version string `json:"version"`
				Arch    string `json:"arch"`
			} `json:"jvm"`
		}
		environmentErr = c.get(ctx, "/jobmanager/environment", &response)
		if environmentErr == nil {
			mutex.Lock()
			jobManager.JVMVersion = response.JVM.Version
			jobManager.Architecture = response.JVM.Arch
			mutex.Unlock()
		}
	}()
	go func() {
		defer wait.Done()
		metrics, err := c.jobManagerMetrics(ctx)
		metricsErr = err
		if err == nil {
			mutex.Lock()
			jobManager.CPUPercent = finite(metrics["Status.JVM.CPU.Load"] * 100)
			jobManager.HeapUsed = int64(metrics["Status.JVM.Memory.Heap.Used"])
			jobManager.HeapCommitted = int64(metrics["Status.JVM.Memory.Heap.Committed"])
			jobManager.HeapMax = int64(metrics["Status.JVM.Memory.Heap.Max"])
			jobManager.NonHeapUsed = int64(metrics["Status.JVM.Memory.NonHeap.Used"])
			jobManager.NonHeapCommitted = int64(metrics["Status.JVM.Memory.NonHeap.Committed"])
			jobManager.NonHeapMax = int64(metrics["Status.JVM.Memory.NonHeap.Max"])
			jobManager.DirectCount = int64(metrics["Status.JVM.Memory.Direct.Count"])
			jobManager.DirectUsed = int64(metrics["Status.JVM.Memory.Direct.MemoryUsed"])
			jobManager.DirectMax = int64(metrics["Status.JVM.Memory.Direct.TotalCapacity"])
			jobManager.MappedCount = int64(metrics["Status.JVM.Memory.Mapped.Count"])
			jobManager.MappedUsed = int64(metrics["Status.JVM.Memory.Mapped.MemoryUsed"])
			jobManager.MappedMax = int64(metrics["Status.JVM.Memory.Mapped.TotalCapacity"])
			jobManager.Threads = int64(metrics["Status.JVM.Threads.Count"])
			jobManager.GCCount = int64(metrics["Status.JVM.GarbageCollector.All.Count"])
			jobManager.GCTime = time.Duration(metrics["Status.JVM.GarbageCollector.All.Time"]) * time.Millisecond
			jobManager.OpenDescriptors = int64(metrics["Status.JVM.FileDescriptor.Open"])
			jobManager.MaxDescriptors = int64(metrics["Status.JVM.FileDescriptor.Max"])
			jobManager.TaskManagers = int(metrics["numRegisteredTaskManagers"])
			jobManager.RunningJobs = int(metrics["numRunningJobs"])
			jobManager.SlotsAvailable = int(metrics["taskSlotsAvailable"])
			jobManager.SlotsTotal = int(metrics["taskSlotsTotal"])
			mutex.Unlock()
		}
	}()
	go func() {
		defer wait.Done()
		var logs []LogFile
		logs, logsErr = c.LogFiles(ctx, JobManagerProcess())
		if logsErr == nil {
			mutex.Lock()
			jobManager.Logs = logs
			mutex.Unlock()
		}
	}()
	wait.Wait()

	if err := firstInfrastructureError(taskErr, configErr, environmentErr, metricsErr, logsErr); err != nil {
		return Infrastructure{}, err
	}
	return Infrastructure{TaskManagers: taskManagers, JobManager: jobManager, UpdatedAt: time.Now()}, nil
}

func (c *Client) taskManagers(ctx context.Context) ([]TaskManager, error) {
	var response struct {
		TaskManagers []taskManagerResponse `json:"taskmanagers"`
	}
	if err := c.get(ctx, "/taskmanagers", &response); err != nil {
		return nil, err
	}
	result := make([]TaskManager, len(response.TaskManagers))
	errors := make(chan error, len(response.TaskManagers))
	var wait sync.WaitGroup
	slots := make(chan struct{}, taskManagerFetchConcurrency)
	for index, summary := range response.TaskManagers {
		// Reserve capacity before creating the goroutine so large clusters
		// bound both REST requests and waiting goroutines. Cancellation also
		// stops scheduling managers that have not started loading yet.
		if err := ctx.Err(); err != nil {
			wait.Wait()
			return nil, err
		}
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			wait.Wait()
			return nil, ctx.Err()
		}
		if err := ctx.Err(); err != nil {
			<-slots
			wait.Wait()
			return nil, err
		}
		wait.Add(1)
		go func(index int, summary taskManagerResponse) {
			defer wait.Done()
			defer func() { <-slots }()
			var detail taskManagerResponse
			path := "/taskmanagers/" + url.PathEscape(summary.ID)
			if err := c.get(ctx, path, &detail); err != nil {
				errors <- err
				return
			}
			manager := taskManagerFromResponse(detail)
			if metrics, metricErr := c.taskManagerMemoryMetrics(ctx, summary.ID); metricErr == nil {
				applyTaskManagerMemoryMetrics(&manager, metrics)
			}
			result[index] = manager
		}(index, summary)
	}
	wait.Wait()
	close(errors)
	if err := <-errors; err != nil {
		return nil, err
	}
	slices.SortStableFunc(result, func(left, right TaskManager) int {
		return cmp.Compare(left.ID, right.ID)
	})
	return result, nil
}

func (c *Client) taskManagerMemoryMetrics(ctx context.Context, taskManagerID string) (map[string]float64, error) {
	metricNames := []string{
		"Status.JVM.Memory.Heap.Used",
		"Status.JVM.Memory.Heap.Committed",
		"Status.JVM.Memory.Heap.Max",
		"Status.JVM.Memory.NonHeap.Used",
		"Status.JVM.Memory.NonHeap.Committed",
		"Status.JVM.Memory.NonHeap.Max",
		"Status.JVM.Memory.Direct.Count",
		"Status.JVM.Memory.Direct.MemoryUsed",
		"Status.JVM.Memory.Direct.TotalCapacity",
		"Status.JVM.Memory.Mapped.Count",
		"Status.JVM.Memory.Mapped.MemoryUsed",
		"Status.JVM.Memory.Mapped.TotalCapacity",
		"Status.Shuffle.Netty.UsedMemory",
		"Status.Shuffle.Netty.TotalMemory",
		"Status.Flink.Memory.Managed.Used",
		"Status.Flink.Memory.Managed.Total",
		"Status.JVM.Memory.Metaspace.Used",
		"Status.JVM.Memory.Metaspace.Max",
	}
	query := url.Values{}
	query.Set("get", strings.Join(metricNames, ","))
	path := "/taskmanagers/" + url.PathEscape(taskManagerID) + "/metrics?" + query.Encode()
	var response []metricValue
	if err := c.get(ctx, path, &response); err != nil {
		return nil, err
	}
	metrics := make(map[string]float64, len(response))
	for _, metric := range response {
		metrics[metric.ID] = metric.Value.Float64()
	}
	return metrics, nil
}

func applyTaskManagerMemoryMetrics(manager *TaskManager, metrics map[string]float64) {
	if value, ok := metrics["Status.JVM.Memory.Heap.Used"]; ok {
		manager.HeapUsed = int64(value)
	}
	if value, ok := metrics["Status.JVM.Memory.Heap.Committed"]; ok {
		manager.HeapCommitted = int64(value)
	}
	if value, ok := metrics["Status.JVM.Memory.Heap.Max"]; ok {
		manager.HeapMax = int64(value)
	}
	if value, ok := metrics["Status.JVM.Memory.NonHeap.Used"]; ok {
		manager.NonHeapUsed = int64(value)
	}
	if value, ok := metrics["Status.JVM.Memory.NonHeap.Committed"]; ok {
		manager.NonHeapCommitted = int64(value)
	}
	if value, ok := metrics["Status.JVM.Memory.NonHeap.Max"]; ok {
		manager.NonHeapMax = int64(value)
	}
	if value, ok := metrics["Status.JVM.Memory.Direct.Count"]; ok {
		manager.DirectCount = int64(value)
	}
	if value, ok := metrics["Status.JVM.Memory.Direct.MemoryUsed"]; ok {
		manager.DirectUsed = int64(value)
	}
	if value, ok := metrics["Status.JVM.Memory.Direct.TotalCapacity"]; ok {
		manager.DirectMax = int64(value)
	}
	if value, ok := metrics["Status.JVM.Memory.Mapped.Count"]; ok {
		manager.MappedCount = int64(value)
	}
	if value, ok := metrics["Status.JVM.Memory.Mapped.MemoryUsed"]; ok {
		manager.MappedUsed = int64(value)
	}
	if value, ok := metrics["Status.JVM.Memory.Mapped.TotalCapacity"]; ok {
		manager.MappedMax = int64(value)
	}
	if value, ok := metrics["Status.Shuffle.Netty.UsedMemory"]; ok {
		manager.ShuffleUsed = int64(value)
	}
	if value, ok := metrics["Status.Shuffle.Netty.TotalMemory"]; ok {
		manager.ShuffleTotal = int64(value)
	}
	if value, ok := metrics["Status.Flink.Memory.Managed.Used"]; ok {
		manager.ManagedUsed = int64(value)
	}
	if value, ok := metrics["Status.Flink.Memory.Managed.Total"]; ok {
		manager.ManagedTotal = int64(value)
	}
	if value, ok := metrics["Status.JVM.Memory.Metaspace.Used"]; ok {
		manager.MetaspaceUsed = int64(value)
	}
	if value, ok := metrics["Status.JVM.Memory.Metaspace.Max"]; ok {
		manager.MetaspaceMax = int64(value)
	}
}

func (c *Client) configuration(ctx context.Context, path string) ([]ConfigurationEntry, error) {
	var response []struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := c.get(ctx, path, &response); err != nil {
		return nil, err
	}
	entries := make([]ConfigurationEntry, len(response))
	for index, item := range response {
		entries[index] = ConfigurationEntry{Key: item.Key, Value: item.Value}
	}
	slices.SortStableFunc(entries, func(left, right ConfigurationEntry) int {
		return cmp.Compare(left.Key, right.Key)
	})
	return entries, nil
}

func (c *Client) jobManagerMetrics(ctx context.Context) (map[string]float64, error) {
	metricNames := []string{
		"Status.JVM.CPU.Load",
		"Status.JVM.Memory.Heap.Used",
		"Status.JVM.Memory.Heap.Committed",
		"Status.JVM.Memory.Heap.Max",
		"Status.JVM.Memory.NonHeap.Used",
		"Status.JVM.Memory.NonHeap.Committed",
		"Status.JVM.Memory.NonHeap.Max",
		"Status.JVM.Memory.Direct.Count",
		"Status.JVM.Memory.Direct.MemoryUsed",
		"Status.JVM.Memory.Direct.TotalCapacity",
		"Status.JVM.Memory.Mapped.Count",
		"Status.JVM.Memory.Mapped.MemoryUsed",
		"Status.JVM.Memory.Mapped.TotalCapacity",
		"Status.JVM.Threads.Count",
		"Status.JVM.GarbageCollector.All.Count",
		"Status.JVM.GarbageCollector.All.Time",
		"Status.JVM.FileDescriptor.Open",
		"Status.JVM.FileDescriptor.Max",
		"numRegisteredTaskManagers",
		"numRunningJobs",
		"taskSlotsAvailable",
		"taskSlotsTotal",
	}
	query := url.Values{}
	query.Set("get", strings.Join(metricNames, ","))
	var response []metricValue
	if err := c.get(ctx, "/jobmanager/metrics?"+query.Encode(), &response); err != nil {
		return nil, err
	}
	metrics := make(map[string]float64, len(response))
	for _, metric := range response {
		metrics[metric.ID] = metric.Value.Float64()
	}
	return metrics, nil
}

func taskManagerFromResponse(response taskManagerResponse) TaskManager {
	result := TaskManager{
		ID:                   response.ID,
		Path:                 response.Path,
		DataPort:             response.DataPort,
		LastHeartbeat:        unixMillis(response.LastHeartbeat),
		Slots:                response.Slots,
		FreeSlots:            response.FreeSlots,
		CPUCores:             response.Hardware.CPUCores,
		PhysicalMemory:       response.Hardware.PhysicalMemory,
		FreeMemory:           response.Hardware.FreeMemory,
		ManagedMemory:        response.Hardware.ManagedMemory,
		FrameworkHeap:        response.Memory.FrameworkHeap,
		TaskHeap:             response.Memory.TaskHeap,
		FrameworkOffHeap:     response.Memory.FrameworkOffHeap,
		TaskOffHeap:          response.Memory.TaskOffHeap,
		NetworkMemory:        response.Memory.NetworkMemory,
		Metaspace:            response.Memory.JVMMetaspace,
		JVMOverhead:          response.Memory.JVMOverhead,
		TotalFlinkMemory:     response.Memory.TotalFlinkMemory,
		TotalProcessMemory:   response.Memory.TotalProcessMemory,
		HeapUsed:             response.Metrics.HeapUsed,
		HeapCommitted:        response.Metrics.HeapCommitted,
		HeapMax:              response.Metrics.HeapMax,
		NonHeapUsed:          response.Metrics.NonHeapUsed,
		NonHeapCommitted:     response.Metrics.NonHeapCommitted,
		NonHeapMax:           response.Metrics.NonHeapMax,
		DirectCount:          response.Metrics.DirectCount,
		DirectUsed:           response.Metrics.DirectUsed,
		DirectMax:            response.Metrics.DirectMax,
		MappedCount:          response.Metrics.MappedCount,
		MappedUsed:           response.Metrics.MappedUsed,
		MappedMax:            response.Metrics.MappedMax,
		ShuffleUsed:          response.Metrics.ShuffleUsed,
		ShuffleTotal:         response.Metrics.ShuffleTotal,
		ShuffleSegmentsUsed:  response.Metrics.ShuffleSegmentsUsed,
		ShuffleSegmentsTotal: response.Metrics.ShuffleSegmentsTotal,
		Allocations:          make([]TaskManagerAllocation, len(response.AllocatedSlots)),
	}
	if response.AssignedTasks != nil {
		result.AssignedTasks = *response.AssignedTasks
		result.AssignedTasksKnown = true
	}
	result.GarbageCollectors = make([]GarbageCollector, len(response.Metrics.GarbageCollectors))
	foundAggregate := false
	for index, collector := range response.Metrics.GarbageCollectors {
		result.GarbageCollectors[index] = GarbageCollector{
			Name: collector.Name, Count: collector.Count, Time: time.Duration(collector.Time) * time.Millisecond,
		}
		if collector.Name == "All" {
			result.GCCount = collector.Count
			result.GCTime = time.Duration(collector.Time) * time.Millisecond
			foundAggregate = true
		}
	}
	if !foundAggregate {
		for _, collector := range result.GarbageCollectors {
			result.GCCount += collector.Count
			result.GCTime += collector.Time
		}
	}
	for index, allocation := range response.AllocatedSlots {
		result.Allocations[index] = TaskManagerAllocation{JobID: allocation.JobID}
		if allocation.AssignedTasks != nil {
			result.Allocations[index].AssignedTasks = *allocation.AssignedTasks
			result.Allocations[index].AssignedTasksKnown = true
		}
	}
	return result
}

type taskManagerResponse struct {
	ID            string `json:"id"`
	Path          string `json:"path"`
	DataPort      int    `json:"dataPort"`
	LastHeartbeat int64  `json:"timeSinceLastHeartbeat"`
	Slots         int    `json:"slotsNumber"`
	FreeSlots     int    `json:"freeSlots"`
	AssignedTasks *int   `json:"assignedTasks"`
	Hardware      struct {
		CPUCores       int   `json:"cpuCores"`
		PhysicalMemory int64 `json:"physicalMemory"`
		FreeMemory     int64 `json:"freeMemory"`
		ManagedMemory  int64 `json:"managedMemory"`
	} `json:"hardware"`
	Memory struct {
		FrameworkHeap      int64 `json:"frameworkHeap"`
		TaskHeap           int64 `json:"taskHeap"`
		FrameworkOffHeap   int64 `json:"frameworkOffHeap"`
		TaskOffHeap        int64 `json:"taskOffHeap"`
		NetworkMemory      int64 `json:"networkMemory"`
		ManagedMemory      int64 `json:"managedMemory"`
		JVMMetaspace       int64 `json:"jvmMetaspace"`
		JVMOverhead        int64 `json:"jvmOverhead"`
		TotalFlinkMemory   int64 `json:"totalFlinkMemory"`
		TotalProcessMemory int64 `json:"totalProcessMemory"`
	} `json:"memoryConfiguration"`
	Metrics struct {
		HeapUsed             int64 `json:"heapUsed"`
		HeapCommitted        int64 `json:"heapCommitted"`
		HeapMax              int64 `json:"heapMax"`
		NonHeapUsed          int64 `json:"nonHeapUsed"`
		NonHeapCommitted     int64 `json:"nonHeapCommitted"`
		NonHeapMax           int64 `json:"nonHeapMax"`
		DirectCount          int64 `json:"directCount"`
		DirectUsed           int64 `json:"directUsed"`
		DirectMax            int64 `json:"directMax"`
		MappedCount          int64 `json:"mappedCount"`
		MappedUsed           int64 `json:"mappedUsed"`
		MappedMax            int64 `json:"mappedMax"`
		ShuffleUsed          int64 `json:"nettyShuffleMemoryUsed"`
		ShuffleTotal         int64 `json:"nettyShuffleMemoryTotal"`
		ShuffleSegmentsUsed  int64 `json:"nettyShuffleMemorySegmentsUsed"`
		ShuffleSegmentsTotal int64 `json:"nettyShuffleMemorySegmentsTotal"`
		GarbageCollectors    []struct {
			Name  string `json:"name"`
			Count int64  `json:"count"`
			Time  int64  `json:"time"`
		} `json:"garbageCollectors"`
	} `json:"metrics"`
	AllocatedSlots []struct {
		JobID         string `json:"jobId"`
		AssignedTasks *int   `json:"assignedTasks"`
	} `json:"allocatedSlots"`
}

func firstInfrastructureError(errors ...error) error {
	for _, err := range errors {
		if err != nil {
			return fmt.Errorf("load infrastructure: %w", err)
		}
	}
	return nil
}
