package infrastructure

import (
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/x/ansi"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestTaskManagerMemoryModelMapsConfigurationToLiveUsage(t *testing.T) {
	const mib = int64(1024 * 1024)
	manager := flink.TaskManager{
		PhysicalMemory: 8 * 1024 * mib, FreeMemory: 2 * 1024 * mib,
		FrameworkHeap: 128 * mib, TaskHeap: 384 * mib, FrameworkOffHeap: 128 * mib,
		ManagedMemory: 512 * mib, NetworkMemory: 128 * mib, Metaspace: 256 * mib, JVMOverhead: 192 * mib,
		TotalFlinkMemory: 1280 * mib, TotalProcessMemory: 1728 * mib,
		HeapUsed: 256 * mib, HeapCommitted: 512 * mib, HeapMax: 512 * mib,
		ManagedUsed: 128 * mib, ManagedTotal: 512 * mib,
		ShuffleUsed: 32 * mib, ShuffleTotal: 128 * mib,
		MetaspaceUsed: 64 * mib, MetaspaceMax: 256 * mib,
	}
	plain := ansi.Strip(strings.Join(renderTaskManagerMemoryModel(manager, 140), "\n"))
	for _, expected := range []string{
		"MEMORY MODEL  CONFIG -> LIVE",
		"TOTAL PROCESS",
		"F=Flink",
		"FLINK MEMORY",
		"JVM HEAP",
		"framework heap",
		"MANAGED",
		"OFF-HEAP",
		"NETWORK",
		"JVM METASPACE",
		"JVM OVERHEAD",
		"50.0%",
		"25.0%",
		"live 256MiB / 512MiB",
	} {
		if !strings.Contains(plain, expected) {
			t.Fatalf("memory model missing %q:\n%s", expected, plain)
		}
	}
}

func TestMemoryModelDoesNotInventUnavailableLiveMetric(t *testing.T) {
	line := ansi.Strip(memoryModelUsageLine("+--", "MANAGED", 512, 0, 0, 10, 100))
	if !strings.Contains(line, "live unavailable") || strings.Contains(line, "0.0%") {
		t.Fatalf("unavailable metric rendered as measured usage: %q", line)
	}
}

func TestTaskManagerDetailPreservesRuntimeMemoryAndCollectorBreakdown(t *testing.T) {
	const mib = int64(1024 * 1024)
	model := New(nil, nil, nil)
	model.Restore(flink.Infrastructure{TaskManagers: []flink.TaskManager{{
		ID: "tm:1", NonHeapUsed: 99 * mib, NonHeapCommitted: 102 * mib, NonHeapMax: 704 * mib,
		DirectCount: 4155, DirectUsed: 138 * mib, DirectMax: 138 * mib,
		MappedCount: 0, MappedUsed: 0, MappedMax: 0,
		GarbageCollectors: []flink.GarbageCollector{
			{Name: "All", Count: 1626, Time: 8847 * time.Millisecond},
			{Name: "G1_Young_Generation", Count: 1626, Time: 8847 * time.Millisecond},
			{Name: "G1_Old_Generation", Count: 0, Time: 0},
		},
	}}})
	model.Activate(ViewTaskManagerDetail)
	plain := ansi.Strip(model.Render(140, 42))
	for _, expected := range []string{
		"non-heap  used 99.0MiB  |  committed 102MiB  |  max 704MiB",
		"direct buffers  count 4155  |  used 138MiB  |  capacity 138MiB",
		"mapped buffers  count 0  |  used 0B  |  capacity 0B",
		"All", "1626 collections", "8.8s",
		"G1_Young_Generation", "G1_Old_Generation", "0 collections", "0ms",
	} {
		if !strings.Contains(plain, expected) {
			t.Fatalf("TaskManager runtime detail missing %q:\n%s", expected, plain)
		}
	}
}
