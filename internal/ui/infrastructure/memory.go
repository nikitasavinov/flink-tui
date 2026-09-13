package infrastructure

import (
	"fmt"
	"image/color"
	"strings"

	"charm.land/lipgloss/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

func renderTaskManagerMemoryModel(manager flink.TaskManager, width int) []string {
	heapConfig := manager.FrameworkHeap + manager.TaskHeap
	if heapConfig <= 0 {
		heapConfig = manager.HeapMax
	}
	offHeapConfig := manager.FrameworkOffHeap + manager.TaskOffHeap
	processTotal := manager.TotalProcessMemory
	if processTotal <= 0 {
		processTotal = manager.TotalFlinkMemory + manager.Metaspace + manager.JVMOverhead
	}
	barWidth := 18
	if width < 100 {
		barWidth = 10
	}
	compositionWidth := shared.Clamp(width-78, 14, 34)

	return []string{
		renderDetailSection("MEMORY MODEL  CONFIG -> LIVE", width),
		shared.Truncate(fmt.Sprintf(" TOTAL PROCESS  cfg %s  |  physical %s  |  host free %s",
			shared.HumanBytes(processTotal), shared.HumanBytes(manager.PhysicalMemory), shared.HumanBytes(manager.FreeMemory)), width),
		shared.TruncateStyled(" composition "+renderMemoryComposition(manager.TotalFlinkMemory, manager.Metaspace, manager.JVMOverhead, compositionWidth)+
			fmt.Sprintf("  F=Flink %s  M=Metaspace %s  O=Overhead %s",
				shared.HumanBytes(manager.TotalFlinkMemory), shared.HumanBytes(manager.Metaspace), shared.HumanBytes(manager.JVMOverhead)), width),
		shared.Truncate(fmt.Sprintf(" +-- FLINK MEMORY       cfg %s  (%.1f%% of process)",
			shared.HumanBytes(manager.TotalFlinkMemory), percentOf(manager.TotalFlinkMemory, processTotal)), width),
		memoryModelUsageLine(" |   +--", "JVM HEAP", heapConfig, manager.HeapUsed, manager.HeapMax, barWidth, width),
		shared.Truncate(fmt.Sprintf(" |   |     framework heap %s + task heap %s  |  committed %s",
			shared.HumanBytes(manager.FrameworkHeap), shared.HumanBytes(manager.TaskHeap), shared.HumanBytes(manager.HeapCommitted)), width),
		memoryModelUsageLine(" |   +--", "MANAGED", manager.ManagedMemory, manager.ManagedUsed, manager.ManagedTotal, barWidth, width),
		shared.Truncate(fmt.Sprintf(" |   +-- OFF-HEAP        cfg %s  |  framework %s + task %s  |  no direct usage mapping",
			shared.HumanBytes(offHeapConfig), shared.HumanBytes(manager.FrameworkOffHeap), shared.HumanBytes(manager.TaskOffHeap)), width),
		memoryModelUsageLine(" |   +--", "NETWORK", manager.NetworkMemory, manager.ShuffleUsed, manager.ShuffleTotal, barWidth, width),
		memoryModelUsageLine(" +--", "JVM METASPACE", manager.Metaspace, manager.MetaspaceUsed, manager.MetaspaceMax, barWidth, width),
		shared.Truncate(fmt.Sprintf(" +-- JVM OVERHEAD       cfg %s  |  JVM-native reserve (no direct live metric)", shared.HumanBytes(manager.JVMOverhead)), width),
	}
}

func memoryModelUsageLine(prefix, label string, configured, used, total int64, barWidth, width int) string {
	name := shared.PadRight(label, 16)
	configuredText := shared.HumanBytes(configured)
	if total <= 0 {
		line := fmt.Sprintf(" %s %s cfg %s  %s  live unavailable", prefix, name, configuredText, memoryUsageBar(0, 0, barWidth))
		return shared.TruncateStyled(line, width)
	}
	line := fmt.Sprintf(" %s %s cfg %s  %s %5.1f%%  live %s / %s",
		prefix, name, configuredText, memoryUsageBar(used, total, barWidth), percentOf(used, total), shared.HumanBytes(used), shared.HumanBytes(total))
	return shared.TruncateStyled(line, width)
}

func memoryUsageBar(used, total int64, width int) string {
	width = max(4, width)
	filled := 0
	if used > 0 && total > 0 {
		filled = shared.Clamp(int(float64(used)/float64(total)*float64(width)+0.5), 0, width)
	}
	level := percentOf(used, total)
	barColor := shared.C("#34D399")
	if level >= 90 {
		barColor = shared.C("#FB7185")
	} else if level >= 70 {
		barColor = shared.C("#FBBF24")
	}
	fill := lipgloss.NewStyle().Foreground(barColor).Bold(true).Render(strings.Repeat("#", filled))
	empty := lipgloss.NewStyle().Foreground(shared.C("#334155")).Render(strings.Repeat("-", width-filled))
	return lipgloss.NewStyle().Foreground(shared.C("#64748B")).Render("[") + fill + empty +
		lipgloss.NewStyle().Foreground(shared.C("#64748B")).Render("]")
}

func renderMemoryComposition(flinkMemory, metaspace, overhead int64, width int) string {
	width = max(6, width)
	total := flinkMemory + metaspace + overhead
	if total <= 0 {
		return "[" + strings.Repeat("-", width) + "]"
	}
	colors := map[byte]color.Color{
		'F': shared.C("#34D399"),
		'M': shared.C("#60A5FA"),
		'O': shared.C("#A78BFA"),
	}
	segments := []struct {
		label byte
		end   int64
	}{
		{label: 'F', end: flinkMemory},
		{label: 'M', end: flinkMemory + metaspace},
		{label: 'O', end: total},
	}
	var result strings.Builder
	result.WriteString(lipgloss.NewStyle().Foreground(shared.C("#64748B")).Render("["))
	current := byte(0)
	var run strings.Builder
	flush := func() {
		if run.Len() == 0 {
			return
		}
		result.WriteString(lipgloss.NewStyle().Foreground(colors[current]).Bold(true).Render(run.String()))
		run.Reset()
	}
	for index := 0; index < width; index++ {
		position := int64((float64(index) + 0.5) * float64(total) / float64(width))
		label := byte('O')
		for _, segment := range segments {
			if position < segment.end {
				label = segment.label
				break
			}
		}
		if current != 0 && label != current {
			flush()
		}
		current = label
		run.WriteByte(label)
	}
	flush()
	result.WriteString(lipgloss.NewStyle().Foreground(shared.C("#64748B")).Render("]"))
	return result.String()
}
