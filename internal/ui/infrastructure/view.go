package infrastructure

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"charm.land/lipgloss/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

const (
	taskManagerBodyRowStart = 3
	jobManagerBodyRowStart  = 10
)

// Render renders the active infrastructure-owned screen.
func (m Model) Render(width, height int) string {
	switch m.view {
	case ViewTaskManagerDetail:
		return m.renderTaskManagerDetail(width, height)
	case ViewJobManager:
		return m.renderJobManager(width, height)
	default:
		return m.renderTaskManagers(width, height)
	}
}

// Footer returns view-specific key hints.

func (m Model) Footer(mouse, routeBackLabel string) string {
	backLabel := shared.Fallback(routeBackLabel, m.backLabel)
	switch m.view {
	case ViewTaskManagerDetail:
		return fmt.Sprintf("left nav  </> worker  q/esc %s  tab JobManager  up/down/wheel scroll  home/end  p profiler  l current log  L log files  x stdout  d threads  r refresh  m mouse:%s", shared.Truncate(backLabel, 22), mouse)
	case ViewJobManager:
		if m.jobManagerFilter.Active() {
			return "filter JobManager config  type query  enter apply  esc cancel  ctrl+w clear"
		}
		section := "config"
		if m.jobManagerLogsOpen {
			return fmt.Sprintf("left nav  q/esc %s  tab TaskManagers  up/down logs  enter view log  p profiler  l current log  L configuration  x stdout  d threads  r refresh  m mouse:%s", shared.Truncate(backLabel, 22), mouse)
		}
		return fmt.Sprintf("left nav  q/esc %s  tab TaskManagers  up/down %s  / filter config  p profiler  l current log  L log files  x stdout  d threads  r refresh  m mouse:%s", shared.Truncate(backLabel, 22), section, mouse)
	default:
		return fmt.Sprintf("left nav  q/esc %s  tab JobManager  up/down select  enter inspect  p profiler  l current log  L log files  x stdout  d threads  r refresh  m mouse:%s", shared.Truncate(backLabel, 22), mouse)
	}
}

func taskManagerRowsAvailable(bodyHeight int) int {
	return max(1, bodyHeight-taskManagerBodyRowStart-7)
}

func (m Model) taskManagerWindowStart(bodyHeight int) int {
	return shared.WindowStart(m.taskManagerSelection.Index(), taskManagerRowsAvailable(bodyHeight), len(m.infrastructure.TaskManagers))
}

func jobManagerRowsAvailable(bodyHeight int) int {
	return max(1, bodyHeight-jobManagerBodyRowStart-3)
}

func (m Model) jobManagerConfigWindowStart(bodyHeight int) int {
	return shared.WindowStart(m.jobManagerConfig.Index(), jobManagerRowsAvailable(bodyHeight), len(m.filteredJobManagerConfiguration()))
}

func (m Model) jobManagerLogWindowStart(bodyHeight int) int {
	return shared.WindowStart(m.jobManagerLog.Index(), jobManagerRowsAvailable(bodyHeight), len(m.infrastructure.JobManager.Logs))
}

func detailViewportHeight(bodyHeight int) int { return max(1, bodyHeight-2) }

func (m Model) detailMaxOffset(bodyHeight, contentWidth int) int {
	manager, ok := m.SelectedTaskManager()
	if !ok {
		return 0
	}
	return max(0, len(taskManagerDetailContent(manager, contentWidth))-detailViewportHeight(bodyHeight))
}

func (m *Model) moveTaskManagerDetail(delta, bodyHeight, contentWidth int) {
	if delta == 0 {
		return
	}
	m.detailOffset = shared.Clamp(m.detailOffset+delta, 0, m.detailMaxOffset(bodyHeight, contentWidth))
}

func (m Model) renderTaskManagers(width, height int) string {
	title := fmt.Sprintf(" TASK MANAGERS  %d registered", len(m.infrastructure.TaskManagers))
	if m.busy {
		title += "  |  refreshing..."
	}
	status, statusStyle := m.status()
	lines := []string{
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#E2E8F0")).Render(shared.Truncate(title, width)),
		statusStyle.Render(shared.Truncate(status, width)),
		renderTaskManagerColumns(width),
	}
	start := m.taskManagerWindowStart(height)
	end := min(len(m.infrastructure.TaskManagers), start+taskManagerRowsAvailable(height))
	for index := start; index < end; index++ {
		lines = append(lines, renderTaskManagerRow(m.infrastructure.TaskManagers[index], index == m.taskManagerSelection.Index(), width))
	}
	if len(m.infrastructure.TaskManagers) == 0 && !m.busy {
		lines = append(lines, " No TaskManagers registered.")
	}
	for len(lines) < height-7 {
		lines = append(lines, "")
	}
	separator := lipgloss.NewStyle().Foreground(shared.C("#334155")).Render(strings.Repeat("-", max(0, width)))
	lines = append(lines, separator)
	lines = append(lines, m.renderSelectedTaskManager(width)...)
	return shared.FitLines(lines, width, height)
}

func (m Model) status() (string, lipgloss.Style) {
	status := " Baseline Flink process and slot data"
	style := lipgloss.NewStyle().Foreground(shared.C("#94A3B8"))
	if !m.infrastructure.UpdatedAt.IsZero() {
		status += "  |  updated " + m.infrastructure.UpdatedAt.Format("15:04:05")
	}
	if m.err != nil {
		status = " Could not load infrastructure: " + m.errorText(m.err)
		style = style.Foreground(shared.C("#FB7185"))
	}
	return status, style
}

func renderTaskManagerColumns(width int) string {
	line := "   TASKMANAGER                         SLOTS   TASKS     HEAP      NETWORK   HEARTBEAT"
	if width < 94 {
		line = "   TASKMANAGER                    SLOTS  TASKS    HEAP   NETWORK"
	}
	return lipgloss.NewStyle().Bold(true).Foreground(shared.C("#94A3B8")).Background(shared.C("#111827")).Render(
		shared.PadRight(shared.Truncate(line, width), width),
	)
}

func renderTaskManagerRow(manager flink.TaskManager, selected bool, width int) string {
	marker := " "
	if selected {
		marker = ">"
	}
	usedSlots := max(0, manager.Slots-manager.FreeSlots)
	heap := percentOf(manager.HeapUsed, manager.HeapMax)
	network := percentOf(manager.ShuffleUsed, manager.ShuffleTotal)
	nameWidth := 34
	if width < 94 {
		nameWidth = max(16, width-45)
	}
	identity := shared.TaskManagerIdentity(manager.ID)
	line := fmt.Sprintf(" %s %s %3d/%-3d %7s  %6.1f%%  %6.1f%%",
		marker, shared.PadRight(shared.Truncate(identity, nameWidth), nameWidth), usedSlots, manager.Slots,
		assignedTasksLabel(manager.AssignedTasks, manager.AssignedTasksKnown), heap, network)
	if width >= 94 {
		line += "  " + shared.Clock(manager.LastHeartbeat, "-")
	}
	line = shared.PadRight(shared.Truncate(line, width), width)
	if selected {
		return lipgloss.NewStyle().Foreground(shared.C("#FFFFFF")).Background(shared.C("#6D28D9")).Bold(true).Render(line)
	}
	style := lipgloss.NewStyle().Foreground(shared.C("#CBD5E1"))
	if manager.FreeSlots == 0 || heap > 90 || network > 90 {
		style = style.Foreground(shared.C("#FBBF24"))
	}
	return style.Render(line)
}

func assignedTasksLabel(count int, known bool) string {
	if !known {
		return "unknown"
	}
	return strconv.Itoa(count)
}

func (m Model) renderSelectedTaskManager(width int) []string {
	manager, ok := m.SelectedTaskManager()
	if !ok {
		return shared.FixedLineSlice([]string{" Select a TaskManager."}, 6)
	}
	usedSlots := max(0, manager.Slots-manager.FreeSlots)
	lines := []string{
		fmt.Sprintf(" %s  |  data port %d  |  CPU cores %d", shared.TaskManagerIdentity(manager.ID), manager.DataPort, manager.CPUCores),
		fmt.Sprintf(" slots %d/%d used  |  assigned tasks %s  |  allocations %d", usedSlots, manager.Slots, assignedTasksLabel(manager.AssignedTasks, manager.AssignedTasksKnown), len(manager.Allocations)),
		fmt.Sprintf(" heap %s / %s  |  non-heap %s  |  direct %s", shared.HumanBytes(manager.HeapUsed), shared.HumanBytes(manager.HeapMax), shared.HumanBytes(manager.NonHeapUsed), shared.HumanBytes(manager.DirectUsed)),
		fmt.Sprintf(" managed %s  |  network %s / %s  |  process %s", shared.HumanBytes(manager.ManagedMemory), shared.HumanBytes(manager.ShuffleUsed), shared.HumanBytes(manager.ShuffleTotal), shared.HumanBytes(manager.TotalProcessMemory)),
		fmt.Sprintf(" GC %d collections / %s  |  shuffle segments %d/%d", manager.GCCount, shared.HumanDuration(manager.GCTime), manager.ShuffleSegmentsUsed, manager.ShuffleSegmentsTotal),
		" enter details  |  p profiler  |  l current log  |  L log files  |  x stdout  |  d thread dump  |  tab JobManager",
	}
	for index := range lines {
		lines[index] = shared.Truncate(lines[index], width)
	}
	return lines
}

func (m Model) renderTaskManagerDetail(width, height int) string {
	manager, ok := m.SelectedTaskManager()
	if !ok {
		return shared.FitLines([]string{
			" TASK MANAGER DETAIL",
			" No TaskManager is selected.",
			" Press esc to return to TaskManagers.",
		}, width, height)
	}
	lines := []string{
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#E2E8F0")).Render(shared.Truncate(" TASK MANAGER DETAIL  "+manager.ID, width)),
		lipgloss.NewStyle().Foreground(shared.C("#94A3B8")).Render(shared.Truncate(" < "+m.backLabel+"  |  esc returns  |  click this header to return", width)),
	}
	content := taskManagerDetailContent(manager, width)
	viewportHeight := max(1, height-len(lines))
	start := shared.Clamp(m.detailOffset, 0, max(0, len(content)-viewportHeight))
	end := min(len(content), start+viewportHeight)
	lines = append(lines, content[start:end]...)
	return shared.FitLines(lines, width, height)
}

func taskManagerDetailContent(manager flink.TaskManager, width int) []string {
	usedSlots := max(0, manager.Slots-manager.FreeSlots)
	heartbeat := "unknown"
	if !manager.LastHeartbeat.IsZero() {
		heartbeat = manager.LastHeartbeat.Format(time.RFC3339)
	}
	lines := []string{
		renderDetailSection("IDENTITY AND CAPACITY", width),
		shared.Truncate(fmt.Sprintf(" address %s  |  data port %d  |  CPU cores %d", shared.Fallback(manager.Path, manager.ID), manager.DataPort, manager.CPUCores), width),
		shared.Truncate(fmt.Sprintf(" heartbeat %s  |  slots %d/%d used  |  assigned tasks %s", heartbeat, usedSlots, manager.Slots, assignedTasksLabel(manager.AssignedTasks, manager.AssignedTasksKnown)), width),
	}
	lines = append(lines, renderTaskManagerMemoryModel(manager, width)...)
	lines = append(lines, renderTaskManagerRuntimeMemory(manager, width)...)
	lines = append(lines, renderTaskManagerGarbageCollectors(manager, width)...)
	lines = append(lines, renderDetailSection(fmt.Sprintf("ALLOCATIONS  %d", len(manager.Allocations)), width))
	if len(manager.Allocations) == 0 {
		lines = append(lines, " No allocated jobs.")
	}
	for _, allocation := range manager.Allocations {
		lines = append(lines, shared.Truncate(fmt.Sprintf(" job %s  |  assigned tasks %s", allocation.JobID, assignedTasksLabel(allocation.AssignedTasks, allocation.AssignedTasksKnown)), width))
	}
	return lines
}

func renderDetailSection(label string, width int) string {
	return lipgloss.NewStyle().Bold(true).Foreground(shared.C("#94A3B8")).Background(shared.C("#111827")).Render(
		shared.PadRight(shared.Truncate(" "+label, width), width),
	)
}

func renderTaskManagerRuntimeMemory(manager flink.TaskManager, width int) []string {
	return []string{
		renderDetailSection("JVM AND NATIVE RUNTIME", width),
		shared.Truncate(fmt.Sprintf(" non-heap  used %s  |  committed %s  |  max %s",
			shared.HumanBytes(manager.NonHeapUsed), shared.HumanBytes(manager.NonHeapCommitted), shared.HumanBytes(manager.NonHeapMax)), width),
		shared.Truncate(fmt.Sprintf(" direct buffers  count %d  |  used %s  |  capacity %s",
			manager.DirectCount, shared.HumanBytes(manager.DirectUsed), shared.HumanBytes(manager.DirectMax)), width),
		shared.Truncate(fmt.Sprintf(" mapped buffers  count %d  |  used %s  |  capacity %s",
			manager.MappedCount, shared.HumanBytes(manager.MappedUsed), shared.HumanBytes(manager.MappedMax)), width),
		shared.Truncate(fmt.Sprintf(" shuffle network  %s / %s  |  segments %d/%d",
			shared.HumanBytes(manager.ShuffleUsed), shared.HumanBytes(manager.ShuffleTotal), manager.ShuffleSegmentsUsed, manager.ShuffleSegmentsTotal), width),
	}
}

func renderTaskManagerGarbageCollectors(manager flink.TaskManager, width int) []string {
	lines := []string{renderDetailSection(fmt.Sprintf("GARBAGE COLLECTORS  %d", len(manager.GarbageCollectors)), width)}
	if len(manager.GarbageCollectors) == 0 {
		return append(lines, shared.Truncate(fmt.Sprintf(" aggregate  %d collections  |  %s  |  per-collector breakdown unavailable",
			manager.GCCount, humanGCDuration(manager.GCTime)), width))
	}
	for _, collector := range manager.GarbageCollectors {
		lines = append(lines, shared.Truncate(fmt.Sprintf(" %-28s  %8d collections  |  %s",
			collector.Name, collector.Count, humanGCDuration(collector.Time)), width))
	}
	return lines
}

func (m Model) renderJobManager(width, height int) string {
	manager := m.infrastructure.JobManager
	title := " JOB MANAGER"
	if m.busy {
		title += "  |  refreshing..."
	}
	status, statusStyle := m.status()
	lines := []string{
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#E2E8F0")).Render(shared.Truncate(title, width)),
		statusStyle.Render(shared.Truncate(status, width)),
		shared.Truncate(fmt.Sprintf(" process  CPU %.1f%%  |  threads %d  |  fd %d/%d", manager.CPUPercent, manager.Threads, manager.OpenDescriptors, manager.MaxDescriptors), width),
		shared.Truncate(fmt.Sprintf(" heap      used %s  |  committed %s  |  max %s", shared.HumanBytes(manager.HeapUsed), shared.HumanBytes(manager.HeapCommitted), shared.HumanBytes(manager.HeapMax)), width),
		shared.Truncate(fmt.Sprintf(" non-heap  used %s  |  committed %s  |  max %s", shared.HumanBytes(manager.NonHeapUsed), shared.HumanBytes(manager.NonHeapCommitted), shared.HumanBytes(manager.NonHeapMax)), width),
		shared.Truncate(fmt.Sprintf(" buffers   direct %d / %s / %s  |  mapped %d / %s / %s  (count / used / capacity)",
			manager.DirectCount, shared.HumanBytes(manager.DirectUsed), shared.HumanBytes(manager.DirectMax), manager.MappedCount, shared.HumanBytes(manager.MappedUsed), shared.HumanBytes(manager.MappedMax)), width),
		shared.Truncate(fmt.Sprintf(" cluster  TaskManagers %d  |  jobs %d  |  slots %d/%d available", manager.TaskManagers, manager.RunningJobs, manager.SlotsAvailable, manager.SlotsTotal), width),
		shared.Truncate(fmt.Sprintf(" runtime  %s  |  %s  |  GC %d / %s", manager.Architecture, manager.JVMVersion, manager.GCCount, humanGCDuration(manager.GCTime)), width),
	}
	configStatus, configStatusStyle := m.jobManagerConfigStatus()
	lines = append(lines, configStatusStyle.Render(shared.Truncate(configStatus, width)))
	if m.jobManagerLogsOpen {
		lines = append(lines, renderJobManagerLogColumns(width))
		start := m.jobManagerLogWindowStart(height)
		end := min(len(manager.Logs), start+jobManagerRowsAvailable(height))
		for index := start; index < end; index++ {
			lines = append(lines, renderJobManagerLogRow(manager.Logs[index], index == m.jobManagerLog.Index(), width))
		}
		if len(manager.Logs) == 0 {
			lines = append(lines, " No JobManager log metadata returned.")
		}
	} else {
		lines = append(lines, renderJobManagerConfigColumns(width))
		rows := m.filteredJobManagerConfiguration()
		start := m.jobManagerConfigWindowStart(height)
		end := min(len(rows), start+jobManagerRowsAvailable(height))
		for index := start; index < end; index++ {
			lines = append(lines, renderInfrastructureConfigRow(rows[index], index == m.jobManagerConfig.Index(), width))
		}
		if len(rows) == 0 {
			lines = append(lines, " No matching JobManager configuration entries.")
		}
	}
	for len(lines) < height-3 {
		lines = append(lines, "")
	}
	separator := lipgloss.NewStyle().Foreground(shared.C("#334155")).Render(strings.Repeat("-", max(0, width)))
	first, second := m.renderSelectedJobManagerDetail(width)
	lines = append(lines, separator, first, second)
	return shared.FitLines(lines, width, height)
}

func renderJobManagerLogColumns(width int) string {
	line := "   JOBMANAGER LOG FILE                                  SIZE       MODIFIED"
	return lipgloss.NewStyle().Bold(true).Foreground(shared.C("#94A3B8")).Background(shared.C("#111827")).Render(
		shared.PadRight(shared.Truncate(line, width), width))
}

func renderJobManagerLogRow(log flink.LogFile, selected bool, width int) string {
	marker := " "
	if selected {
		marker = ">"
	}
	nameWidth := max(18, width-35)
	modified := "-"
	if !log.ModifiedAt.IsZero() {
		modified = log.ModifiedAt.Format("2006-01-02 15:04:05")
	}
	line := fmt.Sprintf(" %s %s  %9s  %s", marker, shared.PadRight(shared.Truncate(log.Name, nameWidth), nameWidth), shared.HumanBytes(log.Size), modified)
	line = shared.PadRight(shared.Truncate(line, width), width)
	if selected {
		return lipgloss.NewStyle().Foreground(shared.C("#FFFFFF")).Background(shared.C("#6D28D9")).Bold(true).Render(line)
	}
	return lipgloss.NewStyle().Foreground(shared.C("#CBD5E1")).Render(line)
}

func renderJobManagerConfigColumns(width int) string {
	line := "   JOBMANAGER CONFIGURATION                         VALUE"
	return lipgloss.NewStyle().Bold(true).Foreground(shared.C("#94A3B8")).Background(shared.C("#111827")).Render(
		shared.PadRight(shared.Truncate(line, width), width))
}

func renderInfrastructureConfigRow(entry flink.ConfigurationEntry, selected bool, width int) string {
	marker := " "
	if selected {
		marker = ">"
	}
	keyWidth := max(20, min(48, width/2))
	value := displayInfrastructureConfigValue(entry)
	line := " " + marker + " " + shared.PadRight(shared.Truncate(entry.Key, keyWidth), keyWidth) + "  " + value
	line = shared.PadRight(shared.Truncate(line, width), width)
	if selected {
		return lipgloss.NewStyle().Foreground(shared.C("#FFFFFF")).Background(shared.C("#6D28D9")).Bold(true).Render(line)
	}
	return lipgloss.NewStyle().Foreground(shared.C("#CBD5E1")).Render(line)
}

func (m Model) renderSelectedJobManagerConfig(width int) (string, string) {
	rows := m.filteredJobManagerConfiguration()
	index := m.jobManagerConfig.Index()
	if len(rows) == 0 || index < 0 || index >= len(rows) {
		return " No JobManager configuration returned.", " p profiler  |  l shows logs  |  tab switches to TaskManagers"
	}
	entry := rows[index]
	value := displayInfrastructureConfigValue(entry)
	return shared.Truncate(" "+entry.Key+"  |  raw "+redactedInfrastructureConfigValue(entry), width),
		shared.Truncate(" interpreted "+value+"  |  configured value; compare runtime facts above", width)
}

func (m Model) filteredJobManagerConfiguration() []flink.ConfigurationEntry {
	terms := strings.Fields(strings.ToLower(m.jobManagerFilter.Value()))
	if len(terms) == 0 {
		return m.infrastructure.JobManager.Configuration
	}
	rows := make([]flink.ConfigurationEntry, 0, len(m.infrastructure.JobManager.Configuration))
	for _, entry := range m.infrastructure.JobManager.Configuration {
		haystack := strings.ToLower(entry.Key + " " + entry.Value + " " + displayInfrastructureConfigValue(entry))
		matches := true
		for _, term := range terms {
			if !strings.Contains(haystack, term) {
				matches = false
				break
			}
		}
		if matches {
			rows = append(rows, entry)
		}
	}
	return rows
}

func (m Model) jobManagerConfigStatus() (string, lipgloss.Style) {
	style := lipgloss.NewStyle().Foreground(shared.C("#94A3B8"))
	if m.jobManagerLogsOpen {
		return " JobManager process logs  |  enter opens selected content", style
	}
	if m.jobManagerFilter.Active() {
		return " /" + m.jobManagerFilter.Value() + "|", style.Foreground(shared.C("#E2E8F0")).Background(shared.C("#111827"))
	}
	if mismatch := m.taskSlotConfigurationMismatch(); mismatch != "" {
		if m.jobManagerFilter.Value() != "" {
			mismatch += "  |  filter /" + m.jobManagerFilter.Value() + "/"
		}
		return " CONFIG / LIVE MISMATCH  " + mismatch, style.Foreground(shared.C("#FBBF24")).Bold(true)
	}
	status := " Configuration reported by /jobmanager/config; live runtime facts above are authoritative"
	if m.jobManagerFilter.Value() != "" {
		status += fmt.Sprintf("  |  filter /%s/  |  %d entries", m.jobManagerFilter.Value(), len(m.filteredJobManagerConfiguration()))
	}
	return status, style
}

func (m Model) taskSlotConfigurationMismatch() string {
	configured := -1
	for _, entry := range m.infrastructure.JobManager.Configuration {
		if entry.Key != "taskmanager.numberOfTaskSlots" {
			continue
		}
		value, err := strconv.Atoi(strings.TrimSpace(entry.Value))
		if err == nil {
			configured = value
		}
		break
	}
	if configured < 0 || len(m.infrastructure.TaskManagers) == 0 {
		return ""
	}
	total := 0
	perManager := make(map[int]struct{})
	for _, manager := range m.infrastructure.TaskManagers {
		total += manager.Slots
		perManager[manager.Slots] = struct{}{}
	}
	if len(perManager) == 1 {
		for live := range perManager {
			if live != configured {
				return fmt.Sprintf("taskmanager.numberOfTaskSlots=%d configured; registered TaskManagers report %d each (%d total)", configured, live, total)
			}
		}
	}
	minimum, maximum := -1, -1
	for live := range perManager {
		if minimum < 0 || live < minimum {
			minimum = live
		}
		if live > maximum {
			maximum = live
		}
	}
	if minimum != configured || maximum != configured {
		return fmt.Sprintf("taskmanager.numberOfTaskSlots=%d configured; registered TaskManagers report %d-%d each (%d total)", configured, minimum, maximum, total)
	}
	return ""
}

func displayInfrastructureConfigValue(entry flink.ConfigurationEntry) string {
	if sensitiveConfigurationKey(entry.Key) {
		return "********  (redacted)"
	}
	raw := strings.TrimSpace(entry.Value)
	lower := strings.ToLower(raw)
	if strings.HasSuffix(lower, "b") {
		bytes, err := strconv.ParseInt(strings.TrimSpace(lower[:len(lower)-1]), 10, 64)
		if err == nil && bytes >= 0 {
			return shared.HumanBytes(bytes) + " (" + raw + ")"
		}
	}
	fields := strings.Fields(lower)
	if len(fields) == 2 && fields[1] == "ms" {
		milliseconds, err := strconv.ParseInt(fields[0], 10, 64)
		if err == nil && milliseconds >= 0 {
			return humanLatency(time.Duration(milliseconds)*time.Millisecond) + " (" + raw + ")"
		}
	}
	return raw
}

func redactedInfrastructureConfigValue(entry flink.ConfigurationEntry) string {
	if sensitiveConfigurationKey(entry.Key) {
		return "******** (redacted)"
	}
	return entry.Value
}

func (m Model) renderSelectedJobManagerDetail(width int) (string, string) {
	if !m.jobManagerLogsOpen {
		return m.renderSelectedJobManagerConfig(width)
	}
	log, ok := m.SelectedJobManagerLog()
	if !ok {
		return " No JobManager log metadata returned.", " p profiler  |  l shows configuration  |  tab switches to TaskManagers"
	}
	modified := "unknown"
	if !log.ModifiedAt.IsZero() {
		modified = log.ModifiedAt.Format(time.RFC3339)
	}
	return shared.Truncate(" "+log.Name, width), shared.Truncate(fmt.Sprintf(" %s  |  modified %s  |  enter opens content  |  p profiler  |  x stdout  |  d thread dump", shared.HumanBytes(log.Size), modified), width)
}

func percentOf(value, maximum int64) float64 {
	if value <= 0 || maximum <= 0 {
		return 0
	}
	return min(100.0, float64(value)/float64(maximum)*100)
}

func humanLatency(value time.Duration) string {
	if value <= 0 {
		return "-"
	}
	if value < time.Second {
		return fmt.Sprintf("%dms", max(int(value.Round(time.Millisecond)/time.Millisecond), 1))
	}
	if value < 10*time.Second {
		return fmt.Sprintf("%.1fs", value.Seconds())
	}
	if value < time.Minute {
		return fmt.Sprintf("%.0fs", value.Seconds())
	}
	return shared.HumanDuration(value)
}

func humanGCDuration(value time.Duration) string {
	if value <= 0 {
		return "0ms"
	}
	return humanLatency(value)
}

func sensitiveConfigurationKey(key string) bool {
	key = strings.ToLower(key)
	for _, fragment := range []string{"password", "secret", "token", "credential", "access.key", "private.key", "sasl.jaas"} {
		if strings.Contains(key, fragment) {
			return true
		}
	}
	return false
}

func (m Model) errorText(err error) string {
	if err == nil {
		return ""
	}
	if m.formatError != nil {
		return m.formatError(err)
	}
	return shared.SanitizeLine(err.Error())
}
