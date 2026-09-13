package exceptions

import (
	"fmt"
	"slices"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	processmodule "github.com/nikitasavinov/flink-tui/internal/ui/processdiag"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
	shellmodule "github.com/nikitasavinov/flink-tui/internal/ui/shell"
)

const exceptionBodyRowStart = 3

type exceptionSort uint8

const (
	exceptionSortRecent exceptionSort = iota
	exceptionSortTask
	exceptionSortType
	exceptionSortRecurrence
)

func (m *Model) handleExceptionKey(key string) tea.Cmd {
	if m.exceptionFilter.Active() {
		return m.handleExceptionSearchKey(key)
	}
	switch key {
	case "esc":
		m.pendingIntent = IntentGraph
	case "g", "enter":
		m.openSelectedExceptionGraph()
	case "up", "k":
		m.moveExceptionSelection(-1)
	case "down", "j":
		m.moveExceptionSelection(1)
	case "pgup":
		m.moveExceptionSelection(-max(1, m.exceptionRowsAvailable()))
	case "pgdown":
		m.moveExceptionSelection(max(1, m.exceptionRowsAvailable()))
	case "ctrl+u", "left", "h":
		m.exceptionDetailOffset = max(0, m.exceptionDetailOffset-5)
	case "ctrl+d", "right", "l":
		// Plain l opens logs; right and ctrl+d retain trace scrolling.
		if key == "l" {
			return m.openSelectedExceptionLogs()
		}
		m.exceptionDetailOffset += 5
	case "home":
		m.exceptionCursor = 0
		m.exceptionVariantCursor = 0
		m.exceptionDetailOffset = 0
	case "end":
		m.exceptionCursor = max(0, len(m.filteredExceptionIncidents())-1)
		m.exceptionVariantCursor = 0
		m.exceptionDetailOffset = 0
	case "[":
		m.moveExceptionVariant(-1)
	case "]":
		m.moveExceptionVariant(1)
	case "s":
		m.exceptionSort = (m.exceptionSort + 1) % 4
		m.exceptionCursor = 0
		m.exceptionVariantCursor = 0
		m.exceptionDetailOffset = 0
	case "/":
		m.exceptionFilter.Open()
	case "L", "+":
		return m.loadMoreExceptions()
	case "T":
		return m.openSelectedExceptionTaskManager()
	case "d":
		return m.openSelectedExceptionThreadDump()
	}
	return nil
}

func (m *Model) handleExceptionSearchKey(key string) tea.Cmd {
	if m.exceptionFilter.HandleKey(key) == shellmodule.QueryConfirmed {
		m.openSelectedExceptionGraph()
	}
	m.exceptionCursor = 0
	m.exceptionVariantCursor = 0
	m.exceptionDetailOffset = 0
	return nil
}

func (m *Model) loadMoreExceptions() tea.Cmd {
	if m.exceptionLoadingMore {
		return nil
	}
	if !m.snapshot.Exceptions.Truncated {
		m.setNotice("All exception incidents retained by Flink are already loaded.")
		return nil
	}
	next := min(1000, max(flink.DefaultExceptionLimit, m.exceptionLimit)+flink.DefaultExceptionLimit)
	if next == m.exceptionLimit {
		m.setNotice("Exception history limit reached 1000 incidents.")
		return nil
	}
	m.exceptionLimit = next
	m.exceptionLoadingMore = true
	return m.fetchSnapshot()
}

func (m Model) filteredExceptionIncidents() []flink.JobException {
	entries := slices.Clone(m.snapshot.Exceptions.Entries)
	query := strings.ToLower(strings.TrimSpace(m.exceptionFilter.Value()))
	if query != "" {
		filtered := entries[:0]
		for _, entry := range entries {
			if exceptionIncidentMatches(entry, query) {
				filtered = append(filtered, entry)
			}
		}
		entries = filtered
	}
	recurrences := m.exceptionRecurrences()
	slices.SortStableFunc(entries, func(left, right flink.JobException) int {
		switch m.exceptionSort {
		case exceptionSortTask:
			if comparison := strings.Compare(strings.ToLower(exceptionTaskDisplay(left.TaskName)), strings.ToLower(exceptionTaskDisplay(right.TaskName))); comparison != 0 {
				return comparison
			}
		case exceptionSortType:
			if comparison := strings.Compare(strings.ToLower(shortExceptionName(left.Name)), strings.ToLower(shortExceptionName(right.Name))); comparison != 0 {
				return comparison
			}
		case exceptionSortRecurrence:
			leftCount := recurrences[exceptionFingerprint(left)]
			rightCount := recurrences[exceptionFingerprint(right)]
			if leftCount != rightCount {
				return rightCount - leftCount
			}
		}
		return right.At.Compare(left.At)
	})
	return entries
}

func exceptionIncidentMatches(root flink.JobException, query string) bool {
	for _, exception := range exceptionVariants(root) {
		values := []string{
			exception.Name,
			exception.Stacktrace,
			exception.TaskName,
			exception.Endpoint,
			exception.TaskManagerID,
		}
		for key, value := range exception.FailureLabels {
			values = append(values, key, value, key+"="+value)
		}
		for _, value := range values {
			if strings.Contains(strings.ToLower(value), query) {
				return true
			}
		}
	}
	return false
}

func exceptionVariants(root flink.JobException) []flink.JobException {
	result := make([]flink.JobException, 0, 1+len(root.ConcurrentExceptions))
	result = append(result, root)
	result = append(result, root.ConcurrentExceptions...)
	return result
}

func (m Model) selectedExceptionIncident() (flink.JobException, bool) {
	entries := m.filteredExceptionIncidents()
	if m.exceptionCursor < 0 || m.exceptionCursor >= len(entries) {
		return flink.JobException{}, false
	}
	return entries[m.exceptionCursor], true
}

func (m Model) selectedException() (flink.JobException, bool) {
	incident, ok := m.selectedExceptionIncident()
	if !ok {
		return flink.JobException{}, false
	}
	variants := exceptionVariants(incident)
	index := shared.Clamp(m.exceptionVariantCursor, 0, len(variants)-1)
	return variants[index], true
}

func (m *Model) syncExceptionSelection() {
	entries := m.filteredExceptionIncidents()
	if len(entries) == 0 {
		m.exceptionCursor = 0
		m.exceptionVariantCursor = 0
		m.exceptionDetailOffset = 0
		return
	}
	m.exceptionCursor = shared.Clamp(m.exceptionCursor, 0, len(entries)-1)
	variants := exceptionVariants(entries[m.exceptionCursor])
	m.exceptionVariantCursor = shared.Clamp(m.exceptionVariantCursor, 0, len(variants)-1)
}

func (m *Model) moveExceptionSelection(delta int) {
	entries := m.filteredExceptionIncidents()
	if len(entries) == 0 || delta == 0 {
		return
	}
	m.exceptionCursor = shared.Clamp(m.exceptionCursor+delta, 0, len(entries)-1)
	m.exceptionVariantCursor = 0
	m.exceptionDetailOffset = 0
}

func (m *Model) moveExceptionVariant(delta int) {
	incident, ok := m.selectedExceptionIncident()
	if !ok || delta == 0 {
		return
	}
	variants := exceptionVariants(incident)
	m.exceptionVariantCursor = (m.exceptionVariantCursor + delta + len(variants)) % len(variants)
	m.exceptionDetailOffset = 0
}

func (m Model) exceptionRowsAvailable() int {
	return max(2, min(7, (m.bodyHeight()-6)/2))
}

func (m Model) exceptionWindowStart() int {
	available := m.exceptionRowsAvailable()
	return shared.WindowStart(m.exceptionCursor, available, len(m.filteredExceptionIncidents()))
}

func (m *Model) handleExceptionMouseClick(event tea.Mouse) tea.Cmd {
	if event.Button != tea.MouseLeft {
		return nil
	}
	bodyY := event.Y - headerHeight
	position := bodyY - exceptionBodyRowStart
	if position >= 0 && position < m.exceptionRowsAvailable() {
		index := m.exceptionWindowStart() + position
		if index >= 0 && index < len(m.filteredExceptionIncidents()) {
			m.exceptionCursor = index
			m.exceptionVariantCursor = 0
			m.exceptionDetailOffset = 0
		}
		return nil
	}
	actionRow := exceptionBodyRowStart + m.exceptionRowsAvailable() + 5
	if bodyY != actionRow {
		return nil
	}
	switch exceptionActionAt(event.X) {
	case "graph":
		m.openSelectedExceptionGraph()
	case "task-manager":
		return m.openSelectedExceptionTaskManager()
	case "thread-dump":
		return m.openSelectedExceptionThreadDump()
	case "logs":
		return m.openSelectedExceptionLogs()
	}
	return nil
}

func exceptionActionLine() string {
	return " [enter vertex]  [T task manager]  [d thread dump]  [l current log]"
}

func exceptionActionAt(x int) string {
	line := exceptionActionLine()
	for _, action := range []struct {
		text string
		name string
	}{
		{text: "[enter vertex]", name: "graph"},
		{text: "[T task manager]", name: "task-manager"},
		{text: "[d thread dump]", name: "thread-dump"},
		{text: "[l current log]", name: "logs"},
	} {
		start := strings.Index(line, action.text)
		if x >= start && x < start+len(action.text) {
			return action.name
		}
	}
	return ""
}

func (m Model) renderExceptions(width, height int) string {
	summary := m.snapshot.Exceptions
	entries := m.filteredExceptionIncidents()
	title := fmt.Sprintf(" EXCEPTION INCIDENTS  %d/%d visible  |  %d failures  |  sort %s",
		len(entries), len(summary.Entries), totalExceptionFailures(summary.Entries), m.exceptionSortLabel())
	if m.exceptionLoadingMore {
		title += "  |  loading more..."
	}
	status := fmt.Sprintf(" %d loaded  |  server retention: web.exception-history-size", len(summary.Entries))
	if m.exceptionFilter.Active() {
		status = " /" + m.exceptionFilter.Value() + "|"
	} else {
		if m.exceptionFilter.Value() != "" {
			status += "  |  filter /" + m.exceptionFilter.Value() + "/"
		}
		if summary.Truncated {
			status += "  |  more available: L"
		} else {
			status += "  |  all retained incidents loaded"
		}
	}
	lines := []string{
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#E2E8F0")).Render(shared.Truncate(title, width)),
		lipgloss.NewStyle().Foreground(shared.C("#94A3B8")).Render(shared.Truncate(status, width)),
		m.renderExceptionColumns(width),
	}
	start := m.exceptionWindowStart()
	end := min(len(entries), start+m.exceptionRowsAvailable())
	for index := start; index < end; index++ {
		lines = append(lines, m.renderExceptionRow(entries[index], index == m.exceptionCursor, width))
	}
	if len(entries) == 0 {
		message := " No exceptions reported for this job."
		if len(summary.Entries) > 0 {
			message = " No incidents match the current filter."
		}
		lines = append(lines, lipgloss.NewStyle().Foreground(shared.C("#34D399")).Render(message))
	}
	for len(lines) < exceptionBodyRowStart+m.exceptionRowsAvailable() {
		lines = append(lines, "")
	}
	separator := lipgloss.NewStyle().Foreground(shared.C("#334155")).Render(strings.Repeat("-", width))
	lines = append(lines, separator)
	detailHeight := max(1, height-len(lines))
	lines = append(lines, m.renderExceptionDetail(width, detailHeight)...)
	return shared.FitLines(lines, width, height)
}

func (m Model) renderExceptionColumns(width int) string {
	line := "   WHEN                 EXCEPTION"
	if width >= 120 {
		taskWidth := max(12, width-83)
		line = fmt.Sprintf("   %-19s  %-26s  %-*s  FAIL  REPEAT", "WHEN", "TYPE", taskWidth, "TASK")
	} else if width >= 90 {
		line = "   WHEN                 TYPE                        TASK"
	}
	return lipgloss.NewStyle().Bold(true).Foreground(shared.C("#94A3B8")).Background(shared.C("#111827")).Render(
		shared.PadRight(shared.Truncate(line, width), width),
	)
}

func (m Model) renderExceptionRow(exception flink.JobException, selected bool, width int) string {
	marker := " "
	if selected {
		marker = ">"
	}
	typeName := shortExceptionName(exception.Name)
	var line string
	switch {
	case width >= 120:
		taskWidth := max(12, width-83)
		line = fmt.Sprintf(" %s %-19s  %-26s  %-*s  %4d  %6s",
			marker, shared.Timestamp(exception.At), shared.Truncate(typeName, 26), taskWidth, shared.Truncate(exceptionTaskDisplay(exception.TaskName), taskWidth),
			1+len(exception.ConcurrentExceptions), fmt.Sprintf("x%d", m.exceptionRecurrenceCount(exception)))
	case width >= 90:
		taskWidth := max(10, width-55)
		line = fmt.Sprintf(" %s %-19s  %-26s  %s", marker, shared.Timestamp(exception.At), shared.Truncate(typeName, 26), shared.Truncate(exceptionTaskDisplay(exception.TaskName), taskWidth))
	default:
		line = fmt.Sprintf(" %s %-19s  %s", marker, shared.Timestamp(exception.At), typeName)
	}
	line = shared.PadRight(shared.Truncate(line, width), width)
	if selected {
		return lipgloss.NewStyle().Foreground(shared.C("#FFFFFF")).Background(shared.C("#6D28D9")).Bold(true).Render(line)
	}
	return lipgloss.NewStyle().Foreground(shared.C("#FB7185")).Render(line)
}

func (m Model) renderExceptionDetail(width, height int) []string {
	incident, incidentOK := m.selectedExceptionIncident()
	exception, exceptionOK := m.selectedException()
	if !incidentOK || !exceptionOK {
		return shared.FixedLineSlice([]string{" No incident selected."}, height)
	}
	variants := exceptionVariants(incident)
	taskName := exceptionTaskDisplay(exception.TaskName)
	location := "(unassigned)"
	if exception.TaskManagerID != "" || exception.Endpoint != "" {
		location = shared.Fallback(exception.TaskManagerID, "unknown TaskManager")
		if exception.Endpoint != "" {
			location += " @ " + exception.Endpoint
		}
	}
	context := fmt.Sprintf(" incident %s  |  failure %d/%d  |  recurrence x%d",
		shared.Timestamp(incident.At), m.exceptionVariantCursor+1, len(variants), m.exceptionRecurrenceCount(incident))
	labels := exceptionLabels(exception.FailureLabels)
	if labels == "" {
		labels = "none"
	}
	correlation := m.nearestCheckpointContext(exception.At)
	if correlation == "" {
		correlation = "no nearby checkpoint"
	}
	lines := []string{
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#FB7185")).Render(shared.Truncate(" "+exception.Name, width)),
		lipgloss.NewStyle().Foreground(shared.C("#CBD5E1")).Render(shared.Truncate(" task "+taskName+"  |  "+context, width)),
		lipgloss.NewStyle().Foreground(shared.C("#94A3B8")).Render(shared.Truncate(" location "+location, width)),
		lipgloss.NewStyle().Foreground(shared.C("#94A3B8")).Render(shared.Truncate(" labels "+labels+"  |  "+correlation+"  |  [/] concurrent failures", width)),
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#A78BFA")).Render(shared.Truncate(exceptionActionLine(), width)),
	}
	trace := shared.WrapTrace(exception.Stacktrace, width-2)
	traceHeight := max(0, height-len(lines)-1)
	m.exceptionDetailOffset = shared.Clamp(m.exceptionDetailOffset, 0, max(0, len(trace)-traceHeight))
	lines = append(lines, lipgloss.NewStyle().Bold(true).Foreground(shared.C("#64748B")).Render(
		shared.Truncate(fmt.Sprintf(" STACK TRACE  lines %d-%d/%d", m.exceptionDetailOffset+1, min(len(trace), m.exceptionDetailOffset+traceHeight), len(trace)), width)))
	end := min(len(trace), m.exceptionDetailOffset+traceHeight)
	for _, line := range trace[m.exceptionDetailOffset:end] {
		lines = append(lines, lipgloss.NewStyle().Foreground(shared.C("#94A3B8")).Render(" "+shared.Truncate(line, width-1)))
	}
	if len(trace) == 0 {
		lines = append(lines, " No stack trace was returned by Flink.")
	}
	return shared.FixedLineSlice(lines, height)
}

func (m Model) nearestCheckpointContext(at time.Time) string {
	if at.IsZero() {
		return ""
	}
	var nearest flink.Checkpoint
	var nearestAt time.Time
	var nearestDistance time.Duration
	for _, checkpoint := range m.snapshot.Checkpoints.History {
		checkpointAt := checkpoint.TriggeredAt
		if checkpointAt.IsZero() {
			checkpointAt = checkpoint.CompletedAt
		}
		if checkpointAt.IsZero() {
			continue
		}
		distance := at.Sub(checkpointAt)
		if distance < 0 {
			distance = -distance
		}
		if nearestAt.IsZero() || distance < nearestDistance {
			nearest = checkpoint
			nearestAt = checkpointAt
			nearestDistance = distance
		}
	}
	if nearestAt.IsZero() || nearestDistance > 30*time.Minute {
		return ""
	}
	direction := "before"
	if nearestAt.After(at) {
		direction = "after"
	}
	return fmt.Sprintf("checkpoint #%d %s %s %s", nearest.ID, strings.ToLower(nearest.Status), humanDuration(nearestDistance), direction)
}

func (m Model) exceptionRecurrences() map[string]int {
	result := make(map[string]int)
	for _, exception := range m.snapshot.Exceptions.Entries {
		result[exceptionFingerprint(exception)]++
	}
	return result
}

func (m Model) exceptionRecurrenceCount(exception flink.JobException) int {
	return max(1, m.exceptionRecurrences()[exceptionFingerprint(exception)])
}

func exceptionFingerprint(exception flink.JobException) string {
	return strings.ToLower(shortExceptionName(exception.Name) + "|" + normalizedExceptionOperator(exception.TaskName))
}

func shortExceptionName(value string) string {
	value = strings.TrimSpace(value)
	if index := strings.LastIndex(value, "."); index >= 0 && index+1 < len(value) {
		return value[index+1:]
	}
	return shared.Fallback(value, "UnknownException")
}

func exceptionTaskDisplay(taskName string) string {
	taskName = strings.TrimSpace(taskName)
	if taskName == "" {
		return "(global failure)"
	}
	return taskName
}

func normalizedExceptionOperator(taskName string) string {
	value := strings.TrimSpace(taskName)
	if index := strings.LastIndex(value, " - execution #"); index >= 0 {
		value = value[:index]
	}
	if open := strings.LastIndex(value, " ("); open >= 0 && strings.HasSuffix(value, ")") {
		scope := value[open+2 : len(value)-1]
		if strings.Contains(scope, "/") {
			value = value[:open]
		}
	}
	value = strings.TrimPrefix(value, "Source: ")
	value = strings.TrimSuffix(value, ": Writer")
	return strings.ToLower(strings.Join(strings.Fields(value), " "))
}

func exceptionThreadPrefix(taskName string) string {
	value := strings.TrimSpace(taskName)
	if index := strings.LastIndex(value, " - execution #"); index >= 0 {
		value = value[:index]
	}
	if slash := strings.LastIndex(value, "/"); slash >= 0 && strings.HasSuffix(value, ")") {
		return value[:slash+1]
	}
	return value
}

func exceptionLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+labels[key])
	}
	return strings.Join(parts, ", ")
}

func totalExceptionFailures(entries []flink.JobException) int {
	total := 0
	for _, entry := range entries {
		total += 1 + len(entry.ConcurrentExceptions)
	}
	return total
}

func (m Model) exceptionSortLabel() string {
	switch m.exceptionSort {
	case exceptionSortTask:
		return "task"
	case exceptionSortType:
		return "type"
	case exceptionSortRecurrence:
		return "recurrence"
	default:
		return "recent"
	}
}

func (m *Model) openSelectedExceptionGraph() {
	exception, ok := m.selectedException()
	if !ok {
		m.setNotice("Select an exception incident first.")
		return
	}
	target := normalizedExceptionOperator(exception.TaskName)
	if target == "" {
		m.setNotice("This is a global failure with no task to show on the graph.")
		return
	}
	bestID := ""
	bestLength := -1
	for _, node := range m.snapshot.Nodes {
		candidate := normalizedExceptionOperator(node.Name)
		if candidate == "" {
			continue
		}
		if target == candidate || strings.HasPrefix(target, candidate+" ") || strings.HasPrefix(candidate, target+" ") {
			if len(candidate) > bestLength {
				bestID = node.ID
				bestLength = len(candidate)
			}
		}
	}
	if bestID == "" {
		m.setNotice("Could not map exception task to a current graph vertex.")
		return
	}
	m.selected = bestID
	m.pendingIntent = IntentGraph
}

func (m *Model) openSelectedExceptionTaskManager() tea.Cmd {
	exception, ok := m.selectedException()
	if !ok {
		m.setNotice("Select an exception incident first.")
		return nil
	}
	if strings.TrimSpace(exception.TaskManagerID) == "" {
		m.setNotice("This failure is not assigned to a TaskManager.")
		return nil
	}
	return m.openTaskManagerByID(exception.TaskManagerID)
}

func (m *Model) openSelectedExceptionThreadDump() tea.Cmd {
	exception, ok := m.selectedException()
	if !ok {
		m.setNotice("Select an exception incident first.")
		return nil
	}
	if strings.TrimSpace(exception.TaskManagerID) == "" {
		m.setNotice("This failure is not assigned to a TaskManager.")
		return nil
	}
	focus := processmodule.NewFocus(
		exceptionThreadPrefix(exception.TaskName),
		"exception task "+exceptionTaskDisplay(exception.TaskName),
	)
	return m.openThreadDumpWithFocus(flink.TaskManagerProcess(exception.TaskManagerID), focus)
}

func (m *Model) openSelectedExceptionLogs() tea.Cmd {
	exception, ok := m.selectedException()
	if !ok {
		m.setNotice("Select an exception incident first.")
		return nil
	}
	if strings.TrimSpace(exception.TaskManagerID) == "" {
		m.setNotice("This failure is not assigned to a TaskManager.")
		return nil
	}
	return m.openProcessLogs(flink.TaskManagerProcess(exception.TaskManagerID))
}
