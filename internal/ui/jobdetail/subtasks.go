package jobdetail

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	jobgraphmodule "github.com/nikitasavinov/flink-tui/internal/ui/jobgraph"
	processmodule "github.com/nikitasavinov/flink-tui/internal/ui/processdiag"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

const (
	subtaskBodyRowStart  = 4
	subtaskDumpCellWidth = 8
	subtaskDumpMinWidth  = 112
)

func (m *Model) openDiagnostics() tea.Cmd {
	if m.selected == "" || m.snapshot.JobID == "" {
		return nil
	}
	m.view = ViewSubtasks
	m.diagnosticsOpen = m.selected
	m.diagnosticsBusy = true
	m.diagnosticsErr = nil
	m.diagnostics = flink.VertexDiagnostics{}
	m.selectedSubtask = -1
	return m.fetchDiagnostics()
}

func (m *Model) handleSubtaskKey(key string) tea.Cmd {
	if m.subtaskFilter.Active() {
		m.subtaskFilter.HandleKey(key)
		m.ensureSubtaskSelection()
		return nil
	}
	if m.subtaskSortOpen {
		m.handleSubtaskSortKey(key)
		return nil
	}
	switch key {
	case "esc", "enter":
		m.pendingIntent = IntentGraph
	case "up", "k":
		m.moveSubtaskSelection(-1)
	case "down", "j":
		m.moveSubtaskSelection(1)
	case "pgup":
		m.moveSubtaskSelection(-max(1, m.subtaskRowsAvailable()))
	case "pgdown":
		m.moveSubtaskSelection(max(1, m.subtaskRowsAvailable()))
	case "s":
		m.subtaskSortOpen = true
	case "v":
		m.subtaskTotals = !m.subtaskTotals
	case "d":
		return m.openSelectedSubtaskThreadDump()
	case "/":
		m.subtaskFilter.Open()
	}
	return nil
}

func (m Model) sortedSubtasks() []flink.Subtask {
	result := slices.Clone(m.diagnostics.Subtasks)
	query := strings.ToLower(strings.TrimSpace(m.subtaskFilter.Value()))
	if query != "" {
		filtered := result[:0]
		for _, subtask := range result {
			if subtaskMatches(subtask, query) {
				filtered = append(filtered, subtask)
			}
		}
		result = filtered
	}
	slices.SortStableFunc(result, func(left, right flink.Subtask) int {
		comparison := 0
		switch m.subtaskSort {
		case sortSubtaskState:
			comparison = strings.Compare(strings.ToLower(left.State), strings.ToLower(right.State))
		case sortSubtaskBackpressure:
			comparison = compareFloat(left.Metrics.BackpressurePercent, right.Metrics.BackpressurePercent)
		case sortSubtaskBusy:
			comparison = compareFloat(left.Metrics.BusyPercent, right.Metrics.BusyPercent)
		case sortSubtaskIdle:
			comparison = compareFloat(left.Metrics.IdlePercent, right.Metrics.IdlePercent)
		case sortSubtaskInput:
			if m.subtaskTotals {
				comparison = compareFloat(left.Metrics.RecordsIn, right.Metrics.RecordsIn)
			} else {
				comparison = compareFloat(left.Metrics.RecordsInPerSecond, right.Metrics.RecordsInPerSecond)
			}
		case sortSubtaskOutput:
			if m.subtaskTotals {
				comparison = compareFloat(left.Metrics.RecordsOut, right.Metrics.RecordsOut)
			} else {
				comparison = compareFloat(left.Metrics.RecordsOutPerSecond, right.Metrics.RecordsOutPerSecond)
			}
		case sortSubtaskBytesInput:
			comparison = compareFloat(left.Metrics.BytesIn, right.Metrics.BytesIn)
		case sortSubtaskBytesOutput:
			comparison = compareFloat(left.Metrics.BytesOut, right.Metrics.BytesOut)
		case sortSubtaskWatermark:
			comparison = compareWatermark(left.Metrics, right.Metrics)
		case sortSubtaskTaskManager:
			comparison = strings.Compare(strings.ToLower(left.Endpoint), strings.ToLower(right.Endpoint))
		default:
			comparison = left.Index - right.Index
		}
		if comparison != 0 {
			if m.subtaskSortDescending {
				return -comparison
			}
			return comparison
		}
		return cmp.Compare(left.Index, right.Index)
	})
	return result
}

func subtaskMatches(subtask flink.Subtask, query string) bool {
	values := []string{
		fmt.Sprintf("%d", subtask.Index), subtask.State, subtask.Endpoint,
		subtask.TaskManagerID, shared.TaskManagerIdentity(subtask.TaskManagerID),
	}
	for _, value := range values {
		if strings.Contains(strings.ToLower(value), query) {
			return true
		}
	}
	return false
}

func (m *Model) ensureSubtaskSelection() {
	rows := m.sortedSubtasks()
	if len(rows) == 0 {
		m.selectedSubtask = -1
		return
	}
	for _, row := range rows {
		if row.Index == m.selectedSubtask {
			return
		}
	}
	m.selectedSubtask = rows[0].Index
}

func (m *Model) moveSubtaskSelection(delta int) {
	rows := m.sortedSubtasks()
	if len(rows) == 0 || delta == 0 {
		return
	}
	position := 0
	for index, row := range rows {
		if row.Index == m.selectedSubtask {
			position = index
			break
		}
	}
	position = shared.Clamp(position+delta, 0, len(rows)-1)
	m.selectedSubtask = rows[position].Index
}

func (m Model) selectedSubtaskPosition(rows []flink.Subtask) int {
	for index, row := range rows {
		if row.Index == m.selectedSubtask {
			return index
		}
	}
	return 0
}

func (m Model) selectedSubtaskRow() (flink.Subtask, bool) {
	for _, subtask := range m.diagnostics.Subtasks {
		if subtask.Index == m.selectedSubtask {
			return subtask, true
		}
	}
	return flink.Subtask{}, false
}

func (m *Model) openSelectedSubtaskThreadDump() tea.Cmd {
	subtask, ok := m.selectedSubtaskRow()
	if !ok {
		m.setNotice("Select a subtask before opening a thread dump.")
		return nil
	}
	taskManagerID := strings.TrimSpace(subtask.TaskManagerID)
	if !assignedTaskManagerID(taskManagerID) {
		m.setNotice(fmt.Sprintf("Subtask #%d has no TaskManager assignment.", subtask.Index))
		return nil
	}

	vertexName := strings.TrimSpace(m.diagnostics.ExecutionName)
	if vertexName == "" {
		vertexName = strings.TrimSpace(m.diagnostics.Name)
	}
	if vertexName == "" {
		vertexName = strings.TrimSpace(m.nodeName(m.diagnosticsOpen))
	}
	focus := processmodule.NewFocus("", fmt.Sprintf("subtask #%d", subtask.Index))
	if vertexName == "" {
		m.setNotice("Vertex name unavailable; showing the full TaskManager thread dump.")
	} else {
		focus.Prefix = fmt.Sprintf("%s (%d/", vertexName, subtask.Index+1)
	}
	return m.openThreadDumpWithFocus(flink.TaskManagerProcess(taskManagerID), focus)
}

func assignedTaskManagerID(taskManagerID string) bool {
	trimmed := strings.TrimSpace(taskManagerID)
	return trimmed != "" && !strings.EqualFold(trimmed, "(unassigned)")
}

func (m Model) subtaskRowsAvailable() int {
	return max(1, m.bodyHeight()-subtaskBodyRowStart-2)
}

func (m Model) subtaskWindowStart(rows []flink.Subtask) int {
	available := m.subtaskRowsAvailable()
	position := m.selectedSubtaskPosition(rows)
	return shared.WindowStart(position, available, len(rows))
}

func (m *Model) handleSubtaskMouseClick(event tea.Mouse) tea.Cmd {
	if event.Button != tea.MouseLeft {
		return nil
	}
	width := max(40, m.contentWidthAt(max(40, m.width)))
	bodyY := event.Y - headerHeight
	if bodyY == subtaskBodyRowStart-1 {
		if column, ok := subtaskSortColumnAt(event.X, width, m.subtaskTotals); ok {
			m.selectSubtaskSort(column)
			m.ensureSubtaskSelection()
		}
		return nil
	}
	position := bodyY - subtaskBodyRowStart
	if position < 0 || position >= m.subtaskRowsAvailable() {
		return nil
	}
	rows := m.sortedSubtasks()
	index := m.subtaskWindowStart(rows) + position
	if index >= 0 && index < len(rows) {
		m.selectedSubtask = rows[index].Index
		if subtaskDumpCellAt(event.X, width) {
			return m.openSelectedSubtaskThreadDump()
		}
	}
	return nil
}

func subtaskDumpCellAt(x, width int) bool {
	return width >= subtaskDumpMinWidth && x >= width-subtaskDumpCellWidth && x < width
}

func (m Model) renderSubtasks(width, height int) string {
	name := m.diagnostics.Name
	if name == "" {
		name = m.nodeName(m.diagnosticsOpen)
	}
	title := fmt.Sprintf(" SUBTASKS  %s  x%d  |  sort %s", name, m.diagnostics.Parallelism, m.subtaskSortLabel())
	if m.diagnosticsBusy {
		title += "  |  refreshing..."
	}
	lines := []string{
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#E2E8F0")).Render(shared.Truncate(title, width)),
		m.renderDiagnosticsStatus(width),
		shared.Truncate(m.renderTrend(m.diagnosticsOpen, width), width),
		m.renderSubtaskColumns(width),
	}
	if m.subtaskSortOpen {
		lines[1] = m.renderSubtaskSortPicker(width)
	}
	rows := m.sortedSubtasks()
	if len(rows) == 0 && !m.diagnosticsBusy && m.diagnosticsErr == nil {
		message := " No subtasks reported for this vertex."
		if m.subtaskFilter.Value() != "" {
			message = " No subtasks match filter /" + m.subtaskFilter.Value() + "/. Press / to replace it or Ctrl+W while editing to clear."
		}
		lines = append(lines, shared.Truncate(message, width))
	}
	start := m.subtaskWindowStart(rows)
	end := min(len(rows), start+m.subtaskRowsAvailable())
	for index := start; index < end; index++ {
		lines = append(lines, m.renderSubtaskRow(rows[index], rows[index].Index == m.selectedSubtask, width))
	}
	for len(lines) < height-2 {
		lines = append(lines, "")
	}
	separator := lipgloss.NewStyle().Foreground(shared.C("#334155")).Render(strings.Repeat("-", width))
	lines = append(lines, separator, shared.Truncate(m.renderSelectedSubtask(rows), width))
	return shared.FitLines(lines, width, height)
}

func (m Model) renderDiagnosticsStatus(width int) string {
	if m.diagnosticsErr != nil {
		return lipgloss.NewStyle().Foreground(shared.C("#FB7185")).Render(
			shared.Truncate(" Detail refresh failed: "+shared.ErrorText(m.diagnosticsErr), width),
		)
	}
	status := fmt.Sprintf(" %d subtasks", len(m.diagnostics.Subtasks))
	if m.subtaskFilter.Active() {
		return lipgloss.NewStyle().Foreground(shared.C("#E2E8F0")).Background(shared.C("#111827")).Render(
			shared.PadRight(shared.Truncate(" /"+m.subtaskFilter.Value()+"|  filter subtasks", width), width),
		)
	}
	if m.subtaskFilter.Value() != "" {
		status += "  |  filter /" + m.subtaskFilter.Value() + "/  |  pinned"
	}
	if !m.diagnostics.UpdatedAt.IsZero() {
		status += "  |  updated " + m.diagnostics.UpdatedAt.Format("15:04:05")
	}
	if m.diagnostics.MetricWarnings > 0 {
		status += fmt.Sprintf("  |  %d metric warning(s)", m.diagnostics.MetricWarnings)
	}
	return lipgloss.NewStyle().Foreground(shared.C("#94A3B8")).Render(shared.Truncate(status, width))
}

func (m Model) renderSubtaskColumns(width int) string {
	var line string
	if m.subtaskTotals {
		switch {
		case width >= subtaskDumpMinWidth:
			line = "   #   STATE          RECORDS IN    RECORDS OUT       BYTES IN      BYTES OUT  TASKMANAGER"
		case width >= 78:
			line = "   #   STATE          RECORDS IN    RECORDS OUT       BYTES IN      BYTES OUT"
		default:
			line = "   #   STATE          REC IN    REC OUT    BYTES IN   BYTES OUT"
		}
		if width >= subtaskDumpMinWidth {
			line = shared.PadRight(shared.Truncate(line, width-subtaskDumpCellWidth), width-subtaskDumpCellWidth) + shared.PadRight(" DUMP", subtaskDumpCellWidth)
		} else {
			line = shared.PadRight(shared.Truncate(line, width), width)
		}
		return lipgloss.NewStyle().Bold(true).Foreground(shared.C("#94A3B8")).Background(shared.C("#111827")).Render(line)
	}
	switch {
	case width >= subtaskDumpMinWidth:
		line = "   #   STATE        BUSY      BP      IDLE       IN/s      OUT/s  WATERMARK  TASKMANAGER"
	case width >= 82:
		line = "   #   STATE        BUSY      BP      IDLE       IN/s      OUT/s  WATERMARK"
	default:
		line = "   #   STATE       BUSY     BP       IN/s     OUT/s"
	}
	if width >= subtaskDumpMinWidth {
		line = shared.PadRight(shared.Truncate(line, width-subtaskDumpCellWidth), width-subtaskDumpCellWidth) + shared.PadRight(" DUMP", subtaskDumpCellWidth)
	} else {
		line = shared.PadRight(shared.Truncate(line, width), width)
	}
	return lipgloss.NewStyle().Bold(true).Foreground(shared.C("#94A3B8")).Background(shared.C("#111827")).Render(line)
}

func (m Model) renderSubtaskRow(subtask flink.Subtask, selected bool, width int) string {
	cursor := " "
	if selected {
		cursor = ">"
	}
	var line string
	level := pressureLevel(subtask.Metrics)
	if m.subtaskTotals {
		base := fmt.Sprintf(" %s %3d  %-11s", cursor, subtask.Index, shared.Truncate(subtask.State, 11))
		if width >= 78 {
			line = base + fmt.Sprintf("  %12s  %13s  %13s  %13s",
				shared.HumanCount(subtask.Metrics.RecordsIn), shared.HumanCount(subtask.Metrics.RecordsOut),
				humanMetricBytes(subtask.Metrics.BytesIn), humanMetricBytes(subtask.Metrics.BytesOut))
			if width >= subtaskDumpMinWidth {
				line += "  " + subtask.Endpoint
			}
		} else {
			line = base + fmt.Sprintf("  %8s  %9s  %10s  %10s",
				shared.HumanCount(subtask.Metrics.RecordsIn), shared.HumanCount(subtask.Metrics.RecordsOut),
				humanMetricBytes(subtask.Metrics.BytesIn), humanMetricBytes(subtask.Metrics.BytesOut))
		}
	} else {
		base := fmt.Sprintf(" %s %3d  %-11s %6.1f%%  %-4s %5.1f%%", cursor, subtask.Index, shared.Truncate(subtask.State, 11), subtask.Metrics.BusyPercent, level, subtask.Metrics.BackpressurePercent)
		switch {
		case width >= subtaskDumpMinWidth:
			line = base + fmt.Sprintf(" %6.1f%%  %8s  %8s  %-9s  %s",
				subtask.Metrics.IdlePercent,
				rate(subtask.Metrics.RecordsInPerSecond),
				rate(subtask.Metrics.RecordsOutPerSecond),
				formatWatermark(subtask.Metrics),
				shared.Truncate(subtask.Endpoint, max(8, width-subtaskDumpCellWidth-91)),
			)
		case width >= 82:
			line = base + fmt.Sprintf(" %6.1f%%  %8s  %8s  %-9s",
				subtask.Metrics.IdlePercent,
				rate(subtask.Metrics.RecordsInPerSecond),
				rate(subtask.Metrics.RecordsOutPerSecond),
				formatWatermark(subtask.Metrics),
			)
		default:
			line = base + fmt.Sprintf("  %7s  %7s", rate(subtask.Metrics.RecordsInPerSecond), rate(subtask.Metrics.RecordsOutPerSecond))
		}
	}
	if width >= subtaskDumpMinWidth {
		cell := " --"
		if assignedTaskManagerID(subtask.TaskManagerID) {
			cell = " [dump]"
		}
		line = shared.PadRight(shared.Truncate(line, width-subtaskDumpCellWidth), width-subtaskDumpCellWidth) + shared.PadRight(cell, subtaskDumpCellWidth)
	} else {
		line = shared.PadRight(shared.Truncate(line, width), width)
	}
	if selected {
		return lipgloss.NewStyle().Foreground(shared.C("#FFFFFF")).Background(shared.C("#6D28D9")).Bold(true).Render(line)
	}
	style := jobgraphmodule.PressureStyle(level)
	return style.Render(line)
}

func (m Model) renderSelectedSubtask(rows []flink.Subtask) string {
	for _, subtask := range rows {
		if subtask.Index != m.selectedSubtask {
			continue
		}
		location := subtask.Endpoint
		if location == "" {
			location = shared.ShortID(subtask.TaskManagerID)
		}
		return fmt.Sprintf(" #%d  total %s in / %s out records  |  %s in / %s out  |  attempt %d  %s  |  runtime %s  watermark %s  |  d thread dump",
			subtask.Index,
			shared.HumanCount(subtask.Metrics.RecordsIn),
			shared.HumanCount(subtask.Metrics.RecordsOut),
			humanMetricBytes(subtask.Metrics.BytesIn),
			humanMetricBytes(subtask.Metrics.BytesOut),
			subtask.Attempt,
			location,
			shared.HumanDuration(subtask.Duration),
			formatWatermark(subtask.Metrics),
		)
	}
	return " Select a subtask for placement and runtime details."
}

func (m Model) subtaskSortLabel() string {
	direction := " ↑"
	if m.subtaskSortDescending {
		direction = " ↓"
	}
	label := "INDEX"
	switch m.subtaskSort {
	case sortSubtaskState:
		label = "STATE"
	case sortSubtaskBackpressure:
		label = "BACKPRESSURE"
	case sortSubtaskBusy:
		label = "BUSY"
	case sortSubtaskIdle:
		label = "IDLE"
	case sortSubtaskInput:
		label = "INPUT"
	case sortSubtaskOutput:
		label = "OUTPUT"
	case sortSubtaskBytesInput:
		label = "BYTES IN"
	case sortSubtaskBytesOutput:
		label = "BYTES OUT"
	case sortSubtaskWatermark:
		label = "WATERMARK"
	case sortSubtaskTaskManager:
		label = "TASKMANAGER"
	}
	return label + direction
}

func compareFloat(left, right float64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func compareWatermark(left, right flink.Metrics) int {
	if left.WatermarkKnown != right.WatermarkKnown {
		if left.WatermarkKnown {
			return 1
		}
		return -1
	}
	if left.LowWatermark < right.LowWatermark {
		return -1
	}
	if left.LowWatermark > right.LowWatermark {
		return 1
	}
	return 0
}

var subtaskSortColumns = []subtaskSort{
	sortSubtaskIndex,
	sortSubtaskState,
	sortSubtaskBusy,
	sortSubtaskBackpressure,
	sortSubtaskIdle,
	sortSubtaskInput,
	sortSubtaskOutput,
	sortSubtaskBytesInput,
	sortSubtaskBytesOutput,
	sortSubtaskWatermark,
	sortSubtaskTaskManager,
}

func (m *Model) handleSubtaskSortKey(key string) {
	switch key {
	case "esc", "enter", "s":
		m.subtaskSortOpen = false
	case "left", "h":
		m.cycleSubtaskSort(-1)
	case "right", "l":
		m.cycleSubtaskSort(1)
	case "a", "up", "k":
		m.subtaskSortDescending = false
	case "d", "down", "j":
		m.subtaskSortDescending = true
	case "#", "0":
		m.selectSubtaskSort(sortSubtaskIndex)
	case "r":
		m.selectSubtaskSort(sortSubtaskState)
	case "b":
		m.selectSubtaskSort(sortSubtaskBusy)
	case "p":
		m.selectSubtaskSort(sortSubtaskBackpressure)
	case "i":
		m.selectSubtaskSort(sortSubtaskInput)
	case "o":
		m.selectSubtaskSort(sortSubtaskOutput)
	case "I":
		m.selectSubtaskSort(sortSubtaskBytesInput)
	case "O":
		m.selectSubtaskSort(sortSubtaskBytesOutput)
	case "w":
		m.selectSubtaskSort(sortSubtaskWatermark)
	case "t":
		m.selectSubtaskSort(sortSubtaskTaskManager)
	}
	m.ensureSubtaskSelection()
}

func (m *Model) cycleSubtaskSort(delta int) {
	position := slices.Index(subtaskSortColumns, m.subtaskSort)
	if position < 0 {
		position = 0
	}
	position = (position + delta + len(subtaskSortColumns)) % len(subtaskSortColumns)
	m.selectSubtaskSort(subtaskSortColumns[position])
}

func (m *Model) selectSubtaskSort(column subtaskSort) {
	if m.subtaskSort == column {
		m.subtaskSortDescending = !m.subtaskSortDescending
		return
	}
	m.subtaskSort = column
	m.subtaskSortDescending = column != sortSubtaskIndex && column != sortSubtaskState && column != sortSubtaskTaskManager
}

func (m Model) renderSubtaskSortPicker(width int) string {
	status := " SORT  # index  r state  b busy  p pressure  i input  o output  I/O bytes  w watermark  t taskmanager  |  a asc  d desc  |  enter apply"
	return lipgloss.NewStyle().Foreground(shared.C("#422006")).Background(shared.C("#FBBF24")).Bold(true).Render(
		shared.PadRight(shared.Truncate(status, width), width),
	)
}

func subtaskSortColumnAt(x, width int, totals bool) (subtaskSort, bool) {
	if x < 0 || x >= width {
		return 0, false
	}
	if x < 7 {
		return sortSubtaskIndex, true
	}
	if totals {
		if width >= 78 {
			switch {
			case x < 22:
				return sortSubtaskState, true
			case x < 36:
				return sortSubtaskInput, true
			case x < 54:
				return sortSubtaskOutput, true
			case x < 68:
				return sortSubtaskBytesInput, true
			case width < 112 || x < 79:
				return sortSubtaskBytesOutput, true
			default:
				return sortSubtaskTaskManager, true
			}
		}
		switch {
		case x < 22:
			return sortSubtaskState, true
		case x < 32:
			return sortSubtaskInput, true
		case x < 43:
			return sortSubtaskOutput, true
		case x < 54:
			return sortSubtaskBytesInput, true
		default:
			return sortSubtaskBytesOutput, true
		}
	}
	if width < 82 {
		switch {
		case x < 19:
			return sortSubtaskState, true
		case x < 28:
			return sortSubtaskBusy, true
		case x < 37:
			return sortSubtaskBackpressure, true
		case x < 46:
			return sortSubtaskInput, true
		default:
			return sortSubtaskOutput, true
		}
	}
	switch {
	case x < 20:
		return sortSubtaskState, true
	case x < 30:
		return sortSubtaskBusy, true
	case x < 38:
		return sortSubtaskBackpressure, true
	case x < 49:
		return sortSubtaskIdle, true
	case x < 59:
		return sortSubtaskInput, true
	case x < 66:
		return sortSubtaskOutput, true
	case width < 112 || x < 77:
		return sortSubtaskWatermark, true
	default:
		return sortSubtaskTaskManager, true
	}
}

func (m Model) renderTrend(vertexID string, width int) string {
	samples := m.nodeHistory(vertexID)
	barWidth := 12
	if width < 90 {
		barWidth = 8
	}
	if width < 64 {
		barWidth = 6
	}
	busy := sparklineFixed(metricValues(samples, func(metrics flink.Metrics) float64 { return metrics.BusyPercent }), barWidth, 100)
	pressure := sparklineFixed(metricValues(samples, func(metrics flink.Metrics) float64 { return metrics.BackpressurePercent }), barWidth, 100)
	if width < 64 {
		return fmt.Sprintf(" trend  busy %s  bp %s  60s", busy, pressure)
	}
	input := sparklineDynamic(metricValues(samples, func(metrics flink.Metrics) float64 { return metrics.RecordsInPerSecond }), barWidth)
	return fmt.Sprintf(" trend  busy %s  bp %s  in %s  60s", busy, pressure, input)
}

func formatWatermark(metrics flink.Metrics) string {
	if !metrics.WatermarkKnown {
		return "-"
	}
	return time.UnixMilli(metrics.LowWatermark).Format("15:04:05")
}
