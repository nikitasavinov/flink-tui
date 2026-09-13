package checkpoints

import (
	"fmt"
	"image/color"
	"math"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
	shellmodule "github.com/nikitasavinov/flink-tui/internal/ui/shell"
)

const (
	checkpointBodyRowStart = 3
	checkpointDetailRows   = 4
)

type checkpointPage uint8

const (
	checkpointHistory checkpointPage = iota
	checkpointStatistics
)

// Page identifies the history or aggregate-statistics tab.
type Page = checkpointPage

const (
	PageHistory    = checkpointHistory
	PageStatistics = checkpointStatistics
)

func (m *Model) openCheckpoints() {
	if m.snapshot.JobID == "" {
		return
	}
	m.view = ViewHistory
	m.checkpointPage = checkpointHistory
	m.checkpointCursor = 0
	m.checkpointSelectedID = 0
	m.syncCheckpointSelection()
}

func (m *Model) handleCheckpointKey(key string) tea.Cmd {
	if m.checkpointHistoryFilter.Active() {
		action := m.checkpointHistoryFilter.HandleKey(key)
		m.syncCheckpointSelection()
		if action == shellmodule.QueryConfirmed {
			return m.openCheckpointDetail()
		}
		return nil
	}
	switch key {
	case "esc":
		m.pendingIntent = IntentGraph
	case "enter":
		if m.checkpointPage == checkpointHistory {
			return m.openCheckpointDetail()
		}
	case "tab", "v":
		m.checkpointPage = (m.checkpointPage + 1) % 2
	case "up", "k":
		if m.checkpointPage == checkpointHistory {
			m.moveCheckpointSelection(-1)
		}
	case "down", "j":
		if m.checkpointPage == checkpointHistory {
			m.moveCheckpointSelection(1)
		}
	case "pgup":
		m.moveCheckpointSelection(-max(1, m.checkpointRowsAvailable()))
	case "pgdown":
		m.moveCheckpointSelection(max(1, m.checkpointRowsAvailable()))
	case "home":
		m.checkpointCursor = 0
		m.checkpointSelectedID = 0
	case "end":
		if history := m.filteredCheckpointHistory(); len(history) > 0 {
			m.checkpointCursor = len(history) - 1
			m.checkpointSelectedID = history[m.checkpointCursor].ID
		}
	case "/":
		if checkpoint, ok := m.selectedCheckpoint(); ok {
			m.checkpointSelectedID = checkpoint.ID
		}
		m.checkpointHistoryFilter.Open()
	}
	return nil
}

func (m *Model) syncCheckpointSelection() {
	history := m.filteredCheckpointHistory()
	if len(history) == 0 {
		m.checkpointCursor = 0
		m.checkpointSelectedID = 0
		return
	}
	if m.checkpointSelectedID == 0 {
		m.checkpointCursor = 0
		return
	}
	for index, checkpoint := range history {
		if checkpoint.ID == m.checkpointSelectedID {
			m.checkpointCursor = index
			return
		}
	}
	m.checkpointCursor = shared.Clamp(m.checkpointCursor, 0, len(history)-1)
	m.checkpointSelectedID = history[m.checkpointCursor].ID
}

func (m *Model) moveCheckpointSelection(delta int) {
	history := m.filteredCheckpointHistory()
	if len(history) == 0 || delta == 0 {
		return
	}
	m.syncCheckpointSelection()
	m.checkpointCursor = shared.Clamp(m.checkpointCursor+delta, 0, len(history)-1)
	m.checkpointSelectedID = history[m.checkpointCursor].ID
}

func (m Model) checkpointRowsAvailable() int {
	return max(1, m.bodyHeight()-m.checkpointHistoryRowStart()-checkpointDetailRows)
}

func (m Model) checkpointHistoryRowStart() int {
	if agedOutFailedCheckpoints(m.snapshot.Checkpoints) > 0 {
		return checkpointBodyRowStart + 1
	}
	return checkpointBodyRowStart
}

func (m Model) checkpointWindowStart() int {
	available := m.checkpointRowsAvailable()
	return shared.WindowStart(m.checkpointCursor, available, len(m.filteredCheckpointHistory()))
}

func (m *Model) handleCheckpointMouseClick(event tea.Mouse) {
	if event.Button != tea.MouseLeft {
		return
	}
	bodyY := event.Y - headerHeight
	if bodyY == 1 {
		switch {
		case event.X >= 1 && event.X < 10:
			m.checkpointPage = checkpointHistory
		case event.X >= 11 && event.X < 20:
			m.checkpointPage = checkpointStatistics
		}
		return
	}
	if m.checkpointPage != checkpointHistory {
		return
	}
	position := bodyY - m.checkpointHistoryRowStart()
	if position < 0 || position >= m.checkpointRowsAvailable() {
		return
	}
	index := m.checkpointWindowStart() + position
	history := m.filteredCheckpointHistory()
	if index >= 0 && index < len(history) {
		m.checkpointCursor = index
		m.checkpointSelectedID = history[index].ID
	}
}

func (m Model) renderCheckpoints(width, height int) string {
	summary := m.snapshot.Checkpoints
	title := fmt.Sprintf(" CHECKPOINTS  %d total  |  %d completed  |  %d running  |  %d failed",
		summary.Total, summary.Completed, summary.InProgress, summary.Failed)
	if summary.Restored > 0 {
		title += fmt.Sprintf("  |  %d restored", summary.Restored)
	}
	lines := []string{
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#E2E8F0")).Render(shared.Truncate(title, width)),
		m.renderCheckpointTabs(width),
	}
	if m.checkpointPage == checkpointStatistics {
		lines = append(lines, m.renderCheckpointSummaryColumns(width))
		for _, row := range checkpointSummaryRows() {
			lines = append(lines, m.renderCheckpointSummaryRow(row, width))
		}
		for len(lines) < height-checkpointDetailRows {
			lines = append(lines, "")
		}
		separator := lipgloss.NewStyle().Foreground(shared.C("#334155")).Render(strings.Repeat("-", width))
		lines = append(lines, separator)
		lines = append(lines, m.renderCheckpointSummaryDetails(width)...)
		return shared.FitLines(lines, width, height)
	}
	lines = append(lines, m.renderCheckpointColumns(width))
	if agedOut := agedOutFailedCheckpoints(summary); agedOut > 0 {
		message := fmt.Sprintf(" %d failed checkpoint%s aged out of REST history.", agedOut, pluralSuffix(agedOut))
		lines = append(lines, lipgloss.NewStyle().Foreground(shared.C("#FB7185")).Bold(true).Render(shared.Truncate(message, width)))
	}
	history := m.filteredCheckpointHistory()
	if len(history) == 0 && m.checkpointHistoryFilter.Value() == "" {
		lines = append(lines, " No checkpoint history reported for this job.")
	} else if len(history) == 0 {
		lines = append(lines, shared.Truncate(" No retained checkpoints match filter /"+m.checkpointHistoryFilter.Value()+"/. Flink currently exposes "+fmt.Sprintf("%d retained records", len(summary.History))+"; press / to replace the filter.", width))
	}
	start := m.checkpointWindowStart()
	end := min(len(history), start+m.checkpointRowsAvailable())
	for index := start; index < end; index++ {
		lines = append(lines, m.renderCheckpointRow(history[index], index == m.checkpointCursor, width))
	}
	for len(lines) < height-checkpointDetailRows {
		lines = append(lines, "")
	}
	separator := lipgloss.NewStyle().Foreground(shared.C("#334155")).Render(strings.Repeat("-", width))
	lines = append(lines, separator)
	lines = append(lines, m.renderSelectedCheckpoint(width)...)
	return shared.FitLines(lines, width, height)
}

func (m Model) renderCheckpointTabs(width int) string {
	active := lipgloss.NewStyle().Foreground(shared.C("#FFFFFF")).Background(shared.C("#6D28D9")).Bold(true)
	inactive := lipgloss.NewStyle().Foreground(shared.C("#CBD5E1")).Background(shared.C("#1E293B"))
	historyStyle, summaryStyle := inactive, inactive
	if m.checkpointPage == checkpointStatistics {
		summaryStyle = active
	} else {
		historyStyle = active
	}
	line := " " + historyStyle.Render(" History ") + " " + summaryStyle.Render(" Summary ")
	used := 20
	status := fmt.Sprintf("  %d retained / %d total", len(m.snapshot.Checkpoints.History), m.snapshot.Checkpoints.Total)
	if m.checkpointHistoryFilter.Active() {
		status = "  /" + m.checkpointHistoryFilter.Value() + "|  filter retained ID, status, or type  |  pinned"
	} else if m.checkpointHistoryFilter.Value() != "" {
		status += "  |  filter /" + m.checkpointHistoryFilter.Value() + "/  |  pinned"
	}
	latestFailed, hasLatestFailed := latestFailedCheckpoint(m.snapshot.Checkpoints)
	if hasLatestFailed {
		status += fmt.Sprintf("  |  latest failed #%d", latestFailed.ID)
		if latestFailed.Failure != "" {
			status += ": " + latestFailed.Failure
		}
	}
	if m.checkpointPage == checkpointHistory && m.checkpointSelectedID == 0 && len(m.snapshot.Checkpoints.History) > 0 {
		status += "  |  following latest"
	}
	if !m.snapshot.UpdatedAt.IsZero() {
		status += "  |  updated " + m.snapshot.UpdatedAt.Format("15:04:05")
	}
	if m.err != nil {
		status += "  |  refresh failed: " + shared.ErrorText(m.err)
	}
	remaining := max(0, width-used)
	statusStyle := lipgloss.NewStyle().Foreground(shared.C("#94A3B8"))
	if hasLatestFailed {
		statusStyle = statusStyle.Foreground(shared.C("#FB7185")).Bold(true)
	}
	line += statusStyle.Render(shared.PadRight(shared.Truncate(status, remaining), remaining))
	return line
}

type checkpointSummaryRow struct {
	label string
	value func(flink.CheckpointDistribution) float64
}

func checkpointSummaryRows() []checkpointSummaryRow {
	return []checkpointSummaryRow{
		{label: "MIN", value: func(value flink.CheckpointDistribution) float64 { return value.Min }},
		{label: "AVG", value: func(value flink.CheckpointDistribution) float64 { return value.Average }},
		{label: "MAX", value: func(value flink.CheckpointDistribution) float64 { return value.Max }},
		{label: "P50", value: func(value flink.CheckpointDistribution) float64 { return value.P50 }},
		{label: "P90", value: func(value flink.CheckpointDistribution) float64 { return value.P90 }},
		{label: "P95", value: func(value flink.CheckpointDistribution) float64 { return value.P95 }},
		{label: "P99", value: func(value flink.CheckpointDistribution) float64 { return value.P99 }},
		{label: "P99.9", value: func(value flink.CheckpointDistribution) float64 { return value.P999 }},
	}
}

func (m Model) renderCheckpointSummaryColumns(width int) string {
	line := "   STAT       DURATION   CHECKPOINTED         STATE       IN-FLIGHT"
	if width >= 112 {
		line = "   STAT       DURATION   CHECKPOINTED         STATE     ALIGNMENT      PROCESSED      PERSISTED"
	}
	return lipgloss.NewStyle().Bold(true).Foreground(shared.C("#94A3B8")).Background(shared.C("#111827")).Render(
		shared.PadRight(shared.Truncate(line, width), width),
	)
}

func (m Model) renderCheckpointSummaryRow(row checkpointSummaryRow, width int) string {
	statistics := m.snapshot.Checkpoints.Statistics
	duration := checkpointStatisticDuration(row.value(statistics.EndToEndDuration))
	checkpointed := checkpointStatisticBytes(row.value(statistics.CheckpointedSize))
	state := checkpointStatisticBytes(row.value(statistics.StateSize))
	processed := checkpointStatisticBytes(row.value(statistics.ProcessedData))
	persisted := checkpointStatisticBytes(row.value(statistics.PersistedData))
	var line string
	if width >= 112 {
		alignment := checkpointStatisticBytes(row.value(statistics.AlignmentBuffered))
		line = fmt.Sprintf("   %-6s  %12s  %13s  %12s  %12s  %13s  %13s",
			row.label, duration, checkpointed, state, alignment, processed, persisted)
	} else {
		line = fmt.Sprintf("   %-6s  %12s  %13s  %12s  %9s/%-9s",
			row.label, duration, checkpointed, state, processed, persisted)
	}
	foreground := shared.C("#A78BFA")
	switch row.label {
	case "MIN":
		foreground = shared.C("#34D399")
	case "AVG":
		foreground = shared.C("#38BDF8")
	case "MAX":
		foreground = shared.C("#FBBF24")
	case "P99", "P99.9":
		foreground = shared.C("#FB7185")
	}
	return lipgloss.NewStyle().Foreground(foreground).Render(shared.PadRight(shared.Truncate(line, width), width))
}

func (m Model) renderCheckpointSummaryDetails(width int) []string {
	summary := m.snapshot.Checkpoints
	lineOne := fmt.Sprintf(" population  %d completed checkpoints  |  API summary covers the full run", summary.Completed)
	lineTwo := fmt.Sprintf(" history     %d recent records retained by Flink  |  total %d  failed %d  restored %d",
		len(summary.History), summary.Total, summary.Failed, summary.Restored)
	lineThree := " percentiles p50 / p90 / p95 / p99 / p99.9  |  duration, state, checkpointed and in-flight data"
	return []string{shared.Truncate(lineOne, width), shared.Truncate(lineTwo, width), shared.Truncate(lineThree, width)}
}

func checkpointStatisticDuration(milliseconds float64) string {
	if milliseconds <= 0 {
		return "0ms"
	}
	return humanLatency(time.Duration(milliseconds * float64(time.Millisecond)))
}

func checkpointStatisticBytes(value float64) string {
	if value <= 0 {
		return "0B"
	}
	if value < 1024 && math.Abs(value-math.Round(value)) >= 0.05 {
		return fmt.Sprintf("%.1fB", value)
	}
	return shared.HumanBytes(int64(math.Round(value)))
}

func (m Model) renderCheckpointColumns(width int) string {
	var line string
	switch {
	case width >= 110:
		line = "   ID       STATUS       TYPE          TRIGGERED  DURATION       STATE       ACKS       PROCESSED"
	case width >= 78:
		line = "   ID       STATUS       TRIGGERED  DURATION       STATE       ACKS"
	default:
		line = "   ID       STATUS       TIME      DUR       STATE"
	}
	return lipgloss.NewStyle().Bold(true).Foreground(shared.C("#94A3B8")).Background(shared.C("#111827")).Render(
		shared.PadRight(shared.Truncate(line, width), width),
	)
}

func (m Model) renderCheckpointRow(checkpoint flink.Checkpoint, selected bool, width int) string {
	cursor := " "
	if selected {
		cursor = ">"
	}
	prefix := fmt.Sprintf(" %s %7d  %-11s", cursor, checkpoint.ID, shared.Truncate(checkpoint.Status, 11))
	var line string
	switch {
	case width >= 110:
		line = prefix + fmt.Sprintf(" %-13s %s  %10s  %10s  %4d/%-4d  %10s",
			shared.Truncate(checkpointKind(checkpoint), 13),
			shared.Clock(checkpoint.TriggeredAt, "--:--:--"),
			humanLatency(checkpoint.Duration),
			shared.HumanBytes(checkpoint.StateSize),
			checkpoint.AcknowledgedSubtasks,
			checkpoint.Subtasks,
			shared.HumanBytes(checkpoint.ProcessedData),
		)
	case width >= 78:
		line = prefix + fmt.Sprintf(" %s  %10s  %10s  %4d/%-4d",
			shared.Clock(checkpoint.TriggeredAt, "--:--:--"),
			humanLatency(checkpoint.Duration),
			shared.HumanBytes(checkpoint.StateSize),
			checkpoint.AcknowledgedSubtasks,
			checkpoint.Subtasks,
		)
	default:
		line = prefix + fmt.Sprintf(" %s  %8s  %8s",
			shared.Clock(checkpoint.TriggeredAt, "--:--:--"),
			humanLatency(checkpoint.Duration),
			shared.HumanBytes(checkpoint.StateSize),
		)
	}
	line = shared.PadRight(shared.Truncate(line, width), width)
	if selected {
		return lipgloss.NewStyle().Foreground(shared.C("#FFFFFF")).Background(shared.C("#6D28D9")).Bold(true).Render(line)
	}
	return lipgloss.NewStyle().Foreground(checkpointStatusColor(checkpoint.Status)).Render(line)
}

func (m Model) renderSelectedCheckpoint(width int) []string {
	checkpoint, ok := m.selectedCheckpoint()
	if !ok {
		return []string{" Select a checkpoint for details.", "", ""}
	}
	lineOne := fmt.Sprintf(" #%d  %s  %s  |  triggered %s  |  completed %s",
		checkpoint.ID,
		checkpoint.Status,
		checkpointKind(checkpoint),
		formatCheckpointTime(checkpoint.TriggeredAt),
		formatCheckpointTime(checkpoint.CompletedAt),
	)
	lineTwo := fmt.Sprintf(" duration %s  |  state %s  |  checkpointed %s  |  acknowledged %d/%d",
		humanLatency(checkpoint.Duration),
		shared.HumanBytes(checkpoint.StateSize),
		shared.HumanBytes(checkpoint.CheckpointedSize),
		checkpoint.AcknowledgedSubtasks,
		checkpoint.Subtasks,
	)
	lineThree := fmt.Sprintf(" data processed %s  |  persisted %s  |  alignment buffered %s  |  metadata %s",
		shared.HumanBytes(checkpoint.ProcessedData),
		shared.HumanBytes(checkpoint.PersistedData),
		shared.HumanBytes(checkpoint.AlignmentBuffered),
		checkpointRetention(checkpoint),
	)
	if checkpoint.Failure != "" {
		lineThree = " cause  " + checkpoint.Failure
	} else if checkpoint.ExternalPath != "" && checkpoint.ExternalPath != "<checkpoint-not-externally-addressable>" {
		lineThree += "  |  " + checkpoint.ExternalPath
	}
	third := shared.Truncate(lineThree, width)
	if checkpoint.Failure != "" {
		third = lipgloss.NewStyle().Foreground(shared.C("#FB7185")).Bold(true).Render(third)
	}
	return []string{shared.Truncate(lineOne, width), shared.Truncate(lineTwo, width), third}
}

func latestFailedCheckpoint(summary flink.CheckpointSummary) (flink.Checkpoint, bool) {
	if summary.LatestFailed != nil {
		return *summary.LatestFailed, true
	}
	for _, checkpoint := range summary.History {
		if checkpoint.Status == "FAILED" {
			return checkpoint, true
		}
	}
	return flink.Checkpoint{}, false
}

func agedOutFailedCheckpoints(summary flink.CheckpointSummary) int {
	visible := make(map[int64]struct{})
	for _, checkpoint := range summary.History {
		if checkpoint.Status == "FAILED" {
			visible[checkpoint.ID] = struct{}{}
		}
	}
	if summary.LatestFailed != nil {
		visible[summary.LatestFailed.ID] = struct{}{}
	}
	return max(0, summary.Failed-len(visible))
}

func pluralSuffix(count int) string {
	if count == 1 {
		return ""
	}
	return "s"
}

func (m Model) selectedCheckpoint() (flink.Checkpoint, bool) {
	history := m.filteredCheckpointHistory()
	if len(history) == 0 {
		return flink.Checkpoint{}, false
	}
	if m.checkpointSelectedID != 0 {
		for _, checkpoint := range history {
			if checkpoint.ID == m.checkpointSelectedID {
				return checkpoint, true
			}
		}
	}
	index := shared.Clamp(m.checkpointCursor, 0, len(history)-1)
	return history[index], true
}

func (m Model) filteredCheckpointHistory() []flink.Checkpoint {
	history := append([]flink.Checkpoint(nil), m.snapshot.Checkpoints.History...)
	query := strings.ToLower(strings.TrimSpace(m.checkpointHistoryFilter.Value()))
	if query == "" {
		return history
	}
	filtered := history[:0]
	for _, checkpoint := range history {
		values := []string{fmt.Sprintf("%d", checkpoint.ID), checkpoint.Status, checkpointKind(checkpoint)}
		for _, value := range values {
			if strings.Contains(strings.ToLower(value), query) {
				filtered = append(filtered, checkpoint)
				break
			}
		}
	}
	return filtered
}

func checkpointKind(checkpoint flink.Checkpoint) string {
	if checkpoint.Type != "" {
		return checkpoint.Type
	}
	if checkpoint.IsSavepoint {
		return "SAVEPOINT"
	}
	return "CHECKPOINT"
}

func checkpointRetention(checkpoint flink.Checkpoint) string {
	if checkpoint.Discarded {
		return "discarded"
	}
	return "retained"
}

func checkpointStatusColor(status string) color.Color {
	switch status {
	case "COMPLETED":
		return shared.C("#34D399")
	case "IN_PROGRESS":
		return shared.C("#FBBF24")
	case "FAILED":
		return shared.C("#FB7185")
	default:
		return shared.C("#94A3B8")
	}
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
	return shared.ShortDuration(value)
}

func formatCheckpointTime(value time.Time) string {
	if value.IsZero() {
		return "-"
	}
	return value.Format("15:04:05.000")
}
