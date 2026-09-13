package jobops

import (
	"fmt"
	"strings"

	"charm.land/lipgloss/v2"

	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

type configurationRow struct {
	section string
	key     string
	value   string
}

type actionRow struct {
	kind        actionKind
	label       string
	description string
	destructive bool
	enabled     bool
}

// Render draws the active operations view.
func (m Model) Render(width, height int) string {
	if m.view == ViewActions {
		return m.renderActions(width, height)
	}
	return m.renderConfiguration(width, height)
}

func (m Model) configurationRows() []configurationRow {
	rows := m.allConfigurationRows()
	query := strings.ToLower(strings.TrimSpace(m.configurationFilter.Value()))
	if query == "" {
		return rows
	}
	filtered := rows[:0]
	for _, row := range rows {
		if strings.Contains(strings.ToLower(row.key), query) || strings.Contains(strings.ToLower(row.value), query) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func (m Model) allConfigurationRows() []configurationRow {
	rows := []configurationRow{
		{section: "execution", key: "restart strategy", value: m.configuration.RestartStrategy},
		{section: "execution", key: "job parallelism", value: fmt.Sprintf("%d", m.configuration.Parallelism)},
		{section: "execution", key: "object reuse mode", value: boolLabel(m.configuration.ObjectReuse)},
		{section: "job", key: "job type", value: m.context.JobType},
		{section: "job", key: "scheduler", value: shared.Fallback(m.context.Scheduler, "-")},
	}
	for _, entry := range m.configuration.User {
		value := entry.Value
		if sensitiveConfigurationKey(entry.Key) {
			value = "********  (redacted)"
		}
		rows = append(rows, configurationRow{section: "user", key: entry.Key, value: value})
	}
	return rows
}

func (m Model) actionRows() []actionRow {
	active := jobStateActive(m.context.JobState)
	streaming := strings.EqualFold(m.context.JobType, "STREAMING")
	return []actionRow{
		{kind: actionConfiguredCheckpoint, label: "Trigger configured checkpoint", description: "Ask Flink to trigger a checkpoint using the job's configured type.", enabled: active},
		{kind: actionFullCheckpoint, label: "Trigger full checkpoint", description: "Ask Flink for a full checkpoint instead of an incremental one.", enabled: active},
		{kind: actionSavepoint, label: "Trigger canonical savepoint", description: "Create a savepoint using the cluster's configured default savepoint directory.", enabled: active},
		{kind: actionStopSavepoint, label: "Stop with savepoint", description: "Gracefully stop the streaming job after a final canonical savepoint.", destructive: true, enabled: active && streaming},
		{kind: actionStopDrain, label: "Stop, drain, and savepoint", description: "Emit MAX_WATERMARK, fire event-time timers, savepoint, then terminate permanently.", destructive: true, enabled: active && streaming},
		{kind: actionCancel, label: "Cancel job immediately", description: "Cancel without a final savepoint. In-flight work can be lost.", destructive: true, enabled: active},
	}
}

func (m Model) renderConfiguration(width, height int) string {
	rows := m.configurationRows()
	allRows := m.allConfigurationRows()
	title := fmt.Sprintf(" JOB CONFIGURATION  %d/%d entries", len(rows), len(allRows))
	if m.configurationBusy {
		title += "  |  refreshing..."
	}
	status := " Effective execution config returned by Flink"
	if !m.configuration.UpdatedAt.IsZero() {
		status += "  |  updated " + m.configuration.UpdatedAt.Format("15:04:05")
	}
	if m.configurationErr != nil {
		status = " Could not load job configuration: " + shared.ErrorText(m.configurationErr)
	}
	if m.configurationFilter.Active() {
		status = " /" + m.configurationFilter.Value() + "|  filter key or value"
	} else if m.configurationFilter.Value() != "" {
		status += "  |  filter /" + m.configurationFilter.Value() + "/"
	}
	statusStyle := lipgloss.NewStyle().Foreground(shared.C("#94A3B8"))
	if m.configurationErr != nil {
		statusStyle = statusStyle.Foreground(shared.C("#FB7185"))
	}
	lines := []string{
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#E2E8F0")).Render(shared.Truncate(title, width)),
		statusStyle.Render(shared.Truncate(status, width)),
		renderConfigurationColumns(width),
	}
	start := m.configurationWindowStart()
	end := min(len(rows), start+m.configurationRowsAvailable())
	for index := start; index < end; index++ {
		lines = append(lines, renderConfigurationRow(rows[index], index == m.configurationRow.Index(), width))
	}
	if len(rows) == 0 && len(allRows) > 0 {
		lines = append(lines, shared.Truncate(" No configuration entries match filter /"+m.configurationFilter.Value()+"/. Press / to replace it or Ctrl+W while editing to clear.", width))
	}
	for len(lines) < height-3 {
		lines = append(lines, "")
	}
	separator := lipgloss.NewStyle().Foreground(shared.C("#334155")).Render(strings.Repeat("-", width))
	first, second := m.renderSelectedConfiguration(width)
	lines = append(lines, separator, first, second)
	return shared.FitLines(lines, width, height)
}

func renderConfigurationColumns(width int) string {
	keyWidth := max(18, min(42, width/2))
	line := "   SCOPE       " + shared.PadRight("KEY", keyWidth) + " VALUE"
	return lipgloss.NewStyle().Bold(true).Foreground(shared.C("#94A3B8")).Background(shared.C("#111827")).Render(
		shared.PadRight(shared.Truncate(line, width), width),
	)
}

func renderConfigurationRow(row configurationRow, selected bool, width int) string {
	marker := " "
	if selected {
		marker = ">"
	}
	keyWidth := max(18, min(42, width/2))
	line := fmt.Sprintf(" %s %-10s  %s %s", marker, shared.Truncate(row.section, 10),
		shared.PadRight(shared.Truncate(row.key, keyWidth), keyWidth), row.value)
	line = shared.PadRight(shared.Truncate(line, width), width)
	if selected {
		return lipgloss.NewStyle().Foreground(shared.C("#FFFFFF")).Background(shared.C("#6D28D9")).Bold(true).Render(line)
	}
	return lipgloss.NewStyle().Foreground(shared.C("#CBD5E1")).Render(line)
}

func (m Model) renderSelectedConfiguration(width int) (string, string) {
	rows := m.configurationRows()
	index := m.configurationRow.Index()
	if len(rows) == 0 || index < 0 || index >= len(rows) {
		return " Select a configuration entry.", ""
	}
	row := rows[index]
	return shared.Truncate(" "+row.section+" / "+row.key, width), shared.Truncate(" "+row.value, width)
}

func (m Model) renderActions(width, height int) string {
	if m.actionConfirm {
		return m.renderActionConfirmation(width, height)
	}
	rows := m.actionRows()
	title := " JOB ACTIONS  guarded Flink REST operations"
	status := " Select an action and press enter. Every external write requires confirmation."
	if m.actionBusy {
		status = " " + m.actionMessage
	} else if m.actionErr != nil {
		status = " Action failed: " + shared.ErrorText(m.actionErr)
	} else if m.actionMessage != "" {
		status = " " + m.actionMessage
	}
	statusStyle := lipgloss.NewStyle().Foreground(shared.C("#94A3B8"))
	if m.actionErr != nil {
		statusStyle = statusStyle.Foreground(shared.C("#FB7185"))
	} else if m.actionStatus == "COMPLETED" {
		statusStyle = statusStyle.Foreground(shared.C("#34D399"))
	}
	lines := []string{
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#E2E8F0")).Render(shared.Truncate(title, width)),
		statusStyle.Render(shared.Truncate(status, width)),
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#94A3B8")).Background(shared.C("#111827")).Render(
			shared.PadRight(shared.Truncate("   ACTION                                   EFFECT", width), width)),
	}
	for index, row := range rows {
		lines = append(lines, renderActionRow(row, index == m.actionSelection.Index(), width))
	}
	for len(lines) < height-5 {
		lines = append(lines, "")
	}
	separator := lipgloss.NewStyle().Foreground(shared.C("#334155")).Render(strings.Repeat("-", width))
	lines = append(lines, separator)
	lines = append(lines, m.renderSelectedAction(width)...)
	return shared.FitLines(lines, width, height)
}

func (m Model) renderActionConfirmation(width, height int) string {
	rows := m.actionRows()
	index := m.actionSelection.Index()
	if index < 0 || index >= len(rows) {
		return shared.FitLines([]string{" ACTION CONFIRMATION", " No action is selected. Press q or esc to return."}, width, height)
	}
	action := rows[index]
	level := "CONFIRM JOB ACTION"
	accent := shared.C("#FBBF24")
	if action.destructive {
		level = "DESTRUCTIVE JOB ACTION"
		accent = shared.C("#FB7185")
	}
	banner := lipgloss.NewStyle().Bold(true).Foreground(shared.C("#FFFFFF")).Background(shared.C("#991B1B"))
	if !action.destructive {
		banner = banner.Background(shared.C("#92400E"))
	}
	label := lipgloss.NewStyle().Bold(true).Foreground(accent)
	lines := []string{
		banner.Render(shared.PadRight(shared.Truncate(" "+level+" ", width), width)),
		"",
		label.Render(shared.Truncate(" ACTION  "+action.label, width)),
		shared.Truncate(" TARGET  job "+m.context.JobID, width),
		shared.Truncate(" STATE   "+m.context.JobState, width),
		"",
		label.Render(shared.Truncate(" EFFECT", width)),
		shared.Truncate(" "+action.description, width),
		"",
		banner.Render(shared.PadRight(shared.Truncate(" INPUT IS LOCKED TO THIS CONFIRMATION ", width), width)),
		"",
		label.Render(shared.Truncate(" [ y ]  EXECUTE THIS ACTION", width)),
		shared.Truncate(" [ n / q / esc ]  CANCEL AND RETURN", width),
		shared.Truncate(" [ ctrl+c ]  QUIT FLINK TUI", width),
	}
	return shared.FitLines(lines, width, height)
}

func renderActionRow(action actionRow, selected bool, width int) string {
	marker := " "
	if selected {
		marker = ">"
	}
	state := "READY"
	if action.destructive {
		state = "DESTRUCTIVE"
	}
	if !action.enabled {
		state = "UNAVAILABLE"
	}
	labelWidth := max(20, min(38, width-20))
	line := fmt.Sprintf(" %s %s  %s", marker, shared.PadRight(shared.Truncate(action.label, labelWidth), labelWidth), state)
	line = shared.PadRight(shared.Truncate(line, width), width)
	style := lipgloss.NewStyle().Foreground(shared.C("#CBD5E1"))
	if action.destructive {
		style = style.Foreground(shared.C("#FB7185"))
	}
	if !action.enabled {
		style = style.Foreground(shared.C("#475569"))
	}
	if selected {
		style = lipgloss.NewStyle().Foreground(shared.C("#FFFFFF")).Background(shared.C("#6D28D9")).Bold(true)
	}
	return style.Render(line)
}

func (m Model) renderSelectedAction(width int) []string {
	rows := m.actionRows()
	index := m.actionSelection.Index()
	if len(rows) == 0 || index < 0 || index >= len(rows) {
		return shared.FixedLineSlice([]string{" Select an action."}, 4)
	}
	action := rows[index]
	lines := []string{
		shared.Truncate(" "+action.description, width),
		shared.Truncate(" job "+m.context.JobID+"  |  state "+m.context.JobState, width),
	}
	if m.actionConfirm {
		warning := "CONFIRM  press y to execute  |  n/esc to abort"
		if action.destructive {
			warning = "DESTRUCTIVE  press y to execute  |  n/esc to abort"
		}
		lines = append(lines, lipgloss.NewStyle().Bold(true).Foreground(shared.C("#FB7185")).Render(shared.Truncate(" "+warning, width)))
	} else if m.actionTriggerID != "" {
		lines = append(lines, shared.Truncate(fmt.Sprintf(" operation %s  |  %s", shared.ShortID(m.actionTriggerID), shared.Fallback(m.actionStatus, "accepted")), width))
	} else {
		lines = append(lines, " enter reviews this action before execution")
	}
	if m.actionLocation != "" {
		lines = append(lines, shared.Truncate(" result "+m.actionLocation, width))
	} else {
		lines = append(lines, "")
	}
	return shared.FixedLineSlice(lines, 4)
}
