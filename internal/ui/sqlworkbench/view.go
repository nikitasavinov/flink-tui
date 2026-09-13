package sqlworkbench

import (
	"fmt"
	"image/color"
	"strings"

	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

// Render renders the editor and result table into an exact body rectangle.
func (m Model) Render(width, height int) string {
	if width <= 0 || height <= 0 {
		return ""
	}
	editorRows := editorHeight(height)
	editor := m.renderEditor(width, editorRows)
	if editorRows == height {
		return editor
	}
	results := m.renderResults(width, height-editorRows)
	return editor + "\n" + results
}

// Header renders the workbench's three-line application header.
func (m Model) Header(width int) string {
	title := lipgloss.NewStyle().Bold(true).Foreground(shared.C("#A78BFA")).Render(" FLINK TUI ")
	mode := lipgloss.NewStyle().Bold(true).Foreground(shared.C("#34D399")).Render("CONNECTED")
	if m.err != nil && m.session == "" {
		mode = lipgloss.NewStyle().Bold(true).Foreground(shared.C("#FB7185")).Render("DISCONNECTED")
	} else if m.err != nil {
		mode = lipgloss.NewStyle().Bold(true).Foreground(shared.C("#FB7185")).Render("ERROR")
	} else if m.session == "" {
		mode = lipgloss.NewStyle().Bold(true).Foreground(shared.C("#FBBF24")).Render("CONNECTING")
	}
	lineOne := fmt.Sprintf("%s  %s  %s", title, shared.Truncate("SQL Workbench", max(12, width-36)), mode)
	status := "SQL Gateway not configured"
	if m.client != nil {
		status = m.client.Endpoint()
	}
	if m.info.Version != "" {
		status += "  |  Flink " + m.info.Version
	}
	lineTwoStyle := lipgloss.NewStyle().Foreground(shared.C("#94A3B8"))
	if m.err != nil {
		status += "  |  " + m.errorText(m.err) + "  |  ctrl+e details"
		lineTwoStyle = lineTwoStyle.Foreground(shared.C("#FB7185"))
	}
	lineTwo := lineTwoStyle.Render(" " + shared.Truncate(status, width-2))
	operation := fmt.Sprintf(" sql  session %s  |  operation %s  |  %s  |  rows %d",
		shared.ShortID(m.session), shared.ShortID(m.operation), shared.Fallback(m.resultType, "idle"), len(m.rows))
	if m.session == "" {
		operation = " sql  opening a session lazily; i/e/enter enters INSERT, ctrl+enter executes"
	}
	lineThree := lipgloss.NewStyle().Foreground(shared.C("#94A3B8")).Background(shared.C("#0F172A")).Render(
		shared.PadRight(shared.Truncate(operation, width), width),
	)
	return lineOne + "\n" + lineTwo + "\n" + lineThree
}

// Footer returns context-sensitive workbench key hints.
func (m Model) Footer() string {
	if m.editing {
		return "-- INSERT --  type SQL  enter newline  ctrl+enter/F5 run  tab results  esc normal  ctrl+x cancel"
	}
	if m.resultType == "ERROR" && m.operation != "" {
		return "-- NORMAL --  r retry results  ctrl+x cancel  i/e/enter edit  tab focus  q overview  : go"
	}
	if m.focus == FocusEditor {
		return "-- NORMAL --  i/e/enter edit  tab results  ctrl+enter/F5 run  q overview  : go"
	}
	return "-- NORMAL --  up/down rows  i/e/enter edit  ctrl+enter/F5 run  q overview  : go"
}

func editorHeight(height int) int { return min(max(0, height), shared.Clamp(height/3, 6, 10)) }

const resultHeaderRows = 2

func resultRowsAvailable(height int) int { return max(0, height-resultHeaderRows-1) }

func (m Model) renderEditor(width, height int) string {
	focus := "  |  -- NORMAL --"
	if m.editing {
		focus = "  |  -- INSERT --"
	} else if m.focus == FocusResults {
		focus = "  |  RESULTS FOCUSED"
	}
	title := " SQL WORKBENCH" + focus
	lines := []string{
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#A78BFA")).Render(shared.PadRight(shared.Truncate(title, width), width)),
		lipgloss.NewStyle().Foreground(shared.C("#94A3B8")).Render(shared.PadRight(shared.Truncate(m.gatewayStatus(), width), width)),
	}
	editorRows := max(1, height-len(lines))
	textLines, cursorLine, cursorColumn := linesAndCursor(m.text, m.cursor)
	start := shared.Clamp(cursorLine-editorRows/2, 0, max(0, len(textLines)-editorRows))
	end := min(len(textLines), start+editorRows)
	for index := start; index < end; index++ {
		line := textLines[index]
		prefix := fmt.Sprintf(" %3d | ", index+1)
		textWidth := max(0, width-shared.DisplayWidth(prefix))
		if index == cursorLine && m.editing {
			line = renderCursorLine(line, cursorColumn, textWidth)
		} else {
			line = shared.Truncate(line, textWidth)
		}
		style := lipgloss.NewStyle().Foreground(shared.C("#CBD5E1"))
		if index == cursorLine && m.editing {
			style = style.Foreground(shared.C("#FFFFFF")).Background(shared.C("#111827"))
		}
		lines = append(lines, style.Render(shared.PadRight(shared.Truncate(prefix+line, width), width)))
	}
	for len(lines) < height {
		lines = append(lines, shared.PadRight("", width))
	}
	return shared.FitLines(lines, width, height)
}

func renderCursorLine(value string, column, width int) string {
	if width <= 0 {
		return ""
	}
	runes := []rune(value)
	column = shared.Clamp(column, 0, len(runes))
	cursorCell := shared.DisplayWidth(shared.SanitizeLine(string(runes[:column])))
	start := max(0, cursorCell-width+1)
	return ansi.Cut(shared.SanitizeLine(insertCursorGlyph(value, column)), start, start+width)
}

func (m Model) gatewayStatus() string {
	if m.client == nil {
		return " SQL Gateway not configured  |  start it or pass --sql-endpoint"
	}
	status := " " + m.client.Endpoint()
	if m.info.Version != "" {
		status += "  |  Flink SQL Gateway " + m.info.Version
	}
	if m.sessionBusy {
		status += "  |  opening session..."
	} else if m.session != "" {
		status += "  |  session " + shared.ShortID(m.session)
	}
	if m.err != nil {
		status += "  |  " + m.errorText(m.err)
	}
	return status
}

func (m Model) renderResults(width, height int) string {
	status := shared.Fallback(m.resultType, "IDLE")
	title := fmt.Sprintf(" RESULTS  %s  |  %d rows", status, len(m.rows))
	if m.jobID != "" {
		title += "  |  job " + shared.ShortID(m.jobID)
	}
	if m.focus == FocusResults {
		title += "  |  FOCUSED"
	}
	lines := []string{
		lipgloss.NewStyle().Bold(true).Foreground(resultStatusColor(status)).Render(shared.PadRight(shared.Truncate(title, width), width)),
		renderColumns(m.columns, width),
	}
	available := resultRowsAvailable(height)
	start := shared.WindowStart(m.selection.Index(), available, len(m.rows))
	end := min(len(m.rows), start+available)
	for index := start; index < end; index++ {
		lines = append(lines, renderRow(m.rows[index], m.columns, index == m.selection.Index() && m.focus == FocusResults, width))
	}
	if len(m.rows) == 0 {
		message := " i/e/enter enters INSERT; ctrl+enter or F5 executes the editor"
		if m.busy {
			message = " Waiting for SQL Gateway results..."
		} else if m.err != nil {
			message = " " + m.errorText(m.err)
		}
		lines = append(lines, lipgloss.NewStyle().Foreground(shared.C("#94A3B8")).Render(shared.Truncate(message, width)))
	}
	for len(lines) < height-1 {
		lines = append(lines, "")
	}
	detail := " tab switches focus  |  ctrl+enter/F5 run  |  ctrl+x cancel operation"
	selected := m.selection.Index()
	if len(m.rows) > 0 && selected >= 0 && selected < len(m.rows) {
		detail = " " + m.rows[selected].Kind + "  " + strings.Join(m.rows[selected].Fields, "  |  ")
	}
	lines = append(lines, lipgloss.NewStyle().Foreground(shared.C("#94A3B8")).Background(shared.C("#111827")).Render(
		shared.PadRight(shared.Truncate(detail, width), width)))
	return shared.FitLines(lines, width, height)
}

func renderColumns(columns []flink.SQLColumn, width int) string {
	if len(columns) == 0 {
		return lipgloss.NewStyle().Bold(true).Foreground(shared.C("#64748B")).Background(shared.C("#111827")).Render(
			shared.PadRight(shared.Truncate("   KIND  RESULT COLUMNS", width), width))
	}
	columnWidth := max(8, (width-10)/len(columns))
	parts := make([]string, 0, len(columns))
	for _, column := range columns {
		parts = append(parts, shared.PadRight(shared.Truncate(column.Name+" "+column.Type, columnWidth), columnWidth))
	}
	line := "   KIND  " + strings.Join(parts, " ")
	return lipgloss.NewStyle().Bold(true).Foreground(shared.C("#94A3B8")).Background(shared.C("#111827")).Render(
		shared.PadRight(shared.Truncate(line, width), width))
}

func renderRow(row flink.SQLRow, columns []flink.SQLColumn, selected bool, width int) string {
	marker := " "
	if selected {
		marker = ">"
	}
	columnCount := max(1, len(columns))
	columnWidth := max(8, (width-10)/columnCount)
	parts := make([]string, len(row.Fields))
	for index, field := range row.Fields {
		parts[index] = shared.PadRight(shared.Truncate(field, columnWidth), columnWidth)
	}
	kind := rowKind(row.Kind)
	line := fmt.Sprintf(" %s %-5s  %s", marker, kind, strings.Join(parts, " "))
	line = shared.PadRight(shared.Truncate(line, width), width)
	style := lipgloss.NewStyle().Foreground(rowColor(row.Kind))
	if selected {
		style = lipgloss.NewStyle().Foreground(shared.C("#FFFFFF")).Background(shared.C("#6D28D9")).Bold(true)
	}
	return style.Render(line)
}

func rowKind(kind string) string {
	switch kind {
	case "INSERT":
		return "+I"
	case "UPDATE_BEFORE":
		return "-U"
	case "UPDATE_AFTER":
		return "+U"
	case "DELETE":
		return "-D"
	default:
		return shared.Truncate(kind, 5)
	}
}

func rowColor(kind string) color.Color {
	if kind == "DELETE" || kind == "UPDATE_BEFORE" {
		return shared.C("#FB7185")
	}
	if kind == "UPDATE_AFTER" {
		return shared.C("#FBBF24")
	}
	return shared.C("#34D399")
}

func resultStatusColor(status string) color.Color {
	switch status {
	case "EOS":
		return shared.C("#34D399")
	case "ERROR", "CANCELED":
		return shared.C("#FB7185")
	case "PAYLOAD", "RUNNING", "SUBMITTING", "NOT_READY":
		return shared.C("#FBBF24")
	default:
		return shared.C("#94A3B8")
	}
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
