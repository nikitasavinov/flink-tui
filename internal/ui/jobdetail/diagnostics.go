package jobdetail

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	jobgraphmodule "github.com/nikitasavinov/flink-tui/internal/ui/jobgraph"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
	shellmodule "github.com/nikitasavinov/flink-tui/internal/ui/shell"
)

const diagnosticsBodyRowStart = 3

type diagnosticPage uint8

const (
	diagnosticBackpressure diagnosticPage = iota
	diagnosticSkew
	diagnosticMetrics
)

func (m *Model) openDiagnosticOverview(page diagnosticPage) {
	if m.snapshot.JobID == "" {
		return
	}
	m.view = ViewDiagnostics
	m.diagnosticPage = page
	m.ensureNodeSelection()
}

func (m *Model) handleDiagnosticOverviewKey(key string) tea.Cmd {
	if m.diagnosticFilter.Active() {
		action := m.diagnosticFilter.HandleKey(key)
		m.ensureNodeSelection()
		if action == shellmodule.QueryConfirmed {
			return m.openDiagnostics()
		}
		return nil
	}
	switch key {
	case "esc", "g":
		m.pendingIntent = IntentGraph
	case "up", "k":
		m.moveDiagnosticSelection(-1)
	case "down", "j":
		m.moveDiagnosticSelection(1)
	case "pgup":
		m.moveDiagnosticSelection(-max(1, m.diagnosticRowsAvailable()))
	case "pgdown":
		m.moveDiagnosticSelection(max(1, m.diagnosticRowsAvailable()))
	case "[":
		m.diagnosticPage = (m.diagnosticPage + 2) % 3
	case "]":
		m.diagnosticPage = (m.diagnosticPage + 1) % 3
	case "b":
		m.diagnosticPage = diagnosticBackpressure
	case "d":
		m.diagnosticPage = diagnosticSkew
	case "v":
		m.diagnosticPage = diagnosticMetrics
	case "enter":
		return m.openDiagnostics()
	case "/":
		m.diagnosticFilter.Open()
	}
	return nil
}

func (m *Model) ensureNodeSelection() {
	order := m.diagnosticOrder()
	if len(order) == 0 {
		m.selected = ""
		return
	}
	for _, id := range order {
		if id == m.selected {
			return
		}
	}
	m.selected = order[0]
}

func (m *Model) moveDiagnosticSelection(delta int) {
	order := m.diagnosticOrder()
	if len(order) == 0 || delta == 0 {
		return
	}
	m.ensureNodeSelection()
	position := 0
	for index, id := range order {
		if id == m.selected {
			position = index
			break
		}
	}
	position = shared.Clamp(position+delta, 0, len(order)-1)
	m.selected = order[position]
}

func (m Model) diagnosticRowsAvailable() int {
	return max(1, m.bodyHeight()-diagnosticsBodyRowStart-3)
}

func (m Model) diagnosticWindowStart() int {
	order := m.diagnosticOrder()
	available := m.diagnosticRowsAvailable()
	position := 0
	for index, id := range order {
		if id == m.selected {
			position = index
			break
		}
	}
	return shared.WindowStart(position, available, len(order))
}

func (m *Model) handleDiagnosticOverviewMouseClick(event tea.Mouse) {
	if event.Button != tea.MouseLeft {
		return
	}
	position := event.Y - headerHeight - diagnosticsBodyRowStart
	if position < 0 || position >= m.diagnosticRowsAvailable() {
		return
	}
	index := m.diagnosticWindowStart() + position
	order := m.diagnosticOrder()
	if index >= 0 && index < len(order) {
		m.selected = order[index]
	}
}

func (m Model) renderDiagnosticOverview(width, height int) string {
	title := fmt.Sprintf(" DIAGNOSTICS  %s  |  %d vertices", m.diagnosticPageLabel(), len(m.snapshot.Nodes))
	status := " Worst first from Flink subtask metrics  |  [ ] switch page  |  enter opens selected vertex"
	if m.diagnosticFilter.Active() {
		status = " /" + m.diagnosticFilter.Value() + "|  filter vertex name and diagnosis"
	} else if m.diagnosticFilter.Value() != "" {
		status = " filter /" + m.diagnosticFilter.Value() + "/  |  pinned  |  " + status
	}
	if !m.snapshot.UpdatedAt.IsZero() {
		status += "  |  " + m.snapshot.UpdatedAt.Format("15:04:05")
	}
	lines := []string{
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#E2E8F0")).Render(shared.Truncate(title, width)),
		lipgloss.NewStyle().Foreground(shared.C("#94A3B8")).Render(shared.Truncate(status, width)),
		m.renderDiagnosticColumns(width),
	}
	order := m.diagnosticOrder()
	if len(order) == 0 && len(m.snapshot.Nodes) > 0 && m.diagnosticFilter.Value() != "" {
		lines = append(lines, shared.Truncate(" No vertices match filter /"+m.diagnosticFilter.Value()+"/. Press / to replace it or Ctrl+W while editing to clear.", width))
	}
	start := m.diagnosticWindowStart()
	end := min(len(order), start+m.diagnosticRowsAvailable())
	for index := start; index < end; index++ {
		if node, ok := m.nodeByID(order[index]); ok {
			lines = append(lines, m.renderDiagnosticRow(node, node.ID == m.selected, width))
		}
	}
	if len(m.snapshot.Nodes) == 0 {
		lines = append(lines, " Waiting for the job snapshot.")
	}
	for len(lines) < height-3 {
		lines = append(lines, "")
	}
	separator := lipgloss.NewStyle().Foreground(shared.C("#334155")).Render(strings.Repeat("-", width))
	detailOne, detailTwo := m.renderDiagnosticSelection(width)
	lines = append(lines, separator, detailOne, detailTwo)
	return shared.FitLines(lines, width, height)
}

func (m Model) diagnosticOrder() []string {
	order := append([]string(nil), m.layout.Order...)
	slices.SortStableFunc(order, func(leftID, rightID string) int {
		left, leftOK := m.nodeByID(leftID)
		right, rightOK := m.nodeByID(rightID)
		if !leftOK || !rightOK {
			return 0
		}
		return compareDiagnosticSeverity(m.diagnosticSeverity(left), m.diagnosticSeverity(right))
	})
	query := strings.ToLower(strings.TrimSpace(m.diagnosticFilter.Value()))
	if query != "" {
		filtered := order[:0]
		for _, id := range order {
			if node, ok := m.nodeByID(id); ok && diagnosticMatches(node, query) {
				filtered = append(filtered, id)
			}
		}
		order = filtered
	}
	return order
}

func diagnosticMatches(node flink.Node, query string) bool {
	return strings.Contains(strings.ToLower(node.Name), query) ||
		strings.Contains(strings.ToLower(pressureDiagnosis(node.Metrics)), query)
}

func (m Model) diagnosticSeverity(node flink.Node) float64 {
	if m.diagnosticPage == diagnosticSkew {
		return node.Metrics.DataSkewPercent
	}
	return jobgraphmodule.OperationalSeverity(node.Metrics)
}

func compareDiagnosticSeverity(left, right float64) int {
	leftNaN, rightNaN := math.IsNaN(left), math.IsNaN(right)
	if leftNaN && rightNaN {
		return 0
	}
	if leftNaN {
		return 1
	}
	if rightNaN {
		return -1
	}
	return cmp.Compare(right, left)
}

func (m Model) renderDiagnosticColumns(width int) string {
	var columns string
	switch m.diagnosticPage {
	case diagnosticSkew:
		if width >= 92 {
			columns = "   VERTEX                                  PAR      IN/s     OUT/s     SKEW  INPUT DISTRIBUTION"
		} else {
			columns = "   VERTEX                         PAR      IN/s     OUT/s     SKEW"
		}
	case diagnosticMetrics:
		if width >= 100 {
			columns = "   VERTEX                                  IN/s     OUT/s    BUSY      BP    IDLE  WATERMARK"
		} else {
			columns = "   VERTEX                         IN/s     OUT/s    BUSY      BP"
		}
	default:
		if width >= 96 {
			columns = "   VERTEX                                  BUSY       BACKPRESSURE      IDLE  DIAGNOSIS"
		} else {
			nameWidth := max(14, width-55)
			columns = "   " + shared.PadRight("VERTEX", nameWidth) + "    BUSY        BP       DIAGNOSIS"
		}
	}
	return lipgloss.NewStyle().Bold(true).Foreground(shared.C("#94A3B8")).Background(shared.C("#111827")).Render(
		shared.PadRight(shared.Truncate(columns, width), width),
	)
}

func (m Model) renderDiagnosticRow(node flink.Node, selected bool, width int) string {
	marker := " "
	if selected {
		marker = ">"
	}
	nameWidth := 34
	if width < 96 {
		nameWidth = max(14, width-55)
	}
	prefix := " " + marker + " " + shared.PadRight(shared.Truncate(node.Name, nameWidth), nameWidth)
	var line string
	switch m.diagnosticPage {
	case diagnosticSkew:
		line = prefix + fmt.Sprintf(" %4d  %8s  %8s  %6.1f%%", node.Parallelism,
			rate(node.Metrics.RecordsInPerSecond), rate(node.Metrics.RecordsOutPerSecond), node.Metrics.DataSkewPercent)
		if width >= 92 {
			trendWidth := max(8, width-lipgloss.Width(line)-2)
			values := metricValues(m.nodeHistory(node.ID), func(metrics flink.Metrics) float64 { return metrics.RecordsInPerSecond })
			line += "  " + sparklineDynamic(values, trendWidth)
		}
	case diagnosticMetrics:
		line = prefix + fmt.Sprintf(" %8s  %8s  %6.1f%%  %6.1f%%", rate(node.Metrics.RecordsInPerSecond),
			rate(node.Metrics.RecordsOutPerSecond), node.Metrics.BusyPercent, node.Metrics.BackpressurePercent)
		if width >= 100 {
			line += fmt.Sprintf("  %6.1f%%  %-9s", node.Metrics.IdlePercent, formatWatermark(node.Metrics))
		}
	default:
		level := pressureLevel(node.Metrics)
		diagnosis := pressureDiagnosis(node.Metrics)
		if width < 96 {
			line = prefix + fmt.Sprintf(" %6.1f%%  %4s %6.1f%%  %s", node.Metrics.BusyPercent,
				level, node.Metrics.BackpressurePercent, diagnosis)
		} else {
			line = prefix + fmt.Sprintf(" %6.1f%%  %4s %6.1f%%  %6.1f%%  %s", node.Metrics.BusyPercent,
				level, node.Metrics.BackpressurePercent, node.Metrics.IdlePercent, diagnosis)
		}
	}
	line = shared.PadRight(shared.Truncate(line, width), width)
	if selected {
		return lipgloss.NewStyle().Foreground(shared.C("#FFFFFF")).Background(shared.C("#6D28D9")).Bold(true).Render(line)
	}
	return jobgraphmodule.PressureStyle(pressureLevel(node.Metrics)).Render(line)
}

func (m Model) renderDiagnosticSelection(width int) (string, string) {
	node, ok := m.selectedNode()
	if !ok {
		return " Select a vertex.", ""
	}
	first := fmt.Sprintf(" %s  |  %s  |  parallelism %d  |  %s", node.Name, node.State, node.Parallelism, pressureDiagnosis(node.Metrics))
	second := m.renderTrend(node.ID, width)
	return shared.Truncate(first, width), shared.Truncate(second, width)
}

func (m Model) diagnosticPageLabel() string {
	switch m.diagnosticPage {
	case diagnosticSkew:
		return "DATA SKEW"
	case diagnosticMetrics:
		return "METRICS"
	default:
		return "BACKPRESSURE"
	}
}

func pressureDiagnosis(metrics flink.Metrics) string {
	switch pressureLevel(metrics) {
	case flink.BackpressureHigh:
		if metrics.BusyPercent > 80 {
			return "saturated and backpressured"
		}
		return "downstream pressure"
	case flink.BackpressureLow:
		return "intermittent pressure"
	default:
		if metrics.BusyPercent > 85 {
			return "busy"
		}
		if metrics.IdlePercent > 80 {
			return "mostly idle"
		}
		return "healthy"
	}
}
