package metrics

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
	shellmodule "github.com/nikitasavinov/flink-tui/internal/ui/shell"
)

const maxTrackedMetrics = 4

const metricBodyRowStart = 3

var metricWindows = []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute}

type customMetricSample struct {
	at    time.Time
	value float64
}

func (m *Model) fetchMetricCatalog() tea.Cmd {
	client := m.client
	jobID := m.snapshot.JobID
	vertexID := m.metricVertex
	generation := m.generation
	requestID, start := m.catalogRequests.Begin(catalogRequest{jobID, vertexID, generation})
	if !start {
		return nil
	}
	requestTimeout := m.requestTimeout
	return func() tea.Msg {
		ctx, cancel := requestTimeout(defaultRequestTimeout)
		defer cancel()
		names, err := client.MetricNames(ctx, jobID, vertexID)
		return metricCatalogMsg{names: names, err: err, jobID: jobID, vertexID: vertexID, generation: generation, requestID: requestID}
	}
}

func (m *Model) fetchCustomMetricValues() tea.Cmd {
	client := m.client
	jobID := m.snapshot.JobID
	vertexID := m.metricVertex
	subtask := m.metricScope
	aggregation := m.metricAggregation
	tracked := slices.Clone(m.metricTracked)
	requested := m.metricRequestNames()
	generation := m.generation
	requestID, start := m.valuesRequests.Begin(valuesRequest{
		catalogRequest: catalogRequest{jobID, vertexID, generation},
		subtask:        subtask, aggregation: aggregation,
		tracked: fmt.Sprintf("%q", tracked), requested: fmt.Sprintf("%q", requested),
	})
	if !start {
		return nil
	}
	requestTimeout := m.requestTimeout
	return func() tea.Msg {
		ctx, cancel := requestTimeout(defaultRequestTimeout)
		defer cancel()
		values, err := client.MetricValues(ctx, jobID, vertexID, subtask, aggregation, requested)
		return metricValuesMsg{
			values: values, err: err, jobID: jobID, vertexID: vertexID,
			subtask: subtask, aggregation: aggregation, tracked: tracked, requested: requested,
			at: time.Now(), generation: generation, requestID: requestID,
		}
	}
}

func (m Model) metricRequestNames() []string {
	names := slices.Clone(m.metricTracked)
	filtered := m.filteredMetricNames()
	start := m.metricWindowStart()
	end := min(len(filtered), start+m.metricRowsAvailable())
	names = append(names, filtered[start:end]...)
	slices.Sort(names)
	return slices.Compact(names)
}

func defaultTrackedMetrics(names []string) []string {
	preferred := []string{"numRecordsInPerSecond", "numRecordsOutPerSecond"}
	tracked := make([]string, 0, 2)
	for _, candidate := range preferred {
		if slices.Contains(names, candidate) {
			tracked = append(tracked, candidate)
		}
	}
	if len(tracked) == 0 && len(names) > 0 {
		tracked = append(tracked, names[0])
	}
	return tracked
}

func availableTrackedMetrics(tracked, available []string) []string {
	result := make([]string, 0, len(tracked))
	for _, name := range tracked {
		if slices.Contains(available, name) {
			result = append(result, name)
		}
	}
	return result
}

func (m *Model) recordCustomMetricValues(message metricValuesMsg) {
	if m.metricHistory == nil {
		m.metricHistory = make(map[string][]customMetricSample)
	}
	cutoff := message.at.Add(-metricWindows[len(metricWindows)-1])
	for _, name := range message.tracked {
		value, ok := message.values[name]
		if !ok || math.IsNaN(value) || math.IsInf(value, 0) {
			continue
		}
		key := customMetricSeriesKey(message.jobID, message.vertexID, message.subtask, message.aggregation, name)
		samples := append(m.metricHistory[key], customMetricSample{at: message.at, value: value})
		first := 0
		for first < len(samples) && samples[first].at.Before(cutoff) {
			first++
		}
		m.metricHistory[key] = slices.Clone(samples[first:])
	}
}

func customMetricSeriesKey(jobID, vertexID string, subtask int, aggregation flink.MetricAggregation, name string) string {
	return fmt.Sprintf("%s\x00%s\x00%d\x00%s\x00%s", jobID, vertexID, subtask, aggregation, name)
}

func (m Model) currentMetricSamples(name string) []customMetricSample {
	key := customMetricSeriesKey(m.snapshot.JobID, m.metricVertex, m.metricScope, m.metricAggregation, name)
	samples := m.metricHistory[key]
	cutoff := time.Now().Add(-m.metricWindow)
	first := 0
	for first < len(samples) && samples[first].at.Before(cutoff) {
		first++
	}
	return samples[first:]
}

func (m Model) filteredMetricNames() []string {
	query := strings.ToLower(strings.TrimSpace(m.metricFilter.Value()))
	if query == "" {
		return m.metricNames
	}
	result := make([]string, 0)
	for _, name := range m.metricNames {
		if strings.Contains(strings.ToLower(name), query) {
			result = append(result, name)
		}
	}
	return result
}

func (m *Model) handleMetricExplorerKey(key string) tea.Cmd {
	if m.metricFilter.Active() {
		return m.handleMetricSearchKey(key)
	}
	switch key {
	case "esc", "g":
		m.intent = IntentGraph
	case "up", "k":
		m.moveMetricSelection(-1)
		return m.refreshMetricWindow()
	case "down", "j":
		m.moveMetricSelection(1)
		return m.refreshMetricWindow()
	case "pgup":
		m.moveMetricSelection(-max(1, m.metricRowsAvailable()))
		return m.refreshMetricWindow()
	case "pgdown":
		m.moveMetricSelection(max(1, m.metricRowsAvailable()))
		return m.refreshMetricWindow()
	case "home":
		m.metricSelection.Set(0, 0)
		return m.refreshMetricWindow()
	case "end":
		names := m.filteredMetricNames()
		m.metricSelection.Set(len(names)-1, len(names))
		return m.refreshMetricWindow()
	case "enter":
		return m.toggleSelectedMetric()
	case "/":
		m.metricFilter.Restore(shellmodule.QueryState{Value: m.metricFilter.Value(), Open: true})
	case "s":
		m.cycleMetricScope()
		return m.refreshTrackedMetrics()
	case "a":
		if m.metricScope < 0 {
			m.cycleMetricAggregation()
			return m.refreshTrackedMetrics()
		}
	case "[":
		m.cycleMetricWindow(-1)
	case "]":
		m.cycleMetricWindow(1)
	case "x":
		m.metricTracked = nil
		m.metricValues = make(map[string]float64)
		return m.refreshMetricWindow()
	}
	return nil
}

func (m *Model) handleMetricSearchKey(key string) tea.Cmd {
	if key != "ctrl+w" {
		m.metricFilter.HandleKey(key)
	}
	m.metricSelection.Set(0, 0)
	return m.refreshMetricWindow()
}

func (m *Model) moveMetricSelection(delta int) {
	names := m.filteredMetricNames()
	m.metricSelection.Move(delta, len(names))
}

func (m *Model) toggleSelectedMetric() tea.Cmd {
	names := m.filteredMetricNames()
	if m.metricSelection.Index() < 0 || m.metricSelection.Index() >= len(names) {
		return nil
	}
	name := names[m.metricSelection.Index()]
	for index, tracked := range m.metricTracked {
		if tracked == name {
			m.metricTracked = append(m.metricTracked[:index], m.metricTracked[index+1:]...)
			return m.refreshMetricWindow()
		}
	}
	if len(m.metricTracked) >= maxTrackedMetrics {
		m.metricErr = fmt.Errorf("track at most %d metrics; remove one before adding %s", maxTrackedMetrics, name)
		return nil
	}
	m.metricErr = nil
	m.metricTracked = append(m.metricTracked, name)
	return m.refreshTrackedMetrics()
}

func (m *Model) refreshTrackedMetrics() tea.Cmd {
	m.metricBusy = len(m.metricRequestNames()) > 0
	m.metricErr = nil
	m.metricValues = make(map[string]float64)
	return m.fetchCustomMetricValues()
}

func (m *Model) refreshMetricWindow() tea.Cmd {
	if len(m.metricNames) == 0 {
		return nil
	}
	m.metricBusy = true
	m.metricErr = nil
	return m.fetchCustomMetricValues()
}

func (m *Model) cycleMetricScope() {
	parallelism := 0
	if node, ok := m.nodeByID(m.metricVertex); ok {
		parallelism = node.Parallelism
	}
	if parallelism <= 0 || m.metricScope >= parallelism-1 {
		m.metricScope = -1
		return
	}
	m.metricScope++
}

func (m *Model) cycleMetricAggregation() {
	values := []flink.MetricAggregation{flink.MetricSum, flink.MetricAvg, flink.MetricMax, flink.MetricMin}
	index := slices.Index(values, m.metricAggregation)
	if index < 0 {
		index = 0
	}
	m.metricAggregation = values[(index+1)%len(values)]
}

func (m *Model) cycleMetricWindow(delta int) {
	index := slices.Index(metricWindows, m.metricWindow)
	if index < 0 {
		index = 1
	}
	index = shared.Clamp(index+delta, 0, len(metricWindows)-1)
	m.metricWindow = metricWindows[index]
}

func (m Model) metricRowsAvailable() int {
	chartRows := max(1, len(m.metricTracked))
	return max(3, m.bodyHeight()-5-chartRows)
}

func (m Model) metricWindowStart() int {
	available := m.metricRowsAvailable()
	return shared.WindowStart(m.metricSelection.Index(), available, len(m.filteredMetricNames()))
}

func (m *Model) handleMetricMouseClick(event tea.Mouse) {
	if event.Button != tea.MouseLeft {
		return
	}
	position := event.Y - headerHeight - metricBodyRowStart
	if position < 0 || position >= m.metricRowsAvailable() {
		return
	}
	names := m.filteredMetricNames()
	index := m.metricWindowStart() + position
	if index >= 0 && index < len(names) {
		m.metricSelection.Set(index, len(names))
	}
}

func (m Model) renderMetricExplorer(width, height int) string {
	name := m.nodeName(m.metricVertex)
	title := fmt.Sprintf(" CUSTOM METRICS  %s  |  %d exposed  |  %d/%d tracked",
		name, len(m.metricNames), len(m.metricTracked), maxTrackedMetrics)
	if m.metricBusy {
		title += "  |  refreshing..."
	}
	status := fmt.Sprintf(" scope %s  |  aggregation %s  |  history %s  |  / filter",
		m.metricScopeLabel(), strings.ToUpper(string(m.metricAggregation)), metricWindowLabel(m.metricWindow))
	statusStyle := lipgloss.NewStyle().Foreground(shared.C("#94A3B8"))
	if m.metricFilter.Active() {
		status = " /" + m.metricFilter.Value() + "|"
		statusStyle = statusStyle.Foreground(shared.C("#E2E8F0")).Background(shared.C("#111827"))
	} else if m.metricFilter.Value() != "" {
		status += "  |  filter /" + m.metricFilter.Value() + "/"
	}
	if m.metricErr != nil {
		status = " " + shared.ErrorText(m.metricErr)
		statusStyle = statusStyle.Foreground(shared.C("#FB7185"))
	}
	lines := []string{
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#E2E8F0")).Render(shared.Truncate(title, width)),
		statusStyle.Render(shared.PadRight(shared.Truncate(status, width), width)),
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#94A3B8")).Background(shared.C("#111827")).Render(
			shared.PadRight(shared.Truncate("   TRACK  METRIC                                                        CURRENT", width), width)),
	}
	names := m.filteredMetricNames()
	start := m.metricWindowStart()
	end := min(len(names), start+m.metricRowsAvailable())
	for index := start; index < end; index++ {
		lines = append(lines, m.renderMetricRow(names[index], index == m.metricSelection.Index(), width))
	}
	if len(names) == 0 && !m.metricBusy && m.metricErr == nil {
		lines = append(lines, " No matching metrics exposed by this vertex.")
	}
	for len(lines) < 3+m.metricRowsAvailable() {
		lines = append(lines, "")
	}
	separator := lipgloss.NewStyle().Foreground(shared.C("#334155")).Render(strings.Repeat("-", width))
	lines = append(lines, separator,
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#A78BFA")).Render(
			shared.Truncate(" CHARTS  history accumulates while this view is open", width)))
	if len(m.metricTracked) == 0 {
		lines = append(lines, " Press enter on any metric to start charting it.")
	} else {
		for _, metric := range m.metricTracked {
			lines = append(lines, m.renderMetricChart(metric, width))
		}
	}
	return shared.FitLines(lines, width, height)
}

func (m Model) renderMetricRow(name string, selected bool, width int) string {
	marker := " "
	if selected {
		marker = ">"
	}
	tracked := "[ ]"
	if slices.Contains(m.metricTracked, name) {
		tracked = "[x]"
	}
	value := "-"
	if current, ok := m.metricValues[name]; ok {
		value = formatCustomMetricValue(name, current)
	}
	nameWidth := max(12, width-22)
	line := fmt.Sprintf(" %s %s  %s  %12s", marker, tracked, shared.PadRight(shared.Truncate(name, nameWidth), nameWidth), value)
	line = shared.PadRight(shared.Truncate(line, width), width)
	if selected {
		return lipgloss.NewStyle().Foreground(shared.C("#FFFFFF")).Background(shared.C("#6D28D9")).Bold(true).Render(line)
	}
	return lipgloss.NewStyle().Foreground(shared.C("#CBD5E1")).Render(line)
}

func (m Model) renderMetricChart(name string, width int) string {
	samples := m.currentMetricSamples(name)
	values := make([]float64, len(samples))
	minimum, maximum := 0.0, 0.0
	for index, sample := range samples {
		values[index] = sample.value
		if index == 0 || sample.value < minimum {
			minimum = sample.value
		}
		if index == 0 || sample.value > maximum {
			maximum = sample.value
		}
	}
	current := "-"
	if value, ok := m.metricValues[name]; ok {
		current = formatCustomMetricValue(name, value)
	}
	nameWidth := max(10, min(32, width/3))
	prefix := fmt.Sprintf(" %-*s %12s  ", nameWidth, shared.Truncate(name, nameWidth), current)
	chartWidth := max(6, width-shared.DisplayWidth(prefix)-1)
	chart := sparklineDynamic(values, chartWidth)
	line := prefix + chart
	if width >= 116 && len(samples) > 0 {
		line = fmt.Sprintf(" %-*s %12s  min %-10s max %-10s  %s", nameWidth, shared.Truncate(name, nameWidth), current,
			formatCustomMetricValue(name, minimum), formatCustomMetricValue(name, maximum), sparklineDynamic(values, max(6, width-nameWidth-52)))
	}
	return lipgloss.NewStyle().Foreground(shared.C("#C4B5FD")).Render(shared.Truncate(line, width))
}

func (m Model) metricScopeLabel() string {
	if m.metricScope < 0 {
		return "VERTEX"
	}
	return fmt.Sprintf("SUBTASK #%d", m.metricScope)
}

func metricWindowLabel(window time.Duration) string {
	if window%time.Minute == 0 {
		return fmt.Sprintf("%dm", int(window/time.Minute))
	}
	return window.String()
}

func formatCustomMetricValue(name string, value float64) string {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return "N/A"
	}
	lower := strings.ToLower(name)
	if strings.Contains(lower, "bytes") && !strings.Contains(lower, "buffers") {
		formatted := humanMetricBytes(math.Abs(value))
		if value < 0 {
			formatted = "-" + formatted
		}
		if strings.Contains(lower, "persecond") {
			formatted += "/s"
		}
		return formatted
	}
	abs := math.Abs(value)
	switch {
	case abs >= 1_000_000_000:
		return fmt.Sprintf("%.3g", value)
	case abs >= 1000:
		return fmt.Sprintf("%.1fk", value/1000)
	case abs >= 100:
		return fmt.Sprintf("%.0f", value)
	case abs >= 1:
		return fmt.Sprintf("%.2f", value)
	default:
		return fmt.Sprintf("%.4g", value)
	}
}
