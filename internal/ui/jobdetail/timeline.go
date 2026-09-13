package jobdetail

import (
	"fmt"
	"math"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
	shellmodule "github.com/nikitasavinov/flink-tui/internal/ui/shell"
)

const timelineBodyRowStart = 3

func (m *Model) openTimeline() {
	if m.snapshot.JobID == "" {
		return
	}
	m.view = ViewTimeline
	m.ensureTimelineSelection()
}

func (m *Model) handleTimelineKey(key string) tea.Cmd {
	if m.timelineFilter.Active() {
		action := m.timelineFilter.HandleKey(key)
		m.ensureTimelineSelection()
		if action == shellmodule.QueryConfirmed {
			return m.openDiagnostics()
		}
		return nil
	}
	switch key {
	case "esc", "g":
		m.pendingIntent = IntentGraph
	case "up", "k":
		m.moveTimelineSelection(-1)
	case "down", "j":
		m.moveTimelineSelection(1)
	case "pgup":
		m.moveTimelineSelection(-max(1, m.timelineRowsAvailable()))
	case "pgdown":
		m.moveTimelineSelection(max(1, m.timelineRowsAvailable()))
	case "enter":
		return m.openDiagnostics()
	case "/":
		m.timelineFilter.Open()
	}
	return nil
}

func (m Model) timelineRowsAvailable() int {
	return max(1, m.bodyHeight()-timelineBodyRowStart-3)
}

func (m Model) timelineWindowStart() int {
	available := m.timelineRowsAvailable()
	order := m.timelineOrder()
	position := 0
	for index, id := range order {
		if id == m.selected {
			position = index
			break
		}
	}
	return shared.WindowStart(position, available, len(order))
}

func (m Model) timelineOrder() []string {
	order := append([]string(nil), m.layout.Order...)
	query := strings.ToLower(strings.TrimSpace(m.timelineFilter.Value()))
	if query == "" {
		return order
	}
	filtered := order[:0]
	for _, id := range order {
		if node, ok := m.nodeByID(id); ok && strings.Contains(strings.ToLower(node.Name), query) {
			filtered = append(filtered, id)
		}
	}
	return filtered
}

func (m *Model) ensureTimelineSelection() {
	order := m.timelineOrder()
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

func (m *Model) moveTimelineSelection(delta int) {
	order := m.timelineOrder()
	if len(order) == 0 || delta == 0 {
		return
	}
	m.ensureTimelineSelection()
	position := 0
	for index, id := range order {
		if id == m.selected {
			position = index
			break
		}
	}
	m.selected = order[shared.Clamp(position+delta, 0, len(order)-1)]
}

func (m *Model) handleTimelineMouseClick(event tea.Mouse) {
	if event.Button != tea.MouseLeft {
		return
	}
	position := event.Y - headerHeight - timelineBodyRowStart
	if position < 0 || position >= m.timelineRowsAvailable() {
		return
	}
	index := m.timelineWindowStart() + position
	order := m.timelineOrder()
	if index >= 0 && index < len(order) {
		m.selected = order[index]
	}
}

func (m Model) renderTimeline(width, height int) string {
	title := fmt.Sprintf(" TIMELINE  %s  |  %d vertices", m.snapshot.JobState, len(m.snapshot.Nodes))
	caption := " job " + shared.Timestamp(m.snapshot.StartedAt) + " -> now  |  lifetime " + shared.HumanDuration(m.snapshot.Duration)
	if m.snapshot.JobState != "RUNNING" && !m.snapshot.StartedAt.IsZero() {
		caption = " job " + shared.Timestamp(m.snapshot.StartedAt) + "  |  lifetime " + shared.HumanDuration(m.snapshot.Duration)
	}
	if m.timelineFilter.Active() {
		caption = " /" + m.timelineFilter.Value() + "|  filter vertex name"
	} else if m.timelineFilter.Value() != "" {
		caption = " filter /" + m.timelineFilter.Value() + "/  |  pinned  |  " + caption
	}
	transitions := m.renderStateTransitions(width)
	lines := []string{
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#E2E8F0")).Render(shared.Truncate(title, width)),
		lipgloss.NewStyle().Foreground(shared.C("#94A3B8")).Render(shared.Truncate(caption+"  |  "+transitions, width)),
		m.renderTimelineColumns(width),
	}
	order := m.timelineOrder()
	if len(order) == 0 && len(m.snapshot.Nodes) > 0 && m.timelineFilter.Value() != "" {
		lines = append(lines, shared.Truncate(" No vertices match filter /"+m.timelineFilter.Value()+"/. Press / to replace it or Ctrl+W while editing to clear.", width))
	}
	start := m.timelineWindowStart()
	end := min(len(order), start+m.timelineRowsAvailable())
	for index := start; index < end; index++ {
		if node, ok := m.nodeByID(order[index]); ok {
			lines = append(lines, m.renderTimelineRow(node, node.ID == m.selected, width))
		}
	}
	for len(lines) < height-3 {
		lines = append(lines, "")
	}
	separator := lipgloss.NewStyle().Foreground(shared.C("#334155")).Render(strings.Repeat("-", width))
	first, second := m.renderTimelineSelection(width)
	lines = append(lines, separator, first, second)
	return shared.FitLines(lines, width, height)
}

func (m Model) renderStateTransitions(width int) string {
	if len(m.snapshot.Transitions) == 0 {
		return "state timestamps unavailable"
	}
	parts := make([]string, 0, len(m.snapshot.Transitions))
	for _, transition := range m.snapshot.Transitions {
		part := transition.State + " " + transition.At.Format("15:04:05")
		if lipgloss.Width(strings.Join(append(parts, part), " -> ")) > max(20, width/2) {
			break
		}
		parts = append(parts, part)
	}
	return strings.Join(parts, " -> ")
}

func (m Model) renderTimelineColumns(width int) string {
	nameWidth := timelineNameWidth(width)
	barWidth := max(10, width-nameWidth-39)
	axisStart, axisEnd := m.timelineVertexRange()
	axisLabel := "VERTEX RANGE"
	if axisEnd.After(axisStart) {
		axisLabel += " " + shared.HumanDuration(axisEnd.Sub(axisStart))
	}
	line := "   " + shared.PadRight("VERTEX", nameWidth) + " STATE        START     DURATION   " + shared.PadRight(shared.Truncate(axisLabel, barWidth), barWidth)
	return lipgloss.NewStyle().Bold(true).Foreground(shared.C("#94A3B8")).Background(shared.C("#111827")).Render(
		shared.PadRight(shared.Truncate(line, width), width),
	)
}

func (m Model) renderTimelineRow(node flink.Node, selected bool, width int) string {
	marker := " "
	if selected {
		marker = ">"
	}
	nameWidth := timelineNameWidth(width)
	barWidth := max(10, width-nameWidth-39)
	line := fmt.Sprintf(" %s %s %-11s %s  %9s  %s",
		marker,
		shared.PadRight(shared.Truncate(node.Name, nameWidth), nameWidth),
		shared.Truncate(node.State, 11),
		shared.Clock(node.StartedAt, "-"),
		shared.HumanDuration(node.Duration),
		m.timelineBar(node, barWidth),
	)
	line = shared.PadRight(shared.Truncate(line, width), width)
	if selected {
		return lipgloss.NewStyle().Foreground(shared.C("#FFFFFF")).Background(shared.C("#6D28D9")).Bold(true).Render(line)
	}
	return lipgloss.NewStyle().Foreground(shared.StatusColor(node.State)).Render(line)
}

func timelineNameWidth(width int) int {
	if width >= 108 {
		return 34
	}
	return max(14, min(28, width-52))
}

func (m Model) timelineBar(node flink.Node, width int) string {
	if width <= 0 {
		return ""
	}
	axisStart, axisEnd := m.timelineVertexRange()
	if node.StartedAt.IsZero() || axisStart.IsZero() || !axisEnd.After(axisStart) {
		return strings.Repeat("·", width)
	}
	axisDuration := axisEnd.Sub(axisStart)
	offset := node.StartedAt.Sub(axisStart)
	if offset < 0 {
		offset = 0
	}
	start := shared.Clamp(int(math.Floor(float64(offset)/float64(axisDuration)*float64(width))), 0, width-1)
	nodeEnd := m.timelineNodeEnd(node)
	if nodeEnd.Before(node.StartedAt) {
		nodeEnd = node.StartedAt
	}
	endOffset := nodeEnd.Sub(axisStart)
	if endOffset < offset {
		endOffset = offset
	}
	end := shared.Clamp(int(math.Ceil(float64(endOffset)/float64(axisDuration)*float64(width))), start+1, width)
	length := end - start
	return strings.Repeat("·", start) + strings.Repeat("█", length) + strings.Repeat("·", max(0, width-start-length))
}

// timelineVertexRange uses the execution attempts represented by the vertex
// rows, not the job submission timestamp. A restart can leave a job alive for
// days while every current attempt is only minutes old; scaling to this range
// keeps those attempts readable.
func (m Model) timelineVertexRange() (time.Time, time.Time) {
	var earliest, latest time.Time
	for _, node := range m.snapshot.Nodes {
		if node.StartedAt.IsZero() {
			continue
		}
		if earliest.IsZero() || node.StartedAt.Before(earliest) {
			earliest = node.StartedAt
		}
		end := m.timelineNodeEnd(node)
		if end.Before(node.StartedAt) {
			end = node.StartedAt
		}
		if latest.IsZero() || end.After(latest) {
			latest = end
		}
	}
	if earliest.IsZero() {
		earliest = m.snapshot.StartedAt
		latest = earliest.Add(max(m.snapshot.Duration, time.Second))
	}
	if !latest.After(earliest) {
		latest = earliest.Add(time.Second)
	}
	return earliest, latest
}

func (m Model) timelineNodeEnd(node flink.Node) time.Time {
	if node.Duration > 0 {
		return node.StartedAt.Add(node.Duration)
	}
	if m.snapshot.UpdatedAt.After(node.StartedAt) {
		return m.snapshot.UpdatedAt
	}
	jobEnd := m.snapshot.StartedAt.Add(m.snapshot.Duration)
	if jobEnd.After(node.StartedAt) {
		return jobEnd
	}
	return node.StartedAt
}

func (m Model) renderTimelineSelection(width int) (string, string) {
	node, ok := m.selectedNode()
	if !ok {
		return " Select a vertex.", ""
	}
	maximum := "-"
	if node.MaxParallelism > 0 {
		maximum = fmt.Sprintf("%d", node.MaxParallelism)
	}
	first := fmt.Sprintf(" %s  |  %s  |  parallelism %d / max %s", node.Name, node.State, node.Parallelism, maximum)
	second := fmt.Sprintf(" started %s  |  runtime %s  |  enter opens subtasks", shared.Timestamp(node.StartedAt), shared.HumanDuration(node.Duration))
	return shared.Truncate(first, width), shared.Truncate(second, width)
}
