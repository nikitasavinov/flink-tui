package checkpoints

import (
	"cmp"
	"fmt"
	"image/color"
	"slices"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
	shellmodule "github.com/nikitasavinov/flink-tui/internal/ui/shell"
)

const (
	checkpointDiagnosticBodyRowStart = 3
	checkpointDiagnosticDetailRows   = 4
)

type checkpointOperatorSort uint8

const (
	sortCheckpointOperatorDiagnosis checkpointOperatorSort = iota
	sortCheckpointOperatorDuration
	sortCheckpointOperatorState
	sortCheckpointOperatorAcknowledgement
	sortCheckpointOperatorProcessed
)

type checkpointSubtaskSort uint8

const (
	sortCheckpointSubtaskDiagnosis checkpointSubtaskSort = iota
	sortCheckpointSubtaskDuration
	sortCheckpointSubtaskState
	sortCheckpointSubtaskAlignment
	sortCheckpointSubtaskStartDelay
)

func (m *Model) openCheckpointDetail() tea.Cmd {
	checkpoint, ok := m.selectedCheckpoint()
	if !ok || m.snapshot.JobID == "" {
		return nil
	}
	m.view = ViewOperators
	m.checkpointSelectedID = checkpoint.ID
	m.checkpointDetailID = checkpoint.ID
	m.checkpointDetail = flink.CheckpointDetails{}
	m.checkpointConfig = flink.CheckpointConfig{}
	m.checkpointDetailBusy = true
	m.checkpointDetailErr = nil
	m.checkpointConfigErr = nil
	m.checkpointOperatorCursor = 0
	m.checkpointOperatorSelected = ""
	m.checkpointConfigOpen = false
	return m.fetchCheckpointDetail()
}

func (m *Model) fetchCheckpointDetail() tea.Cmd {
	requestID, started := m.detailRequest.Begin(checkpointRequestKey{
		jobID: m.snapshot.JobID, generation: m.generation, checkpointID: m.checkpointDetailID,
	})
	if !started {
		return nil
	}
	client := m.client
	jobID := m.snapshot.JobID
	checkpointID := m.checkpointDetailID
	generation := m.generation
	requestTimeout := m.requestTimeout
	return func() tea.Msg {
		ctx, cancel := requestTimeout(defaultRequestTimeout)
		defer cancel()
		type detailResult struct {
			details flink.CheckpointDetails
			err     error
		}
		type configResult struct {
			config flink.CheckpointConfig
			err    error
		}
		detailsChannel := make(chan detailResult, 1)
		configChannel := make(chan configResult, 1)
		go func() {
			details, err := client.CheckpointDetails(ctx, jobID, checkpointID)
			detailsChannel <- detailResult{details: details, err: err}
		}()
		go func() {
			config, err := client.CheckpointConfig(ctx, jobID)
			configChannel <- configResult{config: config, err: err}
		}()
		details := <-detailsChannel
		config := <-configChannel
		return checkpointDetailMsg{
			details:      details.details,
			config:       config.config,
			err:          details.err,
			configErr:    config.err,
			jobID:        jobID,
			checkpointID: checkpointID,
			generation:   generation,
			requestID:    requestID,
		}
	}
}

func (m *Model) openCheckpointSubtasks() tea.Cmd {
	operator, ok := m.selectedCheckpointOperator()
	if !ok {
		return nil
	}
	m.view = ViewSubtasks
	m.checkpointSubtaskVertex = operator.VertexID
	m.checkpointSubtasks = flink.CheckpointSubtaskDetails{}
	m.checkpointSubtasksBusy = true
	m.checkpointSubtasksErr = nil
	m.checkpointSubtaskCursor = 0
	m.checkpointSubtaskSelected = -1
	return m.fetchCheckpointSubtasks()
}

// StepPeer moves to the adjacent retained checkpoint while keeping the active
// diagnostic level and its sort. The history is newest-first and wraps so a
// repeated lateral scan always visits every retained checkpoint.
func (m *Model) StepPeer(delta int) Result {
	history := m.snapshot.Checkpoints.History
	if len(history) < 2 || delta == 0 {
		return Result{View: m.CurrentView(), Notice: "No other retained checkpoint is available."}
	}
	current := -1
	for index, checkpoint := range history {
		if checkpoint.ID == m.checkpointDetailID {
			current = index
			break
		}
	}
	if current < 0 {
		return Result{View: m.CurrentView(), Notice: "The current checkpoint is no longer in REST history."}
	}
	next := (current + delta%len(history) + len(history)) % len(history)
	if next == current {
		return Result{View: m.CurrentView(), Notice: "No other retained checkpoint is available."}
	}

	previousID := history[current].ID
	nextID := history[next].ID
	m.checkpointSelectedID = nextID
	m.checkpointDetailID = nextID
	m.checkpointDetail = flink.CheckpointDetails{}
	m.checkpointConfig = flink.CheckpointConfig{}
	m.checkpointDetailBusy = true
	m.checkpointDetailErr = nil
	m.checkpointConfigErr = nil
	m.checkpointConfigOpen = false
	m.checkpointPeerSubtasks = m.view == ViewSubtasks
	m.checkpointPeerVertex = m.checkpointSubtaskVertex
	if m.checkpointPeerSubtasks {
		m.checkpointSubtasks = flink.CheckpointSubtaskDetails{}
		m.checkpointSubtasksBusy = false
		m.checkpointSubtasksErr = nil
	} else {
		m.view = ViewOperators
	}
	return Result{
		Command: m.fetchCheckpointDetail(), View: m.CurrentView(),
		Peer: &PeerChange{
			PreviousID: previousID, ID: nextID,
			PreviousPosition: current + 1, Position: next + 1, Total: len(history),
		},
	}
}

func (m *Model) fetchCheckpointSubtasks() tea.Cmd {
	requestID, started := m.subtaskRequest.Begin(checkpointRequestKey{
		jobID: m.snapshot.JobID, generation: m.generation, checkpointID: m.checkpointDetailID,
		vertexID: m.checkpointSubtaskVertex,
	})
	if !started {
		return nil
	}
	client := m.client
	jobID := m.snapshot.JobID
	checkpointID := m.checkpointDetailID
	vertexID := m.checkpointSubtaskVertex
	generation := m.generation
	requestTimeout := m.requestTimeout
	return func() tea.Msg {
		ctx, cancel := requestTimeout(defaultRequestTimeout)
		defer cancel()
		details, err := client.CheckpointSubtasks(ctx, jobID, checkpointID, vertexID)
		return checkpointSubtasksMsg{
			details:      details,
			err:          err,
			jobID:        jobID,
			checkpointID: checkpointID,
			vertexID:     vertexID,
			generation:   generation,
			requestID:    requestID,
		}
	}
}

func (m *Model) handleCheckpointOperatorKey(key string) tea.Cmd {
	if m.checkpointOperatorFilter.Active() {
		action := m.checkpointOperatorFilter.HandleKey(key)
		m.syncCheckpointOperatorSelection()
		if action == shellmodule.QueryConfirmed {
			return m.openCheckpointSubtasks()
		}
		return nil
	}
	switch key {
	case "esc":
		m.view = ViewHistory
	case "enter":
		return m.openCheckpointSubtasks()
	case "up", "k":
		m.moveCheckpointOperatorSelection(-1)
	case "down", "j":
		m.moveCheckpointOperatorSelection(1)
	case "pgup":
		m.moveCheckpointOperatorSelection(-max(1, m.checkpointDiagnosticRowsAvailable()))
	case "pgdown":
		m.moveCheckpointOperatorSelection(max(1, m.checkpointDiagnosticRowsAvailable()))
	case "home":
		m.moveCheckpointOperatorTo(0)
	case "end":
		m.moveCheckpointOperatorTo(len(m.sortedCheckpointOperators()) - 1)
	case "s":
		m.checkpointOperatorSort = (m.checkpointOperatorSort + 1) % 5
		m.syncCheckpointOperatorSelection()
	case "i":
		m.checkpointConfigOpen = !m.checkpointConfigOpen
	case "g":
		m.jumpCheckpointOperatorToGraph()
	case "/":
		m.checkpointOperatorFilter.Open()
	}
	return nil
}

func (m *Model) handleCheckpointSubtaskKey(key string) tea.Cmd {
	switch key {
	case "esc", "enter":
		m.view = ViewOperators
	case "up", "k":
		m.moveCheckpointSubtaskSelection(-1)
	case "down", "j":
		m.moveCheckpointSubtaskSelection(1)
	case "pgup":
		m.moveCheckpointSubtaskSelection(-max(1, m.checkpointDiagnosticRowsAvailable()))
	case "pgdown":
		m.moveCheckpointSubtaskSelection(max(1, m.checkpointDiagnosticRowsAvailable()))
	case "home":
		m.moveCheckpointSubtaskTo(0)
	case "end":
		m.moveCheckpointSubtaskTo(len(m.sortedCheckpointSubtasks()) - 1)
	case "s":
		m.checkpointSubtaskSort = (m.checkpointSubtaskSort + 1) % 5
		m.syncCheckpointSubtaskSelection()
	case "g":
		m.jumpCheckpointVertexToGraph(m.checkpointSubtaskVertex)
	}
	return nil
}

func (m Model) sortedCheckpointOperators() []flink.CheckpointOperator {
	operators := slices.Clone(m.checkpointDetail.Operators)
	order := make(map[string]int, len(m.layout.Order))
	for index, vertexID := range m.layout.Order {
		order[vertexID] = index
	}
	slices.SortStableFunc(operators, func(left, right flink.CheckpointOperator) int {
		switch m.checkpointOperatorSort {
		case sortCheckpointOperatorDuration:
			if left.Duration != right.Duration {
				return cmp.Compare(right.Duration, left.Duration)
			}
		case sortCheckpointOperatorState:
			if left.StateSize != right.StateSize {
				return cmp.Compare(right.StateSize, left.StateSize)
			}
		case sortCheckpointOperatorAcknowledgement:
			leftMissing := max(0, left.Subtasks-left.AcknowledgedSubtasks)
			rightMissing := max(0, right.Subtasks-right.AcknowledgedSubtasks)
			if leftMissing != rightMissing {
				return cmp.Compare(rightMissing, leftMissing)
			}
			if !left.LatestAcknowledgedAt.Equal(right.LatestAcknowledgedAt) {
				return right.LatestAcknowledgedAt.Compare(left.LatestAcknowledgedAt)
			}
		case sortCheckpointOperatorProcessed:
			if left.ProcessedData != right.ProcessedData {
				return cmp.Compare(right.ProcessedData, left.ProcessedData)
			}
		default:
			leftMissing := max(0, left.Subtasks-left.AcknowledgedSubtasks)
			rightMissing := max(0, right.Subtasks-right.AcknowledgedSubtasks)
			if leftMissing != rightMissing {
				return cmp.Compare(rightMissing, leftMissing)
			}
			if checkpointStatusRank(left.Status) != checkpointStatusRank(right.Status) {
				return cmp.Compare(checkpointStatusRank(right.Status), checkpointStatusRank(left.Status))
			}
			if left.Duration != right.Duration {
				return cmp.Compare(right.Duration, left.Duration)
			}
		}
		leftOrder, leftKnown := order[left.VertexID]
		rightOrder, rightKnown := order[right.VertexID]
		if leftKnown && rightKnown && leftOrder != rightOrder {
			return cmp.Compare(leftOrder, rightOrder)
		}
		if leftKnown != rightKnown {
			if leftKnown {
				return -1
			}
			return 1
		}
		return cmp.Compare(left.VertexID, right.VertexID)
	})
	query := strings.ToLower(strings.TrimSpace(m.checkpointOperatorFilter.Value()))
	if query != "" {
		filtered := operators[:0]
		for _, operator := range operators {
			if strings.Contains(strings.ToLower(m.nodeName(operator.VertexID)), query) ||
				strings.Contains(strings.ToLower(operator.VertexID), query) {
				filtered = append(filtered, operator)
			}
		}
		operators = filtered
	}
	return operators
}

func (m *Model) syncCheckpointOperatorSelection() {
	operators := m.sortedCheckpointOperators()
	if len(operators) == 0 {
		m.checkpointOperatorCursor = 0
		m.checkpointOperatorSelected = ""
		return
	}
	if m.checkpointOperatorSelected != "" {
		for index, operator := range operators {
			if operator.VertexID == m.checkpointOperatorSelected {
				m.checkpointOperatorCursor = index
				return
			}
		}
	}
	m.checkpointOperatorCursor = shared.Clamp(m.checkpointOperatorCursor, 0, len(operators)-1)
	m.checkpointOperatorSelected = operators[m.checkpointOperatorCursor].VertexID
}

func (m *Model) moveCheckpointOperatorSelection(delta int) {
	if delta == 0 {
		return
	}
	m.syncCheckpointOperatorSelection()
	m.moveCheckpointOperatorTo(m.checkpointOperatorCursor + delta)
}

func (m *Model) moveCheckpointOperatorTo(position int) {
	operators := m.sortedCheckpointOperators()
	if len(operators) == 0 {
		return
	}
	m.checkpointOperatorCursor = shared.Clamp(position, 0, len(operators)-1)
	m.checkpointOperatorSelected = operators[m.checkpointOperatorCursor].VertexID
}

func (m Model) selectedCheckpointOperator() (flink.CheckpointOperator, bool) {
	operators := m.sortedCheckpointOperators()
	if len(operators) == 0 {
		return flink.CheckpointOperator{}, false
	}
	index := shared.Clamp(m.checkpointOperatorCursor, 0, len(operators)-1)
	return operators[index], true
}

func (m Model) sortedCheckpointSubtasks() []flink.CheckpointSubtask {
	subtasks := slices.Clone(m.checkpointSubtasks.Subtasks)
	slices.SortStableFunc(subtasks, func(left, right flink.CheckpointSubtask) int {
		switch m.checkpointSubtaskSort {
		case sortCheckpointSubtaskDuration:
			if left.Duration != right.Duration {
				return cmp.Compare(right.Duration, left.Duration)
			}
		case sortCheckpointSubtaskState:
			if left.StateSize != right.StateSize {
				return cmp.Compare(right.StateSize, left.StateSize)
			}
		case sortCheckpointSubtaskAlignment:
			if left.AlignmentDuration != right.AlignmentDuration {
				return cmp.Compare(right.AlignmentDuration, left.AlignmentDuration)
			}
			if left.AlignmentProcessed != right.AlignmentProcessed {
				return cmp.Compare(right.AlignmentProcessed, left.AlignmentProcessed)
			}
		case sortCheckpointSubtaskStartDelay:
			if left.StartDelay != right.StartDelay {
				return cmp.Compare(right.StartDelay, left.StartDelay)
			}
		default:
			if checkpointSubtaskRank(left) != checkpointSubtaskRank(right) {
				return cmp.Compare(checkpointSubtaskRank(right), checkpointSubtaskRank(left))
			}
			if left.Duration != right.Duration {
				return cmp.Compare(right.Duration, left.Duration)
			}
		}
		return cmp.Compare(left.Index, right.Index)
	})
	return subtasks
}

func (m *Model) syncCheckpointSubtaskSelection() {
	subtasks := m.sortedCheckpointSubtasks()
	if len(subtasks) == 0 {
		m.checkpointSubtaskCursor = 0
		m.checkpointSubtaskSelected = -1
		return
	}
	if m.checkpointSubtaskSelected >= 0 {
		for index, subtask := range subtasks {
			if subtask.Index == m.checkpointSubtaskSelected {
				m.checkpointSubtaskCursor = index
				return
			}
		}
	}
	m.checkpointSubtaskCursor = shared.Clamp(m.checkpointSubtaskCursor, 0, len(subtasks)-1)
	m.checkpointSubtaskSelected = subtasks[m.checkpointSubtaskCursor].Index
}

func (m *Model) moveCheckpointSubtaskSelection(delta int) {
	if delta == 0 {
		return
	}
	m.syncCheckpointSubtaskSelection()
	m.moveCheckpointSubtaskTo(m.checkpointSubtaskCursor + delta)
}

func (m *Model) moveCheckpointSubtaskTo(position int) {
	subtasks := m.sortedCheckpointSubtasks()
	if len(subtasks) == 0 {
		return
	}
	m.checkpointSubtaskCursor = shared.Clamp(position, 0, len(subtasks)-1)
	m.checkpointSubtaskSelected = subtasks[m.checkpointSubtaskCursor].Index
}

func (m Model) selectedCheckpointSubtask() (flink.CheckpointSubtask, bool) {
	subtasks := m.sortedCheckpointSubtasks()
	if len(subtasks) == 0 {
		return flink.CheckpointSubtask{}, false
	}
	index := shared.Clamp(m.checkpointSubtaskCursor, 0, len(subtasks)-1)
	return subtasks[index], true
}

func (m Model) checkpointDiagnosticRowsAvailable() int {
	return max(1, m.bodyHeight()-checkpointDiagnosticBodyRowStart-checkpointDiagnosticDetailRows)
}

func checkpointDiagnosticWindowStart(cursor, count, available int) int {
	return shared.WindowStart(cursor, available, count)
}

func (m *Model) handleCheckpointOperatorMouseClick(event tea.Mouse) {
	if event.Button != tea.MouseLeft {
		return
	}
	position := event.Y - headerHeight - checkpointDiagnosticBodyRowStart
	if position < 0 || position >= m.checkpointDiagnosticRowsAvailable() {
		return
	}
	operators := m.sortedCheckpointOperators()
	start := checkpointDiagnosticWindowStart(m.checkpointOperatorCursor, len(operators), m.checkpointDiagnosticRowsAvailable())
	index := start + position
	if index >= 0 && index < len(operators) {
		m.checkpointOperatorCursor = index
		m.checkpointOperatorSelected = operators[index].VertexID
	}
}

func (m *Model) handleCheckpointSubtaskMouseClick(event tea.Mouse) {
	if event.Button != tea.MouseLeft {
		return
	}
	position := event.Y - headerHeight - checkpointDiagnosticBodyRowStart
	if position < 0 || position >= m.checkpointDiagnosticRowsAvailable() {
		return
	}
	subtasks := m.sortedCheckpointSubtasks()
	start := checkpointDiagnosticWindowStart(m.checkpointSubtaskCursor, len(subtasks), m.checkpointDiagnosticRowsAvailable())
	index := start + position
	if index >= 0 && index < len(subtasks) {
		m.checkpointSubtaskCursor = index
		m.checkpointSubtaskSelected = subtasks[index].Index
	}
}

func (m *Model) jumpCheckpointOperatorToGraph() {
	operator, ok := m.selectedCheckpointOperator()
	if ok {
		m.jumpCheckpointVertexToGraph(operator.VertexID)
	}
}

func (m *Model) jumpCheckpointVertexToGraph(vertexID string) {
	if _, ok := m.layout.Rects[vertexID]; !ok {
		return
	}
	m.pendingIntent = IntentGraph
	m.pendingVertex = vertexID
}

func (m Model) renderCheckpointOperators(width, height int) string {
	checkpoint := m.checkpointDetail.Checkpoint
	status := checkpoint.Status
	if status == "" {
		if selected, ok := m.selectedCheckpoint(); ok {
			status = selected.Status
		}
	}
	title := fmt.Sprintf(" CHECKPOINT #%d  /  OPERATORS  |  %s  |  sort %s",
		m.checkpointDetailID, status, m.checkpointOperatorSortLabel())
	if m.checkpointDetailBusy {
		title += "  |  refreshing..."
	}
	lines := []string{
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#E2E8F0")).Render(shared.Truncate(title, width)),
		m.renderCheckpointDiagnosticStatus(width),
		m.renderCheckpointOperatorColumns(width),
	}
	operators := m.sortedCheckpointOperators()
	if len(operators) == 0 && !m.checkpointDetailBusy && m.checkpointDetailErr == nil {
		message := " No per-operator checkpoint details reported."
		if m.checkpointOperatorFilter.Value() != "" {
			message = " No checkpoint operators match filter /" + m.checkpointOperatorFilter.Value() + "/. Press / to replace it or Ctrl+W while editing to clear."
		}
		lines = append(lines, shared.Truncate(message, width))
	}
	start := checkpointDiagnosticWindowStart(m.checkpointOperatorCursor, len(operators), m.checkpointDiagnosticRowsAvailable())
	end := min(len(operators), start+m.checkpointDiagnosticRowsAvailable())
	for index := start; index < end; index++ {
		lines = append(lines, m.renderCheckpointOperatorRow(operators[index], index == m.checkpointOperatorCursor, width))
	}
	for len(lines) < height-checkpointDiagnosticDetailRows {
		lines = append(lines, "")
	}
	separator := lipgloss.NewStyle().Foreground(shared.C("#334155")).Render(strings.Repeat("-", width))
	lines = append(lines, separator)
	if m.checkpointConfigOpen {
		lines = append(lines, m.renderCheckpointConfig(width)...)
	} else {
		lines = append(lines, m.renderSelectedCheckpointOperator(width)...)
	}
	return shared.FitLines(lines, width, height)
}

func (m Model) renderCheckpointSubtasks(width, height int) string {
	name := m.nodeName(m.checkpointSubtaskVertex)
	title := fmt.Sprintf(" CHECKPOINT #%d  /  %s  /  SUBTASKS  |  sort %s",
		m.checkpointDetailID, name, m.checkpointSubtaskSortLabel())
	if m.checkpointSubtasksBusy {
		title += "  |  refreshing..."
	}
	lines := []string{
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#E2E8F0")).Render(shared.Truncate(title, width)),
		m.renderCheckpointSubtaskStatus(width),
		m.renderCheckpointSubtaskColumns(width),
	}
	subtasks := m.sortedCheckpointSubtasks()
	if len(subtasks) == 0 && !m.checkpointSubtasksBusy && m.checkpointSubtasksErr == nil {
		lines = append(lines, " No per-subtask checkpoint details reported.")
	}
	start := checkpointDiagnosticWindowStart(m.checkpointSubtaskCursor, len(subtasks), m.checkpointDiagnosticRowsAvailable())
	end := min(len(subtasks), start+m.checkpointDiagnosticRowsAvailable())
	for index := start; index < end; index++ {
		lines = append(lines, m.renderCheckpointSubtaskRow(subtasks[index], index == m.checkpointSubtaskCursor, width))
	}
	for len(lines) < height-checkpointDiagnosticDetailRows {
		lines = append(lines, "")
	}
	separator := lipgloss.NewStyle().Foreground(shared.C("#334155")).Render(strings.Repeat("-", width))
	lines = append(lines, separator)
	lines = append(lines, m.renderSelectedCheckpointSubtask(width)...)
	return shared.FitLines(lines, width, height)
}

func (m Model) renderCheckpointDiagnosticStatus(width int) string {
	config := m.checkpointConfig
	status := fmt.Sprintf(" %s  |  every %s  |  timeout %s  |  %s -> %s  |  %s",
		strings.ToUpper(strings.ReplaceAll(config.Mode, "_", " ")),
		humanConfigDuration(config.Interval),
		humanConfigDuration(config.Timeout),
		shared.Fallback(config.StateBackend, "backend -"),
		shared.Fallback(config.CheckpointStorage, "storage -"),
		onOffLabel(config.UnalignedCheckpoints, "unaligned", "aligned"),
	)
	style := lipgloss.NewStyle().Foreground(shared.C("#94A3B8"))
	if m.checkpointOperatorFilter.Active() {
		status = " /" + m.checkpointOperatorFilter.Value() + "|  filter operator name"
		style = style.Foreground(shared.C("#E2E8F0")).Background(shared.C("#111827"))
	} else if m.checkpointOperatorFilter.Value() != "" {
		status += "  |  filter /" + m.checkpointOperatorFilter.Value() + "/"
	}
	if m.checkpointDetailErr != nil {
		status = " Detail refresh failed: " + shared.ErrorText(m.checkpointDetailErr)
		style = style.Foreground(shared.C("#FB7185"))
	} else if m.checkpointDetail.Checkpoint.Failure != "" {
		status = " Failure: " + m.checkpointDetail.Checkpoint.Failure
		style = style.Foreground(shared.C("#FB7185")).Bold(true)
	} else if m.checkpointConfigErr != nil {
		status += "  |  config unavailable: " + shared.ErrorText(m.checkpointConfigErr)
	}
	return style.Render(shared.Truncate(status, width))
}

func (m Model) renderCheckpointSubtaskStatus(width int) string {
	operator := m.checkpointSubtasks.Operator
	status := fmt.Sprintf(" %s  |  acknowledged %d/%d  |  operator duration %s  |  state %s",
		operator.Status,
		operator.AcknowledgedSubtasks,
		operator.Subtasks,
		humanCheckpointDuration(operator.Duration),
		shared.HumanBytes(operator.StateSize),
	)
	style := lipgloss.NewStyle().Foreground(shared.C("#94A3B8"))
	if m.checkpointSubtasksErr != nil {
		status = " Subtask refresh failed: " + shared.ErrorText(m.checkpointSubtasksErr)
		style = style.Foreground(shared.C("#FB7185"))
	}
	return style.Render(shared.Truncate(status, width))
}

func (m Model) renderCheckpointOperatorColumns(width int) string {
	nameWidth := max(12, width-77)
	var line string
	switch {
	case width >= 110:
		line = "    " + shared.PadRight("OPERATOR", nameWidth) + " ACKS       LATEST    E2E       STATE          CP        DATA"
	case width >= 78:
		nameWidth = max(12, width-46)
		line = "    " + shared.PadRight("OPERATOR", nameWidth) + " ACKS       E2E       STATE"
	default:
		nameWidth = max(10, width-27)
		line = "    " + shared.PadRight("OPERATOR", nameWidth) + " ACKS    E2E"
	}
	return checkpointColumns(line, width)
}

func (m Model) renderCheckpointOperatorRow(operator flink.CheckpointOperator, selected bool, width int) string {
	marker := " "
	if operator.AcknowledgedSubtasks < operator.Subtasks || checkpointStatusRank(operator.Status) > 0 {
		marker = "!"
	} else if m.isCheckpointOperatorDurationOutlier(operator) {
		marker = "!"
	}
	cursor := " "
	if selected {
		cursor = ">"
	}
	name := m.nodeName(operator.VertexID)
	nameWidth := max(12, width-77)
	var line string
	switch {
	case width >= 110:
		line = fmt.Sprintf(" %s%s %s %4d/%-4d  %s  %8s  %10s  %10s  %10s",
			cursor, marker, shared.PadRight(shared.Truncate(name, nameWidth), nameWidth),
			operator.AcknowledgedSubtasks, operator.Subtasks,
			shared.Clock(operator.LatestAcknowledgedAt, "--:--:--"), humanCheckpointDuration(operator.Duration),
			shared.HumanBytes(operator.StateSize), shared.HumanBytes(operator.CheckpointedSize), shared.HumanBytes(operator.ProcessedData),
		)
	case width >= 78:
		nameWidth = max(12, width-46)
		line = fmt.Sprintf(" %s%s %s %4d/%-4d  %8s  %10s",
			cursor, marker, shared.PadRight(shared.Truncate(name, nameWidth), nameWidth),
			operator.AcknowledgedSubtasks, operator.Subtasks, humanCheckpointDuration(operator.Duration), shared.HumanBytes(operator.StateSize),
		)
	default:
		nameWidth = max(10, width-27)
		line = fmt.Sprintf(" %s%s %s %2d/%-2d %7s",
			cursor, marker, shared.PadRight(shared.Truncate(name, nameWidth), nameWidth),
			operator.AcknowledgedSubtasks, operator.Subtasks, humanCheckpointDuration(operator.Duration),
		)
	}
	return checkpointDiagnosticRow(line, width, selected, checkpointOperatorColor(operator, m.isCheckpointOperatorDurationOutlier(operator)))
}

func (m Model) renderCheckpointSubtaskColumns(width int) string {
	var line string
	switch {
	case width >= 112:
		line = "    #   STATUS       ACKED       E2E       STATE         CP      SYNC     ASYNC     ALIGN     START   UA"
	case width >= 80:
		line = "    #   STATUS       E2E       STATE      ALIGN     START   UA"
	default:
		line = "    #   STATUS       E2E       STATE"
	}
	return checkpointColumns(line, width)
}

func (m Model) renderCheckpointSubtaskRow(subtask flink.CheckpointSubtask, selected bool, width int) string {
	marker := " "
	if checkpointSubtaskRank(subtask) > 0 {
		marker = "!"
	} else if m.isCheckpointSubtaskDurationOutlier(subtask) {
		marker = "!"
	}
	cursor := " "
	if selected {
		cursor = ">"
	}
	status := strings.ToUpper(subtask.Status)
	var line string
	switch {
	case width >= 112:
		unaligned := "no"
		if subtask.Unaligned {
			unaligned = "yes"
		}
		line = fmt.Sprintf(" %s%s %3d  %-11s %s  %8s  %10s %8s  %7s  %7s  %7s  %7s  %3s",
			cursor, marker, subtask.Index, shared.Truncate(status, 11), shared.Clock(subtask.AcknowledgedAt, "--:--:--"),
			humanCheckpointDuration(subtask.Duration), shared.HumanBytes(subtask.StateSize), shared.HumanBytes(subtask.CheckpointedSize),
			humanCheckpointDuration(subtask.SyncDuration), humanCheckpointDuration(subtask.AsyncDuration),
			humanCheckpointDuration(subtask.AlignmentDuration), humanCheckpointDuration(subtask.StartDelay), unaligned,
		)
	case width >= 80:
		unaligned := "no"
		if subtask.Unaligned {
			unaligned = "yes"
		}
		line = fmt.Sprintf(" %s%s %3d  %-11s %8s  %10s  %8s  %8s  %3s",
			cursor, marker, subtask.Index, shared.Truncate(status, 11), humanCheckpointDuration(subtask.Duration),
			shared.HumanBytes(subtask.StateSize), humanCheckpointDuration(subtask.AlignmentDuration), humanCheckpointDuration(subtask.StartDelay), unaligned,
		)
	default:
		line = fmt.Sprintf(" %s%s %3d  %-11s %8s  %9s",
			cursor, marker, subtask.Index, shared.Truncate(status, 11), humanCheckpointDuration(subtask.Duration), shared.HumanBytes(subtask.StateSize),
		)
	}
	return checkpointDiagnosticRow(line, width, selected, checkpointSubtaskColor(subtask, m.isCheckpointSubtaskDurationOutlier(subtask)))
}

func (m Model) renderSelectedCheckpointOperator(width int) []string {
	operator, ok := m.selectedCheckpointOperator()
	if !ok {
		return []string{" Select an operator for checkpoint diagnostics.", "", ""}
	}
	flags := make([]string, 0, 2)
	if operator.AcknowledgedSubtasks < operator.Subtasks {
		flags = append(flags, fmt.Sprintf("MISSING %d ACK", operator.Subtasks-operator.AcknowledgedSubtasks))
	}
	if m.isCheckpointOperatorDurationOutlier(operator) {
		flags = append(flags, "DURATION OUTLIER")
	}
	suffix := ""
	if len(flags) > 0 {
		suffix = "  |  " + strings.Join(flags, "  |  ")
	}
	lineOne := fmt.Sprintf(" %s  %s%s", m.nodeName(operator.VertexID), operator.Status, suffix)
	lineTwo := fmt.Sprintf(" acknowledged %d/%d at %s  |  duration %s  |  state %s  |  checkpointed %s",
		operator.AcknowledgedSubtasks, operator.Subtasks, formatCheckpointTime(operator.LatestAcknowledgedAt),
		humanCheckpointDuration(operator.Duration), shared.HumanBytes(operator.StateSize), shared.HumanBytes(operator.CheckpointedSize),
	)
	lineThree := fmt.Sprintf(" data processed %s  |  persisted %s  |  alignment buffered %s  |  enter subtasks  |  g graph",
		shared.HumanBytes(operator.ProcessedData), shared.HumanBytes(operator.PersistedData), shared.HumanBytes(operator.AlignmentBuffered),
	)
	return []string{shared.Truncate(lineOne, width), shared.Truncate(lineTwo, width), shared.Truncate(lineThree, width)}
}

func (m Model) renderCheckpointConfig(width int) []string {
	config := m.checkpointConfig
	externalization := "disabled"
	if config.ExternalizationEnabled {
		externalization = "enabled, retain on cancellation"
		if config.DeleteExternalizedOnCancellation {
			externalization = "enabled, delete on cancellation"
		}
	}
	lineOne := fmt.Sprintf(" config  mode %s  |  interval %s  |  timeout %s  |  minimum pause %s  |  max concurrent %d",
		strings.ToUpper(config.Mode), humanConfigDuration(config.Interval), humanConfigDuration(config.Timeout),
		humanConfigDuration(config.MinimumPause), config.MaximumConcurrent,
	)
	lineTwo := fmt.Sprintf(" storage  backend %s  |  checkpoints %s  |  externalization %s",
		shared.Fallback(config.StateBackend, "-"), shared.Fallback(config.CheckpointStorage, "-"), externalization,
	)
	lineThree := fmt.Sprintf(" advanced  unaligned %s  |  aligned timeout %s  |  tolerated failures %d  |  after finished tasks %s  |  changelog %s (%s, %s)",
		onOff(config.UnalignedCheckpoints), humanConfigDuration(config.AlignedCheckpointTimeout), config.TolerableFailedCheckpoints,
		onOff(config.CheckpointsAfterTasksFinish), onOff(config.StateChangelogEnabled),
		shared.Fallback(config.ChangelogStorage, "-"), humanConfigDuration(config.ChangelogMaterializationInterval),
	)
	if m.checkpointConfigErr != nil {
		lineOne = " config unavailable: " + shared.ErrorText(m.checkpointConfigErr)
		lineTwo, lineThree = "", ""
	}
	return []string{shared.Truncate(lineOne, width), shared.Truncate(lineTwo, width), shared.Truncate(lineThree, width)}
}

func (m Model) renderSelectedCheckpointSubtask(width int) []string {
	subtask, ok := m.selectedCheckpointSubtask()
	if !ok {
		return []string{" Select a subtask for checkpoint phase diagnostics.", "", ""}
	}
	flags := make([]string, 0, 3)
	if m.isCheckpointSubtaskDurationOutlier(subtask) {
		flags = append(flags, "DURATION OUTLIER")
	}
	if subtask.Unaligned {
		flags = append(flags, "UNALIGNED")
	}
	if subtask.Aborted {
		flags = append(flags, "ABORTED")
	}
	suffix := ""
	if len(flags) > 0 {
		suffix = "  |  " + strings.Join(flags, "  |  ")
	}
	lineOne := fmt.Sprintf(" subtask #%d  %s  |  acknowledged %s  |  duration %s%s",
		subtask.Index, strings.ToUpper(subtask.Status), formatCheckpointTime(subtask.AcknowledgedAt), humanCheckpointDuration(subtask.Duration), suffix)
	lineTwo := fmt.Sprintf(" phases  sync %s  |  async %s  |  alignment %s  |  start delay %s  |  alignment data %s processed / %s persisted / %s buffered",
		humanCheckpointDuration(subtask.SyncDuration), humanCheckpointDuration(subtask.AsyncDuration), humanCheckpointDuration(subtask.AlignmentDuration),
		humanCheckpointDuration(subtask.StartDelay), shared.HumanBytes(subtask.AlignmentProcessed), shared.HumanBytes(subtask.AlignmentPersisted), shared.HumanBytes(subtask.AlignmentBuffered))
	summary := m.checkpointSubtasks.Summary
	lineThree := fmt.Sprintf(" min/avg/max  e2e %s  |  state %s  |  start %s",
		durationDistribution(summary.EndToEndDuration), byteDistribution(summary.StateSize), durationDistribution(summary.StartDelay))
	return []string{shared.Truncate(lineOne, width), shared.Truncate(lineTwo, width), shared.Truncate(lineThree, width)}
}

func (m Model) isCheckpointOperatorDurationOutlier(operator flink.CheckpointOperator) bool {
	durations := make([]time.Duration, 0, len(m.checkpointDetail.Operators))
	for _, candidate := range m.checkpointDetail.Operators {
		durations = append(durations, candidate.Duration)
	}
	return checkpointDurationOutlier(operator.Duration, durations)
}

func (m Model) isCheckpointSubtaskDurationOutlier(subtask flink.CheckpointSubtask) bool {
	durations := make([]time.Duration, 0, len(m.checkpointSubtasks.Subtasks))
	for _, candidate := range m.checkpointSubtasks.Subtasks {
		durations = append(durations, candidate.Duration)
	}
	return checkpointDurationOutlier(subtask.Duration, durations)
}

func checkpointDurationOutlier(candidate time.Duration, durations []time.Duration) bool {
	if candidate <= 0 {
		return false
	}
	baseline := make([]time.Duration, 0, len(durations)-1)
	maximumMatches := 0
	for _, duration := range durations {
		if duration > candidate {
			return false
		}
		if duration == candidate {
			maximumMatches++
			continue
		}
		if duration > 0 {
			baseline = append(baseline, duration)
		}
	}
	if maximumMatches != 1 || len(baseline) == 0 {
		return false
	}
	slices.Sort(baseline)
	median := baseline[len(baseline)/2]
	if len(baseline)%2 == 0 {
		median = (baseline[len(baseline)/2-1] + median) / 2
	}
	return candidate >= 2*median && candidate-median >= 100*time.Millisecond
}

func (m Model) checkpointOperatorSortLabel() string {
	switch m.checkpointOperatorSort {
	case sortCheckpointOperatorDuration:
		return "DURATION"
	case sortCheckpointOperatorState:
		return "STATE SIZE"
	case sortCheckpointOperatorAcknowledgement:
		return "ACKNOWLEDGEMENT"
	case sortCheckpointOperatorProcessed:
		return "PROCESSED DATA"
	default:
		return "DIAGNOSIS"
	}
}

func (m Model) checkpointSubtaskSortLabel() string {
	switch m.checkpointSubtaskSort {
	case sortCheckpointSubtaskDuration:
		return "DURATION"
	case sortCheckpointSubtaskState:
		return "STATE SIZE"
	case sortCheckpointSubtaskAlignment:
		return "ALIGNMENT"
	case sortCheckpointSubtaskStartDelay:
		return "START DELAY"
	default:
		return "DIAGNOSIS"
	}
}

func checkpointStatusRank(status string) int {
	switch strings.ToUpper(status) {
	case "FAILED", "CANCELED", "CANCELLED":
		return 3
	case "IN_PROGRESS", "PENDING":
		return 2
	case "COMPLETED":
		return 0
	default:
		return 1
	}
}

func checkpointSubtaskRank(subtask flink.CheckpointSubtask) int {
	if subtask.Aborted {
		return 4
	}
	switch strings.ToLower(subtask.Status) {
	case "pending_or_failed", "failed":
		return 3
	case "pending", "in_progress":
		return 2
	case "completed":
		return 0
	default:
		return 1
	}
}

func checkpointOperatorColor(operator flink.CheckpointOperator, outlier bool) color.Color {
	if operator.AcknowledgedSubtasks < operator.Subtasks || checkpointStatusRank(operator.Status) > 1 {
		return shared.C("#FB7185")
	}
	if outlier {
		return shared.C("#FBBF24")
	}
	return shared.C("#34D399")
}

func checkpointSubtaskColor(subtask flink.CheckpointSubtask, outlier bool) color.Color {
	if checkpointSubtaskRank(subtask) > 1 {
		return shared.C("#FB7185")
	}
	if outlier {
		return shared.C("#FBBF24")
	}
	return shared.C("#34D399")
}

func checkpointColumns(line string, width int) string {
	return lipgloss.NewStyle().Bold(true).Foreground(shared.C("#94A3B8")).Background(shared.C("#111827")).Render(
		shared.PadRight(shared.Truncate(line, width), width),
	)
}

func checkpointDiagnosticRow(line string, width int, selected bool, foreground color.Color) string {
	line = shared.PadRight(shared.Truncate(line, width), width)
	if selected {
		return lipgloss.NewStyle().Foreground(shared.C("#FFFFFF")).Background(shared.C("#6D28D9")).Bold(true).Render(line)
	}
	return lipgloss.NewStyle().Foreground(foreground).Render(line)
}

func durationDistribution(distribution flink.CheckpointDistribution) string {
	return fmt.Sprintf("%s/%s/%s",
		humanCheckpointDuration(distributionDuration(distribution.Min)),
		humanCheckpointDuration(distributionDuration(distribution.Average)),
		humanCheckpointDuration(distributionDuration(distribution.Max)),
	)
}

func byteDistribution(distribution flink.CheckpointDistribution) string {
	return fmt.Sprintf("%s/%s/%s",
		shared.HumanBytes(int64(distribution.Min)),
		shared.HumanBytes(int64(distribution.Average)),
		shared.HumanBytes(int64(distribution.Max)),
	)
}

func distributionDuration(milliseconds float64) time.Duration {
	return time.Duration(milliseconds * float64(time.Millisecond))
}

func humanConfigDuration(value time.Duration) string {
	if value == 0 {
		return "0ms"
	}
	return humanLatency(value)
}

func humanCheckpointDuration(value time.Duration) string {
	if value < 0 {
		return "-"
	}
	if value == 0 {
		return "0ms"
	}
	return humanLatency(value)
}

func onOff(value bool) string {
	if value {
		return "on"
	}
	return "off"
}

func onOffLabel(value bool, enabled, disabled string) string {
	if value {
		return enabled
	}
	return disabled
}
