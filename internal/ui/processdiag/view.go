package processdiag

import (
	"fmt"
	"slices"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
	shellmodule "github.com/nikitasavinov/flink-tui/internal/ui/shell"
)

const (
	processLogBodyRowStart = 3
	threadBodyRowStart     = 3
)

type documentSource uint8

const (
	documentStatic documentSource = iota
	documentLog
	documentCurrentLog
	documentStdout
)

type documentState struct {
	title      string
	lines      []string
	offsetY    int
	offsetX    int
	backMode   screenMode
	source     documentSource
	process    flink.ProcessRef
	filename   string
	loading    bool
	err        error
	filter     shellmodule.QueryInput
	tailOnLoad bool
}

// threadFocusState carries the execution-thread prefix supplied by a subtask
// diagnostic jump. Flink task thread names begin with the vertex name and the
// one-based subtask number, for example "Map (2/4)#0". The prefix deliberately
// stops after the slash, matching Flink Web UI and remaining valid if the
// parallelism changes between loading the subtask table and taking the dump.
func (m *Model) openProcessLogs(process flink.ProcessRef, backMode screenMode) tea.Cmd {
	m.observabilityGeneration++
	m.mode = modeProcessLogs
	m.process = process
	m.processLogBackMode = backMode
	m.processLogs = nil
	m.processLogSelection.Set(0, 0)
	m.processBusy = true
	m.processErr = nil
	return m.fetchProcessLogs()
}

func (m *Model) fetchProcessLogs() tea.Cmd {
	client := m.client
	process := m.process
	generation := m.observabilityGeneration
	m.logsGeneration = generation
	requestTimeout := m.requestTimeout
	return func() tea.Msg {
		ctx, cancel := requestTimeout(defaultRequestTimeout)
		defer cancel()
		logs, err := client.LogFiles(ctx, process)
		return processLogsMsg{logs: logs, err: err, process: process, generation: generation}
	}
}

func (m *Model) openSelectedProcessLog() tea.Cmd {
	if m.processLogSelection.Index() < 0 || m.processLogSelection.Index() >= len(m.processLogs) {
		return nil
	}
	return m.openLogDocument(m.process, m.processLogs[m.processLogSelection.Index()], modeProcessLogs)
}

func (m *Model) openLogDocument(process flink.ProcessRef, log flink.LogFile, backMode screenMode) tea.Cmd {
	m.observabilityGeneration++
	m.mode = modeDocument
	m.process = process
	m.document = documentState{
		title:    "LOG  " + process.Label() + " / " + log.Name,
		backMode: backMode,
		source:   documentLog,
		process:  process,
		filename: log.Name,
		loading:  true,
	}
	return m.fetchDocument()
}

func (m *Model) openCurrentLog(process flink.ProcessRef, backMode screenMode) tea.Cmd {
	m.observabilityGeneration++
	m.mode = modeDocument
	m.process = process
	m.document = documentState{
		title:      "LOG  " + process.Label() + " / current",
		backMode:   backMode,
		source:     documentCurrentLog,
		process:    process,
		loading:    true,
		tailOnLoad: true,
	}
	return m.fetchDocument()
}

func (m *Model) openStdout(process flink.ProcessRef, backMode screenMode) tea.Cmd {
	m.observabilityGeneration++
	m.mode = modeDocument
	m.process = process
	m.document = documentState{
		title:    "STDOUT  " + process.Label(),
		backMode: backMode,
		source:   documentStdout,
		process:  process,
		loading:  true,
	}
	return m.fetchDocument()
}

func (m *Model) openStaticDocument(title, content string, backMode screenMode) {
	m.observabilityGeneration++
	m.documentGeneration = m.observabilityGeneration
	m.mode = modeDocument
	m.document = documentState{
		title:    title,
		lines:    documentLines(content),
		backMode: backMode,
		source:   documentStatic,
	}
}

func (m *Model) fetchDocument() tea.Cmd {
	client := m.client
	document := m.document
	generation := m.observabilityGeneration
	m.documentGeneration = generation
	requestTimeout := m.requestTimeout
	return func() tea.Msg {
		ctx, cancel := requestTimeout(longRequestTimeout)
		defer cancel()
		var content string
		var err error
		switch document.source {
		case documentStdout:
			content, err = client.Stdout(ctx, document.process)
		case documentCurrentLog:
			content, err = client.CurrentLog(ctx, document.process)
		default:
			content, err = client.LogContent(ctx, document.process, document.filename)
		}
		return documentMsg{title: document.title, content: content, err: err, generation: generation}
	}
}

func documentLines(content string) []string {
	content = strings.ReplaceAll(content, "\r\n", "\n")
	content = strings.ReplaceAll(content, "\r", "\n")
	lines := strings.Split(content, "\n")
	if len(lines) == 0 {
		return []string{""}
	}
	for index, line := range lines {
		line = ansi.Strip(line)
		line = strings.ReplaceAll(line, "\t", "    ")
		lines[index] = sanitizeLine(line)
	}
	return lines
}

func (m *Model) handleProcessLogKey(key string) tea.Cmd {
	switch key {
	case "esc", "backspace":
		m.mode = m.processLogBackMode
	case "up", "k":
		m.moveProcessLogSelection(-1)
	case "down", "j":
		m.moveProcessLogSelection(1)
	case "pgup":
		m.moveProcessLogSelection(-max(1, m.processLogRowsAvailable()))
	case "pgdown":
		m.moveProcessLogSelection(max(1, m.processLogRowsAvailable()))
	case "home":
		m.processLogSelection.Set(0, 0)
	case "end":
		m.processLogSelection.Set(len(m.processLogs)-1, len(m.processLogs))
	case "enter":
		return m.openSelectedProcessLog()
	case "x":
		return m.openStdout(m.process, modeProcessLogs)
	case "d":
		return m.openThreadDump(m.process, modeProcessLogs)
	case "p":
		return m.openProfiler(m.process, modeProcessLogs)
	}
	return nil
}

func (m *Model) moveProcessLogSelection(delta int) {
	m.processLogSelection.Move(delta, len(m.processLogs))
}

func (m Model) processLogRowsAvailable() int {
	return max(1, m.bodyHeight()-processLogBodyRowStart-3)
}

func (m Model) processLogWindowStart() int {
	available := m.processLogRowsAvailable()
	return shared.WindowStart(m.processLogSelection.Index(), available, len(m.processLogs))
}

func (m *Model) handleProcessLogMouseClick(event tea.Mouse) tea.Cmd {
	if event.Button != tea.MouseLeft {
		return nil
	}
	position := event.Y - headerHeight - processLogBodyRowStart
	if position < 0 || position >= m.processLogRowsAvailable() {
		return nil
	}
	index := m.processLogWindowStart() + position
	if index >= 0 && index < len(m.processLogs) {
		m.processLogSelection.Set(index, len(m.processLogs))
		return m.openSelectedProcessLog()
	}
	return nil
}

func (m Model) renderProcessLogs(width, height int) string {
	title := fmt.Sprintf(" LOGS  %s  |  %d files", m.process.Label(), len(m.processLogs))
	if m.processBusy {
		title += "  |  loading..."
	}
	status := " enter opens full content  |  x stdout  |  d thread dump"
	statusStyle := lipgloss.NewStyle().Foreground(shared.C("#94A3B8"))
	if m.processErr != nil {
		status = " Could not load logs: " + shared.ErrorText(m.processErr)
		statusStyle = statusStyle.Foreground(shared.C("#FB7185"))
	}
	lines := []string{
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#E2E8F0")).Render(shared.Truncate(title, width)),
		statusStyle.Render(shared.Truncate(status, width)),
		renderProcessLogColumns(width),
	}
	start := m.processLogWindowStart()
	end := min(len(m.processLogs), start+m.processLogRowsAvailable())
	for index := start; index < end; index++ {
		lines = append(lines, renderProcessLogRow(m.processLogs[index], index == m.processLogSelection.Index(), width))
	}
	if len(m.processLogs) == 0 && !m.processBusy && m.processErr == nil {
		lines = append(lines, " No log files returned by Flink.")
	}
	for len(lines) < height-3 {
		lines = append(lines, "")
	}
	separator := lipgloss.NewStyle().Foreground(shared.C("#334155")).Render(strings.Repeat("-", width))
	detailOne, detailTwo := m.renderProcessLogDetail(width)
	lines = append(lines, separator, detailOne, detailTwo)
	return shared.FitLines(lines, width, height)
}

func renderProcessLogColumns(width int) string {
	line := "   LOG FILE                                             SIZE       MODIFIED"
	return lipgloss.NewStyle().Bold(true).Foreground(shared.C("#94A3B8")).Background(shared.C("#111827")).Render(
		shared.PadRight(shared.Truncate(line, width), width))
}

func renderProcessLogRow(log flink.LogFile, selected bool, width int) string {
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

func (m Model) renderProcessLogDetail(width int) (string, string) {
	if m.processLogSelection.Index() < 0 || m.processLogSelection.Index() >= len(m.processLogs) {
		return " No log selected.", " enter opens content  |  x stdout  |  d thread dump"
	}
	log := m.processLogs[m.processLogSelection.Index()]
	modified := "unknown"
	if !log.ModifiedAt.IsZero() {
		modified = log.ModifiedAt.Format(time.RFC3339)
	}
	return shared.Truncate(" "+log.Name, width), shared.Truncate(fmt.Sprintf(" %s  |  modified %s  |  enter opens content", shared.HumanBytes(log.Size), modified), width)
}

func (m *Model) handleDocumentKey(key string) tea.Cmd {
	if m.document.filter.Active() {
		return m.handleDocumentSearchKey(key)
	}
	page := max(1, m.documentRowsAvailable()-1)
	switch key {
	case "esc", "backspace":
		m.mode = m.document.backMode
	case "up", "k":
		m.moveDocumentVertical(-1)
	case "down", "j":
		m.moveDocumentVertical(1)
	case "pgup", "ctrl+u":
		m.moveDocumentVertical(-page)
	case "pgdown", "ctrl+d":
		m.moveDocumentVertical(page)
	case "home":
		m.document.offsetY = 0
	case "end":
		m.document.offsetY = max(0, len(m.document.lines)-m.documentRowsAvailable())
	case "h":
		m.document.offsetX = max(0, m.document.offsetX-4)
	case "l":
		m.document.offsetX += 4
	case "0":
		m.document.offsetX = 0
	case "/":
		m.document.filter.Restore(shellmodule.QueryState{Value: m.document.filter.Value(), Open: true})
	case "n":
		m.findDocumentMatch(1)
	case "N":
		m.findDocumentMatch(-1)
	}
	return nil
}

func (m *Model) handleDocumentSearchKey(key string) tea.Cmd {
	if key != "ctrl+w" && m.document.filter.HandleKey(key) == shellmodule.QueryConfirmed {
		m.findDocumentMatch(1)
	}
	return nil
}

func (m *Model) moveDocumentVertical(delta int) {
	maximum := max(0, len(m.document.lines)-m.documentRowsAvailable())
	m.document.offsetY = shared.Clamp(m.document.offsetY+delta, 0, maximum)
}

func (m *Model) findDocumentMatch(direction int) {
	query := strings.ToLower(strings.TrimSpace(m.document.filter.Value()))
	if query == "" || len(m.document.lines) == 0 {
		return
	}
	start := m.document.offsetY
	for step := 1; step <= len(m.document.lines); step++ {
		index := (start + direction*step) % len(m.document.lines)
		if index < 0 {
			index += len(m.document.lines)
		}
		if strings.Contains(strings.ToLower(m.document.lines[index]), query) {
			m.document.offsetY = shared.Clamp(index, 0, max(0, len(m.document.lines)-m.documentRowsAvailable()))
			return
		}
	}
}

func (m Model) documentRowsAvailable() int {
	return max(1, m.bodyHeight()-3)
}

func (m Model) renderDocument(width, height int) string {
	title := m.document.title
	if m.document.loading {
		title += "  |  loading..."
	}
	status := fmt.Sprintf(" lines %d  |  row %d  |  column %d", len(m.document.lines), m.document.offsetY+1, m.document.offsetX+1)
	statusStyle := lipgloss.NewStyle().Foreground(shared.C("#94A3B8"))
	if m.document.filter.Active() {
		status = " /" + m.document.filter.Value() + "|"
		statusStyle = statusStyle.Foreground(shared.C("#E2E8F0")).Background(shared.C("#111827"))
	} else if m.document.filter.Value() != "" {
		status += "  |  search /" + m.document.filter.Value() + "/"
	}
	if m.document.err != nil {
		status = " Could not load document: " + shared.ErrorText(m.document.err)
		statusStyle = statusStyle.Foreground(shared.C("#FB7185"))
	}
	lines := []string{
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#E2E8F0")).Render(shared.Truncate(" "+title, width)),
		statusStyle.Render(shared.PadRight(shared.Truncate(status, width), width)),
		lipgloss.NewStyle().Foreground(shared.C("#64748B")).Render(
			shared.PadRight(shared.Truncate(" arrows/pgup/pgdown scroll  |  h/l horizontal  |  / search  |  n/N matches", width), width)),
	}
	rows := max(1, height-len(lines))
	end := min(len(m.document.lines), m.document.offsetY+rows)
	numberWidth := len(fmt.Sprintf("%d", max(1, len(m.document.lines))))
	contentWidth := max(1, width-numberWidth-3)
	for index := m.document.offsetY; index < end; index++ {
		content := ansi.Cut(m.document.lines[index], m.document.offsetX, m.document.offsetX+contentWidth)
		line := fmt.Sprintf(" %*d %s", numberWidth, index+1, content)
		style := lipgloss.NewStyle().Foreground(shared.C("#CBD5E1"))
		if m.document.filter.Value() != "" && strings.Contains(strings.ToLower(m.document.lines[index]), strings.ToLower(m.document.filter.Value())) {
			style = style.Foreground(shared.C("#FDE68A")).Bold(true)
		}
		lines = append(lines, style.Render(shared.PadRight(shared.Truncate(line, width), width)))
	}
	if len(m.document.lines) == 0 && !m.document.loading && m.document.err == nil {
		lines = append(lines, " Document is empty.")
	}
	return shared.FitLines(lines, width, height)
}

func (m *Model) openThreadDump(process flink.ProcessRef, backMode screenMode) tea.Cmd {
	return m.openThreadDumpWithFocus(process, backMode, threadFocusState{})
}

func (m *Model) openThreadDumpWithFocus(process flink.ProcessRef, backMode screenMode, focus threadFocusState) tea.Cmd {
	m.observabilityGeneration++
	m.mode = modeThreadDump
	m.process = process
	m.threadBackMode = backMode
	m.threadDump = nil
	m.threadCursor = 0
	m.threadStackOffset = 0
	m.threadFilter.Reset()
	m.threadFocus = focus
	m.threadBusy = true
	m.threadErr = nil
	return m.fetchThreadDump()
}

func (m *Model) fetchThreadDump() tea.Cmd {
	client := m.client
	process := m.process
	generation := m.observabilityGeneration
	m.threadGeneration = generation
	requestTimeout := m.requestTimeout
	return func() tea.Msg {
		ctx, cancel := requestTimeout(longRequestTimeout)
		defer cancel()
		threads, err := client.ThreadDump(ctx, process)
		return threadDumpMsg{threads: threads, err: err, process: process, generation: generation}
	}
}

func (m Model) filteredThreads() []flink.ThreadInfo {
	query := strings.ToLower(strings.TrimSpace(m.threadFilter.Value()))
	if query == "" {
		return m.threadDump
	}
	threads := make([]flink.ThreadInfo, 0)
	for _, thread := range m.threadDump {
		if strings.Contains(strings.ToLower(thread.Name), query) || strings.Contains(strings.ToLower(thread.Stack), query) {
			threads = append(threads, thread)
		}
	}
	return threads
}

func (m *Model) focusThreadDump() {
	m.threadFocus.Matched = false
	if m.threadFocus.Prefix == "" || len(m.threadDump) == 0 {
		m.threadCursor = 0
		return
	}

	// Flink checks the legacy source prefix first, then the regular execution
	// thread prefix. Preserve that ordering so a compatibility thread wins when
	// both names happen to exist in one TaskManager dump.
	for _, prefix := range []string{
		"Legacy Source Thread - " + m.threadFocus.Prefix,
		m.threadFocus.Prefix,
	} {
		prefix = strings.ToLower(prefix)
		for index, thread := range m.threadDump {
			name := strings.ToLower(strings.TrimSpace(thread.Name))
			firstLine := thread.Stack
			if newline := strings.IndexByte(firstLine, '\n'); newline >= 0 {
				firstLine = firstLine[:newline]
			}
			firstLine = strings.ToLower(firstLine)
			if strings.HasPrefix(name, prefix) || strings.Contains(firstLine, `"`+prefix) {
				m.threadCursor = index
				m.threadStackOffset = 0
				m.threadFocus.Matched = true
				return
			}
		}
	}
	m.threadCursor = 0
	m.threadStackOffset = 0
}

func (m *Model) handleThreadDumpKey(key string) tea.Cmd {
	if m.threadFilter.Active() {
		return m.handleThreadSearchKey(key)
	}
	threads := m.filteredThreads()
	switch key {
	case "esc", "backspace":
		m.mode = m.threadBackMode
	case "up", "k":
		m.moveThreadSelection(-1)
	case "down", "j":
		m.moveThreadSelection(1)
	case "pgup":
		m.moveThreadSelection(-max(1, m.threadListRowsAvailable()))
	case "pgdown":
		m.moveThreadSelection(max(1, m.threadListRowsAvailable()))
	case "home":
		m.threadCursor = 0
		m.threadStackOffset = 0
	case "end":
		m.threadCursor = max(0, len(threads)-1)
		m.threadStackOffset = 0
	case "ctrl+u":
		m.threadStackOffset = max(0, m.threadStackOffset-max(1, m.threadStackRowsAvailable()/2))
	case "ctrl+d":
		m.threadStackOffset += max(1, m.threadStackRowsAvailable()/2)
	case "enter":
		if thread, ok := m.selectedThread(); ok {
			m.openStaticDocument("THREAD  "+thread.Name, thread.Stack, modeThreadDump)
		}
	case "/":
		// Manual filtering takes ownership of the selection from the automatic
		// subtask focus so the status never claims a thread is still selected
		// after the user deliberately browses elsewhere.
		m.threadFocus = threadFocusState{}
		m.threadFilter.Restore(shellmodule.QueryState{Value: m.threadFilter.Value(), Open: true})
	case "p":
		return m.openProfiler(m.process, modeThreadDump)
	}
	return nil
}

func (m *Model) handleThreadSearchKey(key string) tea.Cmd {
	if key != "ctrl+w" {
		m.threadFilter.HandleKey(key)
	}
	m.threadCursor = 0
	m.threadStackOffset = 0
	return nil
}

func (m *Model) moveThreadSelection(delta int) {
	threads := m.filteredThreads()
	if len(threads) == 0 || delta == 0 {
		return
	}
	m.threadCursor = shared.Clamp(m.threadCursor+delta, 0, len(threads)-1)
	m.threadStackOffset = 0
}

func (m Model) selectedThread() (flink.ThreadInfo, bool) {
	threads := m.filteredThreads()
	if m.threadCursor < 0 || m.threadCursor >= len(threads) {
		return flink.ThreadInfo{}, false
	}
	return threads[m.threadCursor], true
}

func (m Model) threadListRowsAvailable() int {
	bodyHeight := max(6, m.height-headerHeight-footerHeight)
	return max(3, min(10, bodyHeight/3))
}

func (m Model) threadStackRowsAvailable() int {
	bodyHeight := max(6, m.height-headerHeight-footerHeight)
	return max(1, bodyHeight-m.threadListRowsAvailable()-5)
}

func (m *Model) handleThreadMouseClick(event tea.Mouse) {
	if event.Button != tea.MouseLeft {
		return
	}
	position := event.Y - headerHeight - threadBodyRowStart
	if position < 0 || position >= m.threadListRowsAvailable() {
		return
	}
	threads := m.filteredThreads()
	start := shared.Clamp(m.threadCursor-m.threadListRowsAvailable()/2, 0, max(0, len(threads)-m.threadListRowsAvailable()))
	index := start + position
	if index >= 0 && index < len(threads) {
		m.threadCursor = index
		m.threadStackOffset = 0
	}
}

func (m Model) renderThreadDump(width, height int) string {
	threads := m.filteredThreads()
	title := fmt.Sprintf(" THREAD DUMP  %s  |  %d/%d threads", m.process.Label(), len(threads), len(m.threadDump))
	if m.threadFocus.Label != "" {
		title += "  |  " + m.threadFocus.Label
	}
	if m.threadBusy {
		title += "  |  loading..."
	}
	status := " diagnostic order: Flink tasks, BLOCKED, RUNNABLE  |  / filters name and stack"
	statusStyle := lipgloss.NewStyle().Foreground(shared.C("#94A3B8"))
	if m.threadFilter.Active() {
		status = " /" + m.threadFilter.Value() + "|"
		statusStyle = statusStyle.Foreground(shared.C("#E2E8F0")).Background(shared.C("#111827"))
	} else if m.threadFilter.Value() != "" {
		status += "  |  filter /" + m.threadFilter.Value() + "/"
	} else if m.threadFocus.Prefix != "" && m.threadBusy {
		status = " locating execution thread " + m.threadFocus.Prefix + "..."
	} else if m.threadFocus.Prefix != "" && m.threadFocus.Matched {
		status = " focused execution thread " + m.threadFocus.Prefix + "  |  / filters all TaskManager threads"
	} else if m.threadFocus.Prefix != "" && m.threadErr == nil {
		status = " no thread matched " + m.threadFocus.Prefix + "  |  showing all TaskManager threads"
	}
	if m.threadErr != nil {
		status = " Could not load thread dump: " + shared.ErrorText(m.threadErr)
		statusStyle = statusStyle.Foreground(shared.C("#FB7185"))
	}
	lines := []string{
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#E2E8F0")).Render(shared.Truncate(title, width)),
		statusStyle.Render(shared.PadRight(shared.Truncate(status, width), width)),
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#94A3B8")).Background(shared.C("#111827")).Render(
			shared.PadRight(shared.Truncate("   THREAD                                                     STATE", width), width)),
	}
	listRows := m.threadListRowsAvailable()
	start := shared.Clamp(m.threadCursor-listRows/2, 0, max(0, len(threads)-listRows))
	end := min(len(threads), start+listRows)
	for index := start; index < end; index++ {
		lines = append(lines, renderThreadRow(threads[index], index == m.threadCursor, width))
	}
	for len(lines) < 3+listRows {
		lines = append(lines, "")
	}
	separator := lipgloss.NewStyle().Foreground(shared.C("#334155")).Render(strings.Repeat("-", width))
	thread, ok := m.selectedThread()
	detailTitle := " No thread selected."
	stack := []string(nil)
	if ok {
		detailTitle = fmt.Sprintf(" %s  |  %s", thread.Name, threadState(thread.Stack))
		stack = documentLines(thread.Stack)
	}
	lines = append(lines, separator, lipgloss.NewStyle().Bold(true).Foreground(shared.C("#A78BFA")).Render(shared.Truncate(detailTitle, width)))
	maximum := max(0, len(stack)-m.threadStackRowsAvailable())
	offset := shared.Clamp(m.threadStackOffset, 0, maximum)
	for index := offset; index < min(len(stack), offset+m.threadStackRowsAvailable()); index++ {
		lines = append(lines, lipgloss.NewStyle().Foreground(shared.C("#CBD5E1")).Render(shared.Truncate(" "+stack[index], width)))
	}
	return shared.FitLines(lines, width, height)
}

func renderThreadRow(thread flink.ThreadInfo, selected bool, width int) string {
	marker := " "
	if selected {
		marker = ">"
	}
	state := threadState(thread.Stack)
	nameWidth := max(12, width-18)
	line := fmt.Sprintf(" %s %s  %s", marker, shared.PadRight(shared.Truncate(thread.Name, nameWidth), nameWidth), state)
	line = shared.PadRight(shared.Truncate(line, width), width)
	if selected {
		return lipgloss.NewStyle().Foreground(shared.C("#FFFFFF")).Background(shared.C("#6D28D9")).Bold(true).Render(line)
	}
	style := lipgloss.NewStyle().Foreground(shared.C("#CBD5E1"))
	switch state {
	case "BLOCKED":
		style = style.Foreground(shared.C("#FB7185")).Bold(true)
	case "RUNNABLE":
		style = style.Foreground(shared.C("#34D399"))
	}
	return style.Render(line)
}

func threadState(stack string) string {
	first := stack
	if index := strings.IndexByte(first, '\n'); index >= 0 {
		first = first[:index]
	}
	first = strings.ToUpper(first)
	for _, state := range []string{"TIMED_WAITING", "RUNNABLE", "BLOCKED", "WAITING", "TERMINATED", "NEW"} {
		if strings.Contains(first, state) {
			return state
		}
	}
	return "UNKNOWN"
}

func prioritizeThreads(threads []flink.ThreadInfo) []flink.ThreadInfo {
	result := slices.Clone(threads)
	slices.SortStableFunc(result, func(left, right flink.ThreadInfo) int {
		return threadDiagnosticPriority(left) - threadDiagnosticPriority(right)
	})
	return result
}

func threadDiagnosticPriority(thread flink.ThreadInfo) int {
	if isFlinkTaskThread(thread) {
		return 0
	}
	switch threadState(thread.Stack) {
	case "BLOCKED":
		return 1
	case "RUNNABLE":
		return 2
	case "WAITING":
		return 3
	case "TIMED_WAITING":
		return 4
	default:
		return 5
	}
}

func isFlinkTaskThread(thread flink.ThreadInfo) bool {
	name := strings.ToLower(thread.Name)
	stack := strings.ToLower(thread.Stack)
	return strings.Contains(name, "legacy source thread -") ||
		strings.Contains(name, ")#") && strings.Contains(name, "/") ||
		strings.Contains(stack, "org.apache.flink.streaming.runtime.tasks") ||
		strings.Contains(stack, "org.apache.flink.streaming.runtime.io") ||
		strings.Contains(stack, "mailboxprocessor")
}
