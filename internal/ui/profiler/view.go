package profiler

import (
	"fmt"
	"maps"
	"path/filepath"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

const (
	profilerControlsBodyRow = 2
	profilerBodyRowStart    = 4
)

var (
	profilerModes = []flink.ProfilerMode{
		flink.ProfilerCPU,
		flink.ProfilerLock,
		flink.ProfilerWall,
		flink.ProfilerAlloc,
		flink.ProfilerITimer,
	}
	profilerDurations = []time.Duration{
		3 * time.Second,
		5 * time.Second,
		10 * time.Second,
		30 * time.Second,
		60 * time.Second,
	}
)

type profilerControl struct {
	start    int
	end      int
	mode     flink.ProfilerMode
	duration time.Duration
	run      bool
}

type profilerRunIdentity struct {
	status      string
	mode        flink.ProfilerMode
	triggeredAt int64
	finishedAt  int64
	duration    time.Duration
	message     string
	outputFile  string
}

func (m *Model) openProfiler(process flink.ProcessRef, backMode int) tea.Cmd {
	m.observabilityGeneration++
	m.process = process
	m.profilerBackMode = backMode
	m.profilerEntries = nil
	m.profilerSelection.Set(0, 0)
	m.profilerBusy = true
	m.profilerAction = ""
	m.profilerErr = nil
	m.profilerMessage = ""
	m.profilerMessageStatus = ""
	m.profilerPendingAt = time.Time{}
	m.profilerPendingRuns = nil
	m.profilerReportName = ""
	if !containsProfilerMode(m.profilerMode) {
		m.profilerMode = flink.ProfilerITimer
	}
	if !containsProfilerDuration(m.profilerDuration) {
		m.profilerDuration = 5 * time.Second
	}
	return m.fetchProfilerList()
}

func (m *Model) fetchProfilerList() tea.Cmd {
	client := m.client
	process := m.process
	generation := m.observabilityGeneration
	requestID, start := m.listRequests.Begin(listRequest{process, generation})
	if !start {
		return nil
	}
	requestTimeout := m.requestTimeout
	return func() tea.Msg {
		ctx, cancel := requestTimeout(defaultRequestTimeout)
		defer cancel()
		entries, err := client.ProfilingList(ctx, process)
		return listMsg{entries: entries, err: err, process: process, generation: generation, requestID: requestID}
	}
}

func (m *Model) startProfiler() tea.Cmd {
	if m.profilerAction != "" {
		return nil
	}
	if m.profilerBusy || m.profilerErr != nil {
		m.setNotice("Wait for the profiler history to load successfully before starting a run.")
		return nil
	}
	client := m.client
	process := m.process
	mode := m.profilerMode
	duration := m.profilerDuration
	generation := m.observabilityGeneration
	m.profilerAction = "starting"
	m.profilerErr = nil
	m.profilerMessage = ""
	m.profilerMessageStatus = ""
	m.profilerPendingAt = time.Time{}
	m.profilerPendingRuns = profilerRunCounts(m.profilerEntries)
	m.profilerReportName = ""
	requestTimeout := m.requestTimeout
	return func() tea.Msg {
		ctx, cancel := requestTimeout(duration + profilerCaptureGracePeriod)
		defer cancel()
		entry, err := client.StartProfiling(ctx, process, mode, duration)
		return startMsg{entry: entry, err: err, process: process, generation: generation}
	}
}

func (m *Model) completePendingProfiler(entries []flink.Profiling) {
	if m.profilerAction != "waiting" {
		return
	}
	baseline := maps.Clone(m.profilerPendingRuns)
	for _, entry := range entries {
		if entry.Mode != m.profilerMode {
			continue
		}
		if !m.profilerPendingAt.IsZero() {
			if !entry.TriggeredAt.Equal(m.profilerPendingAt) {
				continue
			}
		} else {
			identity := profilerIdentity(entry)
			if baseline[identity] > 0 {
				baseline[identity]--
				continue
			}
		}
		if entry.Status == "RUNNING" {
			return
		}
		m.profilerAction = ""
		m.profilerPendingAt = time.Time{}
		m.profilerPendingRuns = nil
		m.profilerMessageStatus = entry.Status
		m.profilerMessage = fmt.Sprintf("%s %s", profilerModeLabel(entry.Mode), strings.ToLower(shared.Fallback(entry.Message, entry.Status)))
		return
	}
}

func profilerRunCounts(entries []flink.Profiling) map[profilerRunIdentity]int {
	counts := make(map[profilerRunIdentity]int, len(entries))
	for _, entry := range entries {
		counts[profilerIdentity(entry)]++
	}
	return counts
}

func profilerIdentity(entry flink.Profiling) profilerRunIdentity {
	return profilerRunIdentity{
		status:      entry.Status,
		mode:        entry.Mode,
		triggeredAt: profilerTimeMillis(entry.TriggeredAt),
		finishedAt:  profilerTimeMillis(entry.FinishedAt),
		duration:    entry.Duration,
		message:     entry.Message,
		outputFile:  entry.OutputFile,
	}
}

func profilerTimeMillis(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UnixMilli()
}

func (m *Model) openSelectedProfilerReport() tea.Cmd {
	if m.profilerAction != "" || m.profilerSelection.Index() < 0 || m.profilerSelection.Index() >= len(m.profilerEntries) {
		return nil
	}
	entry := m.profilerEntries[m.profilerSelection.Index()]
	if entry.Status != "FINISHED" || strings.TrimSpace(entry.OutputFile) == "" {
		m.profilerMessageStatus = entry.Status
		m.profilerMessage = "Selected run has no profile report to open."
		return nil
	}

	client := m.client
	process := m.process
	filename := filepath.Base(strings.TrimSpace(entry.OutputFile))
	capturedAt := entry.FinishedAt
	if capturedAt.IsZero() {
		capturedAt = entry.TriggeredAt.Add(entry.Duration)
	}
	generation := m.observabilityGeneration
	m.profilerAction = "loading-report"
	m.profilerErr = nil
	requestTimeout := m.requestTimeout
	return func() tea.Msg {
		ctx, cancel := requestTimeout(profilerReportRequestTimeout)
		defer cancel()
		report, err := client.ProfilerReport(ctx, process, entry.OutputFile)
		var graph flink.FlameGraph
		if err == nil {
			var root flink.FlameGraphNode
			root, err = flink.ParseProfilerReport(report)
			if err == nil {
				if capturedAt.IsZero() {
					capturedAt = time.Now()
				}
				graph = flink.FlameGraph{
					Type:               flink.FlameGraphFull,
					Subtask:            -1,
					EndTimestampMillis: capturedAt.UnixMilli(),
					EndTimestamp:       capturedAt,
					Root:               root,
				}
			}
		}
		return downloadMsg{filename: filename, graph: graph, err: err, process: process, generation: generation}
	}
}

func (m *Model) handleProfilerKey(key string) tea.Cmd {
	switch key {
	case "esc", "backspace":
		m.pendingIntent = IntentBack
	case "up", "k":
		m.moveProfilerSelection(-1)
	case "down", "j":
		m.moveProfilerSelection(1)
	case "pgup":
		m.moveProfilerSelection(-max(1, m.profilerRowsAvailable()))
	case "pgdown":
		m.moveProfilerSelection(max(1, m.profilerRowsAvailable()))
	case "home":
		m.profilerSelection.Set(0, 0)
	case "end":
		m.profilerSelection.Set(len(m.profilerEntries)-1, len(m.profilerEntries))
	case "[":
		m.cycleProfilerMode(-1)
	case "]":
		m.cycleProfilerMode(1)
	case "-", "_":
		m.cycleProfilerDuration(-1)
	case "+", "=":
		m.cycleProfilerDuration(1)
	case "p", "n":
		return m.startProfiler()
	case "l":
		return m.openProcessLogs(m.process)
	case "x":
		return m.openStdout(m.process)
	case "d":
		return m.openThreadDump(m.process)
	case "enter":
		return m.openSelectedProfilerReport()
	}
	return nil
}

func (m *Model) moveProfilerSelection(delta int) {
	m.profilerSelection.Move(delta, len(m.profilerEntries))
}

func (m *Model) cycleProfilerMode(delta int) {
	if m.profilerAction != "" {
		return
	}
	index := 0
	for candidate, mode := range profilerModes {
		if mode == m.profilerMode {
			index = candidate
			break
		}
	}
	index = (index + delta) % len(profilerModes)
	if index < 0 {
		index += len(profilerModes)
	}
	m.profilerMode = profilerModes[index]
}

func (m *Model) cycleProfilerDuration(delta int) {
	if m.profilerAction != "" {
		return
	}
	index := 0
	for candidate, duration := range profilerDurations {
		if duration == m.profilerDuration {
			index = candidate
			break
		}
	}
	index = (index + delta) % len(profilerDurations)
	if index < 0 {
		index += len(profilerDurations)
	}
	m.profilerDuration = profilerDurations[index]
}

func containsProfilerMode(mode flink.ProfilerMode) bool {
	for _, candidate := range profilerModes {
		if candidate == mode {
			return true
		}
	}
	return false
}

func containsProfilerDuration(duration time.Duration) bool {
	for _, candidate := range profilerDurations {
		if candidate == duration {
			return true
		}
	}
	return false
}

func (m Model) profilerRowsAvailable() int {
	return max(1, m.bodyHeight()-profilerBodyRowStart-3)
}

func (m Model) profilerWindowStart() int {
	available := m.profilerRowsAvailable()
	return shared.WindowStart(m.profilerSelection.Index(), available, len(m.profilerEntries))
}

func (m *Model) handleProfilerMouseClick(event tea.Mouse) tea.Cmd {
	if event.Button != tea.MouseLeft {
		return nil
	}
	localY := event.Y - headerHeight
	if localY == profilerControlsBodyRow {
		_, controls := m.renderProfilerControls(max(40, m.contentWidthAt(max(40, m.width))))
		for _, control := range controls {
			if event.X < control.start || event.X >= control.end {
				continue
			}
			if control.run {
				return m.startProfiler()
			}
			if m.profilerAction != "" {
				return nil
			}
			if control.mode != "" {
				m.profilerMode = control.mode
			}
			if control.duration > 0 {
				m.profilerDuration = control.duration
			}
			return nil
		}
		return nil
	}
	position := localY - profilerBodyRowStart
	if position < 0 || position >= m.profilerRowsAvailable() {
		return nil
	}
	index := m.profilerWindowStart() + position
	if index >= 0 && index < len(m.profilerEntries) {
		m.profilerSelection.Set(index, len(m.profilerEntries))
		if m.profilerEntries[index].Status == "FINISHED" && m.profilerEntries[index].OutputFile != "" {
			return m.openSelectedProfilerReport()
		}
	}
	return nil
}

func (m Model) renderProfiler(width, height int) string {
	title := fmt.Sprintf(" PROCESS PROFILER  %s  |  %d runs", m.process.Label(), len(m.profilerEntries))
	if m.profilerBusy {
		title += "  |  refreshing..."
	}
	status, statusStyle := m.profilerStatus()
	controls, _ := m.renderProfilerControls(width)
	lines := []string{
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#E2E8F0")).Render(shared.Truncate(title, width)),
		statusStyle.Render(shared.PadRight(shared.Truncate(status, width), width)),
		controls,
		renderProfilerColumns(width),
	}
	start := m.profilerWindowStart()
	end := min(len(m.profilerEntries), start+m.profilerRowsAvailable())
	for index := start; index < end; index++ {
		lines = append(lines, renderProfilerRow(m.profilerEntries[index], index == m.profilerSelection.Index(), width))
	}
	if len(m.profilerEntries) == 0 && !m.profilerBusy && m.profilerErr == nil {
		lines = append(lines, shared.Truncate(" No profiler runs yet. Choose an event and press p to capture one.", width))
	}
	for len(lines) < height-3 {
		lines = append(lines, "")
	}
	separator := lipgloss.NewStyle().Foreground(shared.C("#334155")).Render(strings.Repeat("-", width))
	first, second := m.renderSelectedProfilerDetail(width)
	lines = append(lines, separator, first, second)
	return shared.FitLines(lines, width, height)
}

func (m Model) profilerStatus() (string, lipgloss.Style) {
	style := lipgloss.NewStyle().Foreground(shared.C("#94A3B8"))
	status := " ITIMER works without Linux perf permissions; CPU may require container capabilities."
	if m.profilerAction == "starting" {
		status = fmt.Sprintf(" Starting %s capture for %s...", profilerModeLabel(m.profilerMode), shared.ShortDuration(m.profilerDuration))
		style = style.Foreground(shared.C("#FBBF24")).Bold(true)
	} else if m.profilerAction == "waiting" {
		status = fmt.Sprintf(" Capturing %s for %s; waiting for Flink to finish...", profilerModeLabel(m.profilerMode), shared.ShortDuration(m.profilerDuration))
		style = style.Foreground(shared.C("#FBBF24")).Bold(true)
	} else if m.profilerAction == "loading-report" {
		status = " Loading the selected profile into the terminal flame graph..."
		style = style.Foreground(shared.C("#60A5FA"))
	} else if m.profilerErr != nil {
		status = " Profiler request failed: " + shared.ErrorText(m.profilerErr)
		style = style.Foreground(shared.C("#FB7185"))
	} else if m.profilerMessage != "" {
		status = " " + m.profilerMessage
		switch m.profilerMessageStatus {
		case "FAILED":
			style = style.Foreground(shared.C("#FB7185"))
		case "FINISHED":
			style = style.Foreground(shared.C("#34D399"))
		}
	}
	return status, style
}

func (m Model) renderProfilerControls(width int) (string, []profilerControl) {
	active := lipgloss.NewStyle().Foreground(shared.C("#FFFFFF")).Background(shared.C("#6D28D9")).Bold(true)
	inactive := lipgloss.NewStyle().Foreground(shared.C("#CBD5E1")).Background(shared.C("#111827"))
	runStyle := lipgloss.NewStyle().Foreground(shared.C("#052E16")).Background(shared.C("#34D399")).Bold(true)
	muted := lipgloss.NewStyle().Foreground(shared.C("#94A3B8"))

	var line strings.Builder
	controls := make([]profilerControl, 0, len(profilerModes)+len(profilerDurations)+1)
	x := 0
	appendPlain := func(value string) {
		line.WriteString(muted.Render(value))
		x += len(value)
	}
	appendPlain(" mode ")
	for _, mode := range profilerModes {
		label := "[" + profilerModeLabel(mode) + "]"
		style := inactive
		if mode == m.profilerMode {
			style = active
		}
		controls = append(controls, profilerControl{start: x, end: x + len(label), mode: mode})
		line.WriteString(style.Render(label))
		line.WriteString(" ")
		x += len(label) + 1
	}
	appendPlain(" duration ")
	for _, duration := range profilerDurations {
		label := "[" + shared.ShortDuration(duration) + "]"
		style := inactive
		if duration == m.profilerDuration {
			style = active
		}
		controls = append(controls, profilerControl{start: x, end: x + len(label), duration: duration})
		line.WriteString(style.Render(label))
		line.WriteString(" ")
		x += len(label) + 1
	}
	appendPlain(" ")
	runLabel := "[ START p ]"
	controls = append(controls, profilerControl{start: x, end: x + len(runLabel), run: true})
	line.WriteString(runStyle.Render(runLabel))
	return shared.PadRightStyled(shared.TruncateStyled(line.String(), width), width), controls
}

func renderProfilerColumns(width int) string {
	line := "   STARTED              MODE        DURATION  STATUS      REPORT / MESSAGE"
	return lipgloss.NewStyle().Bold(true).Foreground(shared.C("#94A3B8")).Background(shared.C("#111827")).Render(
		shared.PadRight(shared.Truncate(line, width), width),
	)
}

func renderProfilerRow(entry flink.Profiling, selected bool, width int) string {
	marker := " "
	if selected {
		marker = ">"
	}
	started := "-"
	if !entry.TriggeredAt.IsZero() {
		started = entry.TriggeredAt.Format("2006-01-02 15:04:05")
	}
	detail := entry.OutputFile
	if detail == "" {
		detail = entry.Message
	}
	line := fmt.Sprintf(" %s %-19s %-10s %8s  %-10s  %s", marker, started, profilerModeLabel(entry.Mode), shared.ShortDuration(entry.Duration), entry.Status, shared.SanitizeLine(detail))
	line = shared.PadRight(shared.Truncate(line, width), width)
	if selected {
		return lipgloss.NewStyle().Foreground(shared.C("#FFFFFF")).Background(shared.C("#6D28D9")).Bold(true).Render(line)
	}
	style := lipgloss.NewStyle().Foreground(shared.C("#CBD5E1"))
	switch entry.Status {
	case "FAILED":
		style = style.Foreground(shared.C("#FB7185"))
	case "FINISHED":
		style = style.Foreground(shared.C("#34D399"))
	case "RUNNING":
		style = style.Foreground(shared.C("#FBBF24"))
	}
	return style.Render(line)
}

func (m Model) renderSelectedProfilerDetail(width int) (string, string) {
	if m.profilerSelection.Index() < 0 || m.profilerSelection.Index() >= len(m.profilerEntries) {
		return shared.Truncate(" No profiler run selected.", width), shared.Truncate(" p starts a capture  |  enter opens a finished report in the terminal", width)
	}
	entry := m.profilerEntries[m.profilerSelection.Index()]
	first := fmt.Sprintf(" %s  |  %s  |  %s  |  %s", shared.Clock(entry.TriggeredAt, "--:--:--"), profilerModeLabel(entry.Mode), shared.ShortDuration(entry.Duration), entry.Status)
	second := shared.Fallback(entry.Message, "No status message from Flink.")
	if entry.OutputFile != "" {
		second += "  |  enter/click opens " + entry.OutputFile + " in terminal"
	}
	return shared.Truncate(first, width), shared.Truncate(" "+shared.SanitizeLine(second), width)
}

func profilerModeLabel(mode flink.ProfilerMode) string {
	switch mode {
	case flink.ProfilerCPU:
		return "CPU"
	case flink.ProfilerLock:
		return "Lock"
	case flink.ProfilerWall:
		return "Wall-Clock"
	case flink.ProfilerAlloc:
		return "Allocation"
	case flink.ProfilerITimer:
		return "ITIMER"
	default:
		return shared.Fallback(string(mode), "Unknown")
	}
}
