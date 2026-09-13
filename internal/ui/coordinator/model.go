package coordinator

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/graph"
	accumulatormodule "github.com/nikitasavinov/flink-tui/internal/ui/accumulators"
	checkpointmodule "github.com/nikitasavinov/flink-tui/internal/ui/checkpoints"
	exceptionmodule "github.com/nikitasavinov/flink-tui/internal/ui/exceptions"
	flamegraphmodule "github.com/nikitasavinov/flink-tui/internal/ui/flamegraph"
	inframodule "github.com/nikitasavinov/flink-tui/internal/ui/infrastructure"
	jobdetailmodule "github.com/nikitasavinov/flink-tui/internal/ui/jobdetail"
	jobgraphmodule "github.com/nikitasavinov/flink-tui/internal/ui/jobgraph"
	joblistmodule "github.com/nikitasavinov/flink-tui/internal/ui/joblist"
	jobopsmodule "github.com/nikitasavinov/flink-tui/internal/ui/jobops"
	metricmodule "github.com/nikitasavinov/flink-tui/internal/ui/metrics"
	processmodule "github.com/nikitasavinov/flink-tui/internal/ui/processdiag"
	profilermodule "github.com/nikitasavinov/flink-tui/internal/ui/profiler"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
	"github.com/nikitasavinov/flink-tui/internal/ui/shell"
	"github.com/nikitasavinov/flink-tui/internal/ui/sqlworkbench"
)

const (
	nodeWidth           = 29
	nodeHeight          = 5
	headerHeight        = 3
	inspectorHeight     = 8
	graphScreenTop      = headerHeight
	wideFooterThreshold = 118
)

// footerHeightAt reserves a dedicated global-key row when the terminal is too
// narrow to keep global discovery and screen-local controls on one line.
func footerHeightAt(width int) int {
	if width >= wideFooterThreshold {
		return 1
	}
	return 2
}

type screenMode uint8

const (
	modeGraph screenMode = iota
	modeJobs
	modeSubtasks
	modeFlameGraph
	modeCheckpoints
	modeCheckpointOperators
	modeCheckpointSubtasks
	modeDiagnostics
	modeTimeline
	modeExceptions
	modeJobConfig
	modeActions
	modeTaskManagers
	modeTaskManagerDetail
	modeJobManager
	modeMetricExplorer
	modeAccumulators
	modeProcessLogs
	modeDocument
	modeThreadDump
	modeProfiler
	modeProfilerFlameGraph
	modeSQL
	modeCount
)

type Model struct {
	client                *flink.Client
	preferredJobID        string
	refreshInterval       time.Duration
	snapshot              flink.Snapshot
	layout                graph.Layout
	selected              string
	width                 int
	height                int
	graphViewport         jobgraphmodule.Viewport
	graphTelemetry        jobgraphmodule.Telemetry
	loading               bool
	paused                bool
	mouseEnabled          bool
	err                   error
	mode                  screenMode
	generation            uint64
	metricPulseGeneration uint64
	errorDetailOpen       bool
	notice                string
	noticeUntil           time.Time
	requests              requestLifecycle
	sql                   sqlworkbench.Model
	infrastructure        inframodule.Model
	checkpoints           checkpointmodule.Model
	processDiagnostics    processmodule.Model
	jobOperations         jobopsmodule.Model
	flameGraphs           flamegraphmodule.Model
	profiler              profilermodule.Model
	jobList               joblistmodule.Model
	jobDetails            jobdetailmodule.Model
	accumulators          accumulatormodule.Model
	metrics               metricmodule.Model
	exceptions            exceptionmodule.Model
	shellState
}

type snapshotMsg struct {
	snapshot       flink.Snapshot
	err            error
	generation     uint64
	exceptionLimit int
}

type tickMsg time.Time

type metricPulseClearMsg struct {
	generation uint64
}

func NewModel(client *flink.Client, preferredJobID string, refreshInterval time.Duration) Model {
	return NewModelWithSQLGateway(client, nil, preferredJobID, refreshInterval)
}

func NewModelWithSQLGateway(client *flink.Client, sqlClient *flink.SQLGatewayClient, preferredJobID string, refreshInterval time.Duration) Model {
	if refreshInterval < 250*time.Millisecond {
		refreshInterval = 250 * time.Millisecond
	}
	model := Model{
		client:          client,
		preferredJobID:  preferredJobID,
		refreshInterval: refreshInterval,
		graphViewport:   jobgraphmodule.NewViewport(preferredJobID != ""),
		graphTelemetry:  jobgraphmodule.NewTelemetry(),
		requests:        newRequestLifecycle(),
	}
	model.configureSQLWorkbench(sqlClient)
	model.configureInfrastructure()
	model.configureCheckpoints()
	model.configureProcessDiagnostics()
	model.configureJobOperations()
	model.configureFlameGraphs()
	model.configureProfiler()
	model.configureJobList()
	model.configureJobDetails()
	model.configureAccumulators()
	model.configureMetrics()
	model.configureExceptions()
	if preferredJobID == "" {
		model.mode = modeJobs
		state := model.jobList.State()
		state.Loading = true
		model.jobList.RestoreState(state)
		model.jobList.ReserveInitialPoll()
	} else {
		model.mode = modeGraph
		model.loading = true
		model.requests.snapshotPending = true
	}
	return model
}

func (m Model) Init() tea.Cmd {
	probe := tea.RequestBackgroundColor
	if m.mode == modeJobs {
		return tea.Batch(m.jobList.InitialPoll(), m.nextTick(), probe)
	}
	// NewModel reserves this initial request because Init has a value receiver.
	return tea.Batch(m.snapshotCommand(), m.nextTick(), probe)
}

func (m Model) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	switch message := message.(type) {
	case tea.BackgroundColorMsg:
		shared.SetDarkBackground(message.IsDark())
		return m, nil

	case tea.WindowSizeMsg:
		return m, m.applyWindowSize(message)

	case snapshotMsg:
		return m, m.applySnapshot(message)

	case metricPulseClearMsg:
		return m, m.applyMetricPulseClear(message)

	case joblistmodule.Message:
		m.syncJobListContext()
		return m, m.jobList.Apply(message)

	case jobdetailmodule.Message:
		m.syncJobDetailContext()
		return m, m.jobDetails.Apply(message)

	case flamegraphmodule.Message:
		m.syncFlameGraphContext()
		return m, m.flameGraphs.Apply(message)

	case checkpointmodule.Message:
		return m, m.applyCheckpointMessage(message)

	case inframodule.Message:
		result := m.infrastructure.Apply(message, m.infrastructureModeActive())
		return m, m.applyInfrastructureResult(result)

	case metricmodule.Message:
		m.syncMetricContext()
		return m, m.metrics.Apply(message)

	case accumulatormodule.Message:
		m.syncAccumulatorContext()
		return m, m.accumulators.Apply(message)

	case profilermodule.Message:
		return m, m.applyProfilerMessage(message)

	case jobopsmodule.Message:
		return m, m.applyJobOperationsMessage(message)

	case sqlworkbench.Message:
		return m, m.sql.Apply(message)

	case processmodule.Message:
		return m, m.processDiagnostics.Apply(message)

	case tickMsg:
		return m, m.applyTick()

	case tea.PasteMsg:
		if m.mode == modeSQL && !m.help.Open() && !m.palette.Open() && !m.errorDetailOpen && !m.navigation.Focused() {
			m.sql.HandlePaste(message.Content)
		}
		return m, nil

	case tea.MouseClickMsg:
		// A new press always ends a stale drag, including presses in another view.
		m.graphViewport.CancelDrag()
		if m.acceptsPointerInput() {
			event := message.Mouse()
			if handled, command := m.handleNavigationMouseClick(event); handled {
				return m, command
			}
			if m.navigationOnlyAt(max(40, m.width)) {
				return m, nil
			}
			m.navigation.Blur()
			event.X -= m.contentStartX()
			return m, m.activeScreen().click(&m, event)
		}
		return m, nil

	case tea.MouseMotionMsg:
		if m.acceptsPointerInput() && m.graphViewport.DragActive() {
			event := message.Mouse()
			event.X -= m.contentStartX()
			m.handleMouseMotion(event)
		}
		return m, nil

	case tea.MouseReleaseMsg:
		if m.acceptsPointerInput() && m.graphViewport.DragActive() {
			event := message.Mouse()
			event.X -= m.contentStartX()
			m.handleMouseRelease(event)
		}
		return m, nil

	case tea.MouseWheelMsg:
		if m.acceptsPointerInput() {
			event := message.Mouse()
			width := max(40, m.width)
			navigationWidth := m.navigationWidthAt(width)
			if navigationWidth > 0 && (navigationWidth >= width || event.X < navigationWidth) {
				if !m.navigation.Focused() {
					m.focusNavigation()
				}
				m.moveNavigationCursor(wheelDelta(event))
				return m, nil
			}
			if navigationWidth > 0 && event.X == navigationWidth {
				return m, nil
			}
			event.X -= m.contentStartX()
			return m, m.activeScreen().wheel(&m, event)
		}
		return m, nil

	case tea.KeyPressMsg:
		key := message.String()
		if key == "ctrl+c" {
			return m, m.quit()
		}
		if m.help.Open() {
			screen := m.activeScreen()
			local := screen.footer(m, mouseLabel(m.mouseEnabled), m.backDestinationLabel(screen.backFallback))
			m.help.HandleKey(key, m.bodyHeight(), local, m.resumeDestinationLabel())
			return m, nil
		}
		if m.errorDetailOpen {
			if key == "e" || key == "ctrl+e" || key == "q" || key == "esc" || key == "backspace" {
				m.errorDetailOpen = false
			}
			return m, nil
		}
		if m.palette.Open() {
			return m, m.handlePaletteKey(key)
		}
		screen := m.activeScreen()
		if key == "ctrl+n" || key == "ctrl+b" {
			if !m.locksNavigationChrome() {
				m.toggleNavigationFocus()
				return m, nil
			}
		}
		if key == "ctrl+g" {
			if !m.locksNavigationChrome() {
				m.toggleNavigationVisibility()
				return m, nil
			}
		}
		// The palette keys are not sidebar aliases, so a focused sidebar must
		// not swallow them. Without this the advertised ":" is unreachable
		// exactly when an operator is already hunting for a destination.
		if m.navigation.Focused() && (key == ":" || key == "ctrl+p") {
			m.openPalette()
			return m, nil
		}
		if m.navigation.Focused() {
			return m, m.handleFocusedNavigationKey(key)
		}
		if screen.capturesKeys(m) {
			return m, screen.key(&m, message)
		}
		if key == "ctrl+p" {
			m.openPalette()
			return m, nil
		}
		if key == "ctrl+e" {
			if len(m.currentErrors()) == 0 {
				m.setNotice("No current error details.")
			} else {
				m.errorDetailOpen = true
			}
			return m, nil
		}
		if key == "space" {
			m.togglePause()
			return m, nil
		}
		if key == "backspace" && !m.scopeUpReservedByScreen() {
			return m, m.scopeUp()
		}
		if key == "left" && screen.leftOpensNavigation {
			m.focusNavigation()
			return m, nil
		}
		if key == "?" {
			m.help.Show()
			return m, nil
		}
		if key == "esc" {
			return m, m.handleScreenBack()
		}
		if key == "q" {
			return m, m.handleBack()
		}
		if key == ":" {
			m.openPalette()
			return m, nil
		}
		if key == "r" {
			return m, screen.refresh(&m)
		}
		if handled, command := m.handleGlobalNavigationKey(key); handled {
			return m, command
		}
		if key == "m" {
			m.mouseEnabled = !m.mouseEnabled
			m.graphViewport.CancelDrag()
			return m, nil
		}
		return m, screen.key(&m, message)
	}
	return m, nil
}

func (m *Model) togglePause() {
	m.paused = !m.paused
}

func (m Model) View() tea.View {
	view := tea.NewView(m.render())
	view.AltScreen = true
	view.WindowTitle = "Flink TUI"
	if m.mouseEnabled {
		view.MouseMode = tea.MouseModeCellMotion
	}
	return view
}

func (m Model) acceptsPointerInput() bool {
	return m.mouseEnabled && !m.palette.Open() && !m.help.Open() && !m.errorDetailOpen && !m.locksNavigationChrome()
}

func (m *Model) fetchSnapshot() tea.Cmd {
	if m.requests.snapshotPending {
		return nil
	}
	m.requests.snapshotPending = true
	return m.snapshotCommand()
}

func (m Model) snapshotCommand() tea.Cmd {
	client := m.client
	jobID := m.preferredJobID
	generation := m.generation
	exceptionLimit := m.exceptions.Limit()
	return func() tea.Msg {
		ctx, cancel := m.requestTimeout(defaultRequestTimeout)
		defer cancel()
		snapshot, err := client.SnapshotWithExceptionLimit(ctx, jobID, exceptionLimit)
		return snapshotMsg{snapshot: snapshot, err: err, generation: generation, exceptionLimit: exceptionLimit}
	}
}

func (m Model) nextTick() tea.Cmd {
	return tea.Tick(m.refreshInterval, func(now time.Time) tea.Msg { return tickMsg(now) })
}

func (m *Model) selectRelative(delta int) {
	if len(m.layout.Order) == 0 {
		return
	}
	index := 0
	for candidate, id := range m.layout.Order {
		if id == m.selected {
			index = candidate
			break
		}
	}
	index = (index + delta + len(m.layout.Order)) % len(m.layout.Order)
	m.selected = m.layout.Order[index]
}

func (m *Model) selectConnected(candidates []string) {
	if len(candidates) == 0 {
		return
	}
	current := m.layout.Rects[m.selected]
	slices.SortStableFunc(candidates, func(leftID, rightID string) int {
		left := abs(m.layout.Rects[leftID].CenterY() - current.CenterY())
		right := abs(m.layout.Rects[rightID].CenterY() - current.CenterY())
		return cmp.Compare(left, right)
	})
	m.selected = candidates[0]
}

func (m *Model) selectSibling(delta int) {
	for _, layer := range m.layout.Layers {
		for index, id := range layer {
			if id != m.selected {
				continue
			}
			next := index + delta
			if next >= 0 && next < len(layer) {
				m.selected = layer[next]
			}
			return
		}
	}
}

func (m Model) graphHeight() int {
	return max(3, m.height-headerHeight-inspectorHeight-m.footerHeight())
}

func (m Model) render() string {
	width := max(40, m.width)
	height := max(14, m.height)
	if m.width == 0 || m.height == 0 {
		width, height = 100, 30
	}

	header := m.renderHeader(width)
	bodyHeight := max(3, height-headerHeight-footerHeightAt(width))
	body := m.renderShellBody(width, bodyHeight)
	return strings.Join([]string{header, body, m.renderFooter(width)}, "\n")
}

func (m Model) renderShellBody(width, height int) string {
	if m.errorDetailOpen {
		return m.renderErrorDetails(width, height)
	}
	if m.help.Open() {
		screen := m.activeScreen()
		back := m.backDestinationLabel(screen.backFallback)
		return m.help.Render(width, height, screen.displayTitle(m), screen.footer(m, mouseLabel(m.mouseEnabled), back), m.resumeDestinationLabel())
	}
	if m.palette.Open() && m.navigationOnlyAt(width) {
		return m.renderCommandPalette(width, height)
	}
	navigationWidth := m.navigationWidthAt(width)
	if navigationWidth == 0 {
		if m.palette.Open() {
			return m.renderCommandPalette(width, height)
		}
		return m.renderContent(width, height)
	}
	if navigationWidth >= width {
		return m.renderNavigation(width, height)
	}
	contentWidth := m.contentWidthAt(width)
	content := m.renderContent(contentWidth, height)
	if m.palette.Open() {
		content = m.renderCommandPalette(contentWidth, height)
	}
	return shell.JoinNavigationAndContent(
		m.renderNavigation(navigationWidth, height),
		content,
		height,
	)
}

func (m Model) renderContent(width, height int) string {
	return m.activeScreen().render(m, width, height)
}

func (m Model) renderHeader(width int) string {
	screen := m.activeScreen()
	return screen.header(m, width, screen.displayTitle(m))
}

func (m Model) renderJobHeader(screen string, width int) string {
	title := lipgloss.NewStyle().Bold(true).Foreground(shared.C("#A78BFA")).Render(" FLINK TUI ")
	mode := shell.RenderRefreshBadge(shell.DeriveRefreshBadge(
		time.Now(), m.snapshot.UpdatedAt, m.refreshInterval, m.err, len(m.snapshot.Issues) > 0, m.paused,
	))
	nameWidth := max(8, width-shared.DisplayWidth(title)-shared.DisplayWidth(mode)-4)
	location := m.breadcrumb(screen)
	if m.snapshot.JobName == "" {
		location = "discovering job › " + screen
	}
	lineOne := fmt.Sprintf("%s  %s  %s", title, fitBreadcrumb(location, nameWidth), mode)
	status := m.client.Endpoint()
	if !m.snapshot.UpdatedAt.IsZero() {
		status += "  |  refreshed " + m.snapshot.UpdatedAt.Format("15:04:05")
	}
	lineTwoStyle := lipgloss.NewStyle().Foreground(shared.C("#94A3B8"))
	if len(m.snapshot.Issues) > 0 {
		status += fmt.Sprintf("  |  %d data warning(s)  |  e details", len(m.snapshot.Issues))
		lineTwoStyle = lineTwoStyle.Foreground(shared.C("#FBBF24"))
	}
	if m.err != nil {
		status += "  |  " + conciseError(m.err) + "  |  e details"
		lineTwoStyle = lineTwoStyle.Foreground(shared.C("#FB7185"))
	}
	if !m.mouseEnabled {
		status += "  |  mouse off"
	}
	lineTwo := lineTwoStyle.Render(" " + shared.Truncate(status, width-2))
	return lineOne + "\n" + lineTwo + "\n" + m.renderHealth(width)
}

func (m Model) renderInfrastructureHeader(screen string, width int) string {
	state := m.infrastructure.State()
	value := state.Infrastructure
	title := lipgloss.NewStyle().Bold(true).Foreground(shared.C("#A78BFA")).Render(" FLINK TUI ")
	mode := shell.RenderRefreshBadge(shell.DeriveRefreshBadge(
		time.Now(), value.UpdatedAt, m.refreshInterval, state.Err, false, m.paused,
	))
	nameWidth := max(8, width-shared.DisplayWidth(title)-shared.DisplayWidth(mode)-4)
	lineOne := fmt.Sprintf("%s  %s  %s", title, fitBreadcrumb(m.breadcrumb(screen), nameWidth), mode)
	status := m.client.Endpoint()
	if !value.UpdatedAt.IsZero() {
		status += "  |  refreshed " + value.UpdatedAt.Format("15:04:05")
	}
	lineTwoStyle := lipgloss.NewStyle().Foreground(shared.C("#94A3B8"))
	if state.Err != nil {
		status += "  |  " + conciseError(state.Err) + "  |  e details"
		lineTwoStyle = lineTwoStyle.Foreground(shared.C("#FB7185"))
	}
	lineTwo := lineTwoStyle.Render(" " + shared.Truncate(status, width-2))
	cluster := " infrastructure  task managers —  |  slots —/— available  |  jobs —"
	if !value.UpdatedAt.IsZero() {
		manager := value.JobManager
		taskManagerCount := len(value.TaskManagers)
		slotsAvailable, slotsTotal := 0, 0
		for _, taskManager := range value.TaskManagers {
			slotsAvailable += taskManager.FreeSlots
			slotsTotal += taskManager.Slots
		}
		cluster = fmt.Sprintf(" infrastructure  task managers %d  |  slots %d/%d available  |  jobs %d",
			taskManagerCount, slotsAvailable, slotsTotal, manager.RunningJobs)
	}
	lineThree := lipgloss.NewStyle().Foreground(shared.C("#94A3B8")).Background(shared.C("#0F172A")).Render(
		shared.PadRight(shared.Truncate(cluster, width), width),
	)
	return lineOne + "\n" + lineTwo + "\n" + lineThree
}

func (m Model) renderHomeHeader(width int) string {
	overview := m.jobList.State()
	cluster := overview.Cluster
	title := lipgloss.NewStyle().Bold(true).Foreground(shared.C("#A78BFA")).Render(" FLINK TUI ")
	homeError := shell.FirstError(overview.ClusterErr, overview.JobsErr)
	partialFailure := (overview.ClusterErr == nil) != (overview.JobsErr == nil)
	badgeError := homeError
	lastSuccess := shell.OlderSuccess(cluster.UpdatedAt, overview.UpdatedAt)
	if partialFailure {
		badgeError = nil
		lastSuccess = shell.NewerSuccess(cluster.UpdatedAt, overview.UpdatedAt)
	}
	mode := shell.RenderRefreshBadge(shell.DeriveRefreshBadge(
		time.Now(), lastSuccess, m.refreshInterval, badgeError, partialFailure, m.paused,
	))
	nameWidth := max(8, width-shared.DisplayWidth(title)-shared.DisplayWidth(mode)-4)
	lineOne := fmt.Sprintf("%s  %s  %s", title, shared.Truncate("Cluster Overview", nameWidth), mode)

	status := m.client.Endpoint()
	if cluster.FlinkVersion != "" {
		status += "  |  Flink " + cluster.FlinkVersion
		if cluster.FlinkCommit != "" && width >= 90 {
			status += " (" + cluster.FlinkCommit + ")"
		}
	}
	if !cluster.UpdatedAt.IsZero() {
		status += "  |  refreshed " + cluster.UpdatedAt.Format("15:04:05")
	}
	if !m.mouseEnabled {
		status += "  |  mouse off"
	}
	lineTwoStyle := lipgloss.NewStyle().Foreground(shared.C("#94A3B8"))
	if homeError != nil {
		status += "  |  " + conciseError(homeError) + "  |  e details"
		lineTwoStyle = lineTwoStyle.Foreground(shared.C("#FB7185"))
	}
	lineTwo := lineTwoStyle.Render(" " + shared.Truncate(status, width-2))

	usedSlots := max(0, cluster.SlotsTotal-cluster.SlotsAvailable)
	clusterLine := " cluster  connecting..."
	if !cluster.UpdatedAt.IsZero() {
		if width < 90 {
			clusterLine = fmt.Sprintf(" cluster  TM %d  |  slots %d/%d  |  jobs %d run  %d done  %d fail",
				cluster.TaskManagers, usedSlots, cluster.SlotsTotal,
				cluster.JobsRunning, cluster.JobsFinished, cluster.JobsFailed)
		} else {
			clusterLine = fmt.Sprintf(" cluster  task managers %d  |  slots %d/%d used  |  jobs %d running  %d finished  %d canceled  %d failed",
				cluster.TaskManagers, usedSlots, cluster.SlotsTotal,
				cluster.JobsRunning, cluster.JobsFinished, cluster.JobsCanceled, cluster.JobsFailed)
		}
	}
	lineThreeStyle := lipgloss.NewStyle().Foreground(shared.C("#94A3B8")).Background(shared.C("#0F172A"))
	if cluster.JobsFailed > 0 || overview.ClusterErr != nil {
		lineThreeStyle = lineThreeStyle.Foreground(shared.C("#FB7185")).Bold(true)
	}
	return lineOne + "\n" + lineTwo + "\n" + lineThreeStyle.Render(shared.PadRight(shared.Truncate(clusterLine, width), width))
}

func (m Model) renderHealth(width int) string {
	if m.snapshot.JobID == "" {
		return lipgloss.NewStyle().Foreground(shared.C("#64748B")).Render(
			shared.PadRight(shared.Truncate(" health  waiting for a job snapshot", width), width),
		)
	}
	running := 0
	for _, node := range m.snapshot.Nodes {
		if node.State == "RUNNING" {
			running++
		}
	}
	checkpoint, checkpointUnhealthy := checkpointmodule.Health(m.snapshot.Checkpoints, time.Now())
	exceptions := "exceptions 0"
	if summary := m.snapshot.Exceptions; summary.Count > 0 {
		count := fmt.Sprintf("%d", summary.Count)
		if summary.Truncated {
			count += "+"
		}
		exceptions = "exceptions " + count
		if summary.Latest != "" {
			exceptions += ": " + summary.Latest
		}
	}
	vertexLabel := "vertices"
	if width < 100 {
		vertexLabel = "vtx"
		checkpoint = strings.Replace(checkpoint, "checkpoints", "cp", 1)
		checkpoint = strings.Replace(checkpoint, "checkpoint", "cp", 1)
		checkpoint = strings.Replace(checkpoint, " running", " active", 1)
		exceptions = strings.Replace(exceptions, "exceptions", "ex", 1)
	}
	line := fmt.Sprintf(" health  %s  |  vertices %d/%d  |  %s  |  %s",
		m.snapshot.JobState,
		running,
		len(m.snapshot.Nodes),
		checkpoint,
		exceptions,
	)
	line = strings.Replace(line, "vertices", vertexLabel, 1)
	style := lipgloss.NewStyle().Foreground(shared.C("#94A3B8")).Background(shared.C("#0F172A"))
	if m.snapshot.Exceptions.Count > 0 || checkpointUnhealthy {
		style = style.Foreground(shared.C("#FB7185")).Bold(true)
	}
	return style.Render(shared.PadRight(shared.Truncate(line, width), width))
}

func renderFooterRows(width int, style lipgloss.Style, rows ...string) string {
	height := footerHeightAt(width)
	if len(rows) > height {
		rows = rows[:height]
	}
	for len(rows) < height {
		rows = append(rows, "")
	}
	for index, row := range rows {
		rows[index] = style.Render(shared.PadRight(shared.Truncate(row, width), width))
	}
	return strings.Join(rows, "\n")
}

func (m Model) globalFooterKeys(width int) string {
	resume := ""
	if destination := m.resumeDestinationLabel(); destination != "" {
		resume = "  ctrl+o resume " + shared.Truncate(destination, 18)
	}
	if width >= wideFooterThreshold {
		return ": go  1 home" + resume + "  g graph  ? help  q back  ctrl+c quit  ctrl+n focus  ctrl+g sidebar"
	}
	if width >= 80 {
		if destination := m.resumeDestinationLabel(); destination != "" {
			resume = "  ^O resume " + shared.Truncate(destination, 12)
		}
		return ": go  1 home" + resume + "  g graph  ? help  q back  ^C quit  ^N focus  ^G sidebar"
	}
	// Put discovery and navigation first at genuinely narrow widths. The
	// screen-local row still advertises back and current-screen actions.
	if m.parked.active {
		return ": go  1 home  ^O resume  ? help  q back  ^C quit  ^N nav"
	}
	return ": go  1 home  g graph  ? help  q back  ^C quit  ^N nav"
}

func (m Model) renderFooter(width int) string {
	if m.errorDetailOpen {
		keys := " error details  e/ctrl+e/q/esc close  ctrl+c quit "
		style := lipgloss.NewStyle().Foreground(shared.C("#FEE2E2")).Background(shared.C("#991B1B")).Bold(true)
		return renderFooterRows(width, style, keys)
	}
	if m.help.Open() {
		keys := " help  ?/q/esc close  ctrl+c quit "
		style := lipgloss.NewStyle().Foreground(shared.C("#E2E8F0")).Background(shared.C("#6D28D9"))
		return renderFooterRows(width, style, keys)
	}
	if notice := m.activeNotice(); notice != "" {
		style := lipgloss.NewStyle().Foreground(shared.C("#422006")).Background(shared.C("#FBBF24")).Bold(true)
		return renderFooterRows(width, style, " "+notice)
	}
	if m.palette.Open() {
		keys := " alias, job, vertex, or process  |  up/down select  |  enter open  |  esc close "
		style := lipgloss.NewStyle().Foreground(shared.C("#E2E8F0")).Background(shared.C("#111827"))
		return renderFooterRows(width, style, keys)
	}
	mouse := mouseLabel(m.mouseEnabled)
	if m.navigation.Focused() {
		keys := " navigation  enter use  type alias  right/esc content  up/down  ctrl+n content  ctrl+g hide "
		if prefix := m.navigation.Prefix(); prefix != "" {
			keys = " navigation  " + prefix + "▮ → " + strings.Join(m.navigationAliasMatches(prefix), ", ") + "  type next letter  esc back  ctrl+n content  ctrl+g hide "
		}
		style := lipgloss.NewStyle().Foreground(shared.C("#E2E8F0")).Background(shared.C("#6D28D9"))
		return renderFooterRows(width, style, keys)
	}
	if m.navigationOnlyAt(width) {
		keys := " navigation  1 home  enter use  esc/q content  ctrl+n content  ctrl+g hide "
		style := lipgloss.NewStyle().Foreground(shared.C("#94A3B8")).Background(shared.C("#111827"))
		return renderFooterRows(width, style, keys)
	}

	global := m.globalFooterKeys(width)
	screen := m.activeScreen()
	local := screen.footer(m, mouse, m.backDestinationLabel(screen.backFallback))
	if !screen.capturesKeys(m) && m.scopeUpAvailable() {
		local = "⌫ up  " + local
	}
	keys := " " + local + " "
	if screen.capturesKeys(m) {
		style := lipgloss.NewStyle().Foreground(shared.C("#E2E8F0")).Background(shared.C("#111827"))
		if m.mode == modeActions {
			style = style.Foreground(shared.C("#FEE2E2")).Background(shared.C("#991B1B")).Bold(true)
		}
		return renderFooterRows(width, style, keys)
	}
	style := lipgloss.NewStyle().Foreground(shared.C("#94A3B8")).Background(shared.C("#111827"))
	if width >= wideFooterThreshold {
		keys = " " + global + "  |  " + local + " "
		return renderFooterRows(width, style, keys)
	}
	return renderFooterRows(width, style, " "+global+" ", keys)
}

func mouseLabel(enabled bool) string {
	if enabled {
		return "on"
	}
	return "off"
}

func (m Model) selectedNode() (flink.Node, bool) {
	for _, node := range m.snapshot.Nodes {
		if node.ID == m.selected {
			return node, true
		}
	}
	return flink.Node{}, false
}

func (m Model) nodeByID(id string) (flink.Node, bool) {
	for _, node := range m.snapshot.Nodes {
		if node.ID == id {
			return node, true
		}
	}
	return flink.Node{}, false
}

func centerText(value string, width, height int) string {
	value = shared.Truncate(value, width-4)
	top := max(0, height/2)
	lines := make([]string, height)
	for index := range lines {
		if index == top {
			left := max(0, (width-shared.DisplayWidth(value))/2)
			lines[index] = strings.Repeat(" ", left) + value
		}
	}
	return strings.Join(lines, "\n")
}

func abs(value int) int {
	if value < 0 {
		return -value
	}
	return value
}
