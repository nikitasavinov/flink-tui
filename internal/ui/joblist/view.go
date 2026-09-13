package joblist

import (
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
	shellmodule "github.com/nikitasavinov/flink-tui/internal/ui/shell"
)

const (
	jobTabsRow      = 1
	jobBodyRowStart = 4
)

type jobView int

const (
	jobViewActive jobView = iota
	jobViewCompleted
	jobViewAll
)

func (m *Model) openJobPicker() tea.Cmd {
	m.jobsLoading = true
	m.jobsErr = nil
	m.clusterErr = nil
	m.syncJobCursor()
	return m.startJobsRefresh()
}

func (m *Model) startJobsRefresh() tea.Cmd {
	if m.jobsPending {
		return nil
	}
	m.jobsPending = true
	m.jobsGeneration++
	return m.fetchJobs()
}

func (m *Model) handleJobKey(key string) tea.Cmd {
	switch key {
	case "esc":
		if m.context.CurrentJobID != "" {
			m.pendingBack = true
		}
	case "up", "k":
		m.moveJobCursor(-1)
	case "down", "j":
		m.moveJobCursor(1)
	case "pgup":
		m.moveJobCursor(-max(1, m.jobRowsAvailable()))
	case "pgdown":
		m.moveJobCursor(max(1, m.jobRowsAvailable()))
	case "enter":
		return m.switchToSelectedJob()
	case "tab", "v":
		m.changeJobView(1)
	case "shift+tab":
		m.changeJobView(-1)
	case "/":
		m.jobFilter.Open()
	}
	return nil
}

// handleJobSearchKey reduces one keystroke typed into the job filter. It
// reports whether the operator confirmed the filter, because "type a name and
// press enter" is the shortest path to a job and it should land on that job
// rather than only dismissing the search box.
func (m *Model) handleJobSearchKey(key string) bool {
	action := m.jobFilter.HandleKey(key)
	switch action {
	case shellmodule.QueryConfirmed:
		m.jobSelection.Set(0, 0)
		return true
	}
	m.jobSelection.Set(0, 0)
	return false
}

func (m *Model) switchToSelectedJob() tea.Cmd {
	job, ok := m.jobAtCursor()
	if !ok {
		return nil
	}
	m.pendingJobID = job.ID
	return nil
}

func (m *Model) moveJobCursor(delta int) {
	jobs := m.jobsForView()
	m.jobSelection.Move(delta, len(jobs))
}

func (m *Model) syncJobCursor() {
	jobs := m.jobsForView()
	if len(jobs) == 0 {
		m.jobSelection.Set(0, 0)
		return
	}
	for index, job := range jobs {
		if job.ID == m.context.CurrentJobID || job.ID == m.context.PreferredJobID {
			m.jobSelection.Set(index, len(jobs))
			return
		}
	}
	m.jobSelection.Constrain(len(jobs))
}

func (m Model) jobIDAtCursor() string {
	job, ok := m.jobAtCursor()
	if !ok {
		return ""
	}
	return job.ID
}

func (m *Model) syncJobCursorTo(jobID string) {
	jobs := m.jobsForView()
	if len(jobs) == 0 {
		m.jobSelection.Set(0, 0)
		return
	}
	if jobID != "" {
		for index, job := range jobs {
			if job.ID == jobID {
				m.jobSelection.Set(index, len(jobs))
				return
			}
		}
	}
	m.syncJobCursor()
}

func (m Model) jobRowsAvailable() int {
	return max(1, m.bodyHeight()-jobBodyRowStart-1)
}

func (m Model) jobWindowStart() int {
	jobs := m.jobsForView()
	available := m.jobRowsAvailable()
	return shared.WindowStart(m.jobSelection.Index(), available, len(jobs))
}

func (m *Model) handleJobMouseClick(event tea.Mouse) tea.Cmd {
	if event.Button != tea.MouseLeft {
		return nil
	}
	bodyY := event.Y - headerHeight
	if bodyY == jobTabsRow {
		if view, ok := m.jobViewAt(event.X); ok {
			m.setJobView(view)
		}
		return nil
	}
	position := bodyY - jobBodyRowStart
	if position < 0 || position >= m.jobRowsAvailable() {
		return nil
	}
	index := m.jobWindowStart() + position
	jobs := m.jobsForView()
	if index >= 0 && index < len(jobs) {
		m.jobSelection.Set(index, len(jobs))
		return m.switchToSelectedJob()
	}
	return nil
}

func (m Model) renderJobPicker(width, height int) string {
	running, completed := m.jobCounts()
	title := fmt.Sprintf(" JOBS  %d active  %d completed", running, completed)
	if m.jobsLoading {
		title += "  |  refreshing..."
	}
	lines := []string{
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#E2E8F0")).Render(shared.Truncate(title, width)),
		m.renderJobTabs(width),
		m.renderJobStatus(width),
		m.renderJobColumns(width),
	}
	if m.jobsErr != nil {
		lines[2] = lipgloss.NewStyle().Foreground(shared.C("#FB7185")).Render(
			shared.Truncate(" Could not load jobs: "+conciseError(m.jobsErr), width),
		)
	}
	jobs := m.jobsForView()
	if len(jobs) == 0 && !m.jobsLoading && m.jobsErr == nil {
		if m.jobFilter.Value() != "" && len(m.jobsForViewWithoutFilter()) > 0 {
			lines = append(lines,
				lipgloss.NewStyle().Foreground(shared.C("#FBBF24")).Bold(true).Render(
					shared.Truncate(fmt.Sprintf(" Filter /%s/ hides all %s jobs in this tab.", m.jobFilter.Value(), m.jobView.emptyLabel()), width)),
				shared.Truncate(" Press / to edit it; Ctrl+W clears it.", width),
			)
		} else {
			lines = append(lines, " No "+m.jobView.emptyLabel()+" jobs.")
		}
	}
	start := m.jobWindowStart()
	end := min(len(jobs), start+m.jobRowsAvailable())
	for index := start; index < end; index++ {
		lines = append(lines, m.renderJobRow(jobs[index], index == m.jobSelection.Index(), width))
	}
	return shared.FitLines(lines, width, height)
}

func (m Model) renderJobStatus(width int) string {
	muted := lipgloss.NewStyle().Foreground(shared.C("#94A3B8"))
	if m.jobFilter.Value() == "" && !m.jobFilter.Active() {
		status := " Select a Flink job and press enter to open its execution graph."
		if selected, ok := m.jobAtCursor(); ok {
			status = " Selected Job ID " + selected.ID + "  |  " + selected.Name
		}
		return muted.Render(shared.Truncate(status, width))
	}

	queryWidth := max(1, min(len([]rune(m.jobFilter.Value()))+1, max(6, width/3)))
	query := shared.Truncate(m.jobFilter.Value(), queryWidth)
	marker := "/"
	if m.jobFilter.Active() {
		marker = "|"
	}
	if state := m.jobFilter.State(); state.Open && state.ReplaceAll {
		query = "[" + query + "]"
		marker = "/"
	}
	chip := lipgloss.NewStyle().Foreground(shared.C("#FFFFFF")).Background(shared.C("#6D28D9")).Bold(true).Render(
		" Filter /" + query + marker + " ",
	)
	detail := fmt.Sprintf("  %d matching %s jobs", len(m.jobsForView()), m.jobView.emptyLabel())
	if selected, ok := m.jobAtCursor(); ok && !m.jobFilter.Active() {
		detail = "  Job ID " + selected.ID
	}
	remaining := max(0, width-lipgloss.Width(chip))
	return chip + muted.Render(shared.Truncate(detail, remaining))
}

func (m Model) jobsForView() []flink.JobSummary {
	return m.jobsForViewWithQuery(m.jobFilter.Value())
}

func (m Model) jobsForViewWithoutFilter() []flink.JobSummary {
	return m.jobsForViewWithQuery("")
}

func (m Model) jobsForViewWithQuery(query string) []flink.JobSummary {
	active := m.jobView == jobViewActive
	terms := strings.Fields(strings.ToLower(query))
	jobs := make([]flink.JobSummary, 0, len(m.jobs))
	for _, job := range m.jobs {
		if m.jobView != jobViewAll && jobStateActive(job.State) != active {
			continue
		}
		haystack := strings.ToLower(strings.Join([]string{job.ID, job.Name, job.State, job.Type}, " "))
		matches := true
		for _, term := range terms {
			if !strings.Contains(haystack, term) {
				matches = false
				break
			}
		}
		if matches {
			jobs = append(jobs, job)
		}
	}
	return jobs
}

func (m Model) jobAtCursor() (flink.JobSummary, bool) {
	jobs := m.jobsForView()
	if m.jobSelection.Index() < 0 || m.jobSelection.Index() >= len(jobs) {
		return flink.JobSummary{}, false
	}
	return jobs[m.jobSelection.Index()], true
}

func (m *Model) changeJobView(delta int) {
	viewCount := int(jobViewAll) + 1
	target := jobView((int(m.jobView) + delta%viewCount + viewCount) % viewCount)
	m.setJobView(target)
}

func (m *Model) setJobView(view jobView) {
	if view > jobViewAll || view == m.jobView {
		return
	}
	selectedID := m.jobIDAtCursor()
	m.jobView = view
	m.jobSelection.Set(0, 0)
	m.syncJobCursorTo(selectedID)
}

func (m Model) jobCounts() (active, completed int) {
	for _, job := range m.jobs {
		if jobStateActive(job.State) {
			active++
		} else {
			completed++
		}
	}
	return active, completed
}

func (m Model) renderJobTabs(width int) string {
	active, completed := m.jobCounts()
	counts := []int{active, completed, len(m.jobs)}
	parts := make([]string, 0, len(counts)*2+1)
	parts = append(parts, " ")
	for index, count := range counts {
		view := jobView(index)
		label := m.jobTabLabel(view, count)
		style := lipgloss.NewStyle().Foreground(shared.C("#94A3B8")).Background(shared.C("#111827"))
		if view == m.jobView {
			style = style.Foreground(shared.C("#FFFFFF")).Background(shared.C("#6D28D9")).Bold(true)
		}
		parts = append(parts, style.Render(label), " ")
	}
	line := strings.Join(parts, "")
	return shared.PadRightStyled(shared.TruncateStyled(line, width), width)
}

func (m Model) jobViewAt(x int) (jobView, bool) {
	active, completed := m.jobCounts()
	counts := []int{active, completed, len(m.jobs)}
	start := 1
	for index, count := range counts {
		view := jobView(index)
		end := start + shared.DisplayWidth(m.jobTabLabel(view, count))
		if x >= start && x < end {
			return view, true
		}
		start = end + 1
	}
	return jobViewActive, false
}

func (m Model) jobTabLabel(view jobView, count int) string {
	return fmt.Sprintf(" %s %d ", view.label(), count)
}

func (view jobView) label() string {
	switch view {
	case jobViewCompleted:
		return "Completed"
	case jobViewAll:
		return "All"
	default:
		return "Active"
	}
}

func (view jobView) emptyLabel() string {
	if view == jobViewAll {
		return "available"
	}
	return strings.ToLower(view.label())
}

func (m Model) renderJobColumns(width int) string {
	var line string
	switch {
	case width >= 110:
		nameWidth := max(12, width-67)
		line = "   STATE       " + shared.PadRight("NAME", nameWidth) + " TYPE       STARTED   DURATION   TASKS      JOB ID"
	case width >= 78:
		nameWidth := max(12, width-43)
		line = "   STATE       " + shared.PadRight("NAME", nameWidth) + " STARTED   TASKS      JOB ID"
	default:
		line = "   STATE       NAME"
	}
	return lipgloss.NewStyle().Bold(true).Foreground(shared.C("#94A3B8")).Background(shared.C("#111827")).Render(
		shared.PadRight(shared.Truncate(line, width), width),
	)
}

func (m Model) renderJobRow(job flink.JobSummary, selected bool, width int) string {
	marker := " "
	if job.ID == m.context.CurrentJobID {
		marker = "*"
	}
	cursor := " "
	if selected {
		cursor = ">"
	}
	prefix := cursor + marker + " " + shared.PadRight(shared.Truncate(job.State, 11), 11) + " "
	var line string
	switch {
	case width >= 110:
		nameWidth := max(12, width-67)
		line = prefix + shared.PadRight(shared.Truncate(job.Name, nameWidth), nameWidth) + " " +
			shared.PadRight(shared.Truncate(job.Type, 10), 10) + " " + shared.PadRight(shared.Clock(job.StartedAt, "-"), 9) + " " +
			shared.PadRight(shared.HumanDuration(job.Duration), 10) + " " + fmt.Sprintf("%3d/%-3d  %s", job.RunningTasks, job.TotalTasks, shared.ShortID(job.ID))
	case width >= 78:
		nameWidth := max(12, width-43)
		line = prefix + shared.PadRight(shared.Truncate(job.Name, nameWidth), nameWidth) + " " +
			shared.PadRight(shared.Clock(job.StartedAt, "-"), 9) + " " + fmt.Sprintf("%3d/%-3d  %s", job.RunningTasks, job.TotalTasks, shared.ShortID(job.ID))
	default:
		line = prefix + job.Name
	}
	line = shared.PadRight(shared.Truncate(line, width), width)
	if selected {
		return lipgloss.NewStyle().Foreground(shared.C("#FFFFFF")).Background(shared.C("#6D28D9")).Bold(true).Render(line)
	}
	return lipgloss.NewStyle().Foreground(shared.StatusColor(job.State)).Render(line)
}

func jobStateActive(state string) bool {
	switch state {
	case "RUNNING", "RESTARTING", "INITIALIZING", "CREATED", "FAILING", "CANCELLING", "RECONCILING":
		return true
	default:
		return false
	}
}
