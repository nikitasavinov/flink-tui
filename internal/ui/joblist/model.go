package joblist

import (
	"context"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
	shellmodule "github.com/nikitasavinov/flink-tui/internal/ui/shell"
)

const (
	headerHeight          = 3
	footerHeight          = 1
	defaultRequestTimeout = 8 * time.Second
)

// Context contains shell-owned selection and viewport data.
type Context struct {
	CurrentJobID   string
	PreferredJobID string
	BodyHeight     int
}

// Intent asks the application shell to leave the overview.
type Intent uint8

const (
	IntentNone Intent = iota
	IntentGraph
	IntentOpenJob
)

// Result combines optional navigation with a refresh command.
type Result struct {
	Command tea.Cmd
	Intent  Intent
	JobID   string
}

// Message is the sealed family of overview refresh replies.
type Message interface {
	jobListMessage()
}

type refreshMsg struct {
	jobs       []flink.JobSummary
	cluster    flink.ClusterOverview
	err        error
	clusterErr error
	at         time.Time
	generation uint64
}

func (refreshMsg) jobListMessage() {}

// State is a detached snapshot for shell headers and tests.
type State struct {
	Jobs          []flink.JobSummary
	Cluster       flink.ClusterOverview
	Loading       bool
	JobsErr       error
	ClusterErr    error
	UpdatedAt     time.Time
	Generation    uint64
	Selection     int
	View          int
	Query         string
	SearchOpen    bool
	SearchReplace bool
}

// DataSource is the Overview screen's read-only Flink dependency.
type DataSource interface {
	Jobs(context.Context) ([]flink.JobSummary, error)
	ClusterOverview(context.Context) (flink.ClusterOverview, error)
}

// Model owns the Overview screen.
type Model struct {
	client  DataSource
	parent  context.Context
	context Context

	jobs           []flink.JobSummary
	cluster        flink.ClusterOverview
	jobsLoading    bool
	jobsErr        error
	clusterErr     error
	jobsUpdatedAt  time.Time
	jobsGeneration uint64
	jobsPending    bool
	jobSelection   shared.Cursor
	jobView        jobView
	jobFilter      shellmodule.QueryInput
	pendingJobID   string
	pendingBack    bool
}

// New creates an empty overview model.
func New(client DataSource, parent context.Context) Model {
	return Model{client: client, parent: parent}
}

// Sync updates shell-owned context.
func (m *Model) Sync(value Context) { m.context = value }

// Open activates Overview and starts a refresh.
func (m *Model) Open(value Context) tea.Cmd {
	m.Sync(value)
	return m.openJobPicker()
}

// Refresh starts a new generation and clears visible request errors.
func (m *Model) Refresh() tea.Cmd {
	m.jobsLoading = true
	m.jobsErr = nil
	m.clusterErr = nil
	return m.startJobsRefresh()
}

// Poll starts a generation without blanking last-known errors or data.
func (m *Model) Poll() tea.Cmd { return m.startJobsRefresh() }

// ReserveInitialPoll reserves the first refresh before Bubble Tea calls Init
// on a value copy, so ticks cannot supersede that initial request.
func (m *Model) ReserveInitialPoll() { m.jobsPending = true }

// InitialPoll fetches the generation already stored in the model. It is safe
// to call from a value-receiver Bubble Tea Init method because it does not
// mutate a discarded copy before stamping the asynchronous reply.
func (m Model) InitialPoll() tea.Cmd { return m.fetchJobs() }

// HandleKey reduces overview keyboard input.
func (m *Model) HandleKey(key string) Result {
	m.pendingJobID, m.pendingBack = "", false
	if m.jobFilter.Active() {
		if !m.handleJobSearchKey(key) {
			return Result{}
		}
		// Confirming the filter opens the row it narrowed to, so the common
		// "/name enter" motion reaches the job in one pass.
		if command := m.switchToSelectedJob(); m.pendingJobID != "" {
			return Result{Command: command, Intent: IntentOpenJob, JobID: m.pendingJobID}
		}
		return Result{}
	}
	command := m.handleJobKey(key)
	if m.pendingJobID != "" {
		return Result{Command: command, Intent: IntentOpenJob, JobID: m.pendingJobID}
	}
	if m.pendingBack {
		return Result{Command: command, Intent: IntentGraph}
	}
	return Result{Command: command}
}

// HandleClick reduces an absolute terminal mouse click.
func (m *Model) HandleClick(event tea.Mouse) Result {
	m.pendingJobID = ""
	command := m.handleJobMouseClick(event)
	if m.pendingJobID != "" {
		return Result{Command: command, Intent: IntentOpenJob, JobID: m.pendingJobID}
	}
	return Result{Command: command}
}

// HandleWheel moves the visible selection.
func (m *Model) HandleWheel(delta int) { m.moveJobCursor(delta) }

// Apply reduces one generation-checked jobs/cluster reply.
func (m *Model) Apply(message Message) tea.Cmd {
	refresh, ok := message.(refreshMsg)
	if !ok || refresh.generation != m.jobsGeneration {
		return nil
	}
	m.jobsPending = false
	m.jobsLoading = false
	m.jobsErr = refresh.err
	m.clusterErr = refresh.clusterErr
	if refresh.err == nil {
		selectedJobID := m.jobIDAtCursor()
		m.jobs = refresh.jobs
		if selectedJobID != "" {
			m.syncJobCursorTo(selectedJobID)
		} else {
			m.syncJobCursor()
		}
		m.jobsUpdatedAt = refresh.at
	}
	if refresh.clusterErr == nil {
		m.cluster = refresh.cluster
	}
	return nil
}

// Render draws Overview.
func (m Model) Render(width, height int) string { return m.renderJobPicker(width, height) }

// State returns a detached snapshot.
func (m Model) State() State {
	return State{
		Jobs: append([]flink.JobSummary(nil), m.jobs...), Cluster: m.cluster,
		Loading: m.jobsLoading, JobsErr: m.jobsErr, ClusterErr: m.clusterErr,
		UpdatedAt: m.jobsUpdatedAt, Generation: m.jobsGeneration,
		Selection: m.jobSelection.Index(), View: int(m.jobView),
		Query: m.jobFilter.Value(), SearchOpen: m.jobFilter.Active(), SearchReplace: m.jobFilter.State().ReplaceAll,
	}
}

// RestoreState replaces local state while retaining dependencies and context.
func (m *Model) RestoreState(state State) {
	m.jobs = append([]flink.JobSummary(nil), state.Jobs...)
	m.cluster = state.Cluster
	m.jobsLoading = state.Loading
	m.jobsErr = state.JobsErr
	m.clusterErr = state.ClusterErr
	m.jobsUpdatedAt = state.UpdatedAt
	m.jobsGeneration = state.Generation
	m.jobView = jobView(state.View)
	m.jobFilter.Restore(shellmodule.QueryState{Value: state.Query, Open: state.SearchOpen, ReplaceAll: state.SearchReplace})
	m.jobSelection.Set(state.Selection, len(m.jobsForView()))
}

// ViewLabel returns the active tab label for the shell footer.
func (m Model) ViewLabel() string { return m.jobView.label() }

// CapturesKeys reports whether the visible job-filter field owns printable input.
func (m Model) CapturesKeys() bool { return m.jobFilter.Active() }

// SelectedJob returns the visible job at the current cursor.
func (m Model) SelectedJob() (flink.JobSummary, bool) { return m.jobAtCursor() }

// SelectPeer selects the previous or next job in the current tab and filter.
// It wraps at the ends so repeated lateral navigation can scan the whole view.
func (m *Model) SelectPeer(currentJobID string, delta int) (flink.JobSummary, flink.JobSummary, bool) {
	jobs := m.jobsForView()
	if len(jobs) < 2 || delta == 0 {
		return flink.JobSummary{}, flink.JobSummary{}, false
	}
	current := -1
	for index, job := range jobs {
		if job.ID == currentJobID {
			current = index
			break
		}
	}
	if current < 0 {
		return flink.JobSummary{}, flink.JobSummary{}, false
	}
	next := (current + delta%len(jobs) + len(jobs)) % len(jobs)
	m.jobSelection.Set(next, len(jobs))
	return jobs[current], jobs[next], next != current
}

// SelectedPosition reports the one-based position of the selected job in the
// currently visible tab and filter. It uses the same row set as SelectPeer so
// shell notices cannot drift from what the operator is walking.
func (m Model) SelectedPosition() (int, int, bool) {
	jobs := m.jobsForView()
	index := m.jobSelection.Index()
	if index < 0 || index >= len(jobs) {
		return 0, len(jobs), false
	}
	return index + 1, len(jobs), true
}

func (m Model) fetchJobs() tea.Cmd {
	client := m.client
	generation := m.jobsGeneration
	return func() tea.Msg {
		ctx, cancel := m.requestTimeout(defaultRequestTimeout)
		defer cancel()
		type jobsResult struct {
			jobs []flink.JobSummary
			err  error
		}
		type clusterResult struct {
			cluster flink.ClusterOverview
			err     error
		}
		jobsChannel := make(chan jobsResult, 1)
		clusterChannel := make(chan clusterResult, 1)
		go func() {
			jobs, err := client.Jobs(ctx)
			jobsChannel <- jobsResult{jobs: jobs, err: err}
		}()
		go func() {
			cluster, err := client.ClusterOverview(ctx)
			clusterChannel <- clusterResult{cluster: cluster, err: err}
		}()
		jobs := <-jobsChannel
		cluster := <-clusterChannel
		return refreshMsg{jobs: jobs.jobs, cluster: cluster.cluster, err: jobs.err, clusterErr: cluster.err, at: time.Now(), generation: generation}
	}
}

func (m Model) requestTimeout(timeout time.Duration) (context.Context, context.CancelFunc) {
	parent := m.parent
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, timeout)
}

func (m Model) bodyHeight() int {
	if m.context.BodyHeight > 0 {
		return m.context.BodyHeight
	}
	return 20
}

func conciseError(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
