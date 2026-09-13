package coordinator

import (
	tea "charm.land/bubbletea/v2"
	"github.com/nikitasavinov/flink-tui/internal/flink"
	jobopsmodule "github.com/nikitasavinov/flink-tui/internal/ui/jobops"
	"github.com/nikitasavinov/flink-tui/internal/ui/sqlworkbench"
)

func (m *Model) configureJobOperations() {
	m.jobOperations = jobopsmodule.New(m.client, m.requests.context)
	m.syncJobOperationsContext()
}

func (m Model) jobOperationsContext() jobopsmodule.Context {
	return jobopsmodule.Context{
		JobID: m.snapshot.JobID, JobState: m.snapshot.JobState,
		JobType: m.snapshot.JobType, Scheduler: m.snapshot.Scheduler,
		Generation: m.generation, BodyHeight: m.bodyHeight(),
	}
}

func (m *Model) syncJobOperationsContext() { m.jobOperations.Sync(m.jobOperationsContext()) }

func (m *Model) applyJobOperationsResult(result jobopsmodule.Result) tea.Cmd {
	commands := make([]tea.Cmd, 0, 2)
	if result.Command != nil {
		commands = append(commands, result.Command)
	}
	switch result.Intent {
	case jobopsmodule.IntentGraph:
		m.mode = modeGraph
		m.centerSelection()
	case jobopsmodule.IntentOverview:
		commands = append(commands, m.openJobPicker())
	case jobopsmodule.IntentRefreshSnapshot:
		commands = append(commands, m.fetchSnapshot())
	}
	return tea.Batch(commands...)
}

func (m *Model) applyJobOperationsMessage(message jobopsmodule.Message) tea.Cmd {
	m.syncJobOperationsContext()
	return m.applyJobOperationsResult(m.jobOperations.Apply(message))
}

func (m *Model) openJobConfiguration() tea.Cmd {
	command := m.jobOperations.OpenConfiguration(m.jobOperationsContext())
	if m.snapshot.JobID != "" {
		m.mode = modeJobConfig
	}
	return command
}

func (m *Model) openActions() {
	m.jobOperations.OpenActions(m.jobOperationsContext())
	if m.snapshot.JobID != "" {
		m.mode = modeActions
	}
}

func (m *Model) handleJobConfigKey(key string) tea.Cmd {
	m.syncJobOperationsContext()
	m.jobOperations.Activate(jobopsmodule.ViewConfiguration)
	return m.applyJobOperationsResult(m.jobOperations.HandleKey(key))
}

func (m *Model) handleActionKey(key string) tea.Cmd {
	m.syncJobOperationsContext()
	m.jobOperations.Activate(jobopsmodule.ViewActions)
	return m.applyJobOperationsResult(m.jobOperations.HandleKey(key))
}

func (m *Model) handleJobConfigMouseClick(event tea.Mouse) tea.Cmd {
	m.syncJobOperationsContext()
	m.jobOperations.Activate(jobopsmodule.ViewConfiguration)
	return m.applyJobOperationsResult(m.jobOperations.HandleClick(event))
}

func (m *Model) handleActionMouseClick(event tea.Mouse) tea.Cmd {
	m.syncJobOperationsContext()
	m.jobOperations.Activate(jobopsmodule.ViewActions)
	return m.applyJobOperationsResult(m.jobOperations.HandleClick(event))
}

func (m *Model) moveJobConfigSelection(delta int) {
	m.jobOperations.Activate(jobopsmodule.ViewConfiguration)
	m.jobOperations.HandleWheel(delta)
}

func (m *Model) moveActionSelection(delta int) {
	m.jobOperations.Activate(jobopsmodule.ViewActions)
	m.jobOperations.HandleWheel(delta)
}

func (m *Model) refreshJobOperations(view jobopsmodule.View) tea.Cmd {
	m.syncJobOperationsContext()
	m.jobOperations.Activate(view)
	return m.applyJobOperationsResult(m.jobOperations.Refresh())
}

func (m *Model) fetchActionOperationIfNeeded() tea.Cmd {
	m.jobOperations.Sync(m.jobOperationsContext())
	return m.jobOperations.Poll()
}

func (m Model) renderJobConfiguration(width, height int) string {
	m.jobOperations.Sync(m.jobOperationsContext())
	m.jobOperations.Activate(jobopsmodule.ViewConfiguration)
	return m.jobOperations.Render(width, height)
}

func (m Model) renderActions(width, height int) string {
	m.jobOperations.Sync(m.jobOperationsContext())
	m.jobOperations.Activate(jobopsmodule.ViewActions)
	return m.jobOperations.Render(width, height)
}

// configureSQLWorkbench is the composition root for the SQL feature. Keeping
// the parent request context and error presentation here prevents the child
// package from depending on the application coordinator.
func (m *Model) configureSQLWorkbench(client *flink.SQLGatewayClient) {
	m.sql = sqlworkbench.New(client, m.requests.context, conciseError)
}

func (m *Model) openSQLWorkbench() tea.Cmd {
	m.mode = modeSQL
	return m.sql.Open()
}

func renderSQLScreen(m Model, width, height int) string {
	return m.sql.Render(width, height)
}

func sqlScreenKey(m *Model, message tea.KeyPressMsg) tea.Cmd {
	result := m.sql.HandleKey(message, m.bodyHeight())
	switch result.Intent {
	case sqlworkbench.IntentOverview:
		return m.openJobPicker()
	case sqlworkbench.IntentPalette:
		m.openPalette()
		return result.Command
	default:
		return result.Command
	}
}

func sqlScreenClick(m *Model, event tea.Mouse) tea.Cmd {
	m.sql.HandleClick(event, event.Y-headerHeight, m.bodyHeight())
	return nil
}

func sqlScreenWheel(m *Model, event tea.Mouse) tea.Cmd {
	m.sql.HandleWheel(wheelDelta(event))
	return nil
}

func refreshSQL(m *Model) tea.Cmd { return m.sql.Refresh() }

func sqlScreenHeader(m Model, width int, _ string) string { return m.sql.Header(width) }

func sqlScreenFooter(m Model, _, _ string) string { return m.sql.Footer() }

func sqlScreenErrors(m Model) []displayedError {
	return appendScreenError(nil, "SQL Gateway", m.sql.Error())
}
