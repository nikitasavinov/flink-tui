package sqlworkbench

import (
	"fmt"
	"slices"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

// Apply reduces an asynchronous workbench reply and returns any follow-up
// command. Messages for stale sessions or operations are ignored.
func (m *Model) Apply(message Message) tea.Cmd {
	switch message := message.(type) {
	case sessionMsg:
		return m.applySession(message)
	case executeMsg:
		return m.applyExecute(message)
	case resultMsg:
		return m.applyResult(message)
	case pollMsg:
		return m.applyPoll(message)
	case cancelMsg:
		return m.applyCancel(message)
	default:
		return nil
	}
}

func (m *Model) applySession(message sessionMsg) tea.Cmd {
	if message.generation != m.sessionGeneration {
		return nil
	}
	m.sessionBusy = false
	m.err = message.err
	if message.err != nil {
		if m.pendingStatement != "" {
			m.pendingStatement = ""
			m.busy = false
			m.resultType = "ERROR"
		}
		return nil
	}
	m.session = message.handle
	m.info = message.info
	pending := m.pendingStatement
	if pending == "" {
		return nil
	}
	return m.submitStatement(pending)
}

func (m *Model) applyExecute(message executeMsg) tea.Cmd {
	if message.session != m.session {
		return nil
	}
	m.err = message.err
	if message.err != nil {
		m.busy = false
		m.canceling = false
		m.resultType = "ERROR"
		if message.previousResultType != "" {
			m.operation = message.operation
			m.resultType = message.previousResultType
		}
		return nil
	}
	m.operation = message.operation
	m.resultType = "RUNNING"
	if m.canceling {
		return m.startCancellation()
	}
	return m.fetchResults(message.session, message.operation, "")
}

func (m *Model) applyResult(message resultMsg) tea.Cmd {
	if message.session != m.session || message.operation != m.operation || !m.operationActive() {
		return nil
	}
	m.busy = m.canceling
	m.err = message.err
	if message.err != nil {
		m.resultType = "ERROR"
		m.nextURI = message.requestedURI
		return nil
	}
	result := message.result
	m.resultType = result.ResultType
	m.resultKind = result.ResultKind
	m.jobID = result.JobID
	if len(result.Columns) > 0 {
		m.columns = result.Columns
	}
	followLatest := len(m.rows) == 0 || m.selection.Index() >= len(m.rows)-1
	m.rows = append(m.rows, result.Rows...)
	if len(m.rows) > maxRows {
		dropped := len(m.rows) - maxRows
		m.rows = slices.Clone(m.rows[dropped:])
		m.selection.Set(max(0, m.selection.Index()-dropped), len(m.rows))
	}
	if followLatest && len(m.rows) > 0 {
		m.selection.Set(len(m.rows)-1, len(m.rows))
	}
	nextURI := result.NextURI
	if nextURI == "" && result.ResultType == "NOT_READY" {
		nextURI = message.requestedURI
		if nextURI == "" {
			nextURI = fmt.Sprintf("/v1/sessions/%s/operations/%s/result/0", message.session, message.operation)
		}
	}
	m.nextURI = nextURI
	if nextURI == "" || result.ResultType == "EOS" {
		return nil
	}
	return tea.Tick(pollInterval, func(time.Time) tea.Msg {
		return pollMsg{session: message.session, operation: message.operation, nextURI: nextURI}
	})
}

func (m *Model) applyPoll(message pollMsg) tea.Cmd {
	if message.session != m.session || message.operation != m.operation || !m.operationActive() {
		return nil
	}
	return m.fetchResults(message.session, message.operation, message.nextURI)
}

func (m *Model) applyCancel(message cancelMsg) tea.Cmd {
	if message.session != m.session || message.operation != m.operation {
		return nil
	}
	m.busy = false
	m.canceling = false
	m.err = message.err
	if message.err != nil {
		return nil
	}
	m.resultType = shared.Fallback(message.status, "CANCELED")
	m.nextURI = ""
	return nil
}
