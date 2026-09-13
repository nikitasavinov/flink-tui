package coordinator

import (
	"context"
	"time"

	tea "charm.land/bubbletea/v2"
)

const defaultRequestTimeout = 8 * time.Second

type requestLifecycle struct {
	context context.Context
	cancel  context.CancelFunc
	// Snapshot refreshes share one request per job generation. Keeping this in
	// the event-loop state prevents slow REST responses from piling up on ticks.
	snapshotPending bool
}

func newRequestLifecycle() requestLifecycle {
	requestContext, cancel := context.WithCancel(context.Background())
	return requestLifecycle{context: requestContext, cancel: cancel}
}

func (m Model) requestTimeout(timeout time.Duration) (context.Context, context.CancelFunc) {
	parent := m.requests.context
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, timeout)
}

func (m Model) cancelInFlightRequests() {
	if m.requests.cancel != nil {
		m.requests.cancel()
	}
}

func (m Model) quit() tea.Cmd {
	m.cancelInFlightRequests()
	cleanup := m.sql.Close()
	if cleanup == nil {
		return tea.Quit
	}
	return tea.Sequence(cleanup, tea.Quit)
}
