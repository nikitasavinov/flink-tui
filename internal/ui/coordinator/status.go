package coordinator

import (
	"errors"
	"strings"
	"time"

	"charm.land/lipgloss/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
	shellmodule "github.com/nikitasavinov/flink-tui/internal/ui/shell"
)

type displayedError struct {
	label string
	err   error
}

func conciseError(err error) string {
	var requestError *flink.RequestError
	if errors.As(err, &requestError) {
		return shellmodule.ConciseError(err, &shellmodule.RequestFailure{
			Service: requestError.Service, StatusCode: requestError.StatusCode,
		})
	}
	return shellmodule.ConciseError(err, nil)
}

func (m Model) currentErrors() []displayedError { return m.activeScreen().errors(m) }

func (m Model) renderErrorDetails(width, height int) string {
	lines := []string{
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#FB7185")).Render(shared.Truncate(" ERROR DETAILS", width)),
		lipgloss.NewStyle().Foreground(shared.C("#94A3B8")).Render(shared.Truncate(" Press e or esc to return. Full request details are retained here.", width)),
		lipgloss.NewStyle().Foreground(shared.C("#334155")).Render(strings.Repeat("-", width)),
	}
	for _, item := range m.currentErrors() {
		lines = append(lines, lipgloss.NewStyle().Bold(true).Foreground(shared.C("#FBBF24")).Render(
			shared.Truncate(" "+item.label, width)))
		for _, line := range shared.WrapTrace(item.err.Error(), max(1, width-2)) {
			lines = append(lines, lipgloss.NewStyle().Foreground(shared.C("#CBD5E1")).Render(
				shared.Truncate("  "+line, width)))
		}
		lines = append(lines, "")
	}
	if len(m.currentErrors()) == 0 {
		lines = append(lines, " No current errors.")
	}
	return shared.FitLines(lines, width, height)
}

func (m *Model) setNotice(message string) {
	m.notice = message
	m.noticeUntil = time.Now().Add(4 * time.Second)
}

func (m Model) activeNotice() string {
	if m.notice == "" || time.Now().After(m.noticeUntil) {
		return ""
	}
	return m.notice
}
