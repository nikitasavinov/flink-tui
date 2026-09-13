package shell

import (
	"fmt"
	"strings"

	"charm.land/lipgloss/v2"

	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

// Help owns the universal key-reference overlay.
type Help struct {
	open   bool
	offset int
}

// Open reports whether help currently owns the terminal body.
func (help Help) Open() bool { return help.open }

// Show opens the key reference.
func (help *Help) Show() { help.open, help.offset = true, 0 }

// Close dismisses the key reference.
func (help *Help) Close() { help.open = false }

// Toggle opens or closes the key reference.
func (help *Help) Toggle() {
	if help.open {
		help.Close()
	} else {
		help.Show()
	}
}

// HandleKey scrolls the complete key reference or returns to screen content.
func (help *Help) HandleKey(key string, height int, localHints, resumeDestination string) {
	available := max(1, height-1)
	maximum := max(0, len(helpLines(1, "", localHints, resumeDestination))-available)
	help.offset = shared.Clamp(help.offset, 0, maximum)
	switch key {
	case "?", "q", "esc":
		help.Close()
	case "up", "k":
		help.offset--
	case "down", "j":
		help.offset++
	case "pgup":
		help.offset -= available
	case "pgdown":
		help.offset += available
	case "home":
		help.offset = 0
	case "end":
		help.offset = maximum
	}
	help.offset = shared.Clamp(help.offset, 0, maximum)
}

// Render draws the global contract and the active screen's local hints.
func (help Help) Render(width, height int, screen, localHints, resumeDestination string) string {
	if width <= 0 || height <= 0 {
		return ""
	}
	lines := helpLines(width, screen, localHints, resumeDestination)
	available := max(0, height-1)
	start := shared.Clamp(help.offset, 0, max(0, len(lines)-available))
	end := min(len(lines), start+available)
	visible := append([]string(nil), lines[start:end]...)
	for len(visible) < available {
		visible = append(visible, "")
	}
	status := " q / esc close"
	if len(lines) > available {
		status = fmt.Sprintf(" up/down scroll  home/end  q close  %d-%d/%d", start+1, end, len(lines))
	}
	visible = append(visible, lipgloss.NewStyle().Foreground(shared.C("#94A3B8")).Background(shared.C("#111827")).Render(
		shared.PadRight(shared.Truncate(status, width), width)))
	return shared.FitLines(visible, width, height)
}

func helpLines(width int, screen, localHints, resumeDestination string) []string {
	lines := []string{
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#A78BFA")).Render(
			shared.PadRight(shared.Truncate(" HELP  "+screen, width), width)),
		lipgloss.NewStyle().Foreground(shared.C("#94A3B8")).Render(
			shared.PadRight(shared.Truncate(" ctrl+n focus stays available during search. Confirmation locks the keyboard.", width), width)),
		helpSection("GLOBAL", width),
		helpBinding("1", "return to Cluster Overview", width),
	}
	if strings.TrimSpace(resumeDestination) != "" {
		lines = append(lines, helpBinding("ctrl+o", "resume "+resumeDestination, width))
	}
	lines = append(lines,
		helpBinding("g", "return to Job Graph", width),
		helpBinding("< / >", "previous / next peer on this screen", width),
		helpBinding(":", "search by name or sidebar alias", width),
		helpBinding("ctrl+n / ctrl+b", "focus navigation or return to content", width),
		helpBinding("ctrl+g", "show or hide the navigation sidebar", width),
		helpBinding("left", "focus navigation on list screens", width),
		helpBinding("?", "open or close this help", width),
		helpBinding("q / esc", "back or cancel; never quits the process", width),
		helpBinding("ctrl+c", "quit Flink TUI", width),
		helpBinding("r / space", "refresh now / pause automatic refresh", width),
		helpBinding("m", "toggle application mouse capture", width),
		helpSection("THIS SCREEN", width),
	)
	for _, hint := range strings.Split(localHints, "  ") {
		hint = strings.TrimSpace(hint)
		if hint == "" {
			continue
		}
		lines = append(lines, lipgloss.NewStyle().Foreground(shared.C("#CBD5E1")).Render(
			shared.PadRight(shared.Truncate("  • "+hint, width), width)))
	}
	return lines
}

// helpKeyGutterWidth leaves one space after "ctrl+n / ctrl+b", the widest key
// the global section prints.
const helpKeyGutterWidth = 16

func helpSection(label string, width int) string {
	return lipgloss.NewStyle().Bold(true).Foreground(shared.C("#E2E8F0")).Background(shared.C("#111827")).Render(
		shared.PadRight(shared.Truncate(" "+label, width), width))
}

func helpBinding(key, description string, width int) string {
	// The gutter must clear the longest binding it prints; a key that overflows
	// it runs straight into its own description ("ctrl+n / ctrl+bfocus …").
	line := "  " + shared.PadRight(key, helpKeyGutterWidth) + description
	return lipgloss.NewStyle().Foreground(shared.C("#CBD5E1")).Render(
		shared.PadRight(shared.Truncate(line, width), width))
}
