package shell

import (
	"slices"
	"strings"

	"charm.land/lipgloss/v2"

	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

// CommandID is the stable application-owned identity of a palette command.
type CommandID string

// Command is one searchable command-palette entry.
type Command struct {
	ID             CommandID
	Label          string
	Shortcut       string
	Description    string
	Aliases        []string
	SearchPriority int
	// Unavailable explains why a visible result cannot be opened. The palette
	// still returns its ID so the coordinator can surface the reason as a
	// normal application notice without teaching this widget about routing.
	Unavailable string
}

// PaletteState is a serializable view of palette interaction state.
type PaletteState struct {
	Open   bool
	Query  string
	Cursor int
}

// Palette owns command filtering and selection independently from routing.
type Palette struct {
	open   bool
	query  string
	cursor int
}

// State returns a copy of palette interaction state.
func (palette Palette) State() PaletteState {
	return PaletteState{Open: palette.open, Query: palette.query, Cursor: palette.cursor}
}

// RestoreState replaces palette state.
func (palette *Palette) RestoreState(state PaletteState) {
	palette.open = state.Open
	palette.query = state.Query
	palette.cursor = state.Cursor
}

// Open reports whether the palette owns keyboard input.
func (palette Palette) Open() bool { return palette.open }

// Show opens a fresh palette session.
func (palette *Palette) Show() {
	palette.open = true
	palette.query = ""
	palette.cursor = 0
}

// Close dismisses the palette.
func (palette *Palette) Close() { palette.open = false }

// Filter returns commands matching every whitespace-separated query term.
func (palette Palette) Filter(commands []Command) []Command {
	query := strings.ToLower(strings.TrimSpace(palette.query))
	if query == "" {
		return commands
	}
	terms := strings.Fields(query)
	type match struct {
		command Command
		tier    int
	}
	matches := make([]match, 0, len(commands))
	for _, command := range commands {
		if tier, ok := commandMatchTier(command, query, terms); ok {
			matches = append(matches, match{command: command, tier: tier})
		}
	}
	slices.SortStableFunc(matches, func(left, right match) int {
		if left.tier != right.tier {
			return right.tier - left.tier
		}
		return right.command.SearchPriority - left.command.SearchPriority
	})
	filtered := make([]Command, len(matches))
	for index, match := range matches {
		filtered[index] = match.command
	}
	return filtered
}

func commandMatchTier(command Command, query string, terms []string) (int, bool) {
	label := strings.ToLower(command.Label)
	// The gutter alias a destination advertises lives in Shortcut, and it is
	// the alias operators actually type. Ranking it below a live object whose
	// path merely contains those letters is what let ":cp" open a TaskManager
	// once one had been fetched, so it joins the alias set here.
	aliases := make([]string, 0, len(command.Aliases)+1)
	for _, alias := range command.Aliases {
		aliases = append(aliases, strings.ToLower(alias))
	}
	if command.Shortcut != "" {
		aliases = append(aliases, strings.ToLower(command.Shortcut))
	}
	aliasText := strings.Join(aliases, " ")
	primary := strings.Join([]string{label, aliasText}, " ")
	full := primary + " " + strings.ToLower(command.Description)
	if !containsEveryTerm(full, terms) {
		return 0, false
	}
	labelTier := 0
	switch {
	case label == query:
		labelTier = 7
	case strings.HasPrefix(label, query):
		labelTier = 5
	case containsEveryTerm(label, terms):
		labelTier = 3
	}
	if labelTier > 0 && command.SearchPriority > 0 {
		labelTier++
	}
	aliasTier := 0
	for _, alias := range aliases {
		switch {
		case alias == query && len(alias) <= 3:
			aliasTier = max(aliasTier, 7)
		case alias == query:
			aliasTier = max(aliasTier, 3)
		case strings.HasPrefix(alias, query):
			aliasTier = max(aliasTier, 2)
		}
	}
	if tier := max(labelTier, aliasTier); tier > 0 {
		return tier, true
	}
	if containsEveryTerm(primary, terms) {
		return 2, true
	}
	return 1, true
}

func containsEveryTerm(value string, terms []string) bool {
	for _, term := range terms {
		if !strings.Contains(value, term) {
			return false
		}
	}
	return true
}

// HandleKey updates search/selection and returns the chosen command on Enter.
func (palette *Palette) HandleKey(key string, commands []Command) (CommandID, bool) {
	switch key {
	case "esc":
		palette.open = false
	case "up", "ctrl+k":
		filtered := palette.Filter(commands)
		if len(filtered) > 0 {
			palette.cursor = (palette.cursor - 1 + len(filtered)) % len(filtered)
		}
	case "down", "ctrl+j":
		filtered := palette.Filter(commands)
		if len(filtered) > 0 {
			palette.cursor = (palette.cursor + 1) % len(filtered)
		}
	case "enter":
		filtered := palette.Filter(commands)
		if len(filtered) == 0 {
			return "", false
		}
		palette.cursor = shared.Clamp(palette.cursor, 0, len(filtered)-1)
		return filtered[palette.cursor].ID, true
	case "backspace":
		runes := []rune(palette.query)
		if len(runes) > 0 {
			palette.query = string(runes[:len(runes)-1])
			palette.cursor = 0
		}
	case "space":
		palette.query += " "
		palette.cursor = 0
	default:
		if len([]rune(key)) == 1 {
			palette.query += key
			palette.cursor = 0
		}
	}
	return "", false
}

// Render renders the searchable command list into a fixed-height block.
func (palette Palette) Render(width, height int, commands []Command) string {
	filtered := palette.Filter(commands)
	lines := []string{
		lipgloss.NewStyle().Bold(true).Foreground(shared.C("#A78BFA")).Render(
			shared.PadRight(shared.Truncate(" COMMAND PALETTE", width), width)),
		lipgloss.NewStyle().Foreground(shared.C("#E2E8F0")).Background(shared.C("#111827")).Render(
			shared.PadRight(shared.Truncate(" > "+palette.query+"|", width), width)),
		lipgloss.NewStyle().Foreground(shared.C("#64748B")).Render(
			shared.PadRight(shared.Truncate(" alias, job, vertex, or process  |  up/down select  |  enter open  |  esc close", width), width)),
	}
	available := max(1, height-len(lines))
	start := shared.WindowStart(palette.cursor, available, len(filtered))
	end := min(len(filtered), start+available)
	if len(filtered) == 0 {
		lines = append(lines, lipgloss.NewStyle().Foreground(shared.C("#94A3B8")).Render(
			shared.PadRight(shared.Truncate(" No matching commands.", width), width)))
	}
	for index := start; index < end; index++ {
		command := filtered[index]
		shortcutWidth := 8
		labelWidth := max(12, min(28, width-shortcutWidth-4))
		line := "  " + shared.PadRight(shared.Truncate(command.Label, labelWidth), labelWidth)
		if width >= 64 {
			descriptionWidth := max(0, width-labelWidth-shortcutWidth-5)
			line += "  " + shared.PadRight(shared.Truncate(command.Description, descriptionWidth), descriptionWidth)
		}
		line += "  " + shared.PadRight(command.Shortcut, shortcutWidth)
		line = shared.PadRight(shared.Truncate(line, width), width)
		style := lipgloss.NewStyle().Foreground(shared.C("#CBD5E1"))
		if command.Unavailable != "" {
			style = style.Foreground(shared.C("#64748B")).Italic(true)
		}
		if index == palette.cursor {
			style = style.Foreground(shared.C("#FFFFFF")).Background(shared.C("#6D28D9")).Bold(true)
		}
		lines = append(lines, style.Render(line))
	}
	return shared.FitLines(lines, width, height)
}
