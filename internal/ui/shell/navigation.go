package shell

import (
	"fmt"
	"image/color"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

const (
	// WideNavigationThreshold is the width at which the sidebar appears in
	// automatic mode.
	WideNavigationThreshold = 100
	// FullNavigationWidth caps the sidebar width in split-pane layouts.
	FullNavigationWidth     = 25
	navigationOnlyThreshold = 72
	aliasGutterWidth        = 3
)

// Display controls whether navigation follows the responsive default or is
// explicitly shown or hidden by the operator.
type Display uint8

const (
	DisplayAuto Display = iota
	DisplayShown
	DisplayHidden
)

// Target identifies an application destination without coupling shell state
// to the root screen-mode type.
type Target uint8

const (
	TargetNone Target = iota
	TargetOverview
	TargetGraph
	TargetSubtasks
	TargetFlameGraph
	TargetCheckpoints
	TargetTimeline
	TargetDiagnostics
	TargetMetrics
	TargetAccumulators
	TargetExceptions
	TargetJobConfig
	TargetActions
	TargetTaskManagers
	TargetJobManager
	TargetSQL
	TargetCheckpointOperators
	TargetCheckpointSubtasks
	TargetTaskManagerDetail
	TargetProcessLogs
	TargetDocument
	TargetThreadDump
	TargetProfiler
	TargetProfilerFlameGraph
)

// Row is one group heading or selectable destination in the sidebar.
type Row struct {
	Label    string
	Shortcut string
	Alias    string
	Target   Target
	Group    bool
	Indent   int
	Enabled  bool
	Active   bool
}

// Hint is the alias shown in the shortcut gutter. Digits stay off this
// column so the catalog is one language; they remain hidden accelerators.
func (row Row) Hint() string {
	return row.Alias
}

// Context is the compact operational summary rendered below navigation when
// vertical space permits.
type Context struct {
	JobName          string
	JobState         string
	Uptime           string
	VertexName       string
	HasCheckpoint    bool
	CheckpointID     int64
	CheckpointStatus string
	ProcessLabel     string
	ProcessSlots     int
	ShowProcessSlots bool
	HasProcessSlots  bool
}

// NavigationState is a serializable snapshot used by tests and by the root
// coordinator when it must preserve sidebar focus across a route change.
type NavigationState struct {
	Display        Display
	Focused        bool
	Cursor         int
	DeferredTarget Target
	Prefix         string
}

// Navigation owns responsive sidebar state and selection behavior.
type Navigation struct {
	display        Display
	focused        bool
	cursor         int
	deferredTarget Target
	prefix         string
}

// State returns a copy of the current sidebar state.
func (navigation Navigation) State() NavigationState {
	return NavigationState{
		Display:        navigation.display,
		Focused:        navigation.focused,
		Cursor:         navigation.cursor,
		DeferredTarget: navigation.deferredTarget,
		Prefix:         navigation.prefix,
	}
}

// RestoreState replaces sidebar state and is primarily useful at coordinator
// boundaries and in interaction tests.
func (navigation *Navigation) RestoreState(state NavigationState) {
	navigation.display = state.Display
	navigation.focused = state.Focused
	navigation.cursor = state.Cursor
	navigation.deferredTarget = state.DeferredTarget
	navigation.prefix = state.Prefix
}

// DeferTarget remembers a job-scoped destination while the highlighted job is
// loading. A later snapshot consumes it exactly once.
func (navigation *Navigation) DeferTarget(target Target) { navigation.deferredTarget = target }

// TakeDeferredTarget returns and clears the destination waiting on a snapshot.
func (navigation *Navigation) TakeDeferredTarget() Target {
	target := navigation.deferredTarget
	navigation.deferredTarget = TargetNone
	return target
}

// ClearDeferredTarget cancels navigation that was waiting on a snapshot.
func (navigation *Navigation) ClearDeferredTarget() { navigation.deferredTarget = TargetNone }

// Focused reports whether keyboard input belongs to the sidebar.
func (navigation Navigation) Focused() bool { return navigation.focused }

// Prefix is the partially typed sidebar alias, if more than one route still
// matches it.
func (navigation Navigation) Prefix() string { return navigation.prefix }

// Blur returns keyboard ownership to screen content.
func (navigation *Navigation) Blur() { navigation.focused = false }

// VisibleAt reports whether navigation is visible at width.
func (navigation Navigation) VisibleAt(width int) bool {
	switch navigation.display {
	case DisplayShown:
		return true
	case DisplayHidden:
		return false
	default:
		return width >= WideNavigationThreshold
	}
}

// OnlyAt reports whether the narrow layout dedicates the whole body to the
// navigation overlay.
func (navigation Navigation) OnlyAt(width int) bool {
	return navigation.VisibleAt(width) && width < navigationOnlyThreshold
}

// WidthAt returns the number of terminal cells reserved for navigation.
func (navigation Navigation) WidthAt(width int) int {
	if !navigation.VisibleAt(width) {
		return 0
	}
	if navigation.OnlyAt(width) {
		return width
	}
	return min(FullNavigationWidth, max(20, width/4))
}

// ContentWidthAt returns the terminal cells left for the active screen.
func (navigation Navigation) ContentWidthAt(width int) int {
	navigationWidth := navigation.WidthAt(width)
	if navigationWidth == 0 {
		return width
	}
	if navigationWidth >= width {
		return 0
	}
	return max(1, width-navigationWidth-1)
}

// ContentStartX returns the first terminal column owned by screen content.
func (navigation Navigation) ContentStartX(width int) int {
	navigationWidth := navigation.WidthAt(width)
	if navigationWidth == 0 || navigationWidth >= width {
		return 0
	}
	return navigationWidth + 1
}

// Resize keeps the pane that owns keyboard input visible across responsive
// boundaries. A focused sidebar stays open; a sidebar beside active content
// collapses when it would otherwise cover that content.
func (navigation *Navigation) Resize(width int) {
	if navigation.focused && !navigation.VisibleAt(width) {
		navigation.display = DisplayShown
	} else if !navigation.focused && navigation.OnlyAt(width) {
		navigation.ReturnToContent(width)
	}
}

// ToggleFocus moves keyboard ownership between navigation and content. On a
// narrow terminal, returning to content also dismisses the full-screen
// navigation overlay; split-pane navigation remains visible.
func (navigation *Navigation) ToggleFocus(width int, rows []Row) {
	if navigation.focused {
		navigation.ReturnToContent(width)
		return
	}
	navigation.Focus(width, rows)
}

// ToggleVisibility explicitly hides or shows navigation. A narrow navigation
// overlay takes focus when shown because no content is visible alongside it;
// split-pane navigation can be shown without stealing keyboard focus.
func (navigation *Navigation) ToggleVisibility(width int, rows []Row) {
	if navigation.VisibleAt(width) {
		navigation.display = DisplayHidden
		navigation.focused = false
		navigation.prefix = ""
		return
	}
	navigation.display = DisplayShown
	navigation.focused = false
	navigation.prefix = ""
	if width < navigationOnlyThreshold {
		navigation.Focus(width, rows)
	}
}

// Focus gives the sidebar keyboard ownership and selects the active route, or
// the first enabled route when there is no active row.
func (navigation *Navigation) Focus(width int, rows []Row) {
	if !navigation.VisibleAt(width) {
		navigation.display = DisplayShown
	}
	navigation.focused = true
	navigation.prefix = ""
	for index, row := range rows {
		if row.Active && row.Enabled && !row.Group {
			navigation.cursor = index
			return
		}
	}
	for index, row := range rows {
		if row.Enabled && !row.Group {
			navigation.cursor = index
			return
		}
	}
}

// Move shifts the selection among enabled rows, wrapping at either edge.
func (navigation *Navigation) Move(delta int, rows []Row) {
	navigation.prefix = ""
	selectable := make([]int, 0, len(rows))
	current := -1
	for index, row := range rows {
		if row.Group || !row.Enabled {
			continue
		}
		if index == navigation.cursor {
			current = len(selectable)
		}
		selectable = append(selectable, index)
	}
	if len(selectable) == 0 {
		return
	}
	if current < 0 {
		current = 0
	}
	current = (current + delta + len(selectable)) % len(selectable)
	navigation.cursor = selectable[current]
}

// MoveToEdge selects the first or last enabled destination.
func (navigation *Navigation) MoveToEdge(last bool, rows []Row) {
	navigation.prefix = ""
	for index := range rows {
		candidate := index
		if last {
			candidate = len(rows) - 1 - index
		}
		if rows[candidate].Enabled && !rows[candidate].Group {
			navigation.cursor = candidate
			return
		}
	}
}

// ReturnToContent closes a narrow overlay and otherwise leaves the visible
// sidebar in place without keyboard focus.
func (navigation *Navigation) ReturnToContent(width int) {
	navigationOnly := navigation.OnlyAt(width)
	navigation.focused = false
	navigation.prefix = ""
	if navigationOnly {
		navigation.display = DisplayAuto
	}
}

// DestinationOpened relinquishes keyboard focus and collapses an explicitly
// opened sidebar when the terminal is below the automatic wide threshold.
func (navigation *Navigation) DestinationOpened(width int) {
	navigation.focused = false
	navigation.prefix = ""
	if width < WideNavigationThreshold && navigation.display == DisplayShown {
		navigation.display = DisplayAuto
	}
}

// KeyAction describes a sidebar key result for the root coordinator.
type KeyAction uint8

const (
	KeyNone KeyAction = iota
	KeyNavigate
	KeyUnavailable
	KeyReturnToContent
)

// KeyResult carries a route when a focused-sidebar key selects one.
type KeyResult struct {
	Action KeyAction
	Target Target
}

// HandleKey applies focused-sidebar selection keys and returns application
// intents without knowing how destinations are opened.
func (navigation *Navigation) HandleKey(key string, rows []Row) KeyResult {
	switch key {
	case "up":
		navigation.Move(-1, rows)
	case "down":
		navigation.Move(1, rows)
	case "k", "j":
		// A vim motion letter that can begin an alias belongs to the alias
		// language first: "j" is the only way to type the documented "jm"
		// Job Manager alias, and losing it silently routed operators to
		// whatever the next letter matched instead.
		if result, consumed := navigation.matchAliasPrefix(key, rows); consumed {
			return result
		}
		if key == "k" {
			navigation.Move(-1, rows)
		} else {
			navigation.Move(1, rows)
		}
	case "home":
		navigation.MoveToEdge(false, rows)
	case "end":
		navigation.MoveToEdge(true, rows)
	case "enter":
		navigation.prefix = ""
		if navigation.cursor >= 0 && navigation.cursor < len(rows) {
			row := rows[navigation.cursor]
			if row.Enabled && !row.Group {
				return KeyResult{Action: KeyNavigate, Target: row.Target}
			}
		}
	case "q":
		// q closes navigation on its own, but completes the advertised SQL
		// alias when the operator has already typed s.
		if navigation.prefix != "" && len(aliasPrefixIndexes(rows, navigation.prefix+key)) > 0 {
			result, _ := navigation.matchAliasPrefix(key, rows)
			return result
		}
		navigation.prefix = ""
		return KeyResult{Action: KeyReturnToContent}
	case "right", "esc":
		navigation.prefix = ""
		return KeyResult{Action: KeyReturnToContent}
	default:
		for _, row := range rows {
			if row.Shortcut != key || row.Group {
				continue
			}
			navigation.prefix = ""
			if !row.Enabled {
				return KeyResult{Action: KeyUnavailable, Target: row.Target}
			}
			return KeyResult{Action: KeyNavigate, Target: row.Target}
		}
		if aliasKey(key) {
			if result, consumed := navigation.matchAliasPrefix(strings.ToLower(key), rows); consumed {
				return result
			}
		}
	}
	return KeyResult{}
}

func aliasKey(key string) bool {
	if len(key) != 1 {
		return false
	}
	letter := key[0]
	return letter >= 'a' && letter <= 'z' || letter >= 'A' && letter <= 'Z'
}

// matchAliasPrefix consumes one typed letter into the pending alias prefix. It
// opens a destination only once the typed text *equals* an alias, never as
// soon as the prefix happens to be unique. Firing early left the operator's
// remaining documented letters to land on the screen that had just replaced
// theirs: typing "cfg" opened Job Configuration on "cf" and then let "g" jump
// to the Job Graph, and "lg" did the same from a TaskManager log. The second
// return value reports whether the letter belonged to the alias language at
// all, so callers can fall back to their own meaning for it.
func (navigation *Navigation) matchAliasPrefix(letter string, rows []Row) (KeyResult, bool) {
	letter = strings.ToLower(letter)
	candidates := []string{navigation.prefix + letter}
	if navigation.prefix != "" {
		candidates = append(candidates, letter)
	}
	for _, prefix := range candidates {
		indexes := aliasPrefixIndexes(rows, prefix)
		if len(indexes) == 0 {
			continue
		}
		navigation.prefix = prefix
		// An alias that is also the prefix of a longer one still opens on its
		// own spelling: "tm" is Task Managers even though "tmd" exists.
		for _, index := range indexes {
			if strings.EqualFold(rows[index].Alias, prefix) {
				navigation.prefix = ""
				row := rows[index]
				if !row.Enabled {
					return KeyResult{Action: KeyUnavailable, Target: row.Target}, true
				}
				return KeyResult{Action: KeyNavigate, Target: row.Target}, true
			}
		}
		for _, index := range indexes {
			if rows[index].Enabled {
				navigation.cursor = index
				break
			}
		}
		return KeyResult{}, true
	}
	navigation.prefix = ""
	return KeyResult{}, false
}

func aliasPrefixIndexes(rows []Row, prefix string) []int {
	indexes := make([]int, 0, 2)
	for index, row := range rows {
		if row.Group || row.Alias == "" {
			continue
		}
		if strings.HasPrefix(strings.ToLower(row.Alias), prefix) {
			indexes = append(indexes, index)
		}
	}
	return indexes
}

// ClickResult describes a sidebar mouse hit without invoking application
// routing itself.
type ClickResult struct {
	Handled bool
	Target  Target
	Enabled bool
}

// HitTest maps a mouse press to the same viewport used for rendering.
func (navigation Navigation) HitTest(event tea.Mouse, width, bodyHeight, headerHeight int, rows []Row) ClickResult {
	navigationWidth := navigation.WidthAt(width)
	if event.Button != tea.MouseLeft || navigationWidth == 0 || event.X > navigationWidth {
		return ClickResult{}
	}
	// The separator belongs to the shell, not either adjacent view.
	if event.X == navigationWidth {
		return ClickResult{Handled: true}
	}
	position := event.Y - headerHeight
	viewport := navigation.viewport(rows, bodyHeight)
	if viewport.moreAbove {
		position--
	}
	if position < 0 || position >= viewport.count {
		return ClickResult{Handled: true}
	}
	row := rows[viewport.start+position]
	if row.Group {
		return ClickResult{Handled: true}
	}
	return ClickResult{Handled: true, Target: row.Target, Enabled: row.Enabled}
}

// Render renders a fixed-height navigation block.
func (navigation Navigation) Render(width, height int, rows []Row, context Context) string {
	if width <= 0 || height <= 0 {
		return ""
	}
	lines := make([]string, 0, height)
	viewport := navigation.viewport(rows, height)
	if viewport.moreAbove {
		lines = append(lines, renderScrollMarker(width, true))
	}
	for index := viewport.start; index < viewport.start+viewport.count; index++ {
		lines = append(lines, renderRow(rows[index], width, navigation.focused && index == navigation.cursor))
	}
	if viewport.moreBelow {
		lines = append(lines, renderScrollMarker(width, false))
	}

	contextLines := renderContext(width, context)
	compactLines := renderCompactContext(width, context)
	if !viewport.moreAbove && !viewport.moreBelow && viewport.count == len(rows) {
		switch {
		case len(contextLines) > 0 && len(lines)+1+len(contextLines) <= height:
			// With real slack, context belongs beside the navigation map—not
			// exiled to the bottom of the terminal.
			lines = append(lines, renderLine("", width, lipgloss.NewStyle()))
			lines = append(lines, contextLines...)
		case len(compactLines) > 0 && len(lines)+1+len(compactLines) <= height:
			lines = append(lines, renderLine("", width, lipgloss.NewStyle()))
			lines = append(lines, compactLines...)
		case len(compactLines) > 0 && len(lines)+len(compactLines) <= height:
			// Tight columns keep every destination and use the old bottom-edge
			// fallback without spending a row on the separator.
			lines = append(lines, compactLines...)
		}
	}
	for len(lines) < height {
		lines = append(lines, renderLine("", width, lipgloss.NewStyle()))
	}
	if len(lines) > height {
		lines = lines[:height]
	}
	return strings.Join(lines, "\n")
}

type viewport struct {
	start     int
	count     int
	moreAbove bool
	moreBelow bool
}

func (navigation Navigation) viewport(rows []Row, height int) viewport {
	rowCount := len(rows)
	if rowCount == 0 || height <= 0 {
		return viewport{}
	}
	if rowCount <= height {
		return viewport{count: rowCount}
	}

	focus := shared.Clamp(navigation.cursor, 0, rowCount-1)
	if !navigation.focused {
		for index, row := range rows {
			if row.Active && row.Enabled && !row.Group {
				focus = index
				break
			}
		}
	}
	if height <= 2 {
		return viewport{start: focus, count: 1, moreAbove: focus > 0, moreBelow: focus < rowCount-1}
	}

	count := max(1, height-2)
	start := shared.Clamp(focus-count/2, 0, max(0, rowCount-count))
	if start == 0 {
		count = min(rowCount, height-1)
	} else if start+count >= rowCount {
		count = min(rowCount, height-1)
		start = max(0, rowCount-count)
	}
	return viewport{
		start:     start,
		count:     count,
		moreAbove: start > 0,
		moreBelow: start+count < rowCount,
	}
}

func renderScrollMarker(width int, above bool) string {
	label := " v more"
	if above {
		label = " ^ more"
	}
	return renderLine(label, width,
		lipgloss.NewStyle().Foreground(shared.C("#64748B")).Italic(true))
}

func renderRow(row Row, width int, focused bool) string {
	if row.Group {
		return renderLine(" "+row.Label, width,
			lipgloss.NewStyle().Foreground(shared.C("#64748B")).Bold(true))
	}
	marker := "  "
	if row.Active {
		marker = "* "
	}
	if focused {
		marker = "> "
	}
	hint := shared.PadRight(row.Hint(), aliasGutterWidth) + " "
	indent := strings.Repeat("  ", max(0, row.Indent))
	line := marker + hint + indent + row.Label
	style := lipgloss.NewStyle().Foreground(shared.C("#CBD5E1"))
	if !row.Enabled {
		style = style.Foreground(shared.C("#475569"))
	}
	if row.Active {
		style = style.Foreground(shared.C("#A78BFA")).Bold(true)
	}
	if focused {
		style = style.Foreground(shared.C("#FFFFFF")).Background(shared.C("#6D28D9")).Bold(true)
	}
	return renderLine(line, width, style)
}

func renderCompactContext(width int, context Context) []string {
	if context.JobName == "" && context.ProcessLabel == "" {
		return nil
	}
	groupStyle := lipgloss.NewStyle().Foreground(shared.C("#64748B")).Bold(true)
	label := " CONTEXT"
	if context.ProcessLabel != "" {
		value := " " + context.ProcessLabel
		if context.ShowProcessSlots {
			slots := "—"
			if context.HasProcessSlots {
				slots = fmt.Sprintf("%d", context.ProcessSlots)
			}
			value += "  slots " + slots
		}
		return []string{
			renderLine(label, width, groupStyle),
			renderLine(value, width, lipgloss.NewStyle().Foreground(shared.C("#94A3B8"))),
		}
	}
	value := " " + context.JobState
	style := lipgloss.NewStyle().Foreground(stateColor(context.JobState)).Bold(true)
	if context.VertexName != "" {
		suffix := ""
		if context.HasCheckpoint {
			suffix = fmt.Sprintf(" cp#%d", context.CheckpointID)
		}
		prefix := " vtx "
		nameWidth := max(1, width-shared.DisplayWidth(prefix)-shared.DisplayWidth(suffix))
		value = prefix + shared.Truncate(context.VertexName, nameWidth) + suffix
		style = lipgloss.NewStyle().Foreground(shared.C("#94A3B8"))
	} else if context.HasCheckpoint {
		value += fmt.Sprintf(" cp#%d", context.CheckpointID)
	}
	return []string{
		renderLine(label, width, groupStyle),
		renderLine(value, width, style),
	}
}

func renderContext(width int, context Context) []string {
	if context.JobName == "" && context.ProcessLabel == "" {
		return nil
	}
	groupStyle := lipgloss.NewStyle().Foreground(shared.C("#64748B")).Bold(true)
	dimStyle := lipgloss.NewStyle().Foreground(shared.C("#94A3B8"))
	if context.ProcessLabel != "" {
		lines := []string{
			renderLine(" CONTEXT", width, groupStyle),
			renderLine(" "+context.ProcessLabel, width, dimStyle),
		}
		if context.ShowProcessSlots {
			slots := "—"
			if context.HasProcessSlots {
				slots = fmt.Sprintf("%d", context.ProcessSlots)
			}
			lines = append(lines, renderLine(" slots "+slots, width, dimStyle))
		}
		return lines
	}
	stateStyle := lipgloss.NewStyle().Foreground(stateColor(context.JobState)).Bold(true)
	state := " " + context.JobState
	if context.Uptime != "" {
		state += "  " + context.Uptime
	}
	lines := []string{
		renderLine(" CONTEXT", width, groupStyle),
		renderLine(state, width, stateStyle),
	}
	if context.VertexName != "" {
		prefix := " vtx "
		lines = append(lines, renderLine(prefix+shared.Truncate(context.VertexName, max(1, width-shared.DisplayWidth(prefix))), width, dimStyle))
	}
	if context.HasCheckpoint {
		checkpointState := strings.ToLower(context.CheckpointStatus)
		checkpointStyle := lipgloss.NewStyle().Foreground(shared.C("#34D399"))
		switch context.CheckpointStatus {
		case "":
			checkpointState = "completed"
		case "IN_PROGRESS":
			checkpointState = "in progress"
			checkpointStyle = checkpointStyle.Foreground(shared.C("#FBBF24"))
		case "FAILED":
			checkpointState = "failed"
			checkpointStyle = checkpointStyle.Foreground(shared.C("#FB7185"))
		}
		lines = append(lines, renderLine(fmt.Sprintf(" cp #%d  %s", context.CheckpointID, checkpointState), width, checkpointStyle))
	}
	return lines
}

func renderLine(value string, width int, style lipgloss.Style) string {
	return style.Render(shared.PadRight(shared.Truncate(value, width), width))
}

func stateColor(state string) color.Color {
	switch state {
	case "RUNNING", "COMPLETED", "FINISHED":
		return shared.C("#34D399")
	case "FAILED", "FAILING":
		return shared.C("#FB7185")
	case "CANCELED", "CANCELLING":
		return shared.C("#FBBF24")
	default:
		return shared.C("#60A5FA")
	}
}

// JoinNavigationAndContent combines two already-rendered fixed-height panes.
func JoinNavigationAndContent(navigation, content string, height int) string {
	left := fixedLines(navigation, height)
	right := fixedLines(content, height)
	separator := lipgloss.NewStyle().Foreground(shared.C("#334155")).Render("|")
	lines := make([]string, height)
	for index := range lines {
		lines[index] = left[index] + separator + right[index]
	}
	return strings.Join(lines, "\n")
}

func fixedLines(value string, height int) []string {
	lines := strings.Split(value, "\n")
	if len(lines) > height {
		lines = lines[:height]
	}
	for len(lines) < height {
		lines = append(lines, "")
	}
	return lines
}
