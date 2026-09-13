package sqlworkbench

import (
	"strings"
	"unicode/utf8"

	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

// HandleKey applies a workbench-owned key and reports cross-screen intents to
// the parent coordinator.
func (m *Model) HandleKey(message tea.KeyPressMsg, bodyHeight int) KeyResult {
	key := message.String()
	if key == "ctrl+enter" || key == "f5" {
		return KeyResult{Command: m.executeStatement()}
	}
	if key == "ctrl+x" {
		return KeyResult{Command: m.cancelOperation()}
	}
	if m.editing {
		return m.handleInsertKey(message)
	}
	if key == "tab" {
		if m.focus == FocusEditor {
			m.focus = FocusResults
		} else {
			m.focus = FocusEditor
		}
		return KeyResult{}
	}
	if m.focus == FocusResults {
		switch key {
		case "e", "i", "enter":
			m.focus = FocusEditor
			m.editing = true
		case "up", "k":
			m.moveResultSelection(-1)
		case "down", "j":
			m.moveResultSelection(1)
		case "pgup":
			m.moveResultSelection(-max(1, m.ResultRowsAvailable(bodyHeight)))
		case "pgdown":
			m.moveResultSelection(max(1, m.ResultRowsAvailable(bodyHeight)))
		case "home":
			m.selection.Set(0, len(m.rows))
		case "end":
			m.selection.Set(max(0, len(m.rows)-1), len(m.rows))
		case "ctrl+o":
			return KeyResult{Intent: IntentOverview}
		case ":":
			return KeyResult{Intent: IntentPalette}
		}
		return KeyResult{}
	}

	switch key {
	case "esc":
		return KeyResult{Intent: IntentOverview}
	case "e", "i", "enter":
		m.editing = true
	case "left":
		m.cursor = max(0, m.cursor-1)
	case "right":
		m.cursor = min(len([]rune(m.text)), m.cursor+1)
	case "up":
		m.moveCursorVertical(-1)
	case "down":
		m.moveCursorVertical(1)
	case "home":
		m.cursor = lineStart([]rune(m.text), m.cursor)
	case "end":
		m.cursor = lineEnd([]rune(m.text), m.cursor)
	}
	return KeyResult{}
}

func (m *Model) handleInsertKey(message tea.KeyPressMsg) KeyResult {
	key := message.String()
	switch key {
	case "esc":
		m.editing = false
	case "tab":
		m.editing = false
		m.focus = FocusResults
	case "left":
		m.cursor = max(0, m.cursor-1)
	case "right":
		m.cursor = min(len([]rune(m.text)), m.cursor+1)
	case "up":
		m.moveCursorVertical(-1)
	case "down":
		m.moveCursorVertical(1)
	case "home":
		m.cursor = lineStart([]rune(m.text), m.cursor)
	case "end":
		m.cursor = lineEnd([]rune(m.text), m.cursor)
	case "backspace":
		m.deleteBackward()
	case "delete":
		m.deleteForward()
	case "enter":
		m.insertText("\n")
	case "ctrl+l":
		m.text = ""
		m.cursor = 0
	default:
		keyValue := message.Key()
		if keyValue.Mod&(tea.ModCtrl|tea.ModAlt|tea.ModMeta|tea.ModSuper|tea.ModHyper) == 0 && keyValue.Text != "" {
			m.insertText(keyValue.Text)
		}
	}
	return KeyResult{}
}

// HandleClick changes editor/result focus and selects a visible result row.
// bodyY is relative to the first row below the application header.
func (m *Model) HandleClick(event tea.Mouse, bodyY, bodyHeight int) {
	if event.Button != tea.MouseLeft || bodyY < 0 || bodyY >= bodyHeight {
		return
	}
	editorHeight := editorHeight(bodyHeight)
	if bodyY >= 2 && bodyY < editorHeight {
		m.focus = FocusEditor
		m.editing = true
		return
	}
	m.focus = FocusResults
	m.editing = false
	row := bodyY - editorHeight - resultHeaderRows
	if row >= 0 && row < m.ResultRowsAvailable(bodyHeight) {
		index := m.resultWindowStart(bodyHeight) + row
		if index >= 0 && index < len(m.rows) {
			m.selection.Set(index, len(m.rows))
		}
	}
}

// HandlePaste inserts literal clipboard text only while the editor owns input.
// Terminal paste uses a separate message from ordinary key presses.
func (m *Model) HandlePaste(value string) {
	if !m.editing {
		return
	}
	value = strings.ReplaceAll(value, "\r\n", "\n")
	value = strings.ReplaceAll(value, "\r", "\n")
	m.insertText(value)
}

// HandleWheel focuses the result table and moves its selection.
func (m *Model) HandleWheel(delta int) {
	m.focus = FocusResults
	m.editing = false
	m.moveResultSelection(delta)
}

func (m *Model) insertText(value string) {
	runes := []rune(m.text)
	m.cursor = shared.Clamp(m.cursor, 0, len(runes))
	inserted := []rune(value)
	result := make([]rune, 0, len(runes)+len(inserted))
	result = append(result, runes[:m.cursor]...)
	result = append(result, inserted...)
	result = append(result, runes[m.cursor:]...)
	m.text = string(result)
	m.cursor += len(inserted)
}

func (m *Model) deleteBackward() {
	runes := []rune(m.text)
	if m.cursor <= 0 || len(runes) == 0 {
		return
	}
	m.cursor = min(m.cursor, len(runes))
	runes = append(runes[:m.cursor-1], runes[m.cursor:]...)
	m.text = string(runes)
	m.cursor--
}

func (m *Model) deleteForward() {
	runes := []rune(m.text)
	if m.cursor < 0 || m.cursor >= len(runes) {
		return
	}
	runes = append(runes[:m.cursor], runes[m.cursor+1:]...)
	m.text = string(runes)
}

func (m *Model) moveCursorVertical(delta int) {
	runes := []rune(m.text)
	m.cursor = shared.Clamp(m.cursor, 0, len(runes))
	start := lineStart(runes, m.cursor)
	column := m.cursor - start
	if delta < 0 {
		if start == 0 {
			return
		}
		previousEnd := start - 1
		previousStart := lineStart(runes, previousEnd)
		m.cursor = min(previousStart+column, previousEnd)
		return
	}
	end := lineEnd(runes, m.cursor)
	if end >= len(runes) {
		return
	}
	nextStart := end + 1
	nextEnd := lineEnd(runes, nextStart)
	m.cursor = min(nextStart+column, nextEnd)
}

func lineStart(runes []rune, cursor int) int {
	cursor = shared.Clamp(cursor, 0, len(runes))
	for cursor > 0 && runes[cursor-1] != '\n' {
		cursor--
	}
	return cursor
}

func lineEnd(runes []rune, cursor int) int {
	cursor = shared.Clamp(cursor, 0, len(runes))
	for cursor < len(runes) && runes[cursor] != '\n' {
		cursor++
	}
	return cursor
}

func (m *Model) moveResultSelection(delta int) {
	m.selection.Move(delta, len(m.rows))
}

// ResultRowsAvailable returns the result rows visible at bodyHeight.
func (m Model) ResultRowsAvailable(bodyHeight int) int {
	return resultRowsAvailable(bodyHeight - editorHeight(bodyHeight))
}

func (m Model) resultWindowStart(bodyHeight int) int {
	available := m.ResultRowsAvailable(bodyHeight)
	return shared.WindowStart(m.selection.Index(), available, len(m.rows))
}

func linesAndCursor(value string, cursor int) ([]string, int, int) {
	runes := []rune(value)
	cursor = shared.Clamp(cursor, 0, len(runes))
	before := string(runes[:cursor])
	line := strings.Count(before, "\n")
	column := utf8.RuneCountInString(before)
	if index := strings.LastIndex(before, "\n"); index >= 0 {
		column = utf8.RuneCountInString(before[index+1:])
	}
	lines := strings.Split(value, "\n")
	if len(lines) == 0 {
		lines = []string{""}
	}
	return lines, line, column
}

func insertCursorGlyph(value string, column int) string {
	runes := []rune(value)
	column = shared.Clamp(column, 0, len(runes))
	return string(runes[:column]) + "|" + string(runes[column:])
}
