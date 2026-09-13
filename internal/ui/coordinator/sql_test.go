package coordinator

import (
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestSQLNormalModePreservesGlobalKeysAndInsertModeIsExplicit(t *testing.T) {
	model := sqlTestModel(t)
	before := model.sql.State().Text
	model = updateWithKey(model, tea.Key{Code: '1', Text: "1"})
	if model.mode != modeJobs || model.sql.State().Text != before {
		t.Fatalf("normal-mode 1 produced mode=%d text=%q", model.mode, model.sql.State().Text)
	}

	model = sqlTestModel(t)
	model = updateWithKey(model, tea.Key{Code: 'i', Text: "i"})
	for _, character := range "qm1:S" {
		model = updateWithKey(model, tea.Key{Code: character, Text: string(character)})
	}
	if state := model.sql.State(); model.mode != modeSQL || !state.Editing || state.Text != before+"qm1:S" {
		t.Fatalf("SQL insert mode produced mode=%d editing=%t text=%q", model.mode, state.Editing, state.Text)
	}

	model = updateWithKey(model, tea.Key{Code: 'p', Mod: tea.ModCtrl})
	if state := model.sql.State(); model.palette.Open() || state.Text != before+"qm1:S" {
		t.Fatalf("insert-mode ctrl+p produced palette=%t text=%q", model.palette.Open(), state.Text)
	}
	model = updateWithKey(model, tea.Key{Code: tea.KeyEscape})
	if model.sql.State().Editing {
		t.Fatal("escape did not leave SQL insert mode")
	}
}

func TestSQLEditorUsesEnterForNewlineAndF5ForExecution(t *testing.T) {
	model := sqlTestModel(t)
	model = updateWithKey(model, tea.Key{Code: 'i', Text: "i"})
	model = updateWithKey(model, tea.Key{Code: tea.KeyEnter})
	if text := model.sql.State().Text; !strings.HasSuffix(text, "\n") {
		t.Fatalf("enter did not insert a newline: %q", text)
	}

	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyF5}))
	executing := updated.(Model)
	state := executing.sql.State()
	if command == nil || !state.Busy || state.ResultType != "CONNECTING" {
		t.Fatalf("F5 produced command=%v busy=%t status=%q", command, state.Busy, state.ResultType)
	}
}

func TestSQLPasteReachesOnlyFocusedInsertMode(t *testing.T) {
	tests := []struct {
		name      string
		prepare   func(*Model)
		wantPaste bool
	}{
		{name: "insert", wantPaste: true},
		{name: "normal", prepare: func(m *Model) { *m = updateWithKey(*m, tea.Key{Code: tea.KeyEscape}) }},
		{name: "other screen", prepare: func(m *Model) { m.mode = modeJobs }},
		{name: "sidebar", prepare: func(m *Model) { setTestNavigation(m, navigationShown, true) }},
		{name: "help", prepare: func(m *Model) { m.help.Show() }},
		{name: "errors", prepare: func(m *Model) { m.errorDetailOpen = true }},
		{name: "palette", prepare: func(m *Model) { m.openPalette() }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			model := sqlTestModel(t)
			model = updateWithKey(model, tea.Key{Code: 'i', Text: "i"})
			if test.prepare != nil {
				test.prepare(&model)
			}
			before := model.sql.State().Text
			updated, command := model.Update(tea.PasteMsg{Content: "\r\nSELECT 2;"})
			want := before
			if test.wantPaste {
				want += "\nSELECT 2;"
			}
			if text := updated.(Model).sql.State().Text; text != want || command != nil {
				t.Fatalf("paste text=%q command=%v, want %q and no execution", text, command, want)
			}
		})
	}
}

func TestSQLWorkbenchRendersWithinResponsiveShell(t *testing.T) {
	model := sqlTestModel(t)
	for _, width := range []int{60, 80, 140} {
		model.width = width
		model.height = 24
		rendered := model.render()
		if !strings.Contains(rendered, "SQL WORKBENCH") || !strings.Contains(rendered, "SELECT 1") {
			t.Fatalf("width %d missing SQL workbench content", width)
		}
		lines := strings.Split(rendered, "\n")
		if len(lines) != model.height {
			t.Fatalf("width %d rendered %d lines, want %d", width, len(lines), model.height)
		}
		for index, line := range lines {
			if got := lipgloss.Width(line); got > width {
				t.Fatalf("width %d line %d is %d cells", width, index, got)
			}
		}
	}
}

func TestSQLIsAFirstClassNavigationDestination(t *testing.T) {
	model := sqlTestModel(t)
	model.mode = modeGraph

	command := model.executePaletteCommand(commandSQL)
	opened := model
	if state := opened.sql.State(); opened.mode != modeSQL || !state.SessionBusy || command == nil {
		t.Fatalf("SQL palette command produced mode=%d busy=%t command=%v", opened.mode, state.SessionBusy, command)
	}
	if commands := opened.paletteCommands(); !containsPaletteCommand(commands, commandSQL) {
		t.Fatalf("SQL command missing from palette: %#v", commands)
	}
}

func TestSQLWorkbenchTakesEditorFocusWhenOpenedFromNavigation(t *testing.T) {
	model := sqlTestModel(t)
	model.mode = modeJobs
	model.width = 80
	setTestNavigation(&model, navigationShown, true)
	for index, row := range model.navigationRows() {
		if row.Target == navigationSQL {
			setTestNavigationCursor(&model, index)
			break
		}
	}

	updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	opened := updated.(Model)
	if state := opened.sql.State(); command == nil || opened.mode != modeSQL || opened.navigation.Focused() || !state.SessionBusy {
		t.Fatalf("navigation enter produced command=%v mode=%d navigationFocused=%t sessionBusy=%t",
			command, opened.mode, opened.navigation.Focused(), state.SessionBusy)
	}

	before := opened.sql.State().Text
	updated, _ = opened.Update(tea.KeyPressMsg(tea.Key{Code: 'i', Text: "i"}))
	opened = updated.(Model)
	updated, _ = opened.Update(tea.KeyPressMsg(tea.Key{Code: 'x', Text: "x"}))
	if got := updated.(Model).sql.State().Text; got != before+"x" {
		t.Fatalf("first editor key produced SQL %q", got)
	}
}

func sqlTestModel(t *testing.T) Model {
	t.Helper()
	model := interactionTestModel(t)
	client, err := flink.NewSQLGatewayClient("http://localhost:8083")
	if err != nil {
		t.Fatal(err)
	}
	model.configureSQLWorkbench(client)
	model.mode = modeSQL
	return model
}

func containsPaletteCommand(commands []paletteCommand, id commandID) bool {
	for _, command := range commands {
		if command.ID == id {
			return true
		}
	}
	return false
}
