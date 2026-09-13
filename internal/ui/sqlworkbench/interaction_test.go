package sqlworkbench

import (
	"fmt"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestResultClicksMatchRenderedRows(t *testing.T) {
	for _, height := range []int{10, 20, 40} {
		for _, selected := range []int{0, 15, 39} {
			t.Run(fmt.Sprintf("height_%d_selection_%d", height, selected), func(t *testing.T) {
				model := testModel(t)
				model.focus = FocusResults
				model.columns = []flink.SQLColumn{{Name: "value", Type: "STRING"}}
				for row := 0; row < 40; row++ {
					model.rows = append(model.rows, flink.SQLRow{Kind: "INSERT", Fields: []string{fmt.Sprintf("row-%03d", row)}})
				}
				model.selection.Set(selected, len(model.rows))
				visible := 0
				for y, line := range strings.Split(ansi.Strip(model.Render(80, height)), "\n") {
					if !strings.Contains(line, "+I") {
						continue
					}
					visible++
					clicked := model
					clicked.HandleClick(tea.Mouse{Button: tea.MouseLeft}, y, height)
					field := clicked.rows[clicked.selection.Index()].Fields[0]
					if !strings.Contains(line, field) {
						t.Fatalf("click on %q selected %q", line, field)
					}
				}
				if visible != model.ResultRowsAvailable(height) {
					t.Fatalf("page movement uses %d rows, rendering shows %d", model.ResultRowsAvailable(height), visible)
				}
			})
		}
	}
}

func TestEditorKeepsLongLineCursorVisible(t *testing.T) {
	for _, value := range []string{strings.Repeat("long_column + ", 20), strings.Repeat("列名 + ", 20)} {
		model := testModel(t)
		model.text = value + "end"
		model.cursor = len([]rune(model.text))
		model.editing = true
		line := strings.Split(ansi.Strip(model.Render(40, 20)), "\n")[2]
		if !strings.HasSuffix(strings.TrimSpace(line), "end|") {
			t.Fatalf("cursor at end of long line is hidden: %q", line)
		}
		model.HandleKey(tea.KeyPressMsg(tea.Key{Code: tea.KeyHome}), 20)
		line = strings.Split(ansi.Strip(model.Render(40, 20)), "\n")[2]
		if !strings.HasPrefix(line, "   1 | |") {
			t.Fatalf("home did not reveal cursor at beginning: %q", line)
		}
	}
}

func TestPasteInsertsMultilineSQLLiterally(t *testing.T) {
	model := testModel(t)
	before := model.text
	model.HandlePaste("ignored")
	if model.text != before {
		t.Fatal("paste changed SQL outside insert mode")
	}
	model.editing = true
	model.cursor = 0
	model.HandlePaste("SELECT 'q';\r\nSELECT 'm';\rSELECT ':exit';\n")
	prefix := "SELECT 'q';\nSELECT 'm';\nSELECT ':exit';\n"
	if model.text != prefix+before || model.cursor != len([]rune(prefix)) {
		t.Fatalf("paste produced text=%q cursor=%d", model.text, model.cursor)
	}
}

func TestRenderFitsShortAndEmptyRectangles(t *testing.T) {
	model := testModel(t)
	for height := 0; height <= 20; height++ {
		rendered := model.Render(40, height)
		gotHeight := lipgloss.Height(rendered)
		if rendered == "" {
			gotHeight = 0
		}
		if gotHeight != height {
			t.Fatalf("height %d rendered %d lines", height, gotHeight)
		}
		for _, line := range strings.Split(rendered, "\n") {
			if lipgloss.Width(line) > 40 {
				t.Fatalf("height %d rendered overwide line %q", height, line)
			}
		}
	}
}
