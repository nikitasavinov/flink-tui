package jobgraph

import (
	"testing"

	"github.com/charmbracelet/x/ansi"
)

func TestCanvasWideRuneKeepsRowWidth(t *testing.T) {
	const width, height = 20, 1
	canvas := newCanvas(width, height)
	canvas.text(0, 0, "日本語ab", styleNormal)

	if !canvas.cells[0][0].wide {
		t.Fatal("first cell should be marked wide")
	}
	if !canvas.cells[0][1].cont {
		t.Fatal("second cell should be a continuation")
	}
	if got := canvas.cells[0][6].r; got != 'a' {
		t.Fatalf("cell 6 = %q, want 'a'", got)
	}
	if got := ansi.StringWidth(canvas.viewport(0, 0, width, height)); got != width {
		t.Fatalf("viewport width = %d, want %d", got, width)
	}
}

func TestCanvasViewportWidthSurvivesScrolling(t *testing.T) {
	canvas := newCanvas(40, 1)
	canvas.text(0, 0, "日本語処理系のジョブ名", styleNormal)
	for offset := 0; offset < 12; offset++ {
		for _, width := range []int{1, 5, 8, 21} {
			got := ansi.StringWidth(canvas.viewport(offset, 0, width, 1))
			if got != width {
				t.Fatalf("viewport(offset=%d, width=%d) width = %d, want %d", offset, width, got, width)
			}
		}
	}
}

func TestCanvasRejectsControlCharacters(t *testing.T) {
	canvas := newCanvas(12, 1)
	canvas.text(0, 0, "a\nb", styleNormal)
	if got := canvas.cells[0][1].r; got != ' ' {
		t.Fatalf("newline reached the canvas as %q", got)
	}
	if got := ansi.StringWidth(canvas.viewport(0, 0, 12, 1)); got != 12 {
		t.Fatalf("viewport width = %d, want 12", got)
	}
}
