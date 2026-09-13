package shared

import (
	"strings"
	"testing"

	"github.com/charmbracelet/x/ansi"
)

func TestPlainTextColumnInvariant(t *testing.T) {
	values := []string{"", "Flink", "a\nb\t", "日本語処理系", "🚀 status", "\x1b[31mred"}
	for _, value := range values {
		for _, width := range []int{1, 3, 8, 24} {
			got := PadRight(Truncate(value, width), width)
			if cells := ansi.StringWidth(got); cells != width {
				t.Fatalf("value %q at width %d occupies %d cells", value, width, cells)
			}
			if strings.ContainsFunc(got, IsLayoutBreaking) {
				t.Fatalf("value %q at width %d retained a control character", value, width)
			}
		}
	}
}

func TestFitLinesAndCursorWindow(t *testing.T) {
	if got := strings.Split(FitLines([]string{"one"}, 5, 3), "\n"); len(got) != 3 || got[1] != "     " {
		t.Fatalf("FitLines returned %#v", got)
	}

	var cursor Cursor
	cursor.Set(2, 5)
	cursor.Move(10, 5)
	if cursor.Index() != 4 {
		t.Fatalf("cursor index = %d, want 4", cursor.Index())
	}
	if start := WindowStart(19, 5, 20); start != 15 {
		t.Fatalf("window start = %d, want 15", start)
	}
}
