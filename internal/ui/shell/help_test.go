package shell

import (
	"strings"
	"testing"

	"github.com/charmbracelet/x/ansi"
)

func TestHelpScrollMakesEveryScreenHintReachable(t *testing.T) {
	const localHints = "up/down select  enter inspect  / search  F capture flame graph  p profile process  l logs  d thread dump"
	var help Help
	help.Show()
	initial := ansi.Strip(help.Render(80, 20, "Task Managers", localHints, ""))
	if !strings.Contains(initial, "up/down scroll") {
		t.Fatalf("long help does not advertise scrolling:\n%s", initial)
	}
	help.HandleKey("end", 20, localHints, "")
	last := ansi.Strip(help.Render(80, 20, "Task Managers", localHints, ""))
	for _, hint := range strings.Split(localHints, "  ") {
		if !strings.Contains(last, hint) {
			t.Fatalf("end of help hides screen hint %q:\n%s", hint, last)
		}
	}
	help.HandleKey("home", 20, localHints, "")
	if got := ansi.Strip(help.Render(80, 20, "Task Managers", localHints, "")); got != initial {
		t.Fatal("home did not restore the beginning of help")
	}
}

func TestHelpScrollingClampsAndResetsWhenReopened(t *testing.T) {
	var help Help
	help.Show()
	for range 100 {
		help.HandleKey("pgdown", 12, "enter open", "Job Graph")
	}
	end := help.offset
	help.HandleKey("up", 12, "enter open", "Job Graph")
	if help.offset != end-1 {
		t.Fatal("scrolling past the end accumulated invisible offset")
	}
	help.HandleKey("q", 12, "enter open", "Job Graph")
	if help.Open() {
		t.Fatal("q did not close help")
	}
	help.Toggle()
	if !help.Open() || help.offset != 0 {
		t.Fatal("reopened help retained the previous scroll position")
	}
}
