package shell

import "testing"

func TestPaletteRanksObjectNamesAboveGenericDestinationMatches(t *testing.T) {
	commands := []Command{
		{ID: "flame", Label: "Open Vertex Flame Graph", Aliases: []string{"flame"}},
		{ID: "flame-lab", Label: "Flink TUI Flame Lab", Description: "job", SearchPriority: 100},
	}
	palette := Palette{}
	palette.RestoreState(PaletteState{Open: true, Query: "flame"})

	filtered := palette.Filter(commands)
	if len(filtered) != 2 || filtered[0].ID != "flame-lab" {
		t.Fatalf("flame results = %#v, want the named job first", filtered)
	}
}

func TestPaletteKeepsExactShortAliasAboveObjectName(t *testing.T) {
	commands := []Command{
		{ID: "checkpoints", Label: "Open Checkpoints", Aliases: []string{"cp"}},
		{ID: "tcp-job", Label: "TCP Payments", Description: "job", SearchPriority: 100},
	}
	palette := Palette{}
	palette.RestoreState(PaletteState{Open: true, Query: "cp"})

	filtered := palette.Filter(commands)
	if len(filtered) != 2 || filtered[0].ID != "checkpoints" {
		t.Fatalf("cp results = %#v, want the exact alias first", filtered)
	}
}
