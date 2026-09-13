package coordinator

import (
	"testing"

	shellmodule "github.com/nikitasavinov/flink-tui/internal/ui/shell"
)

func TestEveryDisplayedDestinationAliasIsFullyTypeable(t *testing.T) {
	rows := make([]navigationRow, 0, len(destinationCatalog))
	for _, spec := range destinationCatalog {
		rows = append(rows, navigationRow{Alias: spec.alias, Target: spec.target, Enabled: true})
	}
	for _, spec := range destinationCatalog {
		t.Run(spec.label, func(t *testing.T) {
			var navigation shellmodule.Navigation
			letters := []rune(spec.alias)
			for index, letter := range letters {
				result := navigation.HandleKey(string(letter), rows)
				if index < len(letters)-1 && result.Action != shellmodule.KeyNone {
					t.Fatalf("alias %q navigated after %q", spec.alias, string(letters[:index+1]))
				}
				if index == len(letters)-1 && (result.Action != shellmodule.KeyNavigate || result.Target != spec.target) {
					t.Fatalf("alias %q opened %#v, want target %d", spec.alias, result, spec.target)
				}
			}
		})
	}
}

func TestTaskManagerDetailPaletteRetainsPreviousAlias(t *testing.T) {
	for _, alias := range []string{"det", "tmd"} {
		model := parityTestModel(t)
		openPaletteWithQuery(&model, alias+" tm:1")
		results := model.filteredPaletteCommands()
		if len(results) == 0 || results[0].Label != "Task Manager Detail · tm:1" || results[0].Unavailable != "" {
			t.Fatalf("detail alias %q produced %#v", alias, results)
		}
	}
}
