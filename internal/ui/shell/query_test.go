package shell

import "testing"

func TestQueryInputReopensWithReplaceAllEditing(t *testing.T) {
	var input QueryInput
	input.Open()
	for _, key := range []string{"o", "l", "d", "enter"} {
		input.HandleKey(key)
	}
	input.Open()
	if state := input.State(); !state.Open || !state.ReplaceAll || state.Value != "old" {
		t.Fatalf("reopened query state = %#v", state)
	}
	input.HandleKey("n")
	input.HandleKey("e")
	input.HandleKey("w")
	if got := input.Value(); got != "new" {
		t.Fatalf("replacement query = %q, want new", got)
	}
}

func TestQueryInputCancelKeepsValueAndBackspaceClearsSelection(t *testing.T) {
	var input QueryInput
	input.Restore(QueryState{Value: "risk"})
	input.Open()
	input.HandleKey("esc")
	if input.Active() || input.Value() != "risk" {
		t.Fatalf("cancelled query = %#v", input.State())
	}
	input.Open()
	input.HandleKey("backspace")
	if input.Value() != "" || !input.Active() {
		t.Fatalf("selected backspace = %#v", input.State())
	}
}
