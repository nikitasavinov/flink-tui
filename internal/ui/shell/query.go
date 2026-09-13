package shell

import "unicode/utf8"

// QueryAction reports whether a query editor consumed a regular edit,
// confirmed the current value, or closed without activating the selected row.
type QueryAction uint8

const (
	QueryEdited QueryAction = iota
	QueryConfirmed
	QueryCancelled
)

// QueryState is the serializable state of a QueryInput.
type QueryState struct {
	Value      string
	Open       bool
	ReplaceAll bool
}

// QueryInput is the shared, headless editor used by table filters and text
// searches. Feature modules remain responsible for deciding which rows match
// and what Enter does after a query is confirmed.
type QueryInput struct {
	value      string
	open       bool
	replaceAll bool
}

// Open starts editing. An existing value is selected so the first printable
// key replaces it; Backspace and Ctrl+W do the same explicitly.
func (input *QueryInput) Open() {
	input.open = true
	input.replaceAll = input.value != ""
}

// Active reports whether the input currently owns printable keys.
func (input QueryInput) Active() bool { return input.open }

// Value returns the applied query, including while it is being edited.
func (input QueryInput) Value() string { return input.value }

// State returns detached editor state for module snapshots and tests.
func (input QueryInput) State() QueryState {
	return QueryState{Value: input.value, Open: input.open, ReplaceAll: input.replaceAll}
}

// Restore replaces the editor state.
func (input *QueryInput) Restore(state QueryState) {
	input.value = state.Value
	input.open = state.Open
	input.replaceAll = state.ReplaceAll
}

// Reset clears and closes the editor.
func (input *QueryInput) Reset() { *input = QueryInput{} }

// HandleKey reduces one Bubble Tea key name. Callers should route global
// chrome keys such as Ctrl+N before invoking it.
func (input *QueryInput) HandleKey(key string) QueryAction {
	switch key {
	case "enter":
		input.open = false
		input.replaceAll = false
		return QueryConfirmed
	case "esc":
		input.open = false
		input.replaceAll = false
		return QueryCancelled
	case "backspace":
		if input.replaceAll {
			input.value = ""
			input.replaceAll = false
			return QueryEdited
		}
		if input.value != "" {
			_, size := utf8.DecodeLastRuneInString(input.value)
			input.value = input.value[:len(input.value)-size]
		}
	case "ctrl+w":
		input.value = ""
		input.replaceAll = false
	case "space":
		input.replaceSelected()
		input.value += " "
	default:
		if utf8.RuneCountInString(key) == 1 {
			input.replaceSelected()
			input.value += key
		}
	}
	return QueryEdited
}

func (input *QueryInput) replaceSelected() {
	if !input.replaceAll {
		return
	}
	input.value = ""
	input.replaceAll = false
}
