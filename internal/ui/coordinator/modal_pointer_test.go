package coordinator

import (
	"reflect"
	"testing"

	tea "charm.land/bubbletea/v2"
)

func TestModalOverlaysBlockUnderlyingPointerInput(t *testing.T) {
	for _, overlay := range []string{"errors", "confirmation"} {
		for _, event := range []tea.Msg{
			tea.MouseClickMsg{Button: tea.MouseLeft, X: 3, Y: headerHeight + 3},
			tea.MouseWheelMsg{Button: tea.MouseWheelDown, X: 3, Y: headerHeight + 3},
			tea.MouseWheelMsg{Button: tea.MouseWheelDown, X: 100, Y: headerHeight + 3},
		} {
			model := interactionTestModel(t)
			model.width = 140
			setTestNavigation(&model, navigationShown, false)
			if overlay == "errors" {
				model.errorDetailOpen = true
			} else {
				model.mode = modeActions
				state := model.jobOperations.State()
				state.ActionConfirm = true
				model.jobOperations.RestoreState(state)
			}
			beforeNavigation := model.navigation.State()
			beforeViewport := model.graphViewport.State()
			beforeActions := model.jobOperations.State()
			updated, command := model.Update(event)
			actual := updated.(Model)
			if command != nil || actual.mode != model.mode ||
				!reflect.DeepEqual(actual.navigation.State(), beforeNavigation) ||
				!reflect.DeepEqual(actual.graphViewport.State(), beforeViewport) ||
				!reflect.DeepEqual(actual.jobOperations.State(), beforeActions) {
				t.Fatalf("%s overlay allowed %T to change the underlying screen", overlay, event)
			}
		}
	}
}
