package shared

// Cursor owns the bounded position of a simple list selection.
type Cursor struct {
	index int
}

// Index returns the selected list index.
func (cursor Cursor) Index() int { return cursor.index }

// Set selects index and constrains it to the current list length.
func (cursor *Cursor) Set(index, length int) {
	cursor.index = Clamp(index, 0, max(0, length-1))
}

// Move shifts the selection by delta without leaving the list bounds.
func (cursor *Cursor) Move(delta, length int) {
	if delta == 0 || length <= 0 {
		return
	}
	cursor.index = Clamp(cursor.index+delta, 0, length-1)
}

// Constrain brings an existing selection back within the list bounds.
func (cursor *Cursor) Constrain(length int) {
	cursor.index = Clamp(cursor.index, 0, max(0, length-1))
}

// WindowStart centers cursor in a viewport when possible and clamps the
// viewport to the available rows.
func WindowStart(cursor, available, total int) int {
	return Clamp(cursor-available/2, 0, max(0, total-available))
}

// Clamp limits value to the inclusive low and high bounds.
func Clamp(value, low, high int) int { return min(max(value, low), high) }

// ShortID keeps compact identifiers readable in narrow terminal layouts.
func ShortID(value string) string {
	if len(value) <= 8 {
		return value
	}
	return value[:8]
}

// Fallback returns fallbackValue when value is empty.
func Fallback(value, fallbackValue string) string {
	if value == "" {
		return fallbackValue
	}
	return value
}
