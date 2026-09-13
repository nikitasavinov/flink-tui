package shared

// RequestGate coalesces repeated requests for the same target and rejects
// superseded replies. Begin and Finish belong to the UI update goroutine;
// commands carry only the returned identifier and immutable request inputs.
type RequestGate[Key comparable] struct {
	key     Key
	version uint64
	pending bool
}

// Begin reserves a request unless this target already has one in flight.
// A changed target can start immediately and supersedes the previous reply.
func (gate *RequestGate[Key]) Begin(key Key) (uint64, bool) {
	if gate.pending && gate.key == key {
		return gate.version, false
	}
	gate.key = key
	gate.version++
	gate.pending = true
	return gate.version, true
}

// Finish releases the current request and reports whether its reply is current.
// Version zero is accepted only before the first request or reset, allowing an
// initial reply supplied with restored state.
func (gate *RequestGate[Key]) Finish(version uint64) bool {
	if version != gate.version || (!gate.pending && version != 0) {
		return false
	}
	gate.pending = false
	return true
}

// Reset invalidates outstanding replies without reusing an earlier identifier.
func (gate *RequestGate[Key]) Reset() {
	gate.version++
	gate.pending = false
	var zero Key
	gate.key = zero
}
