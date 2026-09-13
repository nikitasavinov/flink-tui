package shared

import "testing"

func TestRequestGateRejectsDuplicateAndSupersededReplies(t *testing.T) {
	var gate RequestGate[string]
	if !gate.Finish(0) {
		t.Fatal("initial unversioned reply was rejected")
	}
	first, started := gate.Begin("first")
	if !started {
		t.Fatal("first request did not start")
	}
	if gate.Finish(0) {
		t.Fatal("unversioned reply was accepted after polling started")
	}
	if id, started := gate.Begin("first"); started || id != first {
		t.Fatal("duplicate request did not coalesce")
	}
	if !gate.Finish(first) || gate.Finish(first) {
		t.Fatal("a request completion was accepted more than once")
	}
	second, _ := gate.Begin("second")
	current, _ := gate.Begin("first")
	if gate.Finish(first) || gate.Finish(second) {
		t.Fatal("old reply accepted after a target round trip")
	}
	if _, started := gate.Begin("first"); started {
		t.Fatal("an old reply released the current request")
	}
	if !gate.Finish(current) {
		t.Fatal("current request reply was rejected")
	}
	outstanding, _ := gate.Begin("pending")
	gate.Reset()
	if gate.Finish(outstanding) {
		t.Fatal("reply was accepted after reset")
	}
	next, started := gate.Begin("pending")
	if !started || next <= outstanding {
		t.Fatal("reset did not invalidate the previous request identifier")
	}
}

func TestRequestGateValueCopiesOwnTheirPendingState(t *testing.T) {
	var previous RequestGate[string]
	requestID, _ := previous.Begin("vertex")
	next := previous
	if !next.Finish(requestID) {
		t.Fatal("copied state rejected the current reply")
	}
	if _, started := previous.Begin("vertex"); started {
		t.Fatal("updating the next model changed the previous model's request state")
	}
	if _, started := next.Begin("vertex"); !started {
		t.Fatal("updated model did not release its request")
	}
}
