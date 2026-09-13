package shared

import "testing"

func TestTaskManagerIdentity(t *testing.T) {
	if got := TaskManagerIdentity(" 172.22.0.3:42213-408ff3 "); got != "172.22.0.3:42213-408ff3" {
		t.Fatalf("identity = %q", got)
	}
	if got := TaskManagerIdentity(" "); got != "TaskManager" {
		t.Fatalf("empty identity = %q", got)
	}
}
