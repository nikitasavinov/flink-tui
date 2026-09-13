package shell

import (
	"syscall"
	"testing"
	"time"
)

func TestRefreshBadgeUsesLastGoodAge(t *testing.T) {
	now := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		lastSuccess time.Time
		err         error
		degraded    bool
		paused      bool
		want        string
	}{
		{name: "initial connection failure", err: syscall.ECONNREFUSED, want: "DISCONNECTED"},
		{name: "fresh", lastSuccess: now.Add(-2 * time.Second), want: "LIVE"},
		{name: "recent failed refresh", lastSuccess: now.Add(-12 * time.Second), err: syscall.ECONNREFUSED, want: "STALE 12s"},
		{name: "prolonged failure", lastSuccess: now.Add(-16 * time.Second), err: syscall.ECONNREFUSED, want: "DISCONNECTED"},
		{name: "partial", lastSuccess: now.Add(-2 * time.Second), degraded: true, want: "DEGRADED"},
		{name: "paused", lastSuccess: now.Add(-12 * time.Second), paused: true, want: "PAUSED 12s"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			badge := DeriveRefreshBadge(now, test.lastSuccess, 3*time.Second, test.err, test.degraded, test.paused)
			if badge.Label != test.want {
				t.Fatalf("badge = %q, want %q", badge.Label, test.want)
			}
		})
	}
}
