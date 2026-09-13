package shell

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"syscall"
	"time"

	"charm.land/lipgloss/v2"

	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

// RefreshBadge is the operator-facing connection state derived from refresh
// history rather than from a single request result.
type RefreshBadge struct {
	Label string
	Color string
}

// DeriveRefreshBadge distinguishes initial connection, stale last-good data,
// partial data, and an intentionally paused refresh loop.
func DeriveRefreshBadge(now, lastSuccess time.Time, refreshInterval time.Duration, currentErr error, degraded, paused bool) RefreshBadge {
	age := time.Duration(0)
	if !lastSuccess.IsZero() {
		age = max(time.Duration(0), now.Sub(lastSuccess))
	}
	if paused {
		label := "PAUSED"
		if !lastSuccess.IsZero() {
			label += " " + ShortAge(age)
		}
		return RefreshBadge{Label: label, Color: "#FBBF24"}
	}
	if currentErr != nil {
		disconnectAfter := max(15*time.Second, 5*refreshInterval)
		if lastSuccess.IsZero() || age >= disconnectAfter {
			return RefreshBadge{Label: "DISCONNECTED", Color: "#FB7185"}
		}
		return RefreshBadge{Label: "STALE " + ShortAge(age), Color: "#FBBF24"}
	}
	if lastSuccess.IsZero() {
		return RefreshBadge{Label: "CONNECTING", Color: "#60A5FA"}
	}
	if age > max(5*time.Second, 2*refreshInterval) {
		return RefreshBadge{Label: "STALE " + ShortAge(age), Color: "#FBBF24"}
	}
	if degraded {
		return RefreshBadge{Label: "DEGRADED", Color: "#FBBF24"}
	}
	return RefreshBadge{Label: "LIVE", Color: "#34D399"}
}

// RenderRefreshBadge styles a derived status for a terminal header.
func RenderRefreshBadge(badge RefreshBadge) string {
	return lipgloss.NewStyle().Bold(true).Foreground(shared.C(badge.Color)).Render(badge.Label)
}

// ShortAge formats a compact non-negative duration for status chrome.
func ShortAge(age time.Duration) string {
	if age < 0 {
		age = 0
	}
	age = age.Round(time.Second)
	if age < time.Minute {
		return fmt.Sprintf("%ds", int(age.Seconds()))
	}
	if age < time.Hour {
		return fmt.Sprintf("%dm%02ds", int(age.Minutes()), int(age.Seconds())%60)
	}
	return fmt.Sprintf("%dh%02dm", int(age.Hours()), int(age.Minutes())%60)
}

// RequestFailure carries the domain-specific HTTP fields needed by the shell
// without coupling it to a particular API client implementation.
type RequestFailure struct {
	Service    string
	StatusCode int
}

// ConciseError turns a transport failure into one safe header line. The full
// source error remains available to the application's error-detail screen.
func ConciseError(err error, request *RequestFailure) string {
	if err == nil {
		return ""
	}
	if request != nil && request.StatusCode != 0 {
		switch request.StatusCode {
		case http.StatusUnauthorized:
			return request.Service + " authentication failed (401)"
		case http.StatusForbidden:
			return request.Service + " access denied (403)"
		case http.StatusNotFound:
			return request.Service + " endpoint not found (404)"
		case http.StatusTooManyRequests:
			return request.Service + " rate limited requests (429)"
		default:
			if request.StatusCode >= 500 {
				return fmt.Sprintf("%s unavailable (%d)", request.Service, request.StatusCode)
			}
			return fmt.Sprintf("%s request failed (%d)", request.Service, request.StatusCode)
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "request timed out"
	}
	if errors.Is(err, context.Canceled) {
		return "request canceled"
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return "connection refused"
	}
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, io.EOF) {
		return "connection closed"
	}
	var dnsError *net.DNSError
	if errors.As(err, &dnsError) {
		return "DNS lookup failed for " + dnsError.Name
	}
	var verificationError *tls.CertificateVerificationError
	var unknownAuthority x509.UnknownAuthorityError
	var hostnameError x509.HostnameError
	if errors.As(err, &verificationError) || errors.As(err, &unknownAuthority) || errors.As(err, &hostnameError) {
		return "TLS certificate verification failed"
	}
	message := shared.SanitizeLine(err.Error())
	if newline := strings.IndexAny(message, "\r\n"); newline >= 0 {
		message = message[:newline]
	}
	return shared.Truncate(message, 120)
}

// OlderSuccess returns the older timestamp, or zero if either side has never
// succeeded. It is useful when a view is healthy only when both feeds are.
func OlderSuccess(left, right time.Time) time.Time {
	if left.IsZero() || right.IsZero() {
		return time.Time{}
	}
	if left.Before(right) {
		return left
	}
	return right
}

// NewerSuccess returns the more recent timestamp.
func NewerSuccess(left, right time.Time) time.Time {
	if left.After(right) {
		return left
	}
	return right
}

// FirstError returns the first non-nil value.
func FirstError(values ...error) error {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}
