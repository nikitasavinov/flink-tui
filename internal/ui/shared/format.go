package shared

import (
	"fmt"
	"image/color"
	"strings"
	"time"
)

// HumanDuration formats elapsed time in seconds, minutes, hours, or days.
func HumanDuration(value time.Duration) string {
	if value <= 0 {
		return "-"
	}
	value = value.Round(time.Second)
	if value < time.Minute {
		return fmt.Sprintf("%ds", int(value.Seconds()))
	}
	if value < time.Hour {
		return fmt.Sprintf("%dm%02ds", int(value.Minutes()), int(value.Seconds())%60)
	}
	if value < 24*time.Hour {
		return fmt.Sprintf("%dh%02dm", int(value.Hours()), int(value.Minutes())%60)
	}
	return fmt.Sprintf("%dd%02dh", int(value.Hours())/24, int(value.Hours())%24)
}

// ShortDuration retains subsecond precision for captures and checkpoints.
func ShortDuration(value time.Duration) string {
	if value < time.Second {
		return fmt.Sprintf("%dms", value.Milliseconds())
	}
	if value < time.Minute {
		return fmt.Sprintf("%.1fs", value.Seconds())
	}
	return value.Round(time.Second).String()
}

// HumanBytes formats a nonnegative byte count using binary units.
func HumanBytes(value int64) string {
	if value <= 0 {
		return "0B"
	}
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	amount := float64(value)
	unit := 0
	for amount >= 1024 && unit < len(units)-1 {
		amount /= 1024
		unit++
	}
	if amount >= 100 || unit == 0 {
		return fmt.Sprintf("%.0f%s", amount, units[unit])
	}
	return fmt.Sprintf("%.1f%s", amount, units[unit])
}

// HumanCount formats a nonnegative counter using decimal units.
func HumanCount(value float64) string {
	if value <= 0 {
		return "0"
	}
	units := []string{"", "k", "M", "B", "T"}
	unit := 0
	for value >= 1000 && unit < len(units)-1 {
		value /= 1000
		unit++
	}
	if value >= 100 || unit == 0 {
		return fmt.Sprintf("%.0f%s", value, units[unit])
	}
	return fmt.Sprintf("%.1f%s", value, units[unit])
}

// ErrorText formats a plain error without discarding its details.
func ErrorText(err error) string {
	if err == nil {
		return ""
	}
	value := strings.TrimSpace(err.Error())
	if value == "" {
		return "unknown error"
	}
	return value
}

// StatusColor returns the terminal color for an execution state.
func StatusColor(state string) color.Color {
	switch state {
	case "RUNNING", "FINISHED":
		return C("#34D399")
	case "FAILED", "CANCELED", "CANCELLING":
		return C("#FB7185")
	case "RESTARTING", "FAILING":
		return C("#FBBF24")
	default:
		return C("#94A3B8")
	}
}

// Timestamp formats a date and time, or a dash when unavailable.
func Timestamp(value time.Time) string {
	if value.IsZero() {
		return "-"
	}
	return value.Format("2006-01-02 15:04:05")
}

// Clock formats a time of day, using missing for unavailable timestamps.
func Clock(value time.Time, missing string) string {
	if value.IsZero() {
		return missing
	}
	return value.Format("15:04:05")
}
