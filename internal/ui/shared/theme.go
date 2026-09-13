package shared

import (
	"image/color"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"charm.land/lipgloss/v2"
)

// darkBackground defaults to true so tests and unknown terminals keep the
// designed dark palette. Live terminals update it from COLORFGBG and from
// tea.BackgroundColorMsg.
var darkBackground atomic.Bool

func init() {
	darkBackground.Store(true)
}

// DetectTerminalBackground reads COLORFGBG so the first frame matches a light
// Mac terminal. Tests leave this unset and keep the dark palette.
func DetectTerminalBackground() {
	applyColorFGBG(os.Getenv("COLORFGBG"))
}

// SetDarkBackground selects the dark or light palette.
func SetDarkBackground(dark bool) { darkBackground.Store(dark) }

// DarkBackground reports whether the dark palette is active.
func DarkBackground() bool { return darkBackground.Load() }

// C returns a color that follows the current terminal background. The hex is
// the designed dark-theme value; light terminals receive a mapped pair.
func C(hex string) color.Color {
	return adaptiveColor{hex: hex}
}

type adaptiveColor struct {
	hex string
}

func (tone adaptiveColor) RGBA() (r, g, b, a uint32) {
	hex := tone.hex
	if !darkBackground.Load() {
		if mapped, ok := lightTheme[hex]; ok {
			hex = mapped
		}
	}
	return lipgloss.Color(hex).RGBA()
}

func applyColorFGBG(value string) {
	parts := strings.Split(value, ";")
	if len(parts) < 2 {
		return
	}
	background, err := strconv.Atoi(strings.TrimSpace(parts[len(parts)-1]))
	if err != nil {
		return
	}
	// 7 and 15 are the common light ANSI backgrounds (white / bright white).
	darkBackground.Store(background != 7 && background != 15)
}

// lightTheme remaps designed-for-dark hex values onto a white terminal.
// Unmapped hexes stay as-is (already-dark badge text, notice ink).
var lightTheme = map[string]string{
	"#E2E8F0": "#0F172A",
	"#CBD5E1": "#334155",
	"#94A3B8": "#475569",
	"#C4B5FD": "#5B21B6",
	"#A78BFA": "#5B21B6",
	"#64748B": "#475569",
	"#667085": "#475569",
	"#475569": "#334155",
	"#334155": "#94A3B8",
	"#111827": "#E2E8F0",
	"#0F172A": "#F1F5F9",
	"#1E293B": "#E2E8F0",
	"#34D399": "#047857",
	"#38BDF8": "#0369A1",
	"#67E8F9": "#0E7490",
	"#60A5FA": "#1D4ED8",
	"#40A9FF": "#1D4ED8",
	"#FBBF24": "#B45309",
	"#FB7185": "#DC2626",
	"#FDE68A": "#A16207",
	"#FDE047": "#CA8A04",
}
