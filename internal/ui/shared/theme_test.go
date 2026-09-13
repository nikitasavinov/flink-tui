package shared

import (
	"image/color"
	"testing"

	"charm.land/lipgloss/v2"
)

func TestLightPaletteRemapsPaleTextAndNavyChrome(t *testing.T) {
	t.Cleanup(func() { SetDarkBackground(true) })

	SetDarkBackground(true)
	if !sameRGB(C("#CBD5E1"), lipgloss.Color("#CBD5E1")) {
		t.Fatal("dark body should keep the designed slate")
	}

	SetDarkBackground(false)
	if !sameRGB(C("#CBD5E1"), lipgloss.Color("#334155")) {
		t.Fatal("light body should become ink, not pale slate")
	}
	if !sameRGB(C("#111827"), lipgloss.Color("#E2E8F0")) {
		t.Fatal("light chrome should drop the navy fill")
	}
	if !sameRGB(C("#FFFFFF"), lipgloss.Color("#FFFFFF")) {
		t.Fatal("unmapped white should stay white")
	}
}

func TestColorFGBGDetectsLightMacTerminal(t *testing.T) {
	t.Cleanup(func() { SetDarkBackground(true) })
	applyColorFGBG("0;15")
	if DarkBackground() {
		t.Fatal("COLORFGBG 0;15 should select the light palette")
	}
	applyColorFGBG("15;0")
	if !DarkBackground() {
		t.Fatal("COLORFGBG 15;0 should select the dark palette")
	}
}

func sameRGB(got, want color.Color) bool {
	gr, gg, gb, _ := got.RGBA()
	wr, wg, wb, _ := want.RGBA()
	return gr == wr && gg == wg && gb == wb
}
