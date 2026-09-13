package shared

import (
	"strings"

	"github.com/charmbracelet/x/ansi"
)

// SanitizeLine replaces characters that would move the terminal cursor with
// spaces, preserving the shape of user-controlled text in a single-line cell.
func SanitizeLine(value string) string {
	return strings.Map(func(r rune) rune {
		if IsLayoutBreaking(r) {
			return ' '
		}
		return r
	}, value)
}

// IsLayoutBreaking reports whether r is a C0, DEL, or C1 control character.
func IsLayoutBreaking(r rune) bool {
	return r < 0x20 || r == 0x7f || (r >= 0x80 && r <= 0x9f)
}

// DisplayWidth reports how many terminal cells value occupies.
func DisplayWidth(value string) int { return ansi.StringWidth(value) }

// Truncate fits plain text into width terminal cells and appends an ellipsis
// when possible. User-controlled control characters are sanitized first.
func Truncate(value string, width int) string {
	if width <= 0 {
		return ""
	}
	value = SanitizeLine(value)
	if ansi.StringWidth(value) <= width {
		return value
	}
	if width <= 3 {
		return strings.Repeat(".", width)
	}
	return ansi.Truncate(value, width, "...")
}

// Clip fits plain text into width terminal cells without an ellipsis.
func Clip(value string, width int) string {
	if width <= 0 {
		return ""
	}
	value = SanitizeLine(value)
	if ansi.StringWidth(value) <= width {
		return value
	}
	return ansi.Truncate(value, width, "")
}

// PadRight extends plain text with spaces until it occupies width cells.
func PadRight(value string, width int) string {
	value = SanitizeLine(value)
	missing := width - ansi.StringWidth(value)
	if missing <= 0 {
		return value
	}
	return value + strings.Repeat(" ", missing)
}

// TruncateStyled fits a trusted ANSI-rendered fragment into width cells.
func TruncateStyled(value string, width int) string {
	if width <= 0 {
		return ""
	}
	if ansi.StringWidth(value) <= width {
		return value
	}
	if width <= 3 {
		return strings.Repeat(".", width)
	}
	return ansi.Truncate(value, width, "...")
}

// PadRightStyled extends a trusted ANSI-rendered fragment to width cells.
func PadRightStyled(value string, width int) string {
	missing := width - ansi.StringWidth(value)
	if missing <= 0 {
		return value
	}
	return value + strings.Repeat(" ", missing)
}

// FitLines crops or pads a rendered block to exactly height lines. Blank lines
// are filled to width so background styles and mouse hit areas remain stable.
func FitLines(lines []string, width, height int) string {
	if height <= 0 {
		return ""
	}
	if len(lines) > height {
		lines = lines[:height]
	}
	for len(lines) < height {
		lines = append(lines, "")
	}
	for index, line := range lines {
		if strings.TrimSpace(line) == "" {
			lines[index] = strings.Repeat(" ", max(0, width))
		}
	}
	return strings.Join(lines, "\n")
}

// WrapTrace wraps a multiline trace into fixed rune-width fragments.
func WrapTrace(value string, width int) []string {
	if width <= 0 {
		return nil
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	var result []string
	for _, source := range strings.Split(value, "\n") {
		runes := []rune(strings.TrimRight(source, "\r"))
		if len(runes) == 0 {
			result = append(result, "")
			continue
		}
		for len(runes) > width {
			result = append(result, string(runes[:width]))
			runes = runes[width:]
		}
		result = append(result, string(runes))
	}
	return result
}

// FixedLineSlice crops or pads a block to the requested number of lines.
func FixedLineSlice(lines []string, height int) []string {
	if len(lines) > height {
		lines = lines[:height]
	}
	for len(lines) < height {
		lines = append(lines, "")
	}
	return lines
}
