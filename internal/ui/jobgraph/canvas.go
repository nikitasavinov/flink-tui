package jobgraph

import (
	"strings"

	"charm.land/lipgloss/v2"

	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

type styleKind uint8

const (
	styleNormal styleKind = iota
	styleDim
	styleEdge
	styleEdgeLabel
	styleRunning
	styleWarning
	styleFailed
	styleSelected
	styleMetric
	styleBusy
	styleBusyBorder
	styleSelectedBusy
	styleChanged
	styleSelectedChanged
	stylePressureOK
	stylePressureLow
	stylePressureHigh
	stylePressureOKBadge
	stylePressureLowBadge
	stylePressureHighBadge
	styleMinimapBackground
	styleMinimapFrame
	styleMinimapEdge
	styleMinimapNode
	styleMinimapSelected
	styleMinimapViewport
	styleMinimapLow
	styleMinimapHigh
)

type cell struct {
	r     rune
	style styleKind
	// A wide rune claims two terminal columns. It is stored once, in the cell
	// flagged wide, and the column to its right holds a cont cell that the
	// viewport emits nothing for -- so one grid column stays one screen column
	// and the fixed-width node boxes keep their alignment.
	wide bool
	cont bool
}

type canvas struct {
	width  int
	height int
	cells  [][]cell
}

func newCanvas(width, height int) *canvas {
	cells := make([][]cell, height)
	for y := range cells {
		cells[y] = make([]cell, width)
		for x := range cells[y] {
			cells[y][x].r = ' '
		}
	}
	return &canvas{width: width, height: height, cells: cells}
}

func (c *canvas) set(x, y int, r rune, style styleKind) {
	if x < 0 || y < 0 || x >= c.width || y >= c.height {
		return
	}
	c.cells[y][x] = cell{r: r, style: style}
}

func (c *canvas) text(x, y int, value string, style styleKind) {
	for _, r := range shared.SanitizeLine(value) {
		switch shared.DisplayWidth(string(r)) {
		case 0:
			// Combining marks own no column and would desynchronise the grid.
			continue
		case 1:
			c.set(x, y, r, style)
			x++
		default:
			if x+1 >= c.width {
				// No room for the pair; a blank keeps the row aligned.
				c.set(x, y, ' ', style)
				x += 2
				continue
			}
			c.setWide(x, y, r, style)
			x += 2
		}
	}
}

func (c *canvas) setWide(x, y int, r rune, style styleKind) {
	if x < 0 || y < 0 || x+1 >= c.width || y >= c.height {
		return
	}
	c.cells[y][x] = cell{r: r, style: style, wide: true}
	c.cells[y][x+1] = cell{style: style, cont: true}
}

func (c *canvas) viewport(offsetX, offsetY, width, height int) string {
	if width <= 0 || height <= 0 {
		return ""
	}
	styles := terminalStyles()
	var output strings.Builder
	for screenY := 0; screenY < height; screenY++ {
		canvasY := offsetY + screenY
		currentStyle := styleKind(255)
		var segment strings.Builder
		flush := func() {
			if segment.Len() == 0 {
				return
			}
			value := segment.String()
			segment.Reset()
			if style, ok := styles[currentStyle]; ok {
				output.WriteString(style.Render(value))
			} else {
				output.WriteString(value)
			}
		}
		for screenX := 0; screenX < width; screenX++ {
			canvasX := offsetX + screenX
			value := cell{r: ' ', style: styleNormal}
			if canvasX >= 0 && canvasY >= 0 && canvasX < c.width && canvasY < c.height {
				value = c.cells[canvasY][canvasX]
			}
			if value.style != currentStyle {
				flush()
				currentStyle = value.style
			}
			switch {
			case value.cont && screenX == 0:
				// The wide rune owning this column is scrolled off the left
				// edge; a blank keeps the row at its full width.
				segment.WriteRune(' ')
			case value.cont:
				// Already emitted alongside the wide rune to its left.
			case value.wide && screenX == width-1:
				// The second half would overflow the viewport.
				segment.WriteRune(' ')
			default:
				segment.WriteRune(value.r)
			}
		}
		flush()
		if screenY+1 < height {
			output.WriteByte('\n')
		}
	}
	return output.String()
}

func terminalStyles() map[styleKind]lipgloss.Style {
	return map[styleKind]lipgloss.Style{
		styleDim:       lipgloss.NewStyle().Foreground(shared.C("#667085")),
		styleEdge:      lipgloss.NewStyle().Foreground(shared.C("#64748B")),
		styleEdgeLabel: lipgloss.NewStyle().Foreground(shared.C("#CBD5E1")).Bold(true),
		styleRunning:   lipgloss.NewStyle().Foreground(shared.C("#34D399")),
		styleWarning:   lipgloss.NewStyle().Foreground(shared.C("#FBBF24")).Bold(true),
		styleFailed:    lipgloss.NewStyle().Foreground(shared.C("#FB7185")).Bold(true),
		styleSelected:  lipgloss.NewStyle().Foreground(shared.C("#FFFFFF")).Background(shared.C("#6D28D9")).Bold(true),
		styleMetric:    lipgloss.NewStyle().Foreground(shared.C("#67E8F9")),
		styleBusy:      lipgloss.NewStyle().Foreground(shared.C("#40A9FF")).Bold(true),
		styleBusyBorder: lipgloss.NewStyle().
			Foreground(shared.C("#FFFFFF")).
			Background(shared.C("#1677FF")).
			Bold(true),
		styleSelectedBusy: lipgloss.NewStyle().
			Foreground(shared.C("#67E8F9")).
			Background(shared.C("#6D28D9")).
			Bold(true),
		styleChanged: lipgloss.NewStyle().
			Foreground(shared.C("#111827")).
			Background(shared.C("#FDE047")).
			Bold(true),
		styleSelectedChanged: lipgloss.NewStyle().
			Foreground(shared.C("#0F172A")).
			Background(shared.C("#67E8F9")).
			Bold(true),
		stylePressureOK: lipgloss.NewStyle().Foreground(shared.C("#52C41A")).Bold(true),
		stylePressureLow: lipgloss.NewStyle().
			Foreground(shared.C("#111827")).
			Background(shared.C("#FAAD14")).
			Bold(true),
		stylePressureHigh: lipgloss.NewStyle().
			Foreground(shared.C("#FFFFFF")).
			Background(shared.C("#F5222D")).
			Bold(true),
		stylePressureOKBadge:   lipgloss.NewStyle().Foreground(shared.C("#FFFFFF")).Background(shared.C("#52C41A")).Bold(true),
		stylePressureLowBadge:  lipgloss.NewStyle().Foreground(shared.C("#FFFFFF")).Background(shared.C("#FAAD14")).Bold(true),
		stylePressureHighBadge: lipgloss.NewStyle().Foreground(shared.C("#FFFFFF")).Background(shared.C("#F5222D")).Bold(true),
		styleMinimapBackground: lipgloss.NewStyle().Background(shared.C("#0F172A")),
		styleMinimapFrame:      lipgloss.NewStyle().Foreground(shared.C("#94A3B8")).Background(shared.C("#0F172A")).Bold(true),
		styleMinimapEdge:       lipgloss.NewStyle().Foreground(shared.C("#475569")).Background(shared.C("#0F172A")),
		styleMinimapNode:       lipgloss.NewStyle().Foreground(shared.C("#52C41A")).Background(shared.C("#0F172A")).Bold(true),
		styleMinimapSelected:   lipgloss.NewStyle().Foreground(shared.C("#FFFFFF")).Background(shared.C("#6D28D9")).Bold(true),
		styleMinimapViewport:   lipgloss.NewStyle().Foreground(shared.C("#67E8F9")).Background(shared.C("#0F172A")).Bold(true),
		styleMinimapLow:        lipgloss.NewStyle().Foreground(shared.C("#FAAD14")).Background(shared.C("#0F172A")).Bold(true),
		styleMinimapHigh:       lipgloss.NewStyle().Foreground(shared.C("#F5222D")).Background(shared.C("#0F172A")).Bold(true),
	}
}
