package coordinator

func (m Model) footerHeight() int {
	width := m.width
	if width <= 0 {
		width = 100
	}
	return footerHeightAt(max(40, width))
}

func (m Model) bodyHeight() int { return max(3, m.height-headerHeight-m.footerHeight()) }
