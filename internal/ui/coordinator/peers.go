package coordinator

import (
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	checkpointmodule "github.com/nikitasavinov/flink-tui/internal/ui/checkpoints"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

type peerDescriptor struct {
	name     string
	position int
	total    int
}

type processPeer struct {
	process    flink.ProcessRef
	descriptor peerDescriptor
}

func (descriptor peerDescriptor) valid() bool {
	return descriptor.position > 0 && descriptor.position <= descriptor.total
}

// setPeerNotice presents every lateral move in one compact language. The
// position and wrap marker are protected; long object names yield space first.
func (m *Model) setPeerNotice(previous, next peerDescriptor, delta int) {
	m.setPeerNoticeWithNote(previous, next, delta, "")
}

func (m *Model) setPeerNoticeWithNote(previous, next peerDescriptor, delta int, note string) {
	if !previous.valid() || !next.valid() {
		return
	}
	wrapped := delta > 0 && next.position <= previous.position || delta < 0 && next.position >= previous.position
	suffix := fmt.Sprintf("  (%d/%d)", next.position, next.total)
	if wrapped {
		suffix = fmt.Sprintf("  (%d/%d, wrapped)", next.position, next.total)
	}
	width := max(40, m.width) - 1 // renderFooter adds one leading cell.
	separator := " → "
	if note = strings.TrimSpace(note); note != "" {
		const noteSeparator = "  · "
		noteWidth := max(0, width-shared.DisplayWidth(separator)-shared.DisplayWidth(suffix)-shared.DisplayWidth(noteSeparator)-2)
		if noteWidth > 0 {
			suffix += noteSeparator + shared.Truncate(note, noteWidth)
		}
	}
	available := max(1, width-shared.DisplayWidth(separator)-shared.DisplayWidth(suffix))
	leftWidth := max(1, available/2)
	rightWidth := max(1, available-leftWidth)
	left := shared.Truncate(strings.TrimSpace(previous.name), leftWidth)
	right := shared.Truncate(strings.TrimSpace(next.name), rightWidth)
	m.setNotice(left + separator + right + suffix)
}

// stepPeer moves laterally within the object scope of the active screen. It
// deliberately bypasses route history: changing the compared object is not a
// drill into another screen.
func (m *Model) stepPeer(delta int) tea.Cmd {
	if isVertexWorkflowMode(m.mode) {
		return m.stepVertexPeer(delta)
	}
	if m.mode == modeCheckpointOperators || m.mode == modeCheckpointSubtasks {
		return m.stepCheckpointPeer(delta)
	}
	if isProcessWorkflowMode(m.mode) {
		return m.stepProcessPeer(delta)
	}
	if target, ok := jobPeerTarget(m.mode); ok {
		return m.stepJobPeer(delta, target)
	}
	if m.mode == modeTaskManagerDetail {
		return m.stepTaskManagerPeer(delta)
	}
	m.setNotice("Peer stepping is unavailable on " + screenModeName(m.mode) + ".")
	return nil
}

func (m *Model) stepProcessPeer(delta int) tea.Cmd {
	if m.mode == modeDocument && !documentUsesInfrastructure(*m) {
		m.setNotice("This document is not backed by a Flink process and has no process peer.")
		return nil
	}
	active := m.activeProcess()
	peers, current := m.processPeers(active)
	if delta == 0 || current < 0 || len(peers) < 2 {
		m.setNotice("No other loaded Flink process is available.")
		return nil
	}
	nextIndex := (current + delta%len(peers) + len(peers)) % len(peers)
	if nextIndex == current {
		m.setNotice("No other loaded Flink process is available.")
		return nil
	}
	previous, next := peers[current], peers[nextIndex]
	if next.process.Kind == flink.ProcessTaskManager {
		m.infrastructure.SelectTaskManagerByID(next.process.TaskManagerID)
	}

	var command tea.Cmd
	note := ""
	switch m.mode {
	case modeProcessLogs:
		command = m.processDiagnostics.RetargetLogs(next.process)
	case modeDocument:
		var ok bool
		command, ok = m.processDiagnostics.RetargetDocument(next.process)
		if !ok {
			m.setNotice("This document is not backed by a Flink process and has no process peer.")
			return nil
		}
	case modeThreadDump:
		command = m.processDiagnostics.RetargetThreadDump(next.process)
	case modeProfiler:
		command = tea.Batch(m.profiler.Retarget(next.process), m.processDiagnostics.RetargetLogs(next.process))
	case modeProfilerFlameGraph:
		m.flameGraphs.ClearProfilerReport()
		m.mode = modeProfiler
		// Opening a report records Profiler as its parent. We are returning to
		// that level for the peer, so consume the now-redundant crumb; otherwise
		// the first q would appear to do nothing.
		if crumb, ok := m.history.Peek(); ok && crumb.mode == modeProfiler {
			_, _ = m.history.Pop()
		}
		command = tea.Batch(m.profiler.Retarget(next.process), m.processDiagnostics.RetargetLogs(next.process))
		note = "report not transferred; opened profiler"
	default:
		m.setNotice("Peer stepping is unavailable on " + screenModeName(m.mode) + ".")
		return nil
	}
	m.setPeerNoticeWithNote(previous.descriptor, next.descriptor, delta, note)
	return command
}

func (m Model) processPeers(active flink.ProcessRef) ([]processPeer, int) {
	managers := m.infrastructure.State().Infrastructure.TaskManagers
	peers := make([]processPeer, 0, len(managers)+2)
	current := -1
	for _, manager := range managers {
		process := flink.TaskManagerProcess(manager.ID)
		if process == active {
			current = len(peers)
		}
		peers = append(peers, processPeer{process: process})
	}
	// A direct subtask/thread jump can precede the first infrastructure fetch.
	// Keep its current worker comparable with the JobManager without pretending
	// that unknown sibling workers have been discovered.
	if active.Kind == flink.ProcessTaskManager && current < 0 && strings.TrimSpace(active.TaskManagerID) != "" {
		current = len(peers)
		peers = append(peers, processPeer{process: active})
	}
	jobManager := flink.JobManagerProcess()
	if active == jobManager {
		current = len(peers)
	}
	peers = append(peers, processPeer{process: jobManager})
	for index := range peers {
		peers[index].descriptor = peerDescriptor{
			name: processPeerName(peers[index].process), position: index + 1, total: len(peers),
		}
	}
	return peers, current
}

func processPeerName(process flink.ProcessRef) string {
	if process.Kind == flink.ProcessTaskManager {
		return shared.TaskManagerIdentity(process.TaskManagerID)
	}
	return "JobManager"
}

func (m *Model) stepCheckpointPeer(delta int) tea.Cmd {
	m.syncCheckpointContext()
	if m.mode == modeCheckpointSubtasks {
		m.checkpoints.Activate(checkpointmodule.ViewSubtasks)
	} else {
		m.checkpoints.Activate(checkpointmodule.ViewOperators)
	}
	result := m.checkpoints.StepPeer(delta)
	if result.Peer != nil {
		change := result.Peer
		m.setPeerNotice(
			peerDescriptor{name: fmt.Sprintf("#%d", change.PreviousID), position: change.PreviousPosition, total: change.Total},
			peerDescriptor{name: fmt.Sprintf("#%d", change.ID), position: change.Position, total: change.Total},
			delta,
		)
	}
	return m.applyCheckpointResult(result)
}

func (m *Model) stepVertexPeer(delta int) tea.Cmd {
	oldNode, oldOK := m.selectedNode()
	oldID := m.selected
	oldIndex := orderIndex(m.layout.Order, oldID)
	m.selectRelative(delta)
	newNode, newOK := m.selectedNode()
	if !oldOK || !newOK || oldID == m.selected {
		m.setNotice("No other vertex is available in this job.")
		return nil
	}

	var command tea.Cmd
	switch m.mode {
	case modeSubtasks:
		command = m.openDiagnostics()
	case modeFlameGraph:
		command = m.openFlameGraph()
	case modeMetricExplorer:
		// Metric catalogs are vertex-specific; Open intentionally clears the
		// previous vertex's selection and tracked series.
		command = m.openMetricExplorer()
	case modeAccumulators:
		command = m.openAccumulators()
	}
	m.setPeerNotice(
		peerDescriptor{name: shared.Fallback(oldNode.Name, oldNode.ID), position: oldIndex + 1, total: len(m.layout.Order)},
		peerDescriptor{name: shared.Fallback(newNode.Name, newNode.ID), position: orderIndex(m.layout.Order, m.selected) + 1, total: len(m.layout.Order)},
		delta,
	)
	return command
}

func (m *Model) stepJobPeer(delta int, target navigationTarget) tea.Cmd {
	m.syncJobListContext()
	oldJob, newJob, ok := m.jobList.SelectPeer(m.snapshot.JobID, delta)
	if !ok {
		m.setNotice("No other job is available in the current Overview view.")
		return nil
	}
	if target == navigationGraph {
		m.navigation.ClearDeferredTarget()
	} else {
		m.navigation.DeferTarget(target)
	}
	position, total, _ := m.jobList.SelectedPosition()
	oldPosition := wrappedPreviousPosition(position, total, delta)
	m.setPeerNotice(
		peerDescriptor{name: shared.Fallback(oldJob.Name, oldJob.ID), position: oldPosition, total: total},
		peerDescriptor{name: shared.Fallback(newJob.Name, newJob.ID), position: position, total: total},
		delta,
	)
	return m.switchToPeerJob(newJob.ID)
}

func (m *Model) stepTaskManagerPeer(delta int) tea.Cmd {
	oldManager, newManager, ok := m.infrastructure.SelectTaskManagerPeer(delta)
	if !ok {
		m.setNotice("No other TaskManager is available.")
		return nil
	}
	m.mode = modeTaskManagerDetail
	position, total, _ := m.infrastructure.SelectedTaskManagerPosition()
	m.setPeerNotice(
		peerDescriptor{name: shared.TaskManagerIdentity(oldManager.ID), position: wrappedPreviousPosition(position, total, delta), total: total},
		peerDescriptor{name: shared.TaskManagerIdentity(newManager.ID), position: position, total: total},
		delta,
	)
	return m.infrastructure.Refresh()
}

func orderIndex(order []string, id string) int {
	for index, candidate := range order {
		if candidate == id {
			return index
		}
	}
	return -1
}

func wrappedPreviousPosition(position, total, delta int) int {
	if total <= 0 || position <= 0 {
		return 0
	}
	index := position - 1 - delta
	index = (index%total + total) % total
	return index + 1
}

func jobPeerTarget(mode screenMode) (navigationTarget, bool) {
	switch mode {
	case modeGraph:
		return navigationGraph, true
	case modeCheckpoints:
		return navigationCheckpoints, true
	case modeTimeline:
		return navigationTimeline, true
	case modeDiagnostics:
		return navigationDiagnostics, true
	case modeExceptions:
		return navigationExceptions, true
	case modeJobConfig:
		return navigationJobConfig, true
	case modeActions:
		return navigationActions, true
	default:
		return navigationNone, false
	}
}
