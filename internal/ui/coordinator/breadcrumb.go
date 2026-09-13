package coordinator

import (
	"strings"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

const breadcrumbSeparator = " › "

func (m Model) breadcrumb(screenTitle string) string {
	parts := make([]string, 0, 5)
	appendPart := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" || len(parts) > 0 && parts[len(parts)-1] == value {
			return
		}
		parts = append(parts, value)
	}

	if m.mode == modeJobs {
		return "Cluster Overview"
	}

	jobContext := isJobWorkflowMode(m.mode)
	crumbs := m.history.Crumbs()
	// Walk back through drills only. The first jump ends the chain, because
	// naming a destination leaves the previous screen's scope behind.
	for index := len(crumbs) - 1; index >= 0 && !jobContext; index-- {
		if crumbs[index].jump {
			break
		}
		if isJobWorkflowMode(crumbs[index].mode) {
			jobContext = true
		}
	}
	if jobContext {
		appendPart(shared.Fallback(m.snapshot.JobName, "Job"))
	}

	if isVertexWorkflowMode(m.mode) {
		if node, ok := m.selectedNode(); ok {
			appendPart(node.Name)
		}
	}
	if m.mode == modeDocument && jobContext {
		if node, ok := m.nearestBreadcrumbVertex(); ok {
			appendPart(node.Name)
		}
	}

	if m.mode == modeCheckpointOperators || m.mode == modeCheckpointSubtasks {
		appendPart("Checkpoints")
	}
	if m.mode == modeCheckpointSubtasks {
		appendPart("Operators")
	}
	if m.mode == modeTaskManagerDetail {
		if jobContext {
			if crumb, ok := m.history.Peek(); ok && crumb.mode == modeExceptions {
				appendPart("Exceptions")
			}
		} else {
			appendPart("Task Managers")
			if manager, ok := m.selectedTaskManager(); ok {
				appendPart(shared.ShortID(manager.ID))
			}
		}
	}

	if isProcessWorkflowMode(m.mode) {
		if jobContext {
			if node, ok := m.nearestBreadcrumbVertex(); ok {
				appendPart(node.Name)
			}
		} else {
			appendPart(processBreadcrumbLabel(m.activeProcess()))
		}
	}

	appendPart(screenTitle)
	return strings.Join(parts, breadcrumbSeparator)
}

// nearestBreadcrumbVertex returns the object that owns a process or document
// drill. Previous screen names are deliberately ignored: breadcrumbs describe
// objects and finish with the current screen, not a replay of navigation.
func (m Model) nearestBreadcrumbVertex() (flink.Node, bool) {
	crumbs := m.history.Crumbs()
	for index := len(crumbs) - 1; index >= 0; index-- {
		crumb := crumbs[index]
		if crumb.jump {
			break
		}
		if !isVertexWorkflowMode(crumb.mode) {
			continue
		}
		if node, ok := m.nodeByID(crumb.selected); ok {
			return node, true
		}
	}
	return flink.Node{}, false
}

func screenModeName(mode screenMode) string {
	switch mode {
	case modeGraph:
		return "Job Graph"
	case modeJobs:
		return "Overview"
	case modeSubtasks:
		return "Subtasks"
	case modeFlameGraph:
		return "Vertex Flame Graph"
	case modeCheckpoints:
		return "Checkpoints"
	case modeCheckpointOperators:
		return "Checkpoint Operators"
	case modeCheckpointSubtasks:
		return "Checkpoint Subtasks"
	case modeDiagnostics:
		return "Diagnostics"
	case modeTimeline:
		return "Timeline"
	case modeExceptions:
		return "Exceptions"
	case modeJobConfig:
		return "Job Configuration"
	case modeActions:
		return "Job Actions"
	case modeTaskManagers:
		return "Task Managers"
	case modeTaskManagerDetail:
		return "Task Manager Detail"
	case modeJobManager:
		return "Job Manager"
	case modeMetricExplorer:
		return "Metrics"
	case modeAccumulators:
		return "Accumulators"
	case modeProcessLogs:
		return "Logs"
	case modeDocument:
		return "Document"
	case modeThreadDump:
		return "Thread Dump"
	case modeProfiler:
		return "Process Profiler"
	case modeProfilerFlameGraph:
		return "Profile Flame Graph"
	case modeSQL:
		return "SQL Workbench"
	default:
		return "Flink TUI"
	}
}

func isJobWorkflowMode(mode screenMode) bool {
	switch mode {
	case modeGraph, modeSubtasks, modeFlameGraph, modeCheckpoints, modeCheckpointOperators,
		modeCheckpointSubtasks, modeDiagnostics, modeTimeline, modeExceptions, modeJobConfig,
		modeActions, modeMetricExplorer, modeAccumulators:
		return true
	default:
		return false
	}
}

func isVertexWorkflowMode(mode screenMode) bool {
	switch mode {
	case modeSubtasks, modeFlameGraph, modeMetricExplorer, modeAccumulators:
		return true
	default:
		return false
	}
}

func isProcessWorkflowMode(mode screenMode) bool {
	switch mode {
	case modeProcessLogs, modeDocument, modeThreadDump,
		modeProfiler, modeProfilerFlameGraph:
		return true
	default:
		return false
	}
}

func processBreadcrumbLabel(process flink.ProcessRef) string {
	if process.Kind == flink.ProcessTaskManager {
		return "TaskManager " + shared.TaskManagerIdentity(process.TaskManagerID)
	}
	return "JobManager"
}

// fitBreadcrumb keeps the destination—the part that answers “where am I?”—
// visible when the full entity path does not fit.
func fitBreadcrumb(value string, width int) string {
	if width <= 0 || shared.DisplayWidth(value) <= width {
		return value
	}
	parts := strings.Split(value, breadcrumbSeparator)
	if len(parts) < 2 {
		return shared.Truncate(value, width)
	}
	current := parts[len(parts)-1]
	object := parts[len(parts)-2]
	essential := object + breadcrumbSeparator + current
	if shared.DisplayWidth(essential) <= width {
		elided := "…" + breadcrumbSeparator + essential
		if len(parts) > 2 && shared.DisplayWidth(elided) <= width {
			return elided
		}
		return essential
	}
	if shared.DisplayWidth(current)+shared.DisplayWidth(breadcrumbSeparator)+1 >= width {
		currentWidth := max(1, width/2)
		objectWidth := max(1, width-shared.DisplayWidth(breadcrumbSeparator)-currentWidth)
		return shared.Truncate(object, objectWidth) + breadcrumbSeparator + shared.Truncate(current, currentWidth)
	}
	objectWidth := max(1, width-shared.DisplayWidth(breadcrumbSeparator)-shared.DisplayWidth(current))
	return shared.Truncate(object, objectWidth) + breadcrumbSeparator + current
}
