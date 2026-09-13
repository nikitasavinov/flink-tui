package coordinator

// destinationSpec is the canonical operator-facing identity of a route. The
// sidebar and command palette both project from this catalog, so labels,
// aliases, and availability cannot drift between the two navigation surfaces.
type destinationSpec struct {
	target      navigationTarget
	command     commandID
	label       string
	shortcut    string
	alias       string
	description string
	aliases     []string
}

var destinationCatalog = map[navigationTarget]destinationSpec{
	navigationOverview: {
		target: navigationOverview, command: commandOverview, label: "Overview",
		shortcut: "1", alias: "ov", description: "List cluster jobs and capacity",
	},
	navigationGraph: {
		target: navigationGraph, command: commandGraph, label: "Job Graph",
		alias: "g", description: "Explore the selected job topology",
	},
	navigationSubtasks: {
		target: navigationSubtasks, command: commandSubtasks, label: "Subtasks",
		alias: "st", description: "Inspect the selected vertex subtasks",
	},
	navigationFlameGraph: {
		target: navigationFlameGraph, command: commandFlameGraph, label: "Vertex Flame Graph",
		alias: "fl", description: "Sample the selected vertex call stacks", aliases: []string{"flame"},
	},
	navigationCheckpoints: {
		target: navigationCheckpoints, command: commandCheckpoints, label: "Checkpoints",
		alias: "cp", description: "Inspect checkpoint history, details, and statistics",
	},
	navigationTimeline: {
		target: navigationTimeline, command: commandTimeline, label: "Timeline",
		alias: "tl", description: "Inspect job state and vertex runtimes", aliases: []string{"timeline"},
	},
	navigationDiagnostics: {
		target: navigationDiagnostics, command: commandBackpressure, label: "Diagnostics",
		alias: "dx", description: "Compare pressure, busy time, skew, and watermarks",
		aliases: []string{"backpressure", "watermark"},
	},
	navigationMetrics: {
		target: navigationMetrics, command: commandMetrics, label: "Metrics",
		alias: "mx", description: "Pick exposed metrics and chart them over time",
	},
	navigationAccumulators: {
		target: navigationAccumulators, command: commandAccumulators, label: "Accumulators",
		alias: "acc", description: "Inspect vertex and subtask user accumulators", aliases: []string{"accumulators"},
	},
	navigationExceptions: {
		target: navigationExceptions, command: commandExceptions, label: "Exceptions",
		alias: "ex", description: "Inspect exception incidents and stack traces",
	},
	navigationJobConfig: {
		target: navigationJobConfig, command: commandJobConfig, label: "Job Configuration",
		alias: "cfg", description: "Inspect effective execution and user configuration",
	},
	navigationActions: {
		target: navigationActions, command: commandActions, label: "Job Actions",
		alias: "act", description: "Run guarded checkpoint and lifecycle actions", aliases: []string{"savepoint", "cancel"},
	},
	navigationTaskManagers: {
		target: navigationTaskManagers, command: commandTaskManagers, label: "Task Managers",
		alias: "tm", description: "Inspect workers, slots, memory, and shuffle",
	},
	navigationJobManager: {
		target: navigationJobManager, command: commandJobManager, label: "Job Manager",
		alias: "jm", description: "Inspect JVM health and cluster configuration",
	},
	navigationSQL: {
		target: navigationSQL, command: commandSQL, label: "SQL Workbench",
		alias: "sq", description: "Edit SQL and stream results from SQL Gateway", aliases: []string{"sql"},
	},
	navigationCheckpointOperators: {
		target: navigationCheckpointOperators, label: "Checkpoint Operators", alias: "ops",
		description: "Inspect per-operator checkpoint statistics",
	},
	navigationCheckpointSubtasks: {
		target: navigationCheckpointSubtasks, label: "Checkpoint Subtasks", alias: "cst",
		description: "Inspect per-subtask checkpoint statistics",
	},
	navigationTaskManagerDetail: {
		target: navigationTaskManagerDetail, label: "Task Manager Detail", alias: "det",
		description: "Inspect the selected TaskManager", aliases: []string{"tmd"},
	},
	navigationProcessLogs: {
		target: navigationProcessLogs, command: commandProcessLogs, label: "Logs", alias: "lg",
		description: "Open the selected process log at its tail", aliases: []string{"logs", "tail"},
	},
	navigationDocument: {
		target: navigationDocument, label: "Document", alias: "doc",
		description: "Inspect the open document",
	},
	navigationThreadDump: {
		target: navigationThreadDump, command: commandThreadDump, label: "Thread Dump", alias: "td",
		description: "Inspect live process threads and stacks", aliases: []string{"threads", "dump"},
	},
	navigationProfiler: {
		target: navigationProfiler, command: commandProfiler, label: "Process Profiler", alias: "pr",
		description: "Capture and inspect async-profiler runs", aliases: []string{"profiler"},
	},
	navigationProfilerFlameGraph: {
		target: navigationProfilerFlameGraph, label: "Profile Flame Graph", alias: "pf",
		description: "Inspect the selected profiler report",
	},
}

func destination(target navigationTarget) destinationSpec {
	return destinationCatalog[target]
}

func (m Model) destinationRow(target navigationTarget, indent int) navigationRow {
	spec := destination(target)
	return navigationRow{
		Label: spec.label, Shortcut: spec.shortcut, Alias: spec.alias, Target: spec.target,
		Indent: indent, Enabled: m.navigationUnavailableReason(target) == "", Active: m.activeNavigationTarget() == target,
	}
}

func (m Model) destinationCommand(target navigationTarget) paletteCommand {
	spec := destination(target)
	aliases := append([]string(nil), spec.aliases...)
	if spec.shortcut != "" {
		aliases = append(aliases, spec.shortcut)
	}
	return paletteCommand{
		ID: spec.command, Label: "Open " + spec.label, Shortcut: spec.alias,
		Description: spec.description, Aliases: aliases,
	}
}

func (m Model) activeNavigationTarget() navigationTarget {
	return m.activeScreen().navTarget
}

func (m Model) graphBranchExpanded() bool {
	switch m.activeNavigationTarget() {
	case navigationGraph, navigationSubtasks, navigationFlameGraph, navigationMetrics, navigationAccumulators:
		return true
	case navigationDocument:
		parent, ok := m.history.Peek()
		if !ok {
			return false
		}
		switch screenRegistry[parent.mode].navTarget {
		case navigationGraph, navigationSubtasks, navigationFlameGraph, navigationMetrics, navigationAccumulators:
			return true
		}
	default:
	}
	return false
}
