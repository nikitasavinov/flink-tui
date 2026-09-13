package coordinator

import (
	"fmt"
	"strconv"
	"strings"

	tea "charm.land/bubbletea/v2"

	"github.com/nikitasavinov/flink-tui/internal/flink"
	jobgraphmodule "github.com/nikitasavinov/flink-tui/internal/ui/jobgraph"
	"github.com/nikitasavinov/flink-tui/internal/ui/shared"
)

const commandPairPrefix = "pair:"

type paletteObjectKind uint8

const (
	paletteJob paletteObjectKind = iota + 1
	paletteVertex
	paletteTaskManager
	paletteJobManager
)

type paletteObject struct {
	kind    paletteObjectKind
	id      string
	jobID   string
	label   string
	aliases []string
	process flink.ProcessRef
}

type palettePair struct {
	target navigationTarget
	object paletteObject
}

func (m Model) palettePairCommands() []paletteCommand {
	query := strings.ToLower(strings.TrimSpace(m.palette.State().Query))
	if len(strings.Fields(query)) < 2 || !m.queryNamesPaletteDestination(query) {
		return nil
	}
	objects := m.paletteObjects()
	targets := palettePairTargets()
	terms := strings.Fields(query)
	commands := make([]paletteCommand, 0, len(objects)*len(targets))
	for _, object := range objects {
		for _, target := range targets {
			spec := destination(target)
			pair := palettePair{target: target, object: object}
			if !palettePairMatchesTerms(pair, terms) {
				continue
			}
			reason := palettePairUnavailable(pair)
			description := paletteObjectDescription(object)
			if reason != "" {
				description = "unavailable · " + reason
			}
			aliases := append(destinationSearchTerms(spec), object.aliases...)
			aliases = append(aliases, object.id)
			commands = append(commands, paletteCommand{
				ID: pairCommandID(pair), Label: spec.label + " · " + object.label,
				Shortcut: spec.alias, Description: description, Aliases: aliases,
				SearchPriority: 250, Unavailable: reason,
			})
		}
	}
	return commands
}

func palettePairMatchesTerms(pair palettePair, terms []string) bool {
	destinationTerms := destinationSearchTerms(destination(pair.target))
	objectText := strings.ToLower(strings.Join(append([]string{pair.object.label, pair.object.id}, pair.object.aliases...), " "))
	destinationMatched, objectMatched := false, false
	for _, term := range terms {
		matchesDestination := false
		for _, candidate := range destinationTerms {
			if term == strings.ToLower(candidate) {
				matchesDestination = true
				break
			}
		}
		if matchesDestination {
			destinationMatched = true
			continue
		}
		if !strings.Contains(objectText, term) {
			return false
		}
		objectMatched = true
	}
	return destinationMatched && objectMatched
}

func (m Model) queryNamesPaletteDestination(query string) bool {
	for _, term := range strings.Fields(query) {
		for _, target := range palettePairTargets() {
			for _, candidate := range destinationSearchTerms(destination(target)) {
				if term == strings.ToLower(candidate) {
					return true
				}
			}
		}
	}
	return false
}

func destinationSearchTerms(spec destinationSpec) []string {
	terms := append([]string{spec.alias, spec.shortcut}, spec.aliases...)
	if !strings.Contains(spec.label, " ") {
		terms = append(terms, strings.ToLower(spec.label))
	}
	return terms
}

func palettePairTargets() []navigationTarget {
	return []navigationTarget{
		navigationGraph, navigationSubtasks, navigationFlameGraph, navigationCheckpoints,
		navigationTimeline, navigationDiagnostics, navigationMetrics, navigationAccumulators,
		navigationExceptions, navigationJobConfig, navigationActions,
		navigationTaskManagers, navigationTaskManagerDetail, navigationJobManager,
		navigationProcessLogs, navigationThreadDump, navigationProfiler, navigationSQL,
	}
}

func (m Model) paletteObjects() []paletteObject {
	jobs := m.jobList.State().Jobs
	managers := m.infrastructure.State().Infrastructure.TaskManagers
	objects := make([]paletteObject, 0, len(jobs)+len(m.snapshot.Nodes)+len(managers)+1)
	for _, job := range jobs {
		objects = append(objects, paletteObject{
			kind: paletteJob, id: job.ID, jobID: job.ID, label: shared.Fallback(job.Name, job.ID),
			aliases: []string{"job", job.ID},
		})
	}
	for _, node := range m.snapshot.Nodes {
		objects = append(objects, paletteObject{
			kind: paletteVertex, id: node.ID, jobID: m.snapshot.JobID, label: node.Name,
			aliases: []string{"vertex", node.ID, m.snapshot.JobName},
		})
	}
	for _, manager := range managers {
		objects = append(objects, paletteObject{
			kind: paletteTaskManager, id: manager.ID, label: shared.TaskManagerIdentity(manager.ID),
			aliases: []string{"taskmanager", manager.Path}, process: flink.TaskManagerProcess(manager.ID),
		})
	}
	objects = append(objects, paletteObject{
		kind: paletteJobManager, id: "jobmanager", label: "JobManager",
		aliases: []string{"jobmanager", "job-manager", "jm"}, process: flink.JobManagerProcess(),
	})
	return objects
}

func paletteObjectDescription(object paletteObject) string {
	switch object.kind {
	case paletteJob:
		return "job · explicit destination"
	case paletteVertex:
		return "vertex · " + shared.Fallback(object.jobID, "selected job")
	case paletteTaskManager:
		return "TaskManager process"
	case paletteJobManager:
		return "JobManager process"
	default:
		return "object"
	}
}

func palettePairUnavailable(pair palettePair) string {
	jobDestination := jobScopedNavigation(pair.target)
	switch pair.object.kind {
	case paletteJob, paletteVertex:
		if jobDestination {
			return ""
		}
		return destination(pair.target).label + " does not accept a job or vertex"
	case paletteTaskManager:
		switch pair.target {
		case navigationTaskManagers, navigationTaskManagerDetail, navigationProcessLogs, navigationThreadDump, navigationProfiler:
			return ""
		}
		return destination(pair.target).label + " requires job context, not a TaskManager"
	case paletteJobManager:
		switch pair.target {
		case navigationJobManager, navigationProcessLogs, navigationThreadDump, navigationProfiler:
			return ""
		}
		return destination(pair.target).label + " is not available for the JobManager"
	default:
		return "unsupported object"
	}
}

func pairCommandID(pair palettePair) commandID {
	return commandID(fmt.Sprintf("%s%d:%d:%s", commandPairPrefix, pair.target, pair.object.kind, pair.object.id))
}

func (m Model) parsePalettePair(id commandID) (palettePair, bool) {
	value := strings.TrimPrefix(string(id), commandPairPrefix)
	parts := strings.SplitN(value, ":", 3)
	if len(parts) != 3 {
		return palettePair{}, false
	}
	targetValue, targetErr := strconv.Atoi(parts[0])
	kindValue, kindErr := strconv.Atoi(parts[1])
	if targetErr != nil || kindErr != nil {
		return palettePair{}, false
	}
	target, targetOK := paletteTargetFromInt(targetValue)
	kind, kindOK := paletteObjectKindFromInt(kindValue)
	if !targetOK || !kindOK {
		return palettePair{}, false
	}
	for _, object := range m.paletteObjects() {
		if object.kind == kind && object.id == parts[2] {
			return palettePair{target: target, object: object}, true
		}
	}
	return palettePair{}, false
}

func paletteTargetFromInt(value int) (navigationTarget, bool) {
	for target := navigationNone + 1; target <= shellTargetLast(); target++ {
		if int(target) == value {
			return target, true
		}
	}
	return navigationNone, false
}

func paletteObjectKindFromInt(value int) (paletteObjectKind, bool) {
	switch value {
	case int(paletteJob):
		return paletteJob, true
	case int(paletteVertex):
		return paletteVertex, true
	case int(paletteTaskManager):
		return paletteTaskManager, true
	case int(paletteJobManager):
		return paletteJobManager, true
	default:
		return 0, false
	}
}

func shellTargetLast() navigationTarget { return navigationProfilerFlameGraph }

func (m *Model) executePalettePair(pair palettePair) tea.Cmd {
	if reason := palettePairUnavailable(pair); reason != "" {
		m.setNotice(reason + ".")
		return nil
	}
	switch pair.object.kind {
	case paletteJob:
		return m.openPaletteJobAt(pair.object.id, pair.target)
	case paletteVertex:
		return m.openPaletteVertexAt(pair.object, pair.target)
	case paletteTaskManager, paletteJobManager:
		return m.openPaletteProcessAt(pair.object, pair.target)
	default:
		m.setNotice("That palette object is no longer available.")
		return nil
	}
}

func (m *Model) openPaletteJobAt(jobID string, target navigationTarget) tea.Cmd {
	if jobID == "" {
		return nil
	}
	m.clearRouteHistory()
	if jobID != m.snapshot.JobID {
		if target != navigationGraph {
			m.navigation.DeferTarget(target)
		} else {
			m.navigation.ClearDeferredTarget()
		}
		return m.beginJobSwitch(jobID, false)
	}
	if vertexScopedNavigation(target) {
		if _, ok := m.layout.Rects[m.selected]; !ok {
			m.selected = jobgraphmodule.HottestVertexID(m.snapshot.Nodes, m.layout.Order)
		}
	}
	return m.navigateRoute(target, false)
}

func vertexScopedNavigation(target navigationTarget) bool {
	switch target {
	case navigationSubtasks, navigationFlameGraph, navigationMetrics, navigationAccumulators:
		return true
	default:
		return false
	}
}

func (m *Model) openPaletteVertexAt(object paletteObject, target navigationTarget) tea.Cmd {
	if object.jobID != m.snapshot.JobID {
		m.setNotice("That vertex is no longer part of the loaded job.")
		return nil
	}
	if _, ok := m.layout.Rects[object.id]; !ok {
		m.setNotice("That vertex is no longer present in the current job.")
		return nil
	}
	m.clearRouteHistory()
	m.selected = object.id
	m.syncJobDetailContext()
	return m.navigateRoute(target, false)
}

func (m *Model) openPaletteProcessAt(object paletteObject, target navigationTarget) tea.Cmd {
	m.clearRouteHistory()
	process := object.process
	if object.kind == paletteTaskManager {
		m.infrastructure.SelectTaskManagerByID(object.id)
	}
	switch target {
	case navigationTaskManagers, navigationTaskManagerDetail:
		return m.openTaskManagerByID(object.id, modeTaskManagers)
	case navigationJobManager:
		return m.openJobManager()
	case navigationProcessLogs:
		back := modeJobManager
		if process.Kind == flink.ProcessTaskManager {
			back = modeTaskManagerDetail
		}
		return m.openCurrentProcessLog(process, back)
	case navigationThreadDump:
		back := modeJobManager
		if process.Kind == flink.ProcessTaskManager {
			back = modeTaskManagerDetail
		}
		return m.openThreadDump(process, back)
	case navigationProfiler:
		back := modeJobManager
		if process.Kind == flink.ProcessTaskManager {
			back = modeTaskManagerDetail
		}
		return m.openProfiler(process, back)
	}
	return nil
}
