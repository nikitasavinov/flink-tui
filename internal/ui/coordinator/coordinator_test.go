package coordinator

import (
	tea "charm.land/bubbletea/v2"
	"context"
	"errors"
	"github.com/nikitasavinov/flink-tui/internal/flink"
	jobgraphmodule "github.com/nikitasavinov/flink-tui/internal/ui/jobgraph"
	joblistmodule "github.com/nikitasavinov/flink-tui/internal/ui/joblist"
	processmodule "github.com/nikitasavinov/flink-tui/internal/ui/processdiag"
	profilermodule "github.com/nikitasavinov/flink-tui/internal/ui/profiler"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestInitialOverviewFetchAppliesToLiveGeneration(t *testing.T) {
	client, err := flink.NewClient("http://flink.test")
	if err != nil {
		t.Fatal(err)
	}
	model := NewModel(client, "", time.Hour)
	model.jobList = joblistmodule.New(initialOverviewSource{}, model.requests.context)
	state := model.jobList.State()
	state.Loading = true
	model.jobList.RestoreState(state)
	model.jobList.ReserveInitialPoll()
	initialGeneration := model.jobList.State().Generation
	if model.startJobsRefresh() != nil {
		t.Fatal("a tick started an overlapping request before the initial overview reply")
	}
	command := model.Init()
	if command == nil {
		t.Fatal("Init returned no command")
	}
	batch, ok := command().(tea.BatchMsg)
	if !ok || len(batch) != 3 {
		t.Fatalf("Init message = %T with %d commands, want three-command batch", command(), len(batch))
	}
	reply := batch[0]()
	if _, ok := reply.(joblistmodule.Message); !ok {
		t.Fatalf("first Init reply = %T, want job-list message", reply)
	}
	updated, _ := model.Update(reply)
	state = updated.(Model).jobList.State()
	if state.Generation != initialGeneration || state.Loading || state.JobsErr != nil || state.ClusterErr != nil {
		t.Fatalf("initial reply state = generation:%d loading:%t jobsErr:%v clusterErr:%v",
			state.Generation, state.Loading, state.JobsErr, state.ClusterErr)
	}
	if len(state.Jobs) != 1 || state.Jobs[0].ID != "job" || state.Cluster.TaskManagers != 1 {
		t.Fatalf("initial reply was dropped: jobs=%#v cluster=%#v", state.Jobs, state.Cluster)
	}
	live := updated.(Model)
	if live.startJobsRefresh() == nil {
		t.Fatal("initial overview reply did not allow the next refresh")
	}
}

type initialOverviewSource struct{}

func (initialOverviewSource) Jobs(context.Context) ([]flink.JobSummary, error) {
	return []flink.JobSummary{{ID: "job", Name: "Initial Job", State: "RUNNING", TotalTasks: 2, RunningTasks: 2}}, nil
}

func (initialOverviewSource) ClusterOverview(context.Context) (flink.ClusterOverview, error) {
	return flink.ClusterOverview{TaskManagers: 1, SlotsTotal: 2, JobsRunning: 1}, nil
}

func TestQuitCancelsInFlightRequests(t *testing.T) {
	model := NewModel(nil, "", time.Second)
	requestContext, cancel := model.requestTimeout(time.Hour)
	defer cancel()

	if command := model.quit(); command == nil {
		t.Fatal("quit returned no command")
	}
	if !errors.Is(requestContext.Err(), context.Canceled) {
		t.Fatalf("request context error = %v, want context canceled", requestContext.Err())
	}
}

func TestJobSwitchClearsEveryJobScopedCache(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeJobs
	overview := model.jobList.State()
	overview.Jobs = []flink.JobSummary{{ID: "next-job", Name: "Next", State: "RUNNING"}}
	overview.Selection = 0
	model.jobList.RestoreState(overview)
	model.recordHistory(flink.Snapshot{UpdatedAt: time.Now(), Nodes: []flink.Node{{ID: "source", Metrics: flink.Metrics{RecordsInPerSecond: 1}}}})
	flameState := model.flameGraphs.State()
	flameState.Vertex = "source"
	flameState.LiveGraph = testFlameGraph("old-live-stack")
	flameState.LiveSelected = "0"
	flameState.LiveFocus = "0"
	model.flameGraphs.RestoreState(flameState)
	metricState := model.metrics.State()
	metricState.Vertex = "source"
	metricState.Names = []string{"old.metric"}
	metricState.Tracked = []string{"old.metric"}
	metricState.Values = map[string]float64{"old.metric": 42}
	model.metrics.RestoreState(metricState)
	accumulatorState := model.accumulators.State()
	accumulatorState.Vertex = "source"
	accumulatorState.Values = flink.VertexAccumulators{JobID: "job", VertexID: "source", Vertex: []flink.UserAccumulator{{Name: "old"}}}
	model.accumulators.RestoreState(accumulatorState)
	detailState := model.jobDetails.State()
	detailState.DiagnosticsOpen = "source"
	model.jobDetails.RestoreState(detailState)
	operations := model.jobOperations.State()
	operations.ActionTriggerID = "old-trigger"
	operations.ActionStatus = "IN_PROGRESS"
	model.jobOperations.RestoreState(operations)

	command := model.switchToSelectedJob()
	if command == nil || model.preferredJobID != "next-job" || model.snapshot.JobID != "" {
		t.Fatalf("switch produced command=%v preferred=%q snapshot=%q", command, model.preferredJobID, model.snapshot.JobID)
	}
	flameState = model.flameGraphs.State()
	if flameState.Vertex != "" || flameState.LiveGraph.Ready() || flameState.LiveSelected != "" || flameState.LiveFocus != "" {
		t.Fatalf("live flame graph survived job switch: vertex=%q graph=%#v", flameState.Vertex, flameState.LiveGraph)
	}
	metricState = model.metrics.State()
	if metricState.Vertex != "" || len(metricState.Names) != 0 || len(metricState.Tracked) != 0 ||
		len(metricState.Values) != 0 || metricState.HistorySize != 0 {
		t.Fatalf("metric cache survived job switch: %#v", metricState)
	}
	accumulatorState = model.accumulators.State()
	if accumulatorState.Vertex != "" || accumulatorState.Values.JobID != "" || accumulatorState.Rows != 0 {
		t.Fatalf("accumulator cache survived job switch: vertex=%q values=%#v rows=%#v",
			accumulatorState.Vertex, accumulatorState.Values, accumulatorState.Rows)
	}
	operations = model.jobOperations.State()
	if model.jobDetails.State().DiagnosticsOpen != "" || operations.ActionTriggerID != "" || operations.ActionStatus != "" || len(model.graphTelemetry.Samples("source")) != 0 {
		t.Fatalf("job state survived switch: diagnostics=%q action=%q/%q history=%v",
			model.jobDetails.State().DiagnosticsOpen, operations.ActionTriggerID, operations.ActionStatus, model.graphTelemetry.History())
	}
}

func TestFirstSnapshotForDifferentPreferredJobClearsJobScopedState(t *testing.T) {
	model := interactionTestModel(t)
	model.snapshot = flink.Snapshot{}
	model.preferredJobID = "preferred-job"
	metricState := model.metrics.State()
	metricState.Vertex = "source"
	metricState.Values = map[string]float64{"stale.metric": 42}
	model.metrics.RestoreState(metricState)
	flameState := model.flameGraphs.State()
	flameState.Vertex = "source"
	flameState.LiveGraph = testFlameGraph("stale-stack")
	model.flameGraphs.RestoreState(flameState)

	model.applySnapshot(snapshotMsg{
		generation: model.generation,
		snapshot: flink.Snapshot{
			JobID: "preferred-job",
			Nodes: []flink.Node{{ID: "source", Name: "Source", State: "RUNNING"}},
		},
	})
	metricState = model.metrics.State()
	if metricState.Vertex != "" || len(metricState.Values) != 0 ||
		model.flameGraphs.State().Vertex != "" || model.flameGraphs.State().LiveGraph.Ready() {
		t.Fatalf("first snapshot retained stale state: metric=%q values=%v flame=%q/%#v",
			metricState.Vertex, metricState.Values, model.flameGraphs.State().Vertex, model.flameGraphs.State().LiveGraph)
	}
}

func TestProfilerReportAndVertexFlameGraphsAreIsolated(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeProfiler
	process := flink.TaskManagerProcess("tm")
	flameState := model.flameGraphs.State()
	flameState.Vertex = model.selected
	flameState.Type = flink.FlameGraphFull
	flameState.Subtask = -1
	flameState.LiveGraph = testFlameGraph("live-worker")
	flameState.LiveSelected = "0"
	flameState.LiveFocus = "0"
	model.flameGraphs.RestoreState(flameState)

	model.applyProfilerResult(profilermodule.Result{
		Intent: profilermodule.IntentReport, ReportName: "profile.html",
		Graph: testFlameGraph("profile-worker"), Process: process,
	})
	if model.mode != modeProfilerFlameGraph || !strings.Contains(model.renderFlameGraph(100, 20), "profile-worker") {
		t.Fatalf("downloaded profiler report did not open: mode=%d", model.mode)
	}
	if model.flameGraphs.State().LiveGraph.Root.Name != "live-worker" {
		t.Fatalf("profiler report overwrote live graph with %q", model.flameGraphs.State().LiveGraph.Root.Name)
	}

	model.mode = modeFlameGraph
	flameState = model.flameGraphs.State()
	flameState.View = 0
	flameState.Err = errors.New("sampling failed")
	model.flameGraphs.RestoreState(flameState)
	rendered := model.renderFlameGraph(100, 20)
	if !strings.Contains(rendered, "live-worker") || strings.Contains(rendered, "profile-worker") {
		t.Fatalf("failed live refresh exposed profiler data:\n%s", rendered)
	}
}

func TestLateProfilerDownloadDoesNotStealNavigation(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeGraph
	process := flink.TaskManagerProcess("tm")

	command := model.applyProfilerResult(profilermodule.Result{
		ReportName: "profile.html", Graph: testFlameGraph("profile-worker"), Process: process,
		Notice: "Profiler report is ready; return to Profiler to open it.",
	})
	if command != nil || model.mode != modeGraph || !model.flameGraphs.State().ReportGraph.Ready() {
		t.Fatalf("late profiler reply produced command=%v mode=%d ready=%t",
			command, model.mode, model.flameGraphs.State().ReportGraph.Ready())
	}
}

func TestActionPollingIgnoresScreenPause(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeGraph
	model.paused = true
	operations := model.jobOperations.State()
	operations.ActionTriggerID = "trigger"
	operations.ActionStatus = "IN_PROGRESS"
	operations.ActionKind = 2 // canonical savepoint
	model.jobOperations.RestoreState(operations)

	tick := model.applyTick()
	batch, ok := tick().(tea.BatchMsg)
	if !ok || len(batch) != 2 {
		t.Fatalf("paused tick contains %T with %d commands, want clock and action poll", batch, len(batch))
	}
}

func testFlameGraph(root string) flink.FlameGraph {
	capturedAt := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	return flink.FlameGraph{
		EndTimestampMillis: capturedAt.UnixMilli(),
		EndTimestamp:       capturedAt,
		Root:               flink.FlameGraphNode{Name: root, Value: 10},
	}
}

func TestModelStateRemainsGrouped(t *testing.T) {
	const topLevelFieldBudget = 36

	if fields := reflect.TypeOf(Model{}).NumField(); fields > topLevelFieldBudget {
		t.Fatalf("Model has %d top-level fields, budget is %d; put screen-owned state in its feature group", fields, topLevelFieldBudget)
	}
}

func TestScreenRegistryUsesExpectedRefreshPolicies(t *testing.T) {
	tests := []struct {
		mode        screenMode
		refresh     func(*Model) tea.Cmd
		autoRefresh func(*Model) tea.Cmd
	}{
		{modeGraph, refreshGraph, refreshSnapshot},
		{modeJobs, refreshJobs, tickJobs},
		{modeSubtasks, refreshSubtasks, tickSubtasks},
		{modeFlameGraph, refreshFlameGraph, tickFlameGraph},
		{modeCheckpoints, refreshSnapshot, refreshSnapshot},
		{modeCheckpointOperators, refreshCheckpointOperators, refreshCheckpointOperators},
		{modeCheckpointSubtasks, refreshCheckpointSubtasks, refreshCheckpointSubtasks},
		{modeDiagnostics, refreshSnapshot, refreshSnapshot},
		{modeTimeline, refreshSnapshot, refreshSnapshot},
		{modeExceptions, refreshSnapshot, refreshSnapshot},
		{modeJobConfig, refreshJobConfiguration, tickJobConfiguration},
		{modeActions, refreshActions, tickActions},
		{modeTaskManagers, refreshInfrastructure, tickInfrastructure},
		{modeTaskManagerDetail, refreshInfrastructure, tickInfrastructure},
		{modeJobManager, refreshInfrastructure, tickInfrastructure},
		{modeMetricExplorer, refreshMetrics, tickMetrics},
		{modeAccumulators, refreshAccumulators, tickAccumulators},
		{modeProcessLogs, refreshProcessLogs, manualRefreshOnly},
		{modeDocument, refreshDocument, manualRefreshOnly},
		{modeThreadDump, refreshThreadDump, manualRefreshOnly},
		{modeProfiler, refreshProfiler, tickProfiler},
		{modeProfilerFlameGraph, immutableContent, immutableContent},
		{modeSQL, refreshSQL, pollDrivenRefresh},
	}

	for _, test := range tests {
		screen := screenRegistry[test.mode]
		if !sameScreenCommand(screen.refresh, test.refresh) {
			t.Errorf("screen %q has the wrong manual refresh policy", screen.name)
		}
		if !sameScreenCommand(screen.autoRefresh, test.autoRefresh) {
			t.Errorf("screen %q has the wrong automatic refresh policy", screen.name)
		}
	}
}

func TestRegisteredManualRefreshMutations(t *testing.T) {
	sqlClient, err := flink.NewSQLGatewayClient("http://localhost:8083")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name        string
		mode        screenMode
		prepare     func(*Model, error)
		assert      func(*testing.T, Model)
		wantCommand bool
	}{
		{name: "snapshot", mode: modeGraph, wantCommand: true},
		{name: "jobs", mode: modeJobs, wantCommand: true,
			prepare: func(m *Model, err error) {
				state := m.jobList.State()
				state.JobsErr, state.ClusterErr = err, err
				m.jobList.RestoreState(state)
			},
			assert: func(t *testing.T, m Model) {
				state := m.jobList.State()
				if !state.Loading || state.JobsErr != nil || state.ClusterErr != nil {
					t.Fatalf("jobs refresh state = loading:%t jobs:%v cluster:%v", state.Loading, state.JobsErr, state.ClusterErr)
				}
			}},
		{name: "subtasks", mode: modeSubtasks, wantCommand: true,
			prepare: func(m *Model, err error) {
				state := m.jobDetails.State()
				state.DiagnosticsOpen, state.DiagnosticsErr = "source", err
				m.jobDetails.RestoreState(state)
			},
			assert: func(t *testing.T, m Model) {
				state := m.jobDetails.State()
				if !state.DiagnosticsBusy || state.DiagnosticsErr != nil {
					t.Fatalf("subtask refresh state = busy:%t err:%v", state.DiagnosticsBusy, state.DiagnosticsErr)
				}
			}},
		{name: "flame graph", mode: modeFlameGraph, wantCommand: true,
			prepare: func(m *Model, err error) {
				state := m.flameGraphs.State()
				state.Err = err
				m.flameGraphs.RestoreState(state)
			},
			assert: func(t *testing.T, m Model) {
				state := m.flameGraphs.State()
				if !state.Busy || state.Err != nil {
					t.Fatalf("flame graph refresh state = busy:%t err:%v", state.Busy, state.Err)
				}
			}},
		{name: "checkpoint operators", mode: modeCheckpointOperators, wantCommand: true},
		{name: "checkpoint subtasks", mode: modeCheckpointSubtasks, wantCommand: true},
		{name: "job configuration", mode: modeJobConfig, wantCommand: true,
			prepare: func(m *Model, err error) {
				state := m.jobOperations.State()
				state.ConfigurationErr = err
				m.jobOperations.RestoreState(state)
			},
			assert: func(t *testing.T, m Model) {
				state := m.jobOperations.State()
				if !state.ConfigurationBusy || state.ConfigurationErr != nil {
					t.Fatalf("job config refresh state = busy:%t err:%v", state.ConfigurationBusy, state.ConfigurationErr)
				}
			}},
		{name: "actions", mode: modeActions, wantCommand: true},
		{name: "infrastructure", mode: modeTaskManagers, wantCommand: true,
			prepare: func(m *Model, err error) {
				state := m.infrastructure.State()
				state.Err = err
				m.infrastructure.RestoreState(state)
			},
			assert: func(t *testing.T, m Model) {
				state := m.infrastructure.State()
				if !state.Busy || state.Err != nil {
					t.Fatalf("infrastructure refresh state = busy:%t err:%v", state.Busy, state.Err)
				}
			}},
		{name: "metrics", mode: modeMetricExplorer, wantCommand: true,
			prepare: func(m *Model, err error) {
				state := m.metrics.State()
				state.Vertex, state.Err = "source", err
				m.metrics.RestoreState(state)
			},
			assert: func(t *testing.T, m Model) {
				state := m.metrics.State()
				if !state.Busy || state.Err != nil {
					t.Fatalf("metric refresh state = busy:%t err:%v", state.Busy, state.Err)
				}
			}},
		{name: "accumulators", mode: modeAccumulators, wantCommand: true,
			prepare: func(m *Model, err error) {
				state := m.accumulators.State()
				state.Vertex, state.Err = "source", err
				m.accumulators.RestoreState(state)
			},
			assert: func(t *testing.T, m Model) {
				state := m.accumulators.State()
				if !state.Busy || state.Err != nil {
					t.Fatalf("accumulator refresh state = busy:%t err:%v", state.Busy, state.Err)
				}
			}},
		{name: "process logs", mode: modeProcessLogs, wantCommand: true,
			prepare: func(m *Model, err error) {
				state := m.processDiagnostics.State()
				state.LogsErr = err
				m.processDiagnostics.RestoreState(state)
			},
			assert: func(t *testing.T, m Model) {
				state := m.processDiagnostics.State()
				if !state.LogsBusy || state.LogsErr != nil {
					t.Fatalf("process log refresh state = busy:%t err:%v", state.LogsBusy, state.LogsErr)
				}
			}},
		{name: "remote document", mode: modeDocument, wantCommand: true,
			prepare: func(m *Model, err error) {
				state := m.processDiagnostics.State()
				state.View = processmodule.ViewDocument
				state.DocumentSource = processmodule.SourceLog
				state.DocumentErr = err
				m.processDiagnostics.RestoreState(state)
			},
			assert: func(t *testing.T, m Model) {
				state := m.processDiagnostics.State()
				if !state.DocumentLoading || state.DocumentErr != nil || state.Generation != 1 {
					t.Fatalf("document refresh state = loading:%t err:%v generation:%d", state.DocumentLoading, state.DocumentErr, state.Generation)
				}
			}},
		{name: "thread dump", mode: modeThreadDump, wantCommand: true,
			prepare: func(m *Model, err error) {
				state := m.processDiagnostics.State()
				state.View = processmodule.ViewThreadDump
				state.ThreadErr = err
				m.processDiagnostics.RestoreState(state)
			},
			assert: func(t *testing.T, m Model) {
				state := m.processDiagnostics.State()
				if !state.ThreadBusy || state.ThreadErr != nil || state.Generation != 1 {
					t.Fatalf("thread refresh state = busy:%t err:%v generation:%d", state.ThreadBusy, state.ThreadErr, state.Generation)
				}
			}},
		{name: "profiler", mode: modeProfiler, wantCommand: true,
			prepare: func(m *Model, err error) {
				state := m.profiler.State()
				state.Err = err
				m.profiler.RestoreState(state)
			},
			assert: func(t *testing.T, m Model) {
				state := m.profiler.State()
				if !state.Busy || state.Err != nil {
					t.Fatalf("profiler refresh state = busy:%t err:%v", state.Busy, state.Err)
				}
			}},
		{name: "SQL session", mode: modeSQL, wantCommand: true,
			prepare: func(m *Model, err error) {
				m.configureSQLWorkbench(sqlClient)
			},
			assert: func(t *testing.T, m Model) {
				state := m.sql.State()
				if !state.SessionBusy || state.Err != nil {
					t.Fatalf("SQL refresh state = busy:%t err:%v", state.SessionBusy, state.Err)
				}
			}},
	}

	for _, test := range tests {
		for _, route := range []string{"registry", "keyboard"} {
			if test.mode == modeSQL && route == "keyboard" {
				// In the SQL editor, r is input. Manual refresh remains available
				// through the command palette.
				continue
			}
			t.Run(test.name+"/"+route, func(t *testing.T) {
				model := interactionTestModel(t)
				model.mode = test.mode
				if test.prepare != nil {
					test.prepare(&model, errors.New("stale error"))
				}

				var command tea.Cmd
				if route == "keyboard" {
					updated, keyboardCommand := model.Update(tea.KeyPressMsg(tea.Key{Code: 'r', Text: "r"}))
					model = updated.(Model)
					command = keyboardCommand
				} else {
					command = model.activeScreen().refresh(&model)
				}
				if (command != nil) != test.wantCommand {
					t.Fatalf("refresh command presence = %t, want %t", command != nil, test.wantCommand)
				}
				if test.assert != nil {
					test.assert(t, model)
				}
			})
		}
	}
}

func TestRefreshShortcutRespectsScreenOwnedInput(t *testing.T) {
	t.Run("subtask sort state key", func(t *testing.T) {
		model := interactionTestModel(t)
		model.mode = modeSubtasks
		state := model.jobDetails.State()
		state.SubtaskSortOpen = true
		state.SubtaskSort = 3
		model.jobDetails.RestoreState(state)
		updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: 'r', Text: "r"}))
		result := updated.(Model)
		if sort := result.jobDetails.State().SubtaskSort; command != nil || sort != 1 {
			t.Fatalf("sort dialog r = sort:%d command:%v", sort, command)
		}
	})

	t.Run("action confirmation", func(t *testing.T) {
		model := interactionTestModel(t)
		model.mode = modeActions
		state := model.jobOperations.State()
		state.ActionConfirm = true
		model.jobOperations.RestoreState(state)
		updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: 'r', Text: "r"}))
		result := updated.(Model)
		if confirmed := result.jobOperations.State().ActionConfirm; command != nil || !confirmed {
			t.Fatalf("confirmation r = confirm:%t command:%v", confirmed, command)
		}
	})

	t.Run("SQL editor text", func(t *testing.T) {
		model := interactionTestModel(t)
		model.mode = modeSQL
		model = updateWithKey(model, tea.Key{Code: 'i', Text: "i"})
		before := model.sql.State().Text
		updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: 'r', Text: "r"}))
		result := updated.(Model)
		if text := result.sql.State().Text; command != nil || text != before+"r" {
			t.Fatalf("SQL editor r = text:%q command:%v", text, command)
		}
	})

	t.Run("immutable profiler report", func(t *testing.T) {
		model := interactionTestModel(t)
		model.mode = modeProfilerFlameGraph
		updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: 'r', Text: "r"}))
		if command != nil || updated.(Model).mode != modeProfilerFlameGraph {
			t.Fatalf("immutable report r returned command %v", command)
		}
	})
}

func TestPauseShortcutIsGlobalAndRespectsScreenOwnedInput(t *testing.T) {
	t.Run("profiler", func(t *testing.T) {
		model := interactionTestModel(t)
		model.mode = modeProfiler
		updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeySpace, Text: " "}))
		result := updated.(Model)
		if command != nil || !result.paused {
			t.Fatalf("space produced command=%v paused=%t", command, result.paused)
		}
	})

	t.Run("SQL editor", func(t *testing.T) {
		model := sqlTestModel(t)
		model = updateWithKey(model, tea.Key{Code: 'i', Text: "i"})
		before := model.sql.State().Text
		updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeySpace, Text: " "}))
		result := updated.(Model)
		if text := result.sql.State().Text; command != nil || result.paused || text != before+" " {
			t.Fatalf("space produced command=%v paused=%t SQL=%q", command, result.paused, text)
		}
	})

	t.Run("document search", func(t *testing.T) {
		model := interactionTestModel(t)
		model.mode = modeDocument
		state := model.processDiagnostics.State()
		state.View = processmodule.ViewDocument
		state.DocumentSearch = true
		state.DocumentQuery = "task"
		model.processDiagnostics.RestoreState(state)
		updated, command := model.Update(tea.KeyPressMsg(tea.Key{Code: tea.KeySpace, Text: " "}))
		result := updated.(Model)
		query := result.processDiagnostics.State().DocumentQuery
		if command != nil || result.paused || query != "task " {
			t.Fatalf("space produced command=%v paused=%t query=%q", command, result.paused, query)
		}
	})
}

func TestRefreshPoliciesWithConditionalCommands(t *testing.T) {
	t.Run("static document", func(t *testing.T) {
		model := interactionTestModel(t)
		model.mode = modeDocument
		state := model.processDiagnostics.State()
		state.View = processmodule.ViewDocument
		state.DocumentSource = processmodule.SourceStatic
		state.DocumentErr = errors.New("keep me")
		model.processDiagnostics.RestoreState(state)
		if command := model.activeScreen().refresh(&model); command != nil {
			t.Fatal("static document unexpectedly requested a refresh")
		}
		state = model.processDiagnostics.State()
		if state.DocumentErr == nil || state.Generation != 0 {
			t.Fatalf("static document was mutated: %#v generation:%d", state, state.Generation)
		}
	})

	t.Run("metric resize", func(t *testing.T) {
		model := interactionTestModel(t)
		model.mode = modeMetricExplorer
		if command := model.activeScreen().resize(&model); command != nil {
			t.Fatal("empty metric catalog unexpectedly requested values")
		}
		state := model.metrics.State()
		state.Vertex, state.Names, state.Err = "source", []string{"metric"}, errors.New("stale error")
		model.metrics.RestoreState(state)
		if command := model.activeScreen().resize(&model); command == nil {
			t.Fatal("populated metric catalog did not request visible values")
		}
		state = model.metrics.State()
		if !state.Busy || state.Err != nil {
			t.Fatalf("metric resize state = busy:%t err:%v", state.Busy, state.Err)
		}
	})
}

func TestRegisteredAutomaticRefreshPolicies(t *testing.T) {
	manualModes := map[screenMode]bool{
		modeProcessLogs: true, modeDocument: true, modeThreadDump: true,
		modeProfilerFlameGraph: true, modeSQL: true,
	}
	for mode := screenMode(0); mode < modeCount; mode++ {
		model := interactionTestModel(t)
		model.mode = mode
		command := model.activeScreen().autoRefresh(&model)
		if manualModes[mode] && command != nil {
			t.Errorf("screen %q auto-refreshed manual content", model.activeScreen().name)
		}
		if !manualModes[mode] && command == nil {
			t.Errorf("screen %q did not auto-refresh", model.activeScreen().name)
		}
	}

	t.Run("open subtask diagnostics", func(t *testing.T) {
		model := interactionTestModel(t)
		model.mode = modeSubtasks
		state := model.jobDetails.State()
		state.DiagnosticsOpen = "source"
		model.jobDetails.RestoreState(state)
		if command := model.activeScreen().autoRefresh(&model); command == nil {
			t.Fatal("open subtask diagnostics did not refresh")
		}
	})

	t.Run("running job action", func(t *testing.T) {
		model := interactionTestModel(t)
		model.mode = modeActions
		state := model.jobOperations.State()
		state.ActionTriggerID = "trigger"
		state.ActionStatus = "IN_PROGRESS"
		state.ActionKind = 2
		model.jobOperations.RestoreState(state)
		if command := model.activeScreen().autoRefresh(&model); command == nil {
			t.Fatal("running job action did not poll")
		}
	})
}

func sameScreenCommand(left, right func(*Model) tea.Cmd) bool {
	return reflect.ValueOf(left).Pointer() == reflect.ValueOf(right).Pointer()
}

func TestEveryScreenModeIsRegistered(t *testing.T) {
	if got, want := len(screenRegistry), int(modeCount); got != want {
		t.Fatalf("screen registry has %d entries, want %d", got, want)
	}

	seenNames := make(map[string]screenMode, modeCount)
	for mode := screenMode(0); mode < modeCount; mode++ {
		screen := screenRegistry[mode]
		if screen.name == "" {
			t.Errorf("mode %d has no registered screen", mode)
			continue
		}
		if previous, exists := seenNames[screen.name]; exists {
			t.Errorf("screen name %q is shared by modes %d and %d", screen.name, previous, mode)
		}
		seenNames[screen.name] = mode
		if screen.scope == scopeUnknown {
			t.Errorf("screen %q (mode %d) has no semantic scope", screen.name, mode)
		}
		if screen.upParent > modeCount {
			t.Errorf("screen %q (mode %d) has invalid scope-up parent %d", screen.name, mode, screen.upParent)
		}

		capabilities := []struct {
			name    string
			missing bool
		}{
			{name: "render", missing: screen.render == nil},
			{name: "key", missing: screen.key == nil},
			{name: "click", missing: screen.click == nil},
			{name: "wheel", missing: screen.wheel == nil},
			{name: "refresh", missing: screen.refresh == nil},
			{name: "autoRefresh", missing: screen.autoRefresh == nil},
			{name: "resize", missing: screen.resize == nil},
			{name: "header", missing: screen.header == nil},
			{name: "footer", missing: screen.footer == nil},
			{name: "errors", missing: screen.errors == nil},
			{name: "capturesKeys", missing: screen.capturesKeys == nil},
		}
		for _, capability := range capabilities {
			if capability.missing {
				t.Errorf("screen %q (mode %d) has no %s capability", screen.name, mode, capability.name)
			}
		}
	}
}

func TestGraphZoomKeysReachGraphBeforeGlobalNavigation(t *testing.T) {
	model := interactionTestModel(t)
	model.mode = modeGraph
	state := model.graphViewport.State()
	state.Zoom = graphZoomCompact
	model.graphViewport.RestoreState(state)
	model.layout = model.buildGraphLayout(model.snapshot.Nodes)

	updated, _ := model.Update(tea.KeyPressMsg(tea.Key{Code: '+', Text: "+"}))
	result := updated.(Model)
	if result.mode != modeGraph || result.graphViewport.State().Zoom != graphZoomDetailed {
		t.Fatalf("+ produced mode=%d zoom=%v", result.mode, result.graphViewport.State().Zoom)
	}
}

func TestInitialGraphAutoFitWaitsForTerminalSize(t *testing.T) {
	for _, size := range []struct{ width, height int }{{80, 24}, {100, 30}} {
		model := interactionTestModel(t)
		model.width, model.height = 0, 0
		model.graphViewport = jobgraphmodule.NewViewport(true)
		model.snapshot.Nodes = graphTestChain(8)
		model.layout = model.buildGraphLayout(model.snapshot.Nodes)
		if model.autoFitGraphIfReady() || !model.graphViewport.State().AutoFitPending {
			t.Fatalf("%dx%d auto-fit ran before terminal size: %#v", size.width, size.height, model.graphViewport.State())
		}
		model.width, model.height = size.width, size.height
		if !model.autoFitGraphIfReady() || model.graphViewport.State().AutoFitPending {
			t.Fatalf("%dx%d auto-fit did not run after terminal size: %#v", size.width, size.height, model.graphViewport.State())
		}
		if model.layout.Width > model.graphWidth() || model.layout.Height > model.graphHeight() {
			t.Fatalf("%dx%d auto-fit layout %dx%d exceeds graph viewport %dx%d",
				size.width, size.height, model.layout.Width, model.layout.Height, model.graphWidth(), model.graphHeight())
		}
	}
}

func graphTestChain(count int) []flink.Node {
	nodes := make([]flink.Node, count)
	for index := range nodes {
		nodes[index] = flink.Node{ID: string(rune('a' + index)), Name: "Stage", State: "RUNNING"}
		if index > 0 {
			previous := string(rune('a' + index - 1))
			nodes[index].Inputs = []flink.Input{{ID: previous}}
		}
	}
	return nodes
}

func TestUnknownScreenModeFallsBackToGraph(t *testing.T) {
	model := Model{mode: modeCount + 10}
	if screen := model.activeScreen(); screen.name != screenRegistry[modeGraph].name {
		t.Fatalf("unknown mode resolved to %q, want graph", screen.name)
	}
}

func TestDocumentLoadFailureIsReported(t *testing.T) {
	for _, backMode := range []screenMode{modeGraph, modeProcessLogs} {
		documentError := errors.New("document load failed")
		model := Model{mode: modeDocument}
		state := model.processDiagnostics.State()
		state.DocumentBackToken = int(backMode)
		state.DocumentErr = documentError
		model.processDiagnostics.RestoreState(state)

		displayed := model.currentErrors()
		if len(displayed) != 1 {
			t.Fatalf("back mode %d: currentErrors() returned %d entries, want 1: %#v", backMode, len(displayed), displayed)
		}
		if displayed[0].label != "Document" || !errors.Is(displayed[0].err, documentError) {
			t.Fatalf("back mode %d: document error = %#v, want label Document and %v", backMode, displayed[0], documentError)
		}
	}
}

func TestRegisteredScreenErrorPoliciesExposeOwnedErrors(t *testing.T) {
	tests := []struct {
		name  string
		mode  screenMode
		label string
		set   func(*Model, error)
	}{
		{name: "job snapshot", mode: modeGraph, label: "Job snapshot", set: func(m *Model, err error) { m.err = err }},
		{name: "snapshot issue", mode: modeGraph, label: "Metrics / vertex-1", set: func(m *Model, err error) {
			m.snapshot.Issues = []flink.SnapshotIssue{{Kind: flink.SnapshotIssueMetrics, VertexID: "vertex-123", Err: err}}
		}},
		{name: "cluster overview", mode: modeJobs, label: "Cluster overview", set: func(m *Model, err error) {
			state := m.jobList.State()
			state.ClusterErr = err
			m.jobList.RestoreState(state)
		}},
		{name: "job list", mode: modeJobs, label: "Job list", set: func(m *Model, err error) {
			state := m.jobList.State()
			state.JobsErr = err
			m.jobList.RestoreState(state)
		}},
		{name: "subtasks", mode: modeSubtasks, label: "Subtasks", set: func(m *Model, err error) {
			state := m.jobDetails.State()
			state.DiagnosticsErr = err
			m.jobDetails.RestoreState(state)
		}},
		{name: "flame graph", mode: modeFlameGraph, label: "Flame graph", set: func(m *Model, err error) {
			state := m.flameGraphs.State()
			state.Err = err
			m.flameGraphs.RestoreState(state)
		}},
		{name: "checkpoint details", mode: modeCheckpointOperators, label: "Checkpoint details", set: func(m *Model, err error) {
			state := m.checkpoints.State()
			state.DetailErr = err
			m.checkpoints.RestoreState(state)
		}},
		{name: "checkpoint configuration", mode: modeCheckpointOperators, label: "Checkpoint configuration", set: func(m *Model, err error) {
			state := m.checkpoints.State()
			state.ConfigErr = err
			m.checkpoints.RestoreState(state)
		}},
		{name: "checkpoint subtasks", mode: modeCheckpointSubtasks, label: "Checkpoint subtasks", set: func(m *Model, err error) {
			state := m.checkpoints.State()
			state.SubtasksErr = err
			m.checkpoints.RestoreState(state)
		}},
		{name: "job configuration", mode: modeJobConfig, label: "Job configuration", set: func(m *Model, err error) {
			state := m.jobOperations.State()
			state.ConfigurationErr = err
			m.jobOperations.RestoreState(state)
		}},
		{name: "metrics", mode: modeMetricExplorer, label: "Metrics", set: func(m *Model, err error) {
			state := m.metrics.State()
			state.Err = err
			m.metrics.RestoreState(state)
		}},
		{name: "accumulators", mode: modeAccumulators, label: "Accumulators", set: func(m *Model, err error) {
			state := m.accumulators.State()
			state.Err = err
			m.accumulators.RestoreState(state)
		}},
		{name: "job action", mode: modeActions, label: "Job action", set: func(m *Model, err error) {
			state := m.jobOperations.State()
			state.ActionErr = err
			m.jobOperations.RestoreState(state)
		}},
		{name: "infrastructure", mode: modeTaskManagers, label: "Infrastructure", set: func(m *Model, err error) {
			state := m.infrastructure.State()
			state.Err = err
			m.infrastructure.RestoreState(state)
		}},
		{name: "process logs", mode: modeProcessLogs, label: "Process logs", set: func(m *Model, err error) {
			state := m.processDiagnostics.State()
			state.LogsErr = err
			m.processDiagnostics.RestoreState(state)
		}},
		{name: "thread dump", mode: modeThreadDump, label: "Thread dump", set: func(m *Model, err error) {
			state := m.processDiagnostics.State()
			state.ThreadErr = err
			m.processDiagnostics.RestoreState(state)
		}},
		{name: "profiler", mode: modeProfiler, label: "Profiler", set: func(m *Model, err error) {
			state := m.profiler.State()
			state.Err = err
			m.profiler.RestoreState(state)
		}},
		{name: "profiler report", mode: modeProfilerFlameGraph, label: "Profiler report", set: func(m *Model, err error) {
			state := m.profiler.State()
			state.Err = err
			m.profiler.RestoreState(state)
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ownedError := errors.New("owned screen error")
			model := Model{mode: test.mode}
			test.set(&model, ownedError)

			displayed := model.currentErrors()
			if len(displayed) != 1 {
				t.Fatalf("currentErrors() returned %d entries, want 1: %#v", len(displayed), displayed)
			}
			if displayed[0].label != test.label || !errors.Is(displayed[0].err, ownedError) {
				t.Fatalf("screen error = %#v, want label %q and %v", displayed[0], test.label, ownedError)
			}
		})
	}

	t.Run("SQL Gateway", func(t *testing.T) {
		model := Model{mode: modeSQL}
		model.sql.Open()
		displayed := model.currentErrors()
		if len(displayed) != 1 || displayed[0].label != "SQL Gateway" || displayed[0].err == nil {
			t.Fatalf("SQL screen error = %#v", displayed)
		}
	})
}
