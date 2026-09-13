package flink

import (
	"bufio"
	"cmp"
	"context"
	"errors"
	"fmt"
	"html"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"
)

// ProfilerMode is an async-profiler event supported by Flink's process
// profiler endpoint.
type ProfilerMode string

const (
	ProfilerCPU    ProfilerMode = "CPU"
	ProfilerLock   ProfilerMode = "LOCK"
	ProfilerWall   ProfilerMode = "WALL"
	ProfilerAlloc  ProfilerMode = "ALLOC"
	ProfilerITimer ProfilerMode = "ITIMER"
)

// Profiling is one JobManager or TaskManager async-profiler invocation.
type Profiling struct {
	Status      string
	Mode        ProfilerMode
	TriggeredAt time.Time
	FinishedAt  time.Time
	Duration    time.Duration
	Message     string
	OutputFile  string
}

type profilingResponse struct {
	Status        string `json:"status"`
	Mode          string `json:"mode"`
	ProfilingMode string `json:"profilingMode"`
	TriggerTime   int64  `json:"triggerTime"`
	FinishedTime  int64  `json:"finishedTime"`
	Duration      int64  `json:"duration"`
	Message       string `json:"message"`
	OutputFile    string `json:"outputFile"`
}

// ProfilingList returns profiler runs newest-first for one Flink process.
func (c *Client) ProfilingList(ctx context.Context, process ProcessRef) ([]Profiling, error) {
	base, err := process.basePath()
	if err != nil {
		return nil, err
	}
	var response struct {
		ProfilingList []profilingResponse `json:"profilingList"`
	}
	if err := c.get(ctx, base+"/profiler", &response); err != nil {
		return nil, err
	}
	profiles := make([]Profiling, len(response.ProfilingList))
	for index, item := range response.ProfilingList {
		profiles[index] = profilingFromResponse(item)
	}
	slices.SortStableFunc(profiles, func(left, right Profiling) int {
		return right.TriggeredAt.Compare(left.TriggeredAt)
	})
	return profiles, nil
}

// StartProfiling asks Flink to run async-profiler for a bounded duration.
func (c *Client) StartProfiling(ctx context.Context, process ProcessRef, mode ProfilerMode, duration time.Duration) (Profiling, error) {
	base, err := process.basePath()
	if err != nil {
		return Profiling{}, err
	}
	if !validProfilerMode(mode) {
		return Profiling{}, fmt.Errorf("unsupported profiler mode %q", mode)
	}
	seconds := int64(duration / time.Second)
	if seconds < 1 || seconds > 300 {
		return Profiling{}, errors.New("profiler duration must be between 1 and 300 seconds")
	}
	var response profilingResponse
	request := struct {
		Mode     ProfilerMode `json:"mode"`
		Duration int64        `json:"duration"`
	}{Mode: mode, Duration: seconds}
	if err := c.requestJSON(ctx, http.MethodPost, base+"/profiler", request, &response); err != nil {
		return Profiling{}, err
	}
	return profilingFromResponse(response), nil
}

// ProfilerReport downloads the HTML flame graph produced by a successful run.
func (c *Client) ProfilerReport(ctx context.Context, process ProcessRef, filename string) (string, error) {
	base, err := process.basePath()
	if err != nil {
		return "", err
	}
	filename = strings.TrimSpace(filename)
	if filename == "" {
		return "", errors.New("profiler report filename is required")
	}
	return c.getText(ctx, base+"/profiler/"+url.PathEscape(filename))
}

func profilingFromResponse(response profilingResponse) Profiling {
	mode := response.ProfilingMode
	if mode == "" {
		mode = response.Mode
	}
	return Profiling{
		Status:      strings.ToUpper(strings.TrimSpace(response.Status)),
		Mode:        ProfilerMode(strings.ToUpper(strings.TrimSpace(mode))),
		TriggeredAt: unixMillis(response.TriggerTime),
		FinishedAt:  unixMillis(response.FinishedTime),
		Duration:    time.Duration(response.Duration) * time.Second,
		Message:     strings.TrimSpace(response.Message),
		OutputFile:  strings.TrimSpace(response.OutputFile),
	}
}

func validProfilerMode(mode ProfilerMode) bool {
	switch mode {
	case ProfilerCPU, ProfilerLock, ProfilerWall, ProfilerAlloc, ProfilerITimer:
		return true
	default:
		return false
	}
}

const maxProfilerReportFrames = 250_000

type profilerReportFrame struct {
	level    int
	left     int64
	width    int64
	name     string
	children []*profilerReportFrame
}

// ParseProfilerReport converts async-profiler's self-contained HTML frame
// calls into the same sampled tree used by the terminal flame graph renderer.
// Flink's profiler endpoint emits one f(level,left,width,type,title,...) call
// per frame.
func ParseProfilerReport(report string) (FlameGraphNode, error) {
	levels := make(map[int][]*profilerReportFrame)
	frameCount := 0
	scanner := bufio.NewScanner(strings.NewReader(report))
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "f(") || !strings.HasSuffix(line, ")") {
			continue
		}
		arguments, err := splitJavaScriptArguments(line[2 : len(line)-1])
		if err != nil || len(arguments) < 5 {
			continue
		}
		level, levelErr := strconv.Atoi(strings.TrimSpace(arguments[0]))
		left, leftErr := strconv.ParseInt(strings.TrimSpace(arguments[1]), 10, 64)
		width, widthErr := strconv.ParseInt(strings.TrimSpace(arguments[2]), 10, 64)
		name, nameErr := unquoteJavaScriptString(strings.TrimSpace(arguments[4]))
		if levelErr != nil || leftErr != nil || widthErr != nil || nameErr != nil || level < 0 || level > 2048 || left < 0 || width <= 0 {
			continue
		}
		frame := &profilerReportFrame{level: level, left: left, width: width, name: html.UnescapeString(name)}
		levels[level] = append(levels[level], frame)
		frameCount++
		if frameCount > maxProfilerReportFrames {
			return FlameGraphNode{}, fmt.Errorf("profiler report contains more than %d frames", maxProfilerReportFrames)
		}
	}
	if err := scanner.Err(); err != nil {
		return FlameGraphNode{}, fmt.Errorf("read profiler report: %w", err)
	}
	if len(levels[0]) == 0 {
		return FlameGraphNode{}, errors.New("profiler report contains no supported flame graph frames")
	}
	for level := range levels {
		slices.SortStableFunc(levels[level], func(left, right *profilerReportFrame) int {
			if comparison := cmp.Compare(left.left, right.left); comparison != 0 {
				return comparison
			}
			return cmp.Compare(right.width, left.width)
		})
	}
	for level := 1; len(levels[level]) > 0; level++ {
		parents := levels[level-1]
		parentIndex := 0
		for _, child := range levels[level] {
			for parentIndex < len(parents) && parents[parentIndex].left+parents[parentIndex].width <= child.left {
				parentIndex++
			}
			for candidate := parentIndex; candidate < len(parents); candidate++ {
				parent := parents[candidate]
				if parent.left > child.left {
					break
				}
				if child.left >= parent.left && child.left+child.width <= parent.left+parent.width {
					parent.children = append(parent.children, child)
					break
				}
			}
		}
	}
	root := levels[0][0]
	for _, candidate := range levels[0][1:] {
		if candidate.width > root.width {
			root = candidate
		}
	}
	return profilerFrameNode(root), nil
}

func profilerFrameNode(frame *profilerReportFrame) FlameGraphNode {
	node := FlameGraphNode{Name: frame.name, Value: frame.width, Children: make([]FlameGraphNode, len(frame.children))}
	for index, child := range frame.children {
		node.Children[index] = profilerFrameNode(child)
	}
	return node
}

func splitJavaScriptArguments(value string) ([]string, error) {
	arguments := make([]string, 0, 8)
	start := 0
	quote := byte(0)
	escaped := false
	for index := 0; index < len(value); index++ {
		character := value[index]
		if quote != 0 {
			if escaped {
				escaped = false
				continue
			}
			if character == '\\' {
				escaped = true
				continue
			}
			if character == quote {
				quote = 0
			}
			continue
		}
		if character == '\'' || character == '"' {
			quote = character
			continue
		}
		if character == ',' {
			arguments = append(arguments, strings.TrimSpace(value[start:index]))
			start = index + 1
		}
	}
	if quote != 0 || escaped {
		return nil, errors.New("unterminated JavaScript string")
	}
	arguments = append(arguments, strings.TrimSpace(value[start:]))
	return arguments, nil
}

func unquoteJavaScriptString(value string) (string, error) {
	if len(value) < 2 || value[0] != value[len(value)-1] || value[0] != '\'' && value[0] != '"' {
		return "", errors.New("invalid JavaScript string")
	}
	value = value[1 : len(value)-1]
	var result strings.Builder
	for index := 0; index < len(value); index++ {
		character := value[index]
		if character != '\\' {
			result.WriteByte(character)
			continue
		}
		index++
		if index >= len(value) {
			return "", errors.New("unterminated JavaScript escape")
		}
		switch value[index] {
		case '\\', '\'', '"', '/':
			result.WriteByte(value[index])
		case 'b':
			result.WriteByte('\b')
		case 'f':
			result.WriteByte('\f')
		case 'n':
			result.WriteByte('\n')
		case 'r':
			result.WriteByte('\r')
		case 't':
			result.WriteByte('\t')
		case 'v':
			result.WriteByte('\v')
		case 'x':
			code, next, err := parseJavaScriptEscape(value, index+1, 2)
			if err != nil {
				return "", err
			}
			result.WriteRune(code)
			index = next - 1
		case 'u':
			code, next, err := parseJavaScriptEscape(value, index+1, 4)
			if err != nil {
				return "", err
			}
			index = next - 1
			r := code
			if utf16.IsSurrogate(r) && next+6 <= len(value) && value[next] == '\\' && value[next+1] == 'u' {
				second, afterSecond, secondErr := parseJavaScriptEscape(value, next+2, 4)
				if secondErr == nil {
					r = utf16.DecodeRune(r, second)
					index = afterSecond - 1
				}
			}
			result.WriteRune(r)
		default:
			result.WriteByte(value[index])
		}
	}
	return result.String(), nil
}

func parseJavaScriptEscape(value string, start, length int) (rune, int, error) {
	end := start + length
	if end > len(value) {
		return 0, start, errors.New("short JavaScript escape")
	}
	code, err := strconv.ParseUint(value[start:end], 16, 16)
	if err != nil {
		return 0, start, fmt.Errorf("invalid JavaScript escape: %w", err)
	}
	return rune(code), end, nil
}
