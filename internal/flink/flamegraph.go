package flink

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"time"
)

// FlameGraphType is the sampling mode accepted by Flink's vertex flame-graph
// endpoint. Flink calls the combined on/off-CPU view "full" in REST and
// "Mixed" in the Web UI.
type FlameGraphType string

const (
	FlameGraphOnCPU  FlameGraphType = "on_cpu"
	FlameGraphOffCPU FlameGraphType = "off_cpu"
	FlameGraphFull   FlameGraphType = "full"
)

// FlameGraph is one sampled call tree for a vertex or one of its subtasks.
// Subtask -1 means that Flink aggregated every subtask.
type FlameGraph struct {
	Type               FlameGraphType
	Subtask            int
	EndTimestampMillis int64
	EndTimestamp       time.Time
	Root               FlameGraphNode
}

type FlameGraphNode struct {
	Name     string           `json:"name"`
	Value    int64            `json:"value"`
	Children []FlameGraphNode `json:"children"`
}

func (graph FlameGraph) Disabled() bool {
	return graph.EndTimestampMillis == -2
}

func (graph FlameGraph) Sampling() bool {
	return graph.EndTimestampMillis < 0 && !graph.Disabled()
}

func (graph FlameGraph) Ready() bool {
	return graph.EndTimestampMillis > 0
}

// VertexFlameGraph starts or polls Flink's sampling operation. A negative
// subtask requests the aggregate graph; otherwise subtaskindex scopes the
// sample to exactly one parallel subtask.
func (c *Client) VertexFlameGraph(
	ctx context.Context,
	jobID string,
	vertexID string,
	typeName FlameGraphType,
	subtask int,
) (FlameGraph, error) {
	if !validFlameGraphType(typeName) {
		return FlameGraph{}, fmt.Errorf("unsupported flame graph type %q", typeName)
	}
	if subtask < -1 {
		return FlameGraph{}, fmt.Errorf("invalid flame graph subtask %d", subtask)
	}
	query := url.Values{}
	query.Set("type", string(typeName))
	if subtask >= 0 {
		query.Set("subtaskindex", strconv.Itoa(subtask))
	}
	path := fmt.Sprintf(
		"/jobs/%s/vertices/%s/flamegraph?%s",
		url.PathEscape(jobID),
		url.PathEscape(vertexID),
		query.Encode(),
	)
	var response struct {
		EndTimestamp int64           `json:"endTimestamp"`
		Data         *FlameGraphNode `json:"data"`
	}
	if err := c.get(ctx, path, &response); err != nil {
		return FlameGraph{}, err
	}
	graph := FlameGraph{
		Type:               typeName,
		Subtask:            subtask,
		EndTimestampMillis: response.EndTimestamp,
	}
	if response.EndTimestamp >= 0 {
		graph.EndTimestamp = time.UnixMilli(response.EndTimestamp)
	}
	if response.Data != nil {
		graph.Root = *response.Data
	}
	return graph, nil
}

func validFlameGraphType(typeName FlameGraphType) bool {
	switch typeName {
	case FlameGraphOnCPU, FlameGraphOffCPU, FlameGraphFull:
		return true
	default:
		return false
	}
}
