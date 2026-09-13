package flink

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

type ConfigurationEntry struct {
	Key   string
	Value string
}

type JobConfiguration struct {
	JobID           string
	Name            string
	RestartStrategy string
	Parallelism     int
	ObjectReuse     bool
	User            []ConfigurationEntry
	UpdatedAt       time.Time
}

type AsyncOperation struct {
	Status   string
	Location string
	Failure  string
}

func (c *Client) JobConfiguration(ctx context.Context, jobID string) (JobConfiguration, error) {
	var response struct {
		JobID     string `json:"jid"`
		Name      string `json:"name"`
		Execution struct {
			RestartStrategy string                     `json:"restart-strategy"`
			Parallelism     int                        `json:"job-parallelism"`
			ObjectReuse     bool                       `json:"object-reuse-mode"`
			User            map[string]json.RawMessage `json:"user-config"`
		} `json:"execution-config"`
	}
	path := "/jobs/" + url.PathEscape(jobID) + "/config"
	if err := c.get(ctx, path, &response); err != nil {
		return JobConfiguration{}, err
	}
	configuration := JobConfiguration{
		JobID:           response.JobID,
		Name:            response.Name,
		RestartStrategy: response.Execution.RestartStrategy,
		Parallelism:     response.Execution.Parallelism,
		ObjectReuse:     response.Execution.ObjectReuse,
		User:            make([]ConfigurationEntry, 0, len(response.Execution.User)),
		UpdatedAt:       time.Now(),
	}
	for key, raw := range response.Execution.User {
		configuration.User = append(configuration.User, ConfigurationEntry{Key: key, Value: displayJSON(raw)})
	}
	slices.SortFunc(configuration.User, func(left, right ConfigurationEntry) int {
		return cmp.Compare(left.Key, right.Key)
	})
	return configuration, nil
}

func (c *Client) CancelJob(ctx context.Context, jobID string) error {
	path := "/jobs/" + url.PathEscape(jobID) + "?mode=cancel"
	return c.requestJSON(ctx, http.MethodPatch, path, nil, nil)
}

func (c *Client) TriggerCheckpoint(ctx context.Context, jobID, checkpointType string) (string, error) {
	checkpointType = strings.ToUpper(strings.TrimSpace(checkpointType))
	switch checkpointType {
	case "CONFIGURED", "FULL":
	default:
		return "", fmt.Errorf("unsupported checkpoint type %q", checkpointType)
	}
	return c.trigger(ctx, "/jobs/"+url.PathEscape(jobID)+"/checkpoints", map[string]any{
		"checkpointType": checkpointType,
	})
}

func (c *Client) TriggerSavepoint(ctx context.Context, jobID, targetDirectory string) (string, error) {
	body := map[string]any{
		"cancel-job": false,
		"formatType": "CANONICAL",
	}
	if targetDirectory = strings.TrimSpace(targetDirectory); targetDirectory != "" {
		body["target-directory"] = targetDirectory
	}
	return c.trigger(ctx, "/jobs/"+url.PathEscape(jobID)+"/savepoints", body)
}

func (c *Client) StopWithSavepoint(ctx context.Context, jobID string, drain bool, targetDirectory string) (string, error) {
	body := map[string]any{
		"drain":      drain,
		"formatType": "CANONICAL",
	}
	if targetDirectory = strings.TrimSpace(targetDirectory); targetDirectory != "" {
		body["targetDirectory"] = targetDirectory
	}
	return c.trigger(ctx, "/jobs/"+url.PathEscape(jobID)+"/stop", body)
}

func (c *Client) CheckpointOperation(ctx context.Context, jobID, triggerID string) (AsyncOperation, error) {
	path := "/jobs/" + url.PathEscape(jobID) + "/checkpoints/" + url.PathEscape(triggerID)
	return c.asyncOperation(ctx, path)
}

func (c *Client) SavepointOperation(ctx context.Context, jobID, triggerID string) (AsyncOperation, error) {
	path := "/jobs/" + url.PathEscape(jobID) + "/savepoints/" + url.PathEscape(triggerID)
	return c.asyncOperation(ctx, path)
}

func (c *Client) trigger(ctx context.Context, path string, body any) (string, error) {
	var response struct {
		RequestID string `json:"request-id"`
	}
	if err := c.requestJSON(ctx, http.MethodPost, path, body, &response); err != nil {
		return "", err
	}
	if response.RequestID == "" {
		return "", fmt.Errorf("POST %s returned no request id", path)
	}
	return response.RequestID, nil
}

func (c *Client) asyncOperation(ctx context.Context, path string) (AsyncOperation, error) {
	var response struct {
		Status struct {
			ID string `json:"id"`
		} `json:"status"`
		Operation struct {
			Location          string          `json:"location"`
			SavepointFailure  json.RawMessage `json:"failure-cause"`
			CheckpointFailure json.RawMessage `json:"failureCause"`
		} `json:"operation"`
	}
	if err := c.get(ctx, path, &response); err != nil {
		return AsyncOperation{}, err
	}
	return AsyncOperation{
		Status:   response.Status.ID,
		Location: response.Operation.Location,
		Failure:  cmp.Or(displayFailure(response.Operation.SavepointFailure), displayFailure(response.Operation.CheckpointFailure)),
	}, nil
}

func (c *Client) requestJSON(ctx context.Context, method, path string, body, target any) error {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode %s %s: %w", method, path, err)
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return &RequestError{Service: "Flink", Method: method, Path: path, Err: err}
	}
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.http.Do(request)
	if err != nil {
		return &RequestError{Service: "Flink", Method: method, Path: path, Err: err}
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return newResponseError(response, "Flink", method, path, 2048)
	}
	if target == nil || response.StatusCode == http.StatusNoContent {
		return nil
	}
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		return &RequestError{Service: "Flink", Method: method, Path: path, Err: fmt.Errorf("decode response: %w", err)}
	}
	return nil
}

func displayJSON(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return "null"
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err == nil {
		return compact.String()
	}
	return string(raw)
}

func displayFailure(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" || string(raw) == "{}" {
		return ""
	}
	var structured struct {
		Class      string `json:"class"`
		Stacktrace string `json:"stack-trace"`
	}
	if err := json.Unmarshal(raw, &structured); err == nil {
		if message := firstLine(structured.Stacktrace); message != "" {
			return message
		}
		if structured.Class != "" {
			return structured.Class
		}
	}
	return displayJSON(raw)
}
