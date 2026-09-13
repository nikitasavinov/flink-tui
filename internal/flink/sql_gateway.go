package flink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type SQLGatewayClient struct {
	baseURL string
	http    *http.Client
}

type SQLGatewayInfo struct {
	ProductName string
	Version     string
}

type SQLColumn struct {
	Name string
	Type string
}

type SQLRow struct {
	Kind   string
	Fields []string
}

type SQLResult struct {
	ResultType    string
	IsQueryResult bool
	JobID         string
	ResultKind    string
	Columns       []SQLColumn
	Rows          []SQLRow
	NextURI       string
}

func NewSQLGatewayClient(endpoint string) (*SQLGatewayClient, error) {
	return NewSQLGatewayClientWithConfig(endpoint, HTTPClientConfig{})
}

// NewSQLGatewayClientWithConfig builds a SQL Gateway client with optional
// authentication, custom certificate authorities, and mutual TLS.
func NewSQLGatewayClientWithConfig(endpoint string, config HTTPClientConfig) (*SQLGatewayClient, error) {
	baseURL, err := normalizeEndpoint(endpoint, "SQL Gateway")
	if err != nil {
		return nil, err
	}
	httpClient, err := newHTTPClient(10*time.Second, config)
	if err != nil {
		return nil, fmt.Errorf("configure SQL Gateway HTTP client: %w", err)
	}
	return &SQLGatewayClient{
		baseURL: baseURL,
		http:    httpClient,
	}, nil
}

func (c *SQLGatewayClient) Endpoint() string { return c.baseURL }

func (c *SQLGatewayClient) Info(ctx context.Context) (SQLGatewayInfo, error) {
	var response struct {
		ProductName string `json:"productName"`
		Version     string `json:"version"`
	}
	if err := c.get(ctx, "/v1/info", &response); err != nil {
		return SQLGatewayInfo{}, err
	}
	return SQLGatewayInfo{ProductName: response.ProductName, Version: response.Version}, nil
}

func (c *SQLGatewayClient) OpenSession(ctx context.Context, name string, properties map[string]string) (string, error) {
	body := struct {
		Name       string            `json:"sessionName,omitempty"`
		Properties map[string]string `json:"properties,omitempty"`
	}{Name: name, Properties: properties}
	var response struct {
		Handle string `json:"sessionHandle"`
	}
	if err := c.requestJSON(ctx, http.MethodPost, "/v1/sessions", body, &response); err != nil {
		return "", err
	}
	if response.Handle == "" {
		return "", errorsForSQLGateway("open session returned no handle")
	}
	return response.Handle, nil
}

func (c *SQLGatewayClient) Execute(ctx context.Context, session, statement string) (string, error) {
	statement = strings.TrimSpace(statement)
	if statement == "" {
		return "", errorsForSQLGateway("SQL statement is empty")
	}
	body := struct {
		Statement string `json:"statement"`
	}{Statement: statement}
	path := "/v1/sessions/" + url.PathEscape(session) + "/statements"
	var response struct {
		Handle string `json:"operationHandle"`
	}
	if err := c.requestJSON(ctx, http.MethodPost, path, body, &response); err != nil {
		return "", err
	}
	if response.Handle == "" {
		return "", errorsForSQLGateway("execute statement returned no operation handle")
	}
	return response.Handle, nil
}

func (c *SQLGatewayClient) FetchResults(ctx context.Context, session, operation, nextURI string) (SQLResult, error) {
	path := nextURI
	if path == "" {
		path = "/v1/sessions/" + url.PathEscape(session) + "/operations/" + url.PathEscape(operation) + "/result/0"
	}
	var response struct {
		ResultType    string `json:"resultType"`
		IsQueryResult bool   `json:"isQueryResult"`
		JobID         string `json:"jobID"`
		ResultKind    string `json:"resultKind"`
		Results       struct {
			Columns []struct {
				Name        string `json:"name"`
				LogicalType struct {
					Type string `json:"type"`
				} `json:"logicalType"`
			} `json:"columns"`
			Data []struct {
				Kind   string            `json:"kind"`
				Fields []json.RawMessage `json:"fields"`
			} `json:"data"`
		} `json:"results"`
		NextURI string `json:"nextResultUri"`
	}
	if err := c.get(ctx, path, &response); err != nil {
		return SQLResult{}, err
	}
	result := SQLResult{
		ResultType:    response.ResultType,
		IsQueryResult: response.IsQueryResult,
		JobID:         response.JobID,
		ResultKind:    response.ResultKind,
		Columns:       make([]SQLColumn, len(response.Results.Columns)),
		Rows:          make([]SQLRow, len(response.Results.Data)),
		NextURI:       response.NextURI,
	}
	for index, column := range response.Results.Columns {
		result.Columns[index] = SQLColumn{Name: column.Name, Type: column.LogicalType.Type}
	}
	for index, row := range response.Results.Data {
		result.Rows[index] = SQLRow{Kind: row.Kind, Fields: make([]string, len(row.Fields))}
		for fieldIndex, field := range row.Fields {
			result.Rows[index].Fields[fieldIndex] = displayJSON(field)
		}
	}
	return result, nil
}

func (c *SQLGatewayClient) CancelOperation(ctx context.Context, session, operation string) (string, error) {
	path := "/v1/sessions/" + url.PathEscape(session) + "/operations/" + url.PathEscape(operation) + "/cancel"
	var response struct {
		Status string `json:"status"`
	}
	if err := c.requestJSON(ctx, http.MethodPost, path, map[string]any{}, &response); err != nil {
		return "", err
	}
	return response.Status, nil
}

func (c *SQLGatewayClient) CloseOperation(ctx context.Context, session, operation string) error {
	path := "/v1/sessions/" + url.PathEscape(session) + "/operations/" + url.PathEscape(operation) + "/close"
	return c.requestJSON(ctx, http.MethodDelete, path, nil, nil)
}

func (c *SQLGatewayClient) CloseSession(ctx context.Context, session string) error {
	path := "/v1/sessions/" + url.PathEscape(session)
	return c.requestJSON(ctx, http.MethodDelete, path, nil, nil)
}

func (c *SQLGatewayClient) get(ctx context.Context, path string, target any) error {
	return c.requestJSON(ctx, http.MethodGet, path, nil, target)
}

func (c *SQLGatewayClient) requestJSON(ctx context.Context, method, path string, body, target any) error {
	requestURL, err := c.resolve(path)
	if err != nil {
		return err
	}
	var reader io.Reader
	if body != nil {
		encoded, encodeErr := json.Marshal(body)
		if encodeErr != nil {
			return fmt.Errorf("encode SQL Gateway request: %w", encodeErr)
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, requestURL, reader) // #nosec G704 -- resolve confines result URIs to the validated, operator-selected gateway origin.
	if err != nil {
		return &RequestError{Service: "SQL Gateway", Method: method, Path: path, Err: err}
	}
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.http.Do(request) // #nosec G704 -- Requests target the configured gateway; newHTTPClient blocks redirects to other origins.
	if err != nil {
		return &RequestError{Service: "SQL Gateway", Method: method, Path: path, Err: err}
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return newResponseError(response, "SQL Gateway", method, path, 4096)
	}
	if target == nil || response.StatusCode == http.StatusNoContent {
		return nil
	}
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		return &RequestError{Service: "SQL Gateway", Method: method, Path: path, Err: fmt.Errorf("decode response: %w", err)}
	}
	return nil
}

func (c *SQLGatewayClient) resolve(path string) (string, error) {
	base, err := url.Parse(c.baseURL + "/")
	if err != nil {
		return "", err
	}
	reference, err := url.Parse(path)
	if err != nil {
		return "", err
	}
	if !reference.IsAbs() && reference.Host == "" {
		// Flink returns gateway-relative /v1/... pagination links even when
		// the gateway is served under a reverse-proxy path. Preserve that
		// prefix, including for relative links, without adding it twice to
		// links already rewritten by a proxy.
		prefix := strings.TrimRight(base.EscapedPath(), "/")
		referencePath := reference.EscapedPath()
		if strings.HasPrefix(referencePath, "/") && (prefix == "" || !strings.HasPrefix(referencePath, prefix+"/")) {
			referencePath = prefix + referencePath
			decoded, decodeErr := url.PathUnescape(referencePath)
			if decodeErr != nil {
				return "", decodeErr
			}
			reference.Path, reference.RawPath = decoded, referencePath
		}
	}
	resolved := base.ResolveReference(reference)
	if !strings.EqualFold(resolved.Scheme, base.Scheme) || !strings.EqualFold(resolved.Host, base.Host) || resolved.User != nil {
		return "", errorsForSQLGateway("result URI points outside the configured gateway")
	}
	return resolved.String(), nil
}

func errorsForSQLGateway(message string) error {
	return fmt.Errorf("SQL Gateway: %s", message)
}
