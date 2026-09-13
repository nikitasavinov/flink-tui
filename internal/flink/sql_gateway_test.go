package flink

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestSQLGatewaySessionExecutionAndResults(t *testing.T) {
	client := testSQLGatewayClient(t, func(request *http.Request) (string, int) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/v1/info":
			return `{"productName":"Apache Flink","version":"2.3.0"}`, http.StatusOK
		case request.Method == http.MethodPost && request.URL.Path == "/v1/sessions":
			return `{"sessionHandle":"session"}`, http.StatusOK
		case request.Method == http.MethodPost && request.URL.Path == "/v1/sessions/session/statements":
			payload, _ := io.ReadAll(request.Body)
			if !strings.Contains(string(payload), `"statement":"SELECT 1"`) {
				t.Errorf("execute body = %s", payload)
			}
			return `{"operationHandle":"operation"}`, http.StatusOK
		case request.Method == http.MethodGet && request.URL.Path == "/v1/sessions/session/operations/operation/result/0":
			return `{
				"resultType":"PAYLOAD","isQueryResult":true,"jobID":"job","resultKind":"SUCCESS_WITH_CONTENT",
				"results":{"columns":[{"name":"answer","logicalType":{"type":"INTEGER"}}],"data":[{"kind":"INSERT","fields":[1,null,"text"]}]},
				"nextResultUri":"/v1/sessions/session/operations/operation/result/1"
			}`, http.StatusOK
		case request.Method == http.MethodPost && request.URL.Path == "/v1/sessions/session/operations/operation/cancel":
			return `{"status":"CANCELED"}`, http.StatusOK
		case request.Method == http.MethodDelete && request.URL.Path == "/v1/sessions/session/operations/operation/close":
			return `{"status":"CLOSED"}`, http.StatusOK
		case request.Method == http.MethodDelete && request.URL.Path == "/v1/sessions/session":
			return `{"status":"CLOSED"}`, http.StatusOK
		default:
			return `{"errors":["not found"]}`, http.StatusNotFound
		}
	})

	info, err := client.Info(context.Background())
	if err != nil || info.Version != "2.3.0" {
		t.Fatalf("info = %#v, %v", info, err)
	}
	session, err := client.OpenSession(context.Background(), "test", map[string]string{"table.local-time-zone": "UTC"})
	if err != nil || session != "session" {
		t.Fatalf("session = %q, %v", session, err)
	}
	operation, err := client.Execute(context.Background(), session, "SELECT 1")
	if err != nil || operation != "operation" {
		t.Fatalf("operation = %q, %v", operation, err)
	}
	result, err := client.FetchResults(context.Background(), session, operation, "")
	if err != nil {
		t.Fatal(err)
	}
	if result.ResultType != "PAYLOAD" || result.JobID != "job" || len(result.Columns) != 1 ||
		len(result.Rows) != 1 || strings.Join(result.Rows[0].Fields, ",") != "1,null,text" || result.NextURI == "" {
		t.Fatalf("result = %#v", result)
	}
	status, err := client.CancelOperation(context.Background(), session, operation)
	if err != nil || status != "CANCELED" {
		t.Fatalf("cancel = %q, %v", status, err)
	}
	if err := client.CloseOperation(context.Background(), session, operation); err != nil {
		t.Fatal(err)
	}
	if err := client.CloseSession(context.Background(), session); err != nil {
		t.Fatal(err)
	}
}

func TestSQLGatewayRejectsForeignNextResultURI(t *testing.T) {
	client, err := NewSQLGatewayClient("http://gateway.test:8083")
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.FetchResults(context.Background(), "session", "operation", "http://other.test/result")
	if err == nil || !strings.Contains(err.Error(), "outside") {
		t.Fatalf("foreign URI error = %v", err)
	}
}

func TestSQLGatewayPreservesProxyPrefixAndPagination(t *testing.T) {
	for _, prefix := range []string{"/gateway", "/gateway%2Fcluster"} {
		t.Run(prefix, func(t *testing.T) {
			client, err := NewSQLGatewayClient("https://gateway.test" + prefix + "/")
			if err != nil {
				t.Fatal(err)
			}
			const resultPath = "/v1/sessions/session/operations/operation/result/1"
			client.http.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
				switch request.URL.EscapedPath() {
				case prefix + "/v1/info":
					return testHTTPResponse(request, http.StatusOK, `{"version":"2.3.0"}`), nil
				case prefix + resultPath:
					if request.URL.Query().Get("rowFormat") != "JSON" {
						t.Errorf("pagination query was lost: %s", request.URL)
					}
					return testHTTPResponse(request, http.StatusOK, `{"resultType":"EOS"}`), nil
				default:
					t.Errorf("request escaped gateway prefix: %s", request.URL)
					return testHTTPResponse(request, http.StatusNotFound, "missing"), nil
				}
			})
			if _, err := client.Info(context.Background()); err != nil {
				t.Fatal(err)
			}
			for _, nextURI := range []string{
				resultPath,
				strings.TrimPrefix(resultPath, "/"),
				prefix + resultPath,
				"https://gateway.test" + prefix + resultPath,
			} {
				result, err := client.FetchResults(context.Background(), "session", "operation", nextURI+"?rowFormat=JSON")
				if err != nil || result.ResultType != "EOS" {
					t.Errorf("pagination %q: result = %#v, error = %v", nextURI, result, err)
				}
			}
		})
	}
}

func testSQLGatewayClient(t *testing.T, respond func(*http.Request) (string, int)) *SQLGatewayClient {
	t.Helper()
	client, err := NewSQLGatewayClient("http://gateway.test:8083")
	if err != nil {
		t.Fatal(err)
	}
	client.http.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body, status := respond(request)
		return &http.Response{
			StatusCode: status,
			Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(body)),
			Request:    request,
		}, nil
	})
	return client
}
