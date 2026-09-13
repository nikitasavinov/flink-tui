package sqlworkbench

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/nikitasavinov/flink-tui/internal/flink"
)

func TestFailedResultPageCanResumeWithoutResubmittingStatement(t *testing.T) {
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"resultType":"EOS","results":{"data":[{"kind":"INSERT","fields":[2]}]}}`))
	}))
	defer server.Close()
	client, err := flink.NewSQLGatewayClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	model := New(client, context.Background(), nil)
	model.session, model.operation, model.resultType = "session", "operation", "RUNNING"
	model.rows = []flink.SQLRow{{Kind: "INSERT", Fields: []string{"1"}}}
	page := "/v1/sessions/session/operations/operation/result/2"
	model.Apply(resultMsg{session: "session", operation: "operation", requestedURI: page, err: errors.New("connection interrupted")})
	command := model.Refresh()
	if command == nil || model.Refresh() != nil {
		t.Fatal("failed page did not allow exactly one retry")
	}
	model.Apply(command().(Message))
	want := []string{"GET " + page}
	if !reflect.DeepEqual(requests, want) || len(model.rows) != 2 || model.rows[0].Fields[0] != "1" || model.rows[1].Fields[0] != "2" || model.resultType != "EOS" {
		t.Fatalf("retry repeated or lost results: requests=%v state=%#v", requests, model.State())
	}
}

func TestCancellationDuringSubmissionWaitsForAndCancelsAcceptedHandle(t *testing.T) {
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1/sessions/session/statements" {
			_, _ = w.Write([]byte(`{"operationHandle":"accepted"}`))
		} else {
			_, _ = w.Write([]byte(`{"status":"CANCELED"}`))
		}
	}))
	defer server.Close()
	client, err := flink.NewSQLGatewayClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	model := New(client, context.Background(), nil)
	model.session = "session"
	submit := model.executeStatement()
	if model.cancelOperation() != nil || !model.canceling || model.resultType != "CANCELING" {
		t.Fatal("cancellation before the handle arrived was not queued")
	}
	cancel := model.Apply(submit().(Message))
	if cancel == nil {
		t.Fatal("accepted operation did not trigger the queued cancellation")
	}
	model.Apply(cancel().(Message))
	want := []string{
		"POST /v1/sessions/session/statements",
		"POST /v1/sessions/session/operations/accepted/cancel",
	}
	if !reflect.DeepEqual(requests, want) || model.busy || model.canceling || model.operationActive() {
		t.Fatalf("queued cancellation failed: requests=%v state=%#v", requests, model.State())
	}
}

func TestQuitClosesSessionWhoseOpenReplyHasNotBeenApplied(t *testing.T) {
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			_, _ = w.Write([]byte(`{"productName":"Flink SQL Gateway"}`))
		case http.MethodPost:
			_, _ = w.Write([]byte(`{"sessionHandle":"opened"}`))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	defer server.Close()
	client, err := flink.NewSQLGatewayClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	model := New(client, context.Background(), nil)
	open := model.Refresh()
	// A quit key can arrive before the already-completed command's message.
	if message := open().(sessionMsg); message.err != nil || message.handle != "opened" {
		t.Fatalf("session fixture failed: %#v", message)
	}
	cleanup := model.Close()
	if cleanup == nil {
		t.Fatal("quit ignored the session handle queued for the event loop")
	}
	cleanup()
	want := []string{"GET /v1/info", "POST /v1/sessions", "DELETE /v1/sessions/opened"}
	if !reflect.DeepEqual(requests, want) {
		t.Fatalf("quit leaked the newly opened session: requests=%v", requests)
	}
}

func TestNextStatementClosesPreviousOperationBeforeExecution(t *testing.T) {
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodDelete {
			_, _ = w.Write([]byte(`{}`))
		} else {
			_, _ = w.Write([]byte(`{"operationHandle":"next"}`))
		}
	}))
	defer server.Close()
	client, err := flink.NewSQLGatewayClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	model := New(client, context.Background(), nil)
	model.session, model.operation, model.resultType = "session", "previous", "EOS"
	command := model.executeStatement()
	if command == nil {
		t.Fatal("finished operation prevented another execution")
	}
	model.Apply(command().(Message))
	want := []string{
		"DELETE /v1/sessions/session/operations/previous/close",
		"POST /v1/sessions/session/statements",
	}
	if !reflect.DeepEqual(requests, want) {
		t.Fatalf("requests = %v, want %v", requests, want)
	}
	if model.operation != "next" || model.err != nil {
		t.Fatalf("new execution did not replace cleaned-up operation: %#v", model.State())
	}
}

func TestFailedOperationCleanupIsRetriedBeforeAnotherStatement(t *testing.T) {
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.Path)
		http.Error(w, "temporary failure", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client, err := flink.NewSQLGatewayClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	model := New(client, context.Background(), nil)
	model.session, model.operation, model.resultType = "session", "previous", "CANCELED"
	for range 2 {
		command := model.executeStatement()
		if command == nil {
			t.Fatal("cleanup failure was not retryable")
		}
		model.Apply(command().(Message))
		if model.operation != "previous" || model.err == nil || model.busy || model.operationActive() {
			t.Fatalf("failed cleanup lost the previous handle or blocked retry: %#v", model.State())
		}
	}
	want := []string{
		"DELETE /v1/sessions/session/operations/previous/close",
		"DELETE /v1/sessions/session/operations/previous/close",
	}
	if !reflect.DeepEqual(requests, want) {
		t.Fatalf("requests = %v, want only cleanup retries %v", requests, want)
	}
}
