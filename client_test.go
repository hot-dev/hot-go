package hot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func newTestClient(t *testing.T, handler http.Handler) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := NewClient(Config{Token: "test-token", BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func writeJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Error(err)
	}
}

func envelope(data any) map[string]any {
	return map[string]any{
		"data": data,
		"meta": map[string]any{"request_id": "req_123", "timestamp": "now"},
	}
}

func listEnvelope(data []any) map[string]any {
	result := envelope(data)
	result["pagination"] = map[string]any{
		"page": 1, "per_page": 20, "total": len(data), "total_pages": 1,
	}
	return result
}

func writeSSE(t *testing.T, w http.ResponseWriter, events ...string) {
	t.Helper()
	w.Header().Set("Content-Type", "text/event-stream")
	for _, event := range events {
		fmt.Fprintf(w, "data: %s\n\n", event)
	}
}

func TestPublishEventSendsBearerTokenAndReturnsData(t *testing.T) {
	var mu sync.Mutex
	var seenMethod, seenPath, seenAuth, seenUserAgent string
	var seenBody map[string]any

	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seenMethod = r.Method
		seenPath = r.URL.Path
		seenAuth = r.Header.Get("Authorization")
		seenUserAgent = r.Header.Get("User-Agent")
		_ = json.NewDecoder(r.Body).Decode(&seenBody)
		mu.Unlock()
		writeJSON(t, w, envelope(map[string]any{"event_id": "evt_123", "stream_id": "str_123"}))
	}))

	event, err := client.Events.Publish(context.Background(), map[string]any{
		"event_type": "team-agent:ask",
		"event_data": map[string]any{"question": "what changed?"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if event["event_id"] != "evt_123" {
		t.Errorf("event_id = %v", event["event_id"])
	}
	mu.Lock()
	defer mu.Unlock()
	if seenMethod != "POST" || seenPath != "/v1/events" || seenAuth != "Bearer test-token" {
		t.Errorf("request = %s %s auth=%q", seenMethod, seenPath, seenAuth)
	}
	if seenUserAgent != "hot-sdk-go/"+Version {
		t.Errorf("User-Agent = %q, want %q", seenUserAgent, "hot-sdk-go/"+Version)
	}
	wantBody := map[string]any{
		"event_type": "team-agent:ask",
		"event_data": map[string]any{"question": "what changed?"},
	}
	if !reflect.DeepEqual(seenBody, wantBody) {
		t.Errorf("body = %v, want %v", seenBody, wantBody)
	}
}

func TestListMethodsReturnAPIListEnvelope(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/v1/runs" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if r.URL.Query().Get("status") != "completed" {
			t.Errorf("status = %q", r.URL.Query().Get("status"))
		}
		writeJSON(t, w, listEnvelope([]any{map[string]any{"id": "run_123"}}))
	}))

	response, err := client.Runs.List(context.Background(), url.Values{"status": {"completed"}})
	if err != nil {
		t.Fatal(err)
	}

	data, ok := response["data"].([]any)
	if !ok || len(data) != 1 {
		t.Fatalf("data = %v", response["data"])
	}
	pagination, _ := response["pagination"].(map[string]any)
	if pagination["total"] != float64(1) {
		t.Errorf("pagination.total = %v", pagination["total"])
	}
}

func TestStreamsParseSSEEvents(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/v1/streams/subscribe-with-event" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["event_type"] != "team-agent:ask" {
			t.Errorf("event_type = %v", body["event_type"])
		}
		writeSSE(t, w,
			`{"type":"stream:data","data_type":"text","payload":"hi"}`,
			`{"type":"run:stop","run":{"result":"done"}}`,
		)
	}))

	var events []StreamEvent
	opts := &SubscribeWithEventOptions{DisableReconnect: true}
	for event, err := range client.Streams.SubscribeWithEvent(context.Background(), map[string]any{"event_type": "team-agent:ask"}, opts) {
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}

	want := []StreamEvent{
		{"type": "stream:data", "data_type": "text", "payload": "hi"},
		{"type": "run:stop", "run": map[string]any{"result": "done"}},
	}
	if !reflect.DeepEqual(events, want) {
		t.Errorf("events = %v, want %v", events, want)
	}
}

func TestAPIErrorIsStructured(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "5")
		w.WriteHeader(http.StatusUnauthorized)
		writeJSON(t, w, map[string]any{
			"error": map[string]any{
				"code":        "unauthorized",
				"message":     "Bad token",
				"request_id":  "req_123",
				"retry_after": 5,
			},
		})
	}))

	_, err := client.Events.List(context.Background(), nil)
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIError, got %v", err)
	}
	if apiErr.StatusCode != 401 || apiErr.Code != "unauthorized" ||
		apiErr.RequestID != "req_123" || apiErr.RetryAfter != 5 {
		t.Errorf("APIError = %+v", apiErr)
	}
	if apiErr.Message != "Bad token" {
		t.Errorf("Message = %q", apiErr.Message)
	}
}

func TestRetries429WithRetryAfter(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		first := calls == 1
		mu.Unlock()
		if first {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			writeJSON(t, w, map[string]any{
				"error": map[string]any{"code": "rate_limited", "message": "Slow down"},
			})
			return
		}
		writeJSON(t, w, envelope(map[string]any{"id": "run_123"}))
	}))

	run, err := client.Runs.Get(context.Background(), "run_123")
	if err != nil {
		t.Fatal(err)
	}
	if run["id"] != "run_123" {
		t.Errorf("run = %v", run)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 2 {
		t.Errorf("calls = %d, want 2", calls)
	}
}

func Test429WithoutRetryAfterIsNotRetried(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()
		w.WriteHeader(http.StatusTooManyRequests)
		writeJSON(t, w, map[string]any{
			"error": map[string]any{"code": "rate_limited", "message": "Slow down"},
		})
	}))

	_, err := client.Runs.Get(context.Background(), "run_123")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 429 {
		t.Fatalf("err = %v, want 429 APIError", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Errorf("calls = %d, want 1", calls)
	}
}

func TestAPIErrorFallsBackToRetryAfterHeader(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, "slow down")
	}))

	_, err := client.Events.List(context.Background(), nil)
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIError, got %v", err)
	}
	if apiErr.StatusCode != 503 || apiErr.RetryAfter != 30 || apiErr.Message != "slow down" {
		t.Errorf("APIError = %+v", apiErr)
	}
}

func TestCallHotUsesNestedRunFailureResult(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && r.URL.Path == "/v1/streams/subscribe-with-event":
			writeSSE(t, w,
				`{"type":"event:published","event_id":"evt_123","stream_id":"str_123"}`,
				`{"type":"run:start","run":{"run_id":"run_123","event_id":"evt_123"}}`,
				`{"type":"run:fail","run":{"run_id":"run_123","event_id":"evt_123","status":"failed","result":{"$err":{"message":"boom"}}}}`,
			)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))

	_, err := client.Events.CallHot(context.Background(), "demo/fn", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v, want message containing %q", err, "boom")
	}
}

func TestCallHotSurfacesHotRunFailureShape(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && r.URL.Path == "/v1/streams/subscribe-with-event":
			writeSSE(t, w,
				`{"type":"event:published","event_id":"evt_123","stream_id":"str_123"}`,
				`{"type":"run:start","run":{"run_id":"run_123","event_id":"evt_123"}}`,
				`{"type":"run:fail","run":{"run_id":"run_123","event_id":"evt_123","status":"failed","result":{"$type":"::hot::run/Failure","$origin":{"function":"::hot::exec/fail"},"$val":{"msg":"too hot","err":"too hot"}}}}`,
			)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))

	_, err := client.Events.CallHot(context.Background(), "demo/fn", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "too hot") {
		t.Fatalf("err = %v, want message containing %q", err, "too hot")
	}
}

func TestCallHotUnwrapsOkResult(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && r.URL.Path == "/v1/streams/subscribe-with-event":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body["event_type"] != "hot:call" {
				t.Errorf("event_type = %v", body["event_type"])
			}
			writeSSE(t, w,
				`{"type":"event:published","event_id":"evt_1","stream_id":"str_1"}`,
				`{"type":"run:start","run":{"run_id":"r1","event_id":"evt_1"}}`,
				`{"type":"run:stop","run":{"run_id":"r1","event_id":"evt_1","result":{"$ok":42}}}`,
			)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))

	result, err := client.Events.CallHot(context.Background(), "demo/fn", []any{1, 2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result != float64(42) {
		t.Errorf("result = %v, want 42", result)
	}
}

func TestRequestEscapeHatchAndEmptyBody(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/new-endpoint" {
			t.Errorf("path = %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	response, err := client.Request(context.Background(), "POST", "/new-endpoint", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if response["data"] != nil {
		t.Errorf("data = %v, want nil", response["data"])
	}
}

func TestNewClientRequiresToken(t *testing.T) {
	if _, err := NewClient(Config{}); err == nil {
		t.Fatal("expected an error for missing token")
	}
}

func TestPathSegmentsAreEscaped(t *testing.T) {
	var seenURI string
	var mu sync.Mutex
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seenURI = r.RequestURI
		mu.Unlock()
		writeJSON(t, w, envelope(map[string]any{}))
	}))

	if _, err := client.Projects.Get(context.Background(), "a/b c"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !strings.HasPrefix(seenURI, "/v1/projects/a%2Fb") {
		t.Errorf("request URI = %q, want escaped project segment", seenURI)
	}
}
