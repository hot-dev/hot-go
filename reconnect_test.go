package hot

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
)

const publishedEvent = `{"type":"event:published","stream_id":"s1","event_id":"e1","event_type":"x"}`

// sseSequenceHandler serves each response in order, one per request, and
// records "METHOD /path" calls.
type sseSequenceHandler struct {
	t         *testing.T
	mu        sync.Mutex
	responses [][]string
	calls     []string
}

func (h *sseSequenceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	h.calls = append(h.calls, r.Method+" "+r.URL.Path)
	if len(h.responses) == 0 {
		h.mu.Unlock()
		h.t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		return
	}
	events := h.responses[0]
	h.responses = h.responses[1:]
	h.mu.Unlock()

	writeSSE(h.t, w, events...)
}

func (h *sseSequenceHandler) recordedCalls() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.calls...)
}

func collectEvents(t *testing.T, client *Client, opts *SubscribeWithEventOptions) ([]StreamEvent, error) {
	t.Helper()
	var events []StreamEvent
	body := map[string]any{"event_type": "x", "event_data": map[string]any{}}
	for event, err := range client.Streams.SubscribeWithEvent(context.Background(), body, opts) {
		if err != nil {
			return events, err
		}
		events = append(events, event)
	}
	return events, nil
}

func eventTypes(events []StreamEvent) []string {
	types := make([]string, len(events))
	for i, event := range events {
		types[i] = event.Type()
	}
	return types
}

func TestSubscribeWithEventReconnectsViaStreamID(t *testing.T) {
	handler := &sseSequenceHandler{t: t, responses: [][]string{
		{publishedEvent, `{"type":"run:start","run":{"run_id":"r1","event_id":"e1"}}`},
		{`{"type":"run:start","run":{"run_id":"r1","event_id":"e1"}}`, `{"type":"run:stop","run":{"run_id":"r1","event_id":"e1","result":"done"}}`},
	}}
	client := newTestClient(t, handler)

	events, err := collectEvents(t, client, nil)
	if err != nil {
		t.Fatal(err)
	}

	want := []string{"event:published", "run:start", "run:stop"}
	if got := eventTypes(events); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("event types = %v, want %v", got, want)
	}
	wantCalls := []string{
		"POST /v1/streams/subscribe-with-event",
		"GET /v1/streams/s1/subscribe",
	}
	if got := handler.recordedCalls(); strings.Join(got, ",") != strings.Join(wantCalls, ",") {
		t.Errorf("calls = %v, want %v", got, wantCalls)
	}
}

func TestSubscribeWithEventNoReconnectReturnsAfterFirstConnection(t *testing.T) {
	handler := &sseSequenceHandler{t: t, responses: [][]string{
		{publishedEvent, `{"type":"run:start","run":{"run_id":"r1","event_id":"e1"}}`},
	}}
	client := newTestClient(t, handler)

	events, err := collectEvents(t, client, &SubscribeWithEventOptions{DisableReconnect: true})
	if err != nil {
		t.Fatal(err)
	}

	want := []string{"event:published", "run:start"}
	if got := eventTypes(events); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("event types = %v, want %v", got, want)
	}
	if calls := handler.recordedCalls(); len(calls) != 1 {
		t.Errorf("calls = %v, want exactly one", calls)
	}
}

func TestSubscribeWithEventDedupesRunEventsByRunID(t *testing.T) {
	handler := &sseSequenceHandler{t: t, responses: [][]string{
		{publishedEvent, `{"type":"run:start","run":{"run_id":"r1","event_id":"e1"}}`},
		{
			`{"type":"run:start","run":{"run_id":"r1","event_id":"e1"}}`,
			`{"type":"run:start","run":{"run_id":"r2","event_id":"e2"}}`,
			`{"type":"run:fail","run":{"run_id":"r2","event_id":"e2","result":"other"}}`,
			`{"type":"run:fail","run":{"run_id":"r1","event_id":"e1","result":"err"}}`,
			`{"type":"run:fail","run":{"run_id":"r1","event_id":"e1","result":"err"}}`,
		},
	}}
	client := newTestClient(t, handler)

	events, err := collectEvents(t, client, nil)
	if err != nil {
		t.Fatal(err)
	}

	var startIDs, failIDs []string
	for _, event := range events {
		switch event.Type() {
		case "run:start":
			startIDs = append(startIDs, event.RunID())
		case "run:fail":
			failIDs = append(failIDs, event.RunID())
		}
	}
	if strings.Join(startIDs, ",") != "r1,r2" {
		t.Errorf("run:start ids = %v, want [r1 r2]", startIDs)
	}
	if strings.Join(failIDs, ",") != "r2,r1" {
		t.Errorf("run:fail ids = %v, want [r2 r1]", failIDs)
	}
}

func TestSubscribeWithEventErrorsWhenNoEventPublished(t *testing.T) {
	handler := &sseSequenceHandler{t: t, responses: [][]string{{}}}
	client := newTestClient(t, handler)

	_, err := collectEvents(t, client, nil)
	if err == nil || !strings.Contains(err.Error(), "event:published") {
		t.Fatalf("err = %v, want event:published error", err)
	}
}

func TestSubscribePostUsesPostStreamEndpoint(t *testing.T) {
	handler := &sseSequenceHandler{t: t, responses: [][]string{
		{`{"type":"stream:complete","stream_id":"s1"}`},
	}}
	client := newTestClient(t, handler)

	var events []StreamEvent
	for event, err := range client.Streams.SubscribePost(context.Background(), "s1", nil) {
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}

	if len(events) != 1 || events[0].Type() != "stream:complete" {
		t.Errorf("events = %v", events)
	}
	wantCalls := []string{"POST /v1/streams/s1/subscribe"}
	if got := handler.recordedCalls(); strings.Join(got, ",") != strings.Join(wantCalls, ",") {
		t.Errorf("calls = %v, want %v", got, wantCalls)
	}
}
