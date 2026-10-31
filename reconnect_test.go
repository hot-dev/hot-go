package hot

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

// fastBackoff shrinks the reconnect backoff for the rest of the test.
func fastBackoff(t *testing.T) {
	t.Helper()
	base, maxDelay := reconnectBaseDelay, reconnectMaxDelay
	reconnectBaseDelay, reconnectMaxDelay = 2*time.Millisecond, 10*time.Millisecond
	t.Cleanup(func() { reconnectBaseDelay, reconnectMaxDelay = base, maxDelay })
}

// waiterKinds drives each durable waiter against client and returns its error.
var waiterKinds = map[string]func(ctx context.Context, client *Client, attempts int) error{
	"stream": func(ctx context.Context, client *Client, attempts int) error {
		var last error
		for _, err := range client.Streams.SubscribeWithEvent(ctx, map[string]any{"event_type": "x"}, &SubscribeWithEventOptions{MaxAttempts: attempts}) {
			if err != nil {
				last = err
			}
		}
		return last
	},
	"run": func(ctx context.Context, client *Client, attempts int) error {
		_, err := client.Runs.Wait(ctx, "r1", &WaitForRunOptions{MaxAttempts: attempts})
		return err
	},
	"task": func(ctx context.Context, client *Client, attempts int) error {
		_, err := client.Tasks.Wait(ctx, "t1", &WaitForTaskOptions{MaxAttempts: attempts})
		return err
	},
	"call": func(ctx context.Context, client *Client, attempts int) error {
		_, err := client.Events.CallHot(ctx, "f", nil, &CallHotOptions{MaxAttempts: attempts})
		return err
	},
	"result": func(ctx context.Context, client *Client, attempts int) error {
		_, err := client.Streams.WaitForRunResult(ctx, "s1", "e1", &WaitForRunResultOptions{MaxAttempts: attempts})
		return err
	},
}

// publishesFirst reports whether the waiter's first request publishes via
// subscribe-with-event, which the test server answers with event:published.
func publishesFirst(kind string) bool { return kind == "stream" || kind == "call" }

// failingAfterPublish answers the first publishing request with
// event:published and every later request with status.
func failingAfterPublish(t *testing.T, kind string, status int, calls *atomic.Int32) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 && publishesFirst(kind) {
			writeSSE(t, w, publishedEvent)
			return
		}
		w.WriteHeader(status)
		fmt.Fprint(w, `{"error":{"code":"denied","message":"no"}}`)
	})
}

func TestPermanentErrorsStopWaitersAtOnce(t *testing.T) {
	for _, status := range []int{400, 401, 403, 404, 410, 422} {
		for kind, wait := range waiterKinds {
			t.Run(fmt.Sprintf("%s-%d", kind, status), func(t *testing.T) {
				var calls atomic.Int32
				client := newTestClient(t, failingAfterPublish(t, kind, status, &calls))
				err := wait(context.Background(), client, 5)
				var apiErr *APIError
				if !errors.As(err, &apiErr) || apiErr.StatusCode != status {
					t.Fatalf("error = %v", err)
				}
				want := int32(1)
				if publishesFirst(kind) {
					want = 2
				}
				if got := calls.Load(); got != want {
					t.Fatalf("calls = %d, want %d", got, want)
				}
			})
		}
	}
}

func TestTransientErrorsExhaustConsecutiveBudget(t *testing.T) {
	fastBackoff(t)
	for _, status := range []int{408, 429, 500, 502, 503, 504} {
		for kind, wait := range waiterKinds {
			t.Run(fmt.Sprintf("%s-%d", kind, status), func(t *testing.T) {
				var calls atomic.Int32
				client := newTestClient(t, failingAfterPublish(t, kind, status, &calls))
				err := wait(context.Background(), client, 3)
				var apiErr *APIError
				if !errors.As(err, &apiErr) || apiErr.StatusCode != status {
					t.Fatalf("error = %v", err)
				}
				// One failing connection plus three reconnects.
				want := int32(4)
				if publishesFirst(kind) {
					want = 5
				}
				if got := calls.Load(); got != want {
					t.Fatalf("calls = %d, want %d", got, want)
				}
			})
		}
	}
}

func TestReconnectBacksOffBetweenFailures(t *testing.T) {
	base := reconnectBaseDelay
	reconnectBaseDelay = 40 * time.Millisecond
	t.Cleanup(func() { reconnectBaseDelay = base })
	var mu sync.Mutex
	var times []time.Time
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		times = append(times, time.Now())
		n := len(times)
		mu.Unlock()
		if n < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		writeSSE(t, w, runUpdate("succeeded", "done"))
	}))
	if _, err := client.Runs.Wait(context.Background(), "r1", nil); err != nil {
		t.Fatal(err)
	}
	// Failure n waits at least half of base*2^(n-1): 20ms, then 40ms.
	if gap := times[1].Sub(times[0]); gap < 20*time.Millisecond {
		t.Errorf("first backoff = %v, want >= 20ms", gap)
	}
	if gap := times[2].Sub(times[1]); gap < 40*time.Millisecond {
		t.Errorf("second backoff = %v, want >= 40ms", gap)
	}
}

func TestReconnectHonoursRetryAfter(t *testing.T) {
	fastBackoff(t)
	var mu sync.Mutex
	var times []time.Time
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		times = append(times, time.Now())
		n := len(times)
		mu.Unlock()
		if n == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprint(w, `{"error":{"code":"rate_limited","message":"slow","retry_after":1}}`)
			return
		}
		writeSSE(t, w, runUpdate("succeeded", "done"))
	}))
	if _, err := client.Runs.Wait(context.Background(), "r1", nil); err != nil {
		t.Fatal(err)
	}
	if gap := times[1].Sub(times[0]); gap < time.Second {
		t.Errorf("retry gap = %v, want >= retry_after (1s)", gap)
	}
}

func TestBackoffNeverSleepsPastTheDeadline(t *testing.T) {
	var calls atomic.Int32
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	start := time.Now()
	_, err := client.Runs.Wait(context.Background(), "r1", &WaitForRunOptions{Timeout: 300 * time.Millisecond})
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("wait took %v; the 30s retry_after must not outlive the 300ms timeout", elapsed)
	}
	var apiErr *APIError
	if !errors.Is(err, context.DeadlineExceeded) || !errors.As(err, &apiErr) || apiErr.StatusCode != 503 {
		t.Fatalf("err = %v, want a timeout wrapping DeadlineExceeded and the last 503", err)
	}
	if !strings.Contains(err.Error(), "timeout waiting for run") || calls.Load() != 1 {
		t.Fatalf("err = %v after %d calls", err, calls.Load())
	}
}

func TestRolloversDoNotSpendTheReconnectBudget(t *testing.T) {
	complete := `{"type":"stream:complete","stream_id":"s1"}`
	rollovers := 8 // more windows than MaxAttempts
	sequences := map[string]func(i int) []string{
		"stream": func(i int) []string {
			if i == 0 {
				return []string{publishedEvent, complete}
			}
			if i < rollovers {
				return []string{complete}
			}
			return []string{`{"type":"run:stop","run":{"run_id":"r1","event_id":"e1","result":"done"}}`}
		},
		"run": func(i int) []string {
			if i < rollovers {
				return []string{runUpdate("running", ""), complete}
			}
			return []string{runUpdate("succeeded", "done")}
		},
		"task": func(i int) []string {
			if i < rollovers {
				return []string{taskUpdate("running", ""), complete}
			}
			return []string{taskUpdate("completed", "done")}
		},
	}
	sequences["call"] = sequences["stream"]
	sequences["result"] = func(i int) []string {
		if i < rollovers {
			return []string{complete}
		}
		return []string{`{"type":"run:stop","run":{"run_id":"r1","event_id":"e1","result":"done"}}`}
	}
	for kind, wait := range waiterKinds {
		t.Run(kind, func(t *testing.T) {
			handler := &sseSequenceHandler{t: t}
			for i := 0; i <= rollovers; i++ {
				handler.responses = append(handler.responses, sequences[kind](i))
			}
			client := newTestClient(t, handler)
			if err := wait(context.Background(), client, 2); err != nil {
				t.Fatalf("err = %v after %d connections", err, len(handler.recordedCalls()))
			}
			if got := len(handler.recordedCalls()); got != rollovers+1 {
				t.Fatalf("connections = %d, want %d", got, rollovers+1)
			}
		})
	}
}

func TestWaitersWrapContextErrors(t *testing.T) {
	hang := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	})
	for kind, wait := range waiterKinds {
		t.Run(kind+"-cancel", func(t *testing.T) {
			client := newTestClient(t, hang)
			ctx, cancel := context.WithCancel(context.Background())
			time.AfterFunc(50*time.Millisecond, cancel)
			err := wait(ctx, client, 5)
			if !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "timeout") || !strings.HasPrefix(err.Error(), "hot: waiting for ") {
				t.Fatalf("err = %v, want a non-timeout error wrapping context.Canceled", err)
			}
		})
		t.Run(kind+"-deadline", func(t *testing.T) {
			client := newTestClient(t, hang)
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			err := wait(ctx, client, 5)
			if !errors.Is(err, context.DeadlineExceeded) || !strings.HasPrefix(err.Error(), "hot: timeout waiting for ") {
				t.Fatalf("err = %v, want a timeout wrapping context.DeadlineExceeded", err)
			}
		})
	}
	t.Run("own-timeout", func(t *testing.T) {
		client := newTestClient(t, hang)
		_, err := client.Tasks.Wait(context.Background(), "t1", &WaitForTaskOptions{Timeout: 50 * time.Millisecond})
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "timeout waiting for task") {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestSubscribeWithEventRetriesPublishOnlyWhenSafe(t *testing.T) {
	fastBackoff(t)
	for status, wantPosts := range map[int]int32{503: 2, 429: 2, 500: 1, 502: 1} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var posts atomic.Int32
			client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if posts.Add(1) == 1 {
					w.WriteHeader(status)
					return
				}
				writeSSE(t, w, publishedEvent, `{"type":"run:stop","run":{"run_id":"r1","event_id":"e1","result":"done"}}`)
			}))
			err := waiterKinds["stream"](context.Background(), client, 5)
			if got := posts.Load(); got != wantPosts {
				t.Fatalf("POSTs = %d, want %d (err %v)", got, wantPosts, err)
			}
			if (wantPosts == 2) != (err == nil) {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

func TestDeadStreamReconnectsAfterIdleTimeout(t *testing.T) {
	idle := streamIdleTimeout
	streamIdleTimeout = 100 * time.Millisecond
	t.Cleanup(func() { streamIdleTimeout = idle })
	fastBackoff(t)
	var calls atomic.Int32
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			// Headers, then silence: no events and no keepalives.
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return
		}
		writeSSE(t, w, runUpdate("succeeded", "done"))
	}))
	run, err := client.Runs.Wait(context.Background(), "r1", nil)
	if err != nil || run["status"] != "succeeded" || calls.Load() != 2 {
		t.Fatalf("run = %v, err = %v, calls = %d", run, err, calls.Load())
	}
}

func TestStreamIdleTimeoutIgnoresSlowConsumers(t *testing.T) {
	idle := streamIdleTimeout
	streamIdleTimeout = 100 * time.Millisecond
	t.Cleanup(func() { streamIdleTimeout = idle })
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeSSE(t, w, `{"type":"stream:data","run_id":"r1"}`, `{"type":"stream:data","run_id":"r1"}`)
	}))
	count := 0
	for _, err := range client.Streams.Subscribe(context.Background(), "s1", nil) {
		if err != nil {
			t.Fatal(err)
		}
		count++
		time.Sleep(250 * time.Millisecond) // longer than the idle timeout
	}
	if count != 2 {
		t.Fatalf("events = %d, want 2", count)
	}
}
