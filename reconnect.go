package hot

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"time"
)

// Durable waiters (Runs.Wait, Tasks.Wait, Streams.SubscribeWithEvent,
// Streams.WaitForRunResult and Events.CallHot) share one reconnect policy:
//
//   - Any 4xx except 408 and 429 ends the wait at once: the request, the
//     credential or the resource is wrong, and retrying cannot fix it.
//   - Transport errors, 408, 429 and 5xx reconnect after a jittered
//     exponential backoff, or after the server's retry_after when longer.
//   - MaxAttempts counts consecutive failed connections. A connection that
//     delivered events or ended with stream:complete (the API closes every
//     subscription after 5 minutes) resets the count and reconnects at once,
//     so long runs are not cut short by routine rollovers.
//   - A backoff never sleeps past the wait's deadline.
var (
	reconnectBaseDelay = 500 * time.Millisecond
	reconnectMaxDelay  = 10 * time.Second
)

// errWaitTimeout is the cause recorded when a waiter's own Timeout elapses.
var errWaitTimeout = errors.New("hot: wait timeout")

// withWaitTimeout bounds ctx by a waiter's Timeout option.
func withWaitTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeoutCause(ctx, timeout, errWaitTimeout)
}

// backoffDelay is the jittered delay before consecutive failure n (from 1):
// half of min(reconnectMaxDelay, reconnectBaseDelay*2^(n-1)) plus a random
// share of the other half.
func backoffDelay(n int) time.Duration {
	delay := reconnectMaxDelay
	if n < 32 {
		delay = min(reconnectMaxDelay, reconnectBaseDelay<<(n-1))
	}
	half := delay / 2
	if half <= 0 {
		return delay
	}
	return half + rand.N(half+1)
}

// contextError explains why ctx ended a wait for what. A timeout (the
// waiter's own or the caller's deadline) wraps context.DeadlineExceeded; a
// cancellation wraps the caller's cause, so errors.Is works for both. last,
// the most recent connection error, is wrapped too unless ctx caused it.
func contextError(ctx context.Context, what string, last error) error {
	if last != nil && (errors.Is(last, context.Canceled) || errors.Is(last, context.DeadlineExceeded) || errors.Is(last, errWaitTimeout)) {
		last = nil
	}
	cause := context.Cause(ctx)
	if errors.Is(cause, errWaitTimeout) || errors.Is(cause, context.DeadlineExceeded) {
		if last != nil {
			return fmt.Errorf("hot: timeout waiting for %s: %w (last error: %w)", what, context.DeadlineExceeded, last)
		}
		return fmt.Errorf("hot: timeout waiting for %s: %w", what, context.DeadlineExceeded)
	}
	return fmt.Errorf("hot: waiting for %s: %w", what, cause)
}

// connection reports how one stream connection ended.
type connection struct {
	// delivered is set when the connection yielded at least one event.
	delivered bool
	// complete is set when the connection ended with stream:complete.
	complete bool
	// err is the connection's error; nil when the stream ended cleanly.
	err error
}

// reconnector applies the reconnect policy to one durable wait.
type reconnector struct {
	what        string
	maxAttempts int
	failures    int
}

func newReconnector(what string, maxAttempts int) *reconnector {
	if maxAttempts <= 0 {
		maxAttempts = 5
	}
	return &reconnector{what: what, maxAttempts: maxAttempts}
}

// next waits out the delay before the next connection, or returns the error
// that ends the wait. ended describes a stream that keeps ending without a
// result.
func (r *reconnector) next(ctx context.Context, c connection, ended string) error {
	if ctx.Err() != nil {
		return contextError(ctx, r.what, c.err)
	}
	if c.err != nil && isPermanentError(c.err) {
		return c.err
	}
	if c.complete || (c.delivered && c.err == nil) {
		r.failures = 0
		return nil
	}
	if c.delivered {
		r.failures = 0
	}
	r.failures++
	last := c.err
	if last == nil {
		last = errors.New(ended)
	}
	if r.failures > r.maxAttempts {
		return last
	}
	delay := backoffDelay(r.failures)
	var apiErr *APIError
	if errors.As(c.err, &apiErr) && apiErr.RetryAfter > 0 {
		delay = max(delay, time.Duration(apiErr.RetryAfter)*time.Second)
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < delay {
		return fmt.Errorf("hot: timeout waiting for %s: %w (last error: %w)", r.what, context.DeadlineExceeded, last)
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return contextError(ctx, r.what, c.err)
	case <-timer.C:
		return nil
	}
}

// retryableBeforePublish reports whether a subscribe-with-event request may
// be sent again: only when the server cannot have published the event (it
// refused the request with 429 or 503, or the connection never opened).
// Other failures may follow a publish, and a retry would publish twice.
func retryableBeforePublish(err error) bool {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.StatusCode == http.StatusTooManyRequests || apiErr.StatusCode == http.StatusServiceUnavailable
	}
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == "dial"
}

// maxBufferedChunks bounds the stream:data chunks held for runs that are not
// yet correlated with the published event.
const maxBufferedChunks = 4096

// runTracker follows the runs that answer one published event: the event's
// own run and, after a run:fail with status pending_retry, the retry run,
// which has a new event_id and links back only by origin_run_id.
//
// The API may deliver a run's stream:data and even its terminal event before
// its run:start (hot publishes run:start late), so chunks for runs not yet
// correlated are buffered per run_id and delivered in order once the run is
// correlated, before its result.
type runTracker struct {
	eventID  string
	onChunk  func(text string)
	runs     map[string]bool
	buffered map[string][]string
	count    int
}

func newRunTracker(eventID string, onChunk func(string)) *runTracker {
	return &runTracker{eventID: eventID, onChunk: onChunk, runs: map[string]bool{}, buffered: map[string][]string{}}
}

// correlate reports whether run belongs to the published event, recording it
// and flushing its buffered chunks the first time.
func (t *runTracker) correlate(run map[string]any) bool {
	runID, _ := run["run_id"].(string)
	if runID == "" || t.eventID == "" {
		return false
	}
	if t.runs[runID] {
		return true
	}
	origin, _ := run["origin_run_id"].(string)
	if eventIDOfRun(run) != t.eventID && (origin == "" || !t.runs[origin]) {
		return false
	}
	t.runs[runID] = true
	for _, text := range t.buffered[runID] {
		t.onChunk(text)
	}
	t.count -= len(t.buffered[runID])
	delete(t.buffered, runID)
	return true
}

// handle records event and reports whether it is the final event for the
// published event: the correlated run's run:stop, run:cancel, or a run:fail
// that will not be retried.
func (t *runTracker) handle(event StreamEvent) bool {
	switch event.Type() {
	case "run:start":
		t.correlate(event.Run())
	case "stream:data":
		if t.onChunk == nil {
			return false
		}
		text, ok := chunkText(event)
		if !ok {
			return false
		}
		runID, _ := event["run_id"].(string)
		if t.runs[runID] {
			t.onChunk(text)
		} else if runID != "" && t.count < maxBufferedChunks {
			t.buffered[runID] = append(t.buffered[runID], text)
			t.count++
		}
	case "run:stop", "run:fail", "run:cancel":
		run := event.Run()
		if !t.correlate(run) {
			// Another event's run finished; its chunks will never be needed.
			if runID, _ := run["run_id"].(string); runID != "" {
				t.count -= len(t.buffered[runID])
				delete(t.buffered, runID)
			}
			return false
		}
		status, _ := run["status"].(string)
		return event.Type() != "run:fail" || status != "pending_retry"
	}
	return false
}

// runOutcome turns a final run event into the waiter's result.
func runOutcome(event StreamEvent) (map[string]any, error) {
	if event.Type() == "run:stop" {
		return event.Run(), nil
	}
	return nil, &RunWaitError{Run: event.Run()}
}

// chunkText returns a stream:data event's payload.text.
func chunkText(event StreamEvent) (string, bool) {
	payload, ok := event["payload"].(map[string]any)
	if !ok {
		return "", false
	}
	text, ok := payload["text"].(string)
	return text, ok
}
