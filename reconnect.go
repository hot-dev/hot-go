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

// retryGrace is how long a failure whose retry_attempt shows a scheduled
// retry waits for that retry run before it is reported.
var retryGrace = 5 * time.Second

// errRetryGrace cancels a connection when a held failure's grace ends.
var errRetryGrace = errors.New("hot: retry grace ended")

// runTracker follows the runs that answer one published event: the event's
// own run and its retries, which carry new event_ids and link back only by
// origin_run_id.
//
// The API replays a retried first attempt lossily: the same run can arrive
// as run:fail or run:stop with status pending_retry, or, once the retry is
// queued but before the retry run exists, as run:fail with status failed and
// a raised retry_attempt. So pending_retry is never final; a run that a tied
// run names as its origin is retried and its failure is not final; and a
// failure whose retry_attempt is above its own attempt number (its origin's
// retry_attempt, 0 for a first attempt) is held for retryGrace and reported
// only if no retry run appears by then or by the caller's deadline.
//
// The API may also deliver a run's stream:data and even its terminal event
// before its run:start (hot publishes run:start late), so chunks for runs not
// yet correlated are buffered per run_id and delivered in order once the run
// is correlated, before its result.
type runTracker struct {
	eventID string
	onChunk func(text string)
	// runs holds each tied run and the highest retry_attempt seen for it.
	runs map[string]int
	// retried holds tied runs that a tied run names as its origin.
	retried map[string]bool
	// orphans holds run events whose origin run is not tied yet, by origin.
	orphans  map[string][]StreamEvent
	held     *heldFailure
	buffered map[string][]string
	count    int
}

// heldFailure is a tied run:fail that may still be retried.
type heldFailure struct {
	runID string
	event StreamEvent
	until time.Time
}

func newRunTracker(eventID string, onChunk func(string)) *runTracker {
	return &runTracker{
		eventID:  eventID,
		onChunk:  onChunk,
		runs:     map[string]int{},
		retried:  map[string]bool{},
		orphans:  map[string][]StreamEvent{},
		buffered: map[string][]string{},
	}
}

// handle records event and returns the event that settles the wait, if any:
// the tied run's run:stop or run:cancel, or a run:fail that will not be
// retried. Releasing orphaned retry events can settle it with an event that
// arrived earlier.
func (t *runTracker) handle(event StreamEvent) (StreamEvent, bool) {
	switch event.Type() {
	case "run:start", "run:stop", "run:fail", "run:cancel":
		if event.Run() == nil {
			return nil, false
		}
		return t.runEvent(event)
	case "stream:data":
		if t.onChunk == nil {
			return nil, false
		}
		text, ok := chunkText(event)
		if !ok {
			return nil, false
		}
		runID, _ := event["run_id"].(string)
		if _, tied := t.runs[runID]; tied {
			t.onChunk(text)
		} else if runID != "" && t.count < maxBufferedChunks {
			t.buffered[runID] = append(t.buffered[runID], text)
			t.count++
		}
	}
	return nil, false
}

// holdUntil is when the held failure stands, or zero when none is held.
func (t *runTracker) holdUntil() time.Time {
	if t.held == nil {
		return time.Time{}
	}
	return t.held.until
}

// expire returns the held failure, which no retry run followed in time.
func (t *runTracker) expire() (StreamEvent, bool) {
	if t.held == nil {
		return nil, false
	}
	event := t.held.event
	t.held = nil
	return event, true
}

// ties reports whether run belongs to the published event, by event id or
// through its origin run.
func (t *runTracker) ties(run map[string]any) bool {
	if t.eventID != "" && eventIDOfRun(run) == t.eventID {
		return true
	}
	_, tied := t.runs[originOf(run)]
	return tied
}

func (t *runTracker) runEvent(event StreamEvent) (StreamEvent, bool) {
	run := event.Run()
	runID, _ := run["run_id"].(string)
	if runID == "" {
		return nil, false
	}
	if !t.ties(run) {
		if origin := originOf(run); origin != "" {
			t.orphans[origin] = append(t.orphans[origin], event)
		} else if event.Type() != "run:start" {
			// Another event's run finished; its chunks will never be needed.
			t.count -= len(t.buffered[runID])
			delete(t.buffered, runID)
		}
		return nil, false
	}
	attempt := retryAttempt(run)
	var released []StreamEvent
	if seen, tied := t.runs[runID]; tied {
		t.runs[runID] = max(seen, attempt)
	} else {
		t.runs[runID] = attempt
		for _, text := range t.buffered[runID] {
			t.onChunk(text)
		}
		t.count -= len(t.buffered[runID])
		delete(t.buffered, runID)
		released = t.orphans[runID]
		delete(t.orphans, runID)
		if len(released) > 0 {
			// A retry of this run is already on the stream.
			t.retried[runID] = true
		}
	}
	if origin := originOf(run); origin != "" {
		t.retried[origin] = true
		if t.held != nil && t.held.runID == origin {
			t.held = nil
		}
	}
	if final, ok := t.outcome(event, runID, run); ok {
		return final, true
	}
	for _, orphan := range released {
		if final, ok := t.runEvent(orphan); ok {
			return final, true
		}
	}
	return nil, false
}

// outcome decides whether an event of a tied run settles the wait.
func (t *runTracker) outcome(event StreamEvent, runID string, run map[string]any) (StreamEvent, bool) {
	if status, _ := run["status"].(string); status == "pending_retry" {
		return nil, false
	}
	switch event.Type() {
	case "run:stop", "run:cancel":
		return event, true
	case "run:fail":
		if t.retried[runID] {
			return nil, false
		}
		// retry_attempt goes up when a retry is scheduled; a run's own
		// attempt number is its origin's (0 for the first attempt).
		base := 0
		if origin := originOf(run); origin != "" {
			base = t.runs[origin]
		}
		if retryAttempt(run) > base {
			if t.held == nil {
				t.held = &heldFailure{runID: runID, event: event, until: time.Now().Add(retryGrace)}
			}
			return nil, false
		}
		return event, true
	}
	return nil, false
}

func originOf(run map[string]any) string {
	origin, _ := run["origin_run_id"].(string)
	return origin
}

func retryAttempt(run map[string]any) int {
	attempt, _ := run["retry_attempt"].(float64)
	return int(attempt)
}

// graceWatch cancels the current connection when the tracker's held failure
// is due, so a waiter blocked on a quiet stream can report it.
type graceWatch struct {
	cancel context.CancelCauseFunc
	timer  *time.Timer
}

// sync starts or stops the timer to match the tracker's held failure.
func (g *graceWatch) sync(t *runTracker) {
	until := t.holdUntil()
	switch {
	case until.IsZero() && g.timer != nil:
		g.timer.Stop()
		g.timer = nil
	case !until.IsZero() && g.timer == nil:
		cancel := g.cancel
		g.timer = time.AfterFunc(time.Until(until), func() { cancel(errRetryGrace) })
	}
}

func (g *graceWatch) stop() {
	if g.timer != nil {
		g.timer.Stop()
	}
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
