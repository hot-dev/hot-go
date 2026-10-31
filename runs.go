package hot

import (
	"context"
	"fmt"
	"iter"
	"net/url"
	"time"
)

// RunsResource lists and inspects runs.
type RunsResource struct{ transport *transport }

// WaitForRunOptions configures run waiting.
type WaitForRunOptions struct {
	// Timeout bounds the whole wait, reconnects included. Defaults to 60
	// seconds.
	Timeout time.Duration
	// MaxAttempts caps consecutive failed connections. Defaults to 5.
	MaxAttempts int
}

// RunWaitError is returned when a run fails or is cancelled.
type RunWaitError struct {
	Run map[string]any
}

func (e *RunWaitError) Error() string {
	if message := resultMessage(e.Run["result"]); message != "" {
		return "hot: " + message
	}
	return fmt.Sprintf("hot: run %v", e.Run["status"])
}

// List returns the run list envelope (data + pagination).
func (r *RunsResource) List(ctx context.Context, query url.Values) (map[string]any, error) {
	return r.transport.requestJSON(ctx, "GET", "/runs", nil, query)
}

// Stats returns aggregate run statistics.
func (r *RunsResource) Stats(ctx context.Context) (map[string]any, error) {
	envelope, err := r.transport.requestJSON(ctx, "GET", "/runs/stats", nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// Get returns a single run.
func (r *RunsResource) Get(ctx context.Context, runID string) (map[string]any, error) {
	envelope, err := r.transport.requestJSON(ctx, "GET", "/runs/"+enc(runID), nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// Subscribe yields durable run:update snapshots until the run is terminal.
func (r *RunsResource) Subscribe(ctx context.Context, runID string) iter.Seq2[StreamEvent, error] {
	return r.transport.stream(ctx, "GET", "/runs/"+enc(runID)+"/subscribe", nil, nil)
}

// Wait returns a succeeded run snapshot. Failed and cancelled runs return a
// *RunWaitError containing the final durable snapshot.
//
// Waiters reconnect across the API's 5-minute subscription limit and across
// transient failures (transport errors, 408, 429, 5xx) with jittered
// exponential backoff that honours the server's retry_after. Other 4xx
// responses, such as 401, 403 (including *KeyScopeError) and 404, end the
// wait at once. MaxAttempts counts consecutive failed connections only. A
// timeout wraps context.DeadlineExceeded; a cancelled ctx wraps its cause.
func (r *RunsResource) Wait(ctx context.Context, runID string, opts *WaitForRunOptions) (map[string]any, error) {
	options := WaitForRunOptions{}
	if opts != nil {
		options = *opts
	}
	if options.Timeout == 0 {
		options.Timeout = 60 * time.Second
	}
	ctx, cancel := withWaitTimeout(ctx, options.Timeout)
	defer cancel()

	reconnect := newReconnector("run", options.MaxAttempts)
	for {
		var conn connection
		for event, err := range r.Subscribe(ctx, runID) {
			if err != nil {
				conn.err = err
				break
			}
			conn.delivered = true
			switch event.Type() {
			case "stream:complete":
				conn.complete = true
			case "run:update":
				run, _ := event["run"].(map[string]any)
				status, _ := run["status"].(string)
				switch status {
				case "succeeded":
					return run, nil
				case "failed", "cancelled":
					return nil, &RunWaitError{Run: run}
				}
			}
		}
		if err := reconnect.next(ctx, conn, "hot: run subscription ended before the run completed"); err != nil {
			return nil, err
		}
	}
}

// PurgeCalls deletes the run's call traces and returns the purge outcome
// (calls_deleted, blob_refs_released, complete).
func (r *RunsResource) PurgeCalls(ctx context.Context, runID string) (map[string]any, error) {
	envelope, err := r.transport.requestJSON(ctx, "DELETE", "/runs/"+enc(runID)+"/calls", nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}
