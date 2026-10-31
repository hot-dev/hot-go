package hot

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"net/url"
	"time"
)

// StreamsResource subscribes to Hot run streams.
type StreamsResource struct{ transport *transport }

// StreamOptions carries optional parameters for stream subscriptions.
type StreamOptions struct {
	// Project scopes the subscription to a project id or slug.
	Project string
}

// SubscribeWithEventOptions carries optional parameters for SubscribeWithEvent.
type SubscribeWithEventOptions struct {
	// Project scopes the subscription to a project id or slug.
	Project string
	// DisableReconnect turns off automatic resubscription across the API's
	// 5-minute subscription limit and transient failures.
	DisableReconnect bool
	// MaxAttempts caps consecutive failed connections. Defaults to 5.
	MaxAttempts int
}

// WaitForRunResultOptions carries optional parameters for WaitForRunResult.
type WaitForRunResultOptions struct {
	// Timeout bounds the whole wait, reconnects included. Defaults to 60
	// seconds.
	Timeout time.Duration
	// OnChunk receives streamed text chunks from the matched run.
	OnChunk func(text string)
	// MaxAttempts caps consecutive failed connections. Defaults to 5.
	MaxAttempts int
}

// Subscribe streams events from an existing run stream.
func (r *StreamsResource) Subscribe(ctx context.Context, streamID string, opts *StreamOptions) iter.Seq2[StreamEvent, error] {
	return r.transport.stream(ctx, "GET", "/streams/"+enc(streamID)+"/subscribe", nil, streamQuery(opts))
}

// SubscribePost streams events from an existing run stream via POST.
func (r *StreamsResource) SubscribePost(ctx context.Context, streamID string, opts *StreamOptions) iter.Seq2[StreamEvent, error] {
	return r.transport.stream(ctx, "POST", "/streams/"+enc(streamID)+"/subscribe", nil, streamQuery(opts))
}

// PurgeCalls deletes the call traces of every run on the stream and returns
// the purge outcome (calls_deleted, blob_refs_released, complete).
func (r *StreamsResource) PurgeCalls(ctx context.Context, streamID string) (map[string]any, error) {
	envelope, err := r.transport.requestJSON(ctx, "DELETE", "/streams/"+enc(streamID)+"/calls", nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// WaitForRunResult subscribes to a stream and returns the run object once the
// run created by eventID reaches run:stop. A failure that the handler will
// retry (status pending_retry) is not final: the wait follows the retry run.
// A failure whose retry_attempt shows a scheduled retry (as the API replays
// a retried first attempt) is held for 5 seconds, or until the timeout, and
// returned only if no retry run appears.
// A final run:fail or run:cancel returns a *RunWaitError. Timeouts wrap
// context.DeadlineExceeded and cancellation wraps the context's cause.
func (r *StreamsResource) WaitForRunResult(ctx context.Context, streamID, eventID string, opts *WaitForRunResultOptions) (map[string]any, error) {
	return waitForRunResult(ctx, r.transport, streamID, eventID, opts)
}

// SubscribeWithEvent atomically subscribes then publishes an event, yielding
// stream events until the run completes.
//
// The Hot API closes every subscription after 5 minutes. Unless
// opts.DisableReconnect is set, the SDK resubscribes to the same stream_id and
// keeps yielding events until the final run:stop/run:fail/run:cancel of the
// published event's run. A run event with status pending_retry is not
// final: iteration continues through the retry run, which links back by
// origin_run_id. A run:fail whose retry_attempt shows a scheduled retry ends
// the iteration only if no retry run appears within 5 seconds. Replayed
// run:start and terminal events are deduped by run_id; stream:data chunks
// are not replayed and may be lost across a reconnect. Reconnects follow the
// policy described on Runs.Wait.
func (r *StreamsResource) SubscribeWithEvent(ctx context.Context, body map[string]any, opts *SubscribeWithEventOptions) iter.Seq2[StreamEvent, error] {
	options := SubscribeWithEventOptions{}
	if opts != nil {
		options = *opts
	}
	query := projectQuery(options.Project)
	once := func() iter.Seq2[StreamEvent, error] {
		return r.transport.stream(ctx, "POST", "/streams/subscribe-with-event", body, query)
	}

	if options.DisableReconnect {
		return once()
	}

	return func(yield func(StreamEvent, error) bool) {
		seenStart := map[string]bool{}
		seenTerminal := map[string]bool{}
		streamID := ""
		tracker := newRunTracker("", nil)
		reconnect := newReconnector("stream", options.MaxAttempts)

		for {
			connCtx, cancelConn := context.WithCancelCause(ctx)
			grace := &graceWatch{cancel: cancelConn}
			grace.sync(tracker)
			var source iter.Seq2[StreamEvent, error]
			if streamID == "" {
				source = r.transport.stream(connCtx, "POST", "/streams/subscribe-with-event", body, query)
			} else {
				source = r.Subscribe(connCtx, streamID, &StreamOptions{Project: options.Project})
			}

			var conn connection
			done := false
			for event, err := range source {
				if err != nil {
					conn.err = err
					break
				}
				conn.delivered = true

				switch event.Type() {
				case "event:published":
					if published, ok := event["stream_id"].(string); ok {
						streamID = published
					}
					if published, ok := event["event_id"].(string); ok {
						tracker.eventID = published
					}
				case "stream:complete":
					conn.complete = true
				case "run:start":
					if runID := event.RunID(); runID != "" {
						if seenStart[runID] {
							continue
						}
						seenStart[runID] = true
					}
				case "run:stop", "run:fail", "run:cancel":
					if runID := event.RunID(); runID != "" {
						if seenTerminal[runID] {
							continue
						}
						seenTerminal[runID] = true
					}
				}
				_, final := tracker.handle(event)
				grace.sync(tracker)

				if !yield(event, nil) || final {
					done = true
					break
				}
			}
			grace.stop()
			graceEnded := errors.Is(context.Cause(connCtx), errRetryGrace)
			cancelConn(nil)
			if done {
				return
			}
			// A held failure that no retry followed in time ends the
			// iteration; its run:fail was already yielded.
			if tracker.held != nil && (graceEnded || !time.Now().Before(tracker.holdUntil())) {
				return
			}
			if graceEnded && ctx.Err() == nil {
				// The retry arrived as the grace ended; resubscribe at once.
				conn = connection{complete: true}
			}

			if streamID == "" {
				if ctx.Err() != nil {
					yield(nil, contextError(ctx, "stream", conn.err))
					return
				}
				if conn.err == nil {
					conn.err = errors.New("hot: stream ended before event:published was received")
				}
				if !retryableBeforePublish(conn.err) {
					yield(nil, conn.err)
					return
				}
			}
			if err := reconnect.next(ctx, conn, "hot: stream ended before the published event's run completed"); err != nil {
				yield(nil, err)
				return
			}
		}
	}
}

func waitForRunResult(ctx context.Context, t *transport, streamID, eventID string, opts *WaitForRunResultOptions) (map[string]any, error) {
	if streamID == "" || eventID == "" {
		return nil, errors.New("hot: WaitForRunResult needs the published event's stream_id and event_id")
	}
	return waitForPublishedRun(ctx, t, streamID, eventID, nil, opts)
}

func waitForCallResult(ctx context.Context, t *transport, fn string, args []any, opts *WaitForRunResultOptions) (map[string]any, error) {
	body := map[string]any{
		"event_type": "hot:call",
		"event_data": map[string]any{"fn": fn, "args": args},
	}
	return waitForPublishedRun(ctx, t, "", "", body, opts)
}

// waitForPublishedRun waits for the final run of one published event. With a
// publish body it first publishes through subscribe-with-event; otherwise it
// subscribes to streamID and waits for the run of eventID.
func waitForPublishedRun(ctx context.Context, t *transport, streamID, eventID string, publish map[string]any, opts *WaitForRunResultOptions) (map[string]any, error) {
	options := WaitForRunResultOptions{}
	if opts != nil {
		options = *opts
	}
	if options.Timeout == 0 {
		options.Timeout = 60 * time.Second
	}
	ctx, cancel := withWaitTimeout(ctx, options.Timeout)
	defer cancel()

	tracker := newRunTracker(eventID, options.OnChunk)
	reconnect := newReconnector("run result", options.MaxAttempts)
	for {
		connCtx, cancelConn := context.WithCancelCause(ctx)
		grace := &graceWatch{cancel: cancelConn}
		grace.sync(tracker)
		var source iter.Seq2[StreamEvent, error]
		if streamID == "" {
			source = t.stream(connCtx, "POST", "/streams/subscribe-with-event", publish, nil)
		} else {
			source = t.stream(connCtx, "GET", "/streams/"+enc(streamID)+"/subscribe", nil, nil)
		}
		var conn connection
		var final StreamEvent
		for event, err := range source {
			if err != nil {
				conn.err = err
				break
			}
			conn.delivered = true
			switch event.Type() {
			case "event:published":
				if publish != nil {
					streamID, _ = event["stream_id"].(string)
					tracker.eventID, _ = event["event_id"].(string)
				}
			case "stream:complete":
				conn.complete = true
			}
			var settled bool
			if final, settled = tracker.handle(event); settled {
				break
			}
			grace.sync(tracker)
		}
		grace.stop()
		graceEnded := errors.Is(context.Cause(connCtx), errRetryGrace)
		cancelConn(nil)
		if final != nil {
			return runOutcome(final)
		}
		// A held failure stands when its grace ends without a retry run, and
		// at the caller's deadline.
		if tracker.held != nil && (graceEnded || ctx.Err() != nil || !time.Now().Before(tracker.holdUntil())) {
			held, _ := tracker.expire()
			return runOutcome(held)
		}
		if graceEnded && ctx.Err() == nil {
			// The retry arrived as the grace ended; resubscribe at once.
			conn = connection{complete: true}
		}
		if streamID == "" {
			if ctx.Err() != nil {
				return nil, contextError(ctx, "run result", conn.err)
			}
			if conn.err == nil {
				conn.err = errors.New("hot: stream ended before event:published was received")
			}
			if !retryableBeforePublish(conn.err) {
				return nil, conn.err
			}
		}
		if err := reconnect.next(ctx, conn, "hot: stream ended before run completed"); err != nil {
			if held, ok := tracker.expire(); ok {
				return runOutcome(held)
			}
			return nil, err
		}
	}
}

func eventIDOfRun(run map[string]any) string {
	eventID, _ := run["event_id"].(string)
	return eventID
}

// resultMessage digs a human-readable message out of a run result value.
func resultMessage(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case map[string]any:
		for _, key := range []string{"$err", "$val", "error", "message", "msg", "reason", "err"} {
			if message := resultMessage(v[key]); message != "" {
				return message
			}
		}
	}
	return ""
}

func runResultMessage(run map[string]any, fallback string) string {
	if message := resultMessage(run["result"]); message != "" {
		return message
	}
	if status, _ := run["status"].(string); status != "" {
		return fmt.Sprintf("%s (%s)", fallback, status)
	}
	return fallback
}

// extractRunResult unwraps a {"$ok": ...} run result.
func extractRunResult(result any) any {
	if object, ok := result.(map[string]any); ok {
		if unwrapped, ok := object["$ok"]; ok {
			return unwrapped
		}
	}
	return result
}

func streamQuery(opts *StreamOptions) url.Values {
	if opts == nil {
		return nil
	}
	return projectQuery(opts.Project)
}

func projectQuery(project string) url.Values {
	if project == "" {
		return nil
	}
	return url.Values{"project": {project}}
}
