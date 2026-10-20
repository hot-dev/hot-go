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
	// 5-minute SSE timeout.
	DisableReconnect bool
	// MaxAttempts caps reconnection attempts. Defaults to 5.
	MaxAttempts int
}

// WaitForRunResultOptions carries optional parameters for WaitForRunResult.
type WaitForRunResultOptions struct {
	// Timeout bounds the wait. Defaults to 60 seconds.
	Timeout time.Duration
	// OnChunk receives streamed text chunks from the matched run.
	OnChunk func(text string)
	// MaxAttempts caps reconnection attempts. Defaults to 5.
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

// WaitForRunResult subscribes to a stream and returns the run object once the
// run created by eventID reaches run:stop. run:fail and run:cancel become
// errors carrying the run's result message.
func (r *StreamsResource) WaitForRunResult(ctx context.Context, streamID, eventID string, opts *WaitForRunResultOptions) (map[string]any, error) {
	return waitForRunResult(ctx, r.transport, streamID, eventID, opts)
}

// SubscribeWithEvent atomically subscribes then publishes an event, yielding
// stream events until the run completes.
//
// The Hot API closes SSE subscriptions after 5 minutes of idle time. Unless
// opts.DisableReconnect is set, the SDK resubscribes to the same stream_id and
// keeps yielding events until a terminal run:stop/run:fail/run:cancel arrives.
// Replayed run:start and terminal events are deduped by run_id; stream:data
// chunks are not replayed and may be lost across the disconnect window.
func (r *StreamsResource) SubscribeWithEvent(ctx context.Context, body map[string]any, opts *SubscribeWithEventOptions) iter.Seq2[StreamEvent, error] {
	options := SubscribeWithEventOptions{}
	if opts != nil {
		options = *opts
	}
	if options.MaxAttempts == 0 {
		options.MaxAttempts = 5
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
		eventID := ""
		attempts := 0

		for {
			var source iter.Seq2[StreamEvent, error]
			if streamID == "" {
				source = once()
			} else {
				source = r.Subscribe(ctx, streamID, &StreamOptions{Project: options.Project})
			}

			terminal := false
			for event, err := range source {
				if err != nil {
					if isAuthorizationError(err) || streamID == "" || attempts >= options.MaxAttempts {
						yield(nil, err)
						return
					}
					break
				}

				switch event.Type() {
				case "event:published":
					if published, ok := event["stream_id"].(string); ok {
						streamID = published
					}
					if published, ok := event["event_id"].(string); ok {
						eventID = published
					}
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
					terminal = eventID != "" && eventIDOfRun(event.Run()) == eventID
				}

				if !yield(event, nil) {
					return
				}
				if terminal {
					return
				}
			}

			if terminal {
				return
			}
			if streamID == "" {
				yield(nil, errors.New("hot: stream ended before event:published was received"))
				return
			}
			if attempts >= options.MaxAttempts {
				yield(nil, errors.New("hot: stream ended before the published event's run completed"))
				return
			}

			attempts++
			delay := time.Duration(attempts) * 250 * time.Millisecond
			if delay > 2*time.Second {
				delay = 2 * time.Second
			}
			select {
			case <-ctx.Done():
				yield(nil, ctx.Err())
				return
			case <-time.After(delay):
			}
		}
	}
}

func waitForRunResult(ctx context.Context, t *transport, streamID, eventID string, opts *WaitForRunResultOptions) (map[string]any, error) {
	options := WaitForRunResultOptions{}
	if opts != nil {
		options = *opts
	}
	if options.Timeout == 0 {
		options.Timeout = 60 * time.Second
	}
	if options.MaxAttempts == 0 {
		options.MaxAttempts = 5
	}

	ctx, cancel := context.WithTimeout(ctx, options.Timeout)
	defer cancel()

	currentRunID := ""
	for attempts := 0; ; attempts++ {
		for event, err := range t.stream(ctx, "GET", "/streams/"+enc(streamID)+"/subscribe", nil, nil) {
			if err != nil {
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					return nil, errors.New("hot: timeout waiting for run result")
				}
				if isAuthorizationError(err) || attempts >= options.MaxAttempts {
					return nil, err
				}
				break
			}
			var run map[string]any
			currentRunID, run, err = handleRunEvent(event, eventID, currentRunID, options.OnChunk)
			if err != nil || run != nil {
				return run, err
			}
		}
		if ctx.Err() != nil {
			return nil, errors.New("hot: timeout waiting for run result")
		}
		if attempts >= options.MaxAttempts {
			return nil, errors.New("hot: stream ended before run completed")
		}
	}
}

func waitForCallResult(ctx context.Context, t *transport, fn string, args []any, opts *WaitForRunResultOptions) (map[string]any, error) {
	options := WaitForRunResultOptions{}
	if opts != nil {
		options = *opts
	}
	if options.Timeout == 0 {
		options.Timeout = 60 * time.Second
	}
	if options.MaxAttempts == 0 {
		options.MaxAttempts = 5
	}
	ctx, cancel := context.WithTimeout(ctx, options.Timeout)
	defer cancel()

	streamID := ""
	eventID := ""
	currentRunID := ""
	body := map[string]any{
		"event_type": "hot:call",
		"event_data": map[string]any{"fn": fn, "args": args},
	}
	for attempts := 0; ; attempts++ {
		method := "POST"
		path := "/streams/subscribe-with-event"
		var requestBody any = body
		if streamID != "" {
			method = "GET"
			path = "/streams/" + enc(streamID) + "/subscribe"
			requestBody = nil
		}
		for event, err := range t.stream(ctx, method, path, requestBody, nil) {
			if err != nil {
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					return nil, errors.New("hot: timeout waiting for run result")
				}
				if isAuthorizationError(err) || streamID == "" || attempts >= options.MaxAttempts {
					return nil, err
				}
				break
			}
			if event.Type() == "event:published" {
				streamID, _ = event["stream_id"].(string)
				eventID, _ = event["event_id"].(string)
			}
			if eventID != "" {
				var run map[string]any
				currentRunID, run, err = handleRunEvent(event, eventID, currentRunID, options.OnChunk)
				if err != nil || run != nil {
					return run, err
				}
			}
		}
		if ctx.Err() != nil {
			return nil, errors.New("hot: timeout waiting for run result")
		}
		if streamID == "" || eventID == "" {
			return nil, errors.New("hot: stream ended before event:published was received")
		}
		if attempts >= options.MaxAttempts {
			return nil, errors.New("hot: stream ended before run completed")
		}
	}
}

func handleRunEvent(event StreamEvent, eventID, currentRunID string, onChunk func(string)) (string, map[string]any, error) {
	switch event.Type() {
	case "run:start":
		if eventIDOfRun(event.Run()) == eventID {
			currentRunID = event.RunID()
		}
	case "stream:data":
		if currentRunID != "" && onChunk != nil {
			if runID, _ := event["run_id"].(string); runID == currentRunID {
				if payload, ok := event["payload"].(map[string]any); ok {
					if text, ok := payload["text"].(string); ok {
						onChunk(text)
					}
				}
			}
		}
	case "run:stop", "run:fail", "run:cancel":
		run := event.Run()
		if run == nil || eventIDOfRun(run) != eventID {
			return currentRunID, nil, nil
		}
		if event.Type() == "run:fail" {
			return currentRunID, nil, errors.New("hot: " + runResultMessage(run, "run failed"))
		}
		if event.Type() == "run:cancel" {
			return currentRunID, nil, errors.New("hot: " + runResultMessage(run, "run cancelled"))
		}
		return currentRunID, run, nil
	}
	return currentRunID, nil, nil
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
