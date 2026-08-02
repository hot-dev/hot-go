package hot

import (
	"context"
	"net/url"
	"time"
)

// EventsResource publishes and inspects Hot events.
type EventsResource struct{ transport *transport }

// CallHotOptions carries optional parameters for CallHot.
type CallHotOptions struct {
	// Timeout bounds the wait for the run result. Defaults to 60 seconds.
	Timeout time.Duration
	// OnChunk receives streamed text chunks from the run.
	OnChunk func(text string)
}

// Publish publishes an event and returns the created event data
// (event_id, stream_id, ...).
func (r *EventsResource) Publish(ctx context.Context, body map[string]any) (map[string]any, error) {
	envelope, err := r.transport.requestJSON(ctx, "POST", "/events", body, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// CallHot publishes a hot:call event for fn with args and waits for the run
// result, unwrapping a {"$ok": ...} result value.
func (r *EventsResource) CallHot(ctx context.Context, fn string, args []any, opts *CallHotOptions) (any, error) {
	if args == nil {
		args = []any{}
	}
	waitOpts := &WaitForRunResultOptions{}
	if opts != nil {
		waitOpts.Timeout = opts.Timeout
		waitOpts.OnChunk = opts.OnChunk
	}
	run, err := waitForCallResult(ctx, r.transport, fn, args, waitOpts)
	if err != nil {
		return nil, err
	}
	return extractRunResult(run["result"]), nil
}

// List returns the event list envelope (data + pagination).
func (r *EventsResource) List(ctx context.Context, query url.Values) (map[string]any, error) {
	return r.transport.requestJSON(ctx, "GET", "/events", nil, query)
}

// Get returns a single event.
func (r *EventsResource) Get(ctx context.Context, eventID string) (map[string]any, error) {
	envelope, err := r.transport.requestJSON(ctx, "GET", "/events/"+enc(eventID), nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// GetRuns returns the runs triggered by an event.
func (r *EventsResource) GetRuns(ctx context.Context, eventID string) (map[string]any, error) {
	return r.transport.requestJSON(ctx, "GET", "/events/"+enc(eventID)+"/runs", nil, nil)
}
