package hot

import (
	"context"
	"net/url"
)

// RunsResource lists and inspects runs.
type RunsResource struct{ transport *transport }

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
