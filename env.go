package hot

import (
	"context"
	"iter"
)

// EnvResource reads environment info and environment events.
type EnvResource struct{ transport *transport }

// Get returns environment info.
func (r *EnvResource) Get(ctx context.Context) (map[string]any, error) {
	envelope, err := r.transport.requestJSON(ctx, "GET", "/env", nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// Subscribe streams environment events. Requires API key credentials and a
// live API pub/sub backend.
func (r *EnvResource) Subscribe(ctx context.Context) iter.Seq2[StreamEvent, error] {
	return r.transport.stream(ctx, "GET", "/env/subscribe", nil, nil)
}
