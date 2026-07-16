package hot

import (
	"context"
	"net/url"
)

// SessionsResource creates and revokes scoped sessions.
type SessionsResource struct{ transport *transport }

// Create creates a scoped session.
func (r *SessionsResource) Create(ctx context.Context, body map[string]any) (map[string]any, error) {
	envelope, err := r.transport.requestJSON(ctx, "POST", "/sessions", body, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// List returns the session list envelope.
func (r *SessionsResource) List(ctx context.Context, query url.Values) (map[string]any, error) {
	return r.transport.requestJSON(ctx, "GET", "/sessions", nil, query)
}

// Revoke revokes one session.
func (r *SessionsResource) Revoke(ctx context.Context, sessionID string) error {
	resp, err := r.transport.requestRaw(ctx, "DELETE", "/sessions/"+enc(sessionID), nil, nil, nil)
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

// RevokeAll revokes every session.
func (r *SessionsResource) RevokeAll(ctx context.Context) (map[string]any, error) {
	envelope, err := r.transport.requestJSON(ctx, "DELETE", "/sessions", nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}
