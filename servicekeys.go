package hot

import (
	"context"
	"net/url"
)

// ServiceKeysResource creates and revokes scoped service keys.
type ServiceKeysResource struct{ transport *transport }

// Create creates a scoped service key.
func (r *ServiceKeysResource) Create(ctx context.Context, body map[string]any) (map[string]any, error) {
	envelope, err := r.transport.requestJSON(ctx, "POST", "/service-keys", body, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// List returns the service key list envelope.
func (r *ServiceKeysResource) List(ctx context.Context, query url.Values) (map[string]any, error) {
	return r.transport.requestJSON(ctx, "GET", "/service-keys", nil, query)
}

// Get returns a single service key.
func (r *ServiceKeysResource) Get(ctx context.Context, serviceKeyID string) (map[string]any, error) {
	envelope, err := r.transport.requestJSON(ctx, "GET", "/service-keys/"+enc(serviceKeyID), nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// Update patches a service key.
func (r *ServiceKeysResource) Update(ctx context.Context, serviceKeyID string, body map[string]any) (map[string]any, error) {
	envelope, err := r.transport.requestJSON(ctx, "PATCH", "/service-keys/"+enc(serviceKeyID), body, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// Revoke revokes one service key.
func (r *ServiceKeysResource) Revoke(ctx context.Context, serviceKeyID string) error {
	return r.transport.delete(ctx, "/service-keys/"+enc(serviceKeyID), nil)
}

// RevokeAll revokes every service key.
func (r *ServiceKeysResource) RevokeAll(ctx context.Context) (map[string]any, error) {
	envelope, err := r.transport.requestJSON(ctx, "DELETE", "/service-keys", nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}
