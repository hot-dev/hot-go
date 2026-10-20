package hot

import (
	"context"
	"net/url"
)

// ManagementClient calls the root management API using an hm_ credential.
// Runtime keys, service keys and sessions cannot authenticate here. Use a
// separate Client with a child environment key for runtime operations.
type ManagementClient struct{ transport *transport }

func NewManagementClient(cfg Config) (*ManagementClient, error) {
	t, err := newTransport(cfg)
	if err != nil {
		return nil, err
	}
	return &ManagementClient{transport: t}, nil
}

// ListChildren returns children and total for the credential's authorized targets.
func (c *ManagementClient) ListChildren(ctx context.Context, query url.Values) (map[string]any, error) {
	envelope, err := c.transport.requestJSON(ctx, "GET", "/management/children", nil, query)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// CreateChild provisions a child. Keep body.idempotency_key stable across retries.
func (c *ManagementClient) CreateChild(ctx context.Context, body any) (map[string]any, error) {
	envelope, err := c.transport.requestJSON(ctx, "POST", "/management/children", body, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// SetChildState sets active, suspended or archived. Success has no response body.
func (c *ManagementClient) SetChildState(ctx context.Context, childID, state string) error {
	_, err := c.transport.requestJSON(ctx, "PATCH", "/management/children/"+enc(childID)+"/state", map[string]any{"state": state}, nil)
	return err
}

func (c *ManagementClient) CreateEnvironment(ctx context.Context, childID string, body any) (map[string]any, error) {
	envelope, err := c.transport.requestJSON(ctx, "POST", "/management/children/"+enc(childID)+"/environments", body, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// CreateAPIKey issues a child environment key within the management delegation ceiling.
func (c *ManagementClient) CreateAPIKey(ctx context.Context, childID, envID string, body any) (map[string]any, error) {
	envelope, err := c.transport.requestJSON(ctx, "POST", "/management/children/"+enc(childID)+"/environments/"+enc(envID)+"/api-keys", body, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// Usage returns account-wide usage; requires usage:read and all_children authority.
func (c *ManagementClient) Usage(ctx context.Context) (map[string]any, error) {
	envelope, err := c.transport.requestJSON(ctx, "GET", "/management/usage", nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}
