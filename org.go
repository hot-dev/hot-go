package hot

import "context"

// OrgResource reports organization usage and limits.
type OrgResource struct{ transport *transport }

// Usage returns organization usage and limits.
func (r *OrgResource) Usage(ctx context.Context) (map[string]any, error) {
	envelope, err := r.transport.requestJSON(ctx, "GET", "/org/usage", nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}
