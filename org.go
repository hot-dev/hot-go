package hot

import "context"

// OrgResource reports the usage and limits seen by a runtime credential. For
// account-wide usage with an org API key, use OrgsResource.Usage.
type OrgResource struct{ transport *transport }

// Usage returns the credential's environment usage (usage_scope
// "environment") against the account's shared limits.
func (r *OrgResource) Usage(ctx context.Context) (map[string]any, error) {
	envelope, err := r.transport.requestJSON(ctx, "GET", "/org/usage", nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}
