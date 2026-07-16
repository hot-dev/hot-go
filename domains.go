package hot

import "context"

// DomainsResource manages custom domains.
type DomainsResource struct{ transport *transport }

// Create registers a custom domain.
func (r *DomainsResource) Create(ctx context.Context, body map[string]any) (map[string]any, error) {
	envelope, err := r.transport.requestJSON(ctx, "POST", "/domains", body, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// List returns the domain list envelope.
func (r *DomainsResource) List(ctx context.Context) (map[string]any, error) {
	return r.transport.requestJSON(ctx, "GET", "/domains", nil, nil)
}

// Get returns a single domain.
func (r *DomainsResource) Get(ctx context.Context, domainID string) (map[string]any, error) {
	envelope, err := r.transport.requestJSON(ctx, "GET", "/domains/"+enc(domainID), nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// Delete deletes a domain.
func (r *DomainsResource) Delete(ctx context.Context, domainID string) error {
	resp, err := r.transport.requestRaw(ctx, "DELETE", "/domains/"+enc(domainID), nil, nil, nil)
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

// Verify triggers domain verification.
func (r *DomainsResource) Verify(ctx context.Context, domainID string) (map[string]any, error) {
	envelope, err := r.transport.requestJSON(ctx, "POST", "/domains/"+enc(domainID)+"/verify", nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}
