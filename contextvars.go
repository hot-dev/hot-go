package hot

import "context"

// ContextResource manages encrypted project context variables.
type ContextResource struct{ transport *transport }

// List returns the context variable list envelope for a project.
func (r *ContextResource) List(ctx context.Context, project string) (map[string]any, error) {
	return r.transport.requestJSON(ctx, "GET", "/projects/"+enc(project)+"/context", nil, nil)
}

// Create creates a context variable.
func (r *ContextResource) Create(ctx context.Context, project string, body map[string]any) (map[string]any, error) {
	envelope, err := r.transport.requestJSON(ctx, "POST", "/projects/"+enc(project)+"/context", body, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// Update replaces a context variable.
func (r *ContextResource) Update(ctx context.Context, project, key string, body map[string]any) (map[string]any, error) {
	envelope, err := r.transport.requestJSON(ctx, "PUT", "/projects/"+enc(project)+"/context/"+enc(key), body, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// Delete deletes a context variable.
func (r *ContextResource) Delete(ctx context.Context, project, key string) error {
	resp, err := r.transport.requestRaw(ctx, "DELETE", "/projects/"+enc(project)+"/context/"+enc(key), nil, nil, nil)
	if err != nil {
		return err
	}
	return resp.Body.Close()
}
