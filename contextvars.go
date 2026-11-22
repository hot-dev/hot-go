package hot

import "context"

// ContextResource manages encrypted project context variables.
type ContextResource struct{ transport *transport }

// List returns the context variable list envelope for a project.
func (r *ContextResource) List(ctx context.Context, project string) (map[string]any, error) {
	projectSeg, err := encSegment("project", project)
	if err != nil {
		return nil, err
	}
	return r.transport.requestJSON(ctx, "GET", "/projects/"+projectSeg+"/context", nil, nil)
}

// Create creates a context variable.
func (r *ContextResource) Create(ctx context.Context, project string, body map[string]any) (map[string]any, error) {
	projectSeg, err := encSegment("project", project)
	if err != nil {
		return nil, err
	}
	envelope, err := r.transport.requestJSON(ctx, "POST", "/projects/"+projectSeg+"/context", body, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// Update replaces a context variable.
func (r *ContextResource) Update(ctx context.Context, project, key string, body map[string]any) (map[string]any, error) {
	projectSeg, err := encSegment("project", project)
	if err != nil {
		return nil, err
	}
	keySeg, err := encSegment("key", key)
	if err != nil {
		return nil, err
	}
	envelope, err := r.transport.requestJSON(ctx, "PUT", "/projects/"+projectSeg+"/context/"+keySeg, body, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// Delete deletes a context variable.
func (r *ContextResource) Delete(ctx context.Context, project, key string) error {
	projectSeg, err := encSegment("project", project)
	if err != nil {
		return err
	}
	keySeg, err := encSegment("key", key)
	if err != nil {
		return err
	}
	return r.transport.delete(ctx, "/projects/"+projectSeg+"/context/"+keySeg, nil)
}
