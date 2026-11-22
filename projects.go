package hot

import (
	"context"
	"net/url"
)

// ProjectsResource manages projects.
type ProjectsResource struct{ transport *transport }

// List returns the project list envelope (data + pagination).
func (r *ProjectsResource) List(ctx context.Context, query url.Values) (map[string]any, error) {
	return r.transport.requestJSON(ctx, "GET", "/projects", nil, query)
}

// Create creates a project.
func (r *ProjectsResource) Create(ctx context.Context, body map[string]any) (map[string]any, error) {
	envelope, err := r.transport.requestJSON(ctx, "POST", "/projects", body, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// Get returns a project by id or slug.
func (r *ProjectsResource) Get(ctx context.Context, project string) (map[string]any, error) {
	projectSeg, err := encSegment("project", project)
	if err != nil {
		return nil, err
	}
	envelope, err := r.transport.requestJSON(ctx, "GET", "/projects/"+projectSeg, nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// Update patches a project.
func (r *ProjectsResource) Update(ctx context.Context, project string, body map[string]any) (map[string]any, error) {
	projectSeg, err := encSegment("project", project)
	if err != nil {
		return nil, err
	}
	envelope, err := r.transport.requestJSON(ctx, "PATCH", "/projects/"+projectSeg, body, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// Delete deletes a project.
func (r *ProjectsResource) Delete(ctx context.Context, project string) error {
	projectSeg, err := encSegment("project", project)
	if err != nil {
		return err
	}
	return r.transport.delete(ctx, "/projects/"+projectSeg, nil)
}

// Activate activates a project.
func (r *ProjectsResource) Activate(ctx context.Context, project string) (map[string]any, error) {
	projectSeg, err := encSegment("project", project)
	if err != nil {
		return nil, err
	}
	envelope, err := r.transport.requestJSON(ctx, "POST", "/projects/"+projectSeg+"/activate", nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// Deactivate deactivates a project.
func (r *ProjectsResource) Deactivate(ctx context.Context, project string) (map[string]any, error) {
	projectSeg, err := encSegment("project", project)
	if err != nil {
		return nil, err
	}
	envelope, err := r.transport.requestJSON(ctx, "POST", "/projects/"+projectSeg+"/deactivate", nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// EventHandlers returns the project's event handlers.
func (r *ProjectsResource) EventHandlers(ctx context.Context, project string) (map[string]any, error) {
	projectSeg, err := encSegment("project", project)
	if err != nil {
		return nil, err
	}
	return r.transport.requestJSON(ctx, "GET", "/projects/"+projectSeg+"/event-handlers", nil, nil)
}

// Schedules returns the project's schedules.
func (r *ProjectsResource) Schedules(ctx context.Context, project string) (map[string]any, error) {
	projectSeg, err := encSegment("project", project)
	if err != nil {
		return nil, err
	}
	return r.transport.requestJSON(ctx, "GET", "/projects/"+projectSeg+"/schedules", nil, nil)
}
