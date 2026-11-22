package hot

import (
	"context"
	"net/url"
)

// OrgsResource manages sub-organizations, their environments and their API
// keys through the org API (/v1/orgs). It needs a Client configured with an
// org API key, which an administrator of the parent organization creates on its
// Sub-orgs tab in the Hot App; any other credential gets a *KeyScopeError
// with RequiredScope "org". Org API keys in turn cannot call runtime
// resources such as Events or Streams.
//
// Mint an environment key with CreateEnvAPIKey and give it to a separate
// Client for runtime calls in that environment.
type OrgsResource struct{ transport *transport }

// List returns the list envelope (data + pagination) of the sub-organizations the key
// may read. query accepts limit and offset.
func (r *OrgsResource) List(ctx context.Context, query url.Values) (map[string]any, error) {
	return r.transport.requestJSON(ctx, "GET", "/orgs", nil, query)
}

// Create provisions a sub-organization with a default environment and
// returns {org, env_id, created}. body needs name, slug (32 characters or
// fewer) and idempotency_key, and accepts external_id. Reuse the same
// idempotency_key to retry: a replay returns the same sub-organization with created
// false.
func (r *OrgsResource) Create(ctx context.Context, body map[string]any) (map[string]any, error) {
	envelope, err := r.transport.requestJSON(ctx, "POST", "/orgs", body, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// Get returns an organization: the key's own organization or one of its sub-organizations.
func (r *OrgsResource) Get(ctx context.Context, orgID string) (map[string]any, error) {
	orgSeg, err := encSegment("org_id", orgID)
	if err != nil {
		return nil, err
	}
	envelope, err := r.transport.requestJSON(ctx, "GET", "/orgs/"+orgSeg, nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// Update changes a sub-organization's lifecycle state and returns the organization.
// body is {"state": "active" | "suspended" | "archived"}.
func (r *OrgsResource) Update(ctx context.Context, orgID string, body map[string]any) (map[string]any, error) {
	orgSeg, err := encSegment("org_id", orgID)
	if err != nil {
		return nil, err
	}
	envelope, err := r.transport.requestJSON(ctx, "PATCH", "/orgs/"+orgSeg, body, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// Usage returns account usage for the current billing period, across the
// parent organization and every sub-organization. parentOrgID must be the key's own organization.
func (r *OrgsResource) Usage(ctx context.Context, parentOrgID string) (map[string]any, error) {
	orgSeg, err := encSegment("org_id", parentOrgID)
	if err != nil {
		return nil, err
	}
	envelope, err := r.transport.requestJSON(ctx, "GET", "/orgs/"+orgSeg+"/usage", nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// PurgeCalls deletes every call trace in the organization's environments, and
// in its sub-organizations' when includeSubOrgs is set, and returns the purge outcome
// (calls_deleted, blob_refs_released, complete).
func (r *OrgsResource) PurgeCalls(ctx context.Context, orgID string, includeSubOrgs bool) (map[string]any, error) {
	orgSeg, err := encSegment("org_id", orgID)
	if err != nil {
		return nil, err
	}
	var query url.Values
	if includeSubOrgs {
		query = url.Values{"include_sub_orgs": {"true"}}
	}
	envelope, err := r.transport.requestJSON(ctx, "DELETE", "/orgs/"+orgSeg+"/calls", nil, query)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// ListEnvs returns the list envelope of an organization's environments.
// To pass pagination parameters such as limit and offset, use ListEnvsWithQuery.
func (r *OrgsResource) ListEnvs(ctx context.Context, orgID string) (map[string]any, error) {
	return r.ListEnvsWithQuery(ctx, orgID, nil)
}

// ListEnvsWithQuery returns the paginated list envelope of an organization's
// environments. query accepts pagination parameters such as limit and offset.
func (r *OrgsResource) ListEnvsWithQuery(ctx context.Context, orgID string, query url.Values) (map[string]any, error) {
	orgSeg, err := encSegment("org_id", orgID)
	if err != nil {
		return nil, err
	}
	return r.transport.requestJSON(ctx, "GET", "/orgs/"+orgSeg+"/envs", nil, query)
}

// CreateEnv provisions an environment, idempotently by exact name within the
// organization. body is {"name": ...}.
func (r *OrgsResource) CreateEnv(ctx context.Context, orgID string, body map[string]any) (map[string]any, error) {
	orgSeg, err := encSegment("org_id", orgID)
	if err != nil {
		return nil, err
	}
	envelope, err := r.transport.requestJSON(ctx, "POST", "/orgs/"+orgSeg+"/envs", body, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// GetEnv returns one environment of an organization.
func (r *OrgsResource) GetEnv(ctx context.Context, orgID, envID string) (map[string]any, error) {
	orgSeg, err := encSegment("org_id", orgID)
	if err != nil {
		return nil, err
	}
	envSeg, err := encSegment("env_id", envID)
	if err != nil {
		return nil, err
	}
	envelope, err := r.transport.requestJSON(ctx, "GET", "/orgs/"+orgSeg+"/envs/"+envSeg, nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// ListAPIKeys returns the list envelope of the org API keys in the key's own
// organization.
func (r *OrgsResource) ListAPIKeys(ctx context.Context, orgID string, query url.Values) (map[string]any, error) {
	orgSeg, err := encSegment("org_id", orgID)
	if err != nil {
		return nil, err
	}
	return r.transport.requestJSON(ctx, "GET", "/orgs/"+orgSeg+"/api-keys", nil, query)
}

// CreateAPIKey mints an org API key in the key's own organization. body needs
// description and permissions (qualified grants such as {"org:*": ["read"]}
// that fit inside the caller's own) and accepts expires_at. The returned
// key's token is shown only once.
func (r *OrgsResource) CreateAPIKey(ctx context.Context, orgID string, body map[string]any) (map[string]any, error) {
	orgSeg, err := encSegment("org_id", orgID)
	if err != nil {
		return nil, err
	}
	envelope, err := r.transport.requestJSON(ctx, "POST", "/orgs/"+orgSeg+"/api-keys", body, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// ListEnvAPIKeys returns the list envelope of an environment's API keys.
func (r *OrgsResource) ListEnvAPIKeys(ctx context.Context, orgID, envID string, query url.Values) (map[string]any, error) {
	orgSeg, err := encSegment("org_id", orgID)
	if err != nil {
		return nil, err
	}
	envSeg, err := encSegment("env_id", envID)
	if err != nil {
		return nil, err
	}
	return r.transport.requestJSON(ctx, "GET", "/orgs/"+orgSeg+"/envs/"+envSeg+"/api-keys", nil, query)
}

// CreateEnvAPIKey mints an environment API key for runtime calls in that
// environment. body needs description and permissions (environment grants
// such as {"event:*": ["create"]} that fit inside the org key's grants for
// the environment) and accepts expires_at. The returned key's token is shown
// only once.
func (r *OrgsResource) CreateEnvAPIKey(ctx context.Context, orgID, envID string, body map[string]any) (map[string]any, error) {
	orgSeg, err := encSegment("org_id", orgID)
	if err != nil {
		return nil, err
	}
	envSeg, err := encSegment("env_id", envID)
	if err != nil {
		return nil, err
	}
	envelope, err := r.transport.requestJSON(ctx, "POST", "/orgs/"+orgSeg+"/envs/"+envSeg+"/api-keys", body, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// Env addresses one environment of an organization through the org API. Its
// resources call /v1/orgs/{org_id}/envs/{env_id}/... with the org key's
// grants for that environment, so an org key can manage projects, builds and
// context in a sub-organization without minting an environment key first.
func (r *OrgsResource) Env(orgID, envID string) *OrgEnv {
	orgSeg, err := encSegment("org_id", orgID)
	var envSeg string
	if err == nil {
		envSeg, err = encSegment("env_id", envID)
	}
	t := r.transport.scoped("/orgs/"+orgSeg+"/envs/"+envSeg, err)
	return &OrgEnv{
		Projects: &ProjectsResource{t},
		Builds:   &BuildsResource{t},
		Context:  &ContextResource{t},
	}
}

// OrgEnv holds the environment resources an org API key can reach in one
// environment: projects (including event handlers and schedules), builds and
// deploys, and project context. Construct it with OrgsResource.Env.
type OrgEnv struct {
	Projects *ProjectsResource
	Builds   *BuildsResource
	Context  *ContextResource
}

// APIKeysResource reads, narrows, revokes and rotates API keys by ID through
// the org API. It needs an org API key; the target may be an org key or an
// environment key in the key's organization or one of its sub-organizations.
// Narrowing, revoking and rotating also require that the caller issued the
// target, directly or through keys it issued.
type APIKeysResource struct{ transport *transport }

// Get returns an API key's metadata (never its secret).
func (r *APIKeysResource) Get(ctx context.Context, keyID string) (map[string]any, error) {
	keySeg, err := encSegment("key_id", keyID)
	if err != nil {
		return nil, err
	}
	envelope, err := r.transport.requestJSON(ctx, "GET", "/api-keys/"+keySeg, nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// Update replaces an API key's grants and returns the key. body is
// {"permissions": ...}; the new grants must fit inside the caller's own.
// Narrowing a key also narrows everything it issued.
func (r *APIKeysResource) Update(ctx context.Context, keyID string, body map[string]any) (map[string]any, error) {
	keySeg, err := encSegment("key_id", keyID)
	if err != nil {
		return nil, err
	}
	envelope, err := r.transport.requestJSON(ctx, "PATCH", "/api-keys/"+keySeg, body, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// Revoke revokes an API key. Everything it issued stops on its next request.
func (r *APIKeysResource) Revoke(ctx context.Context, keyID string) error {
	keySeg, err := encSegment("key_id", keyID)
	if err != nil {
		return err
	}
	return r.transport.delete(ctx, "/api-keys/"+keySeg, nil)
}

// Rotate issues a new secret for the same key and returns the key with its
// new token (and previous_secret_expires_at while an overlap is active).
// body may be nil or {"overlap_seconds": n}: how long the previous secret
// keeps working (default 86400, at most 604800; 0 retires it immediately).
func (r *APIKeysResource) Rotate(ctx context.Context, keyID string, body map[string]any) (map[string]any, error) {
	keySeg, err := encSegment("key_id", keyID)
	if err != nil {
		return nil, err
	}
	envelope, err := r.transport.requestJSON(ctx, "POST", "/api-keys/"+keySeg+"/rotate", body, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}
