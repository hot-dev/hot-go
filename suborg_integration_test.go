package hot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

// Sub-org tests run against integration/suborg_fixture.py, which owns a
// disposable server and passes its credentials in HOT_TEST_SUBORG_FIXTURE.
type suborgFixture struct {
	RootOrgID string   `json:"root_org_id"`
	RootEnvID string   `json:"root_env_id"`
	RootToken string   `json:"root_token"`
	OrgToken  string   `json:"org_token"`
	Revoked   []string `json:"revoked"`
	Narrowed  []string `json:"narrowed"`
}

func suborgConfig(t *testing.T) suborgFixture {
	t.Helper()
	raw := os.Getenv("HOT_TEST_SUBORG_FIXTURE")
	if raw == "" {
		t.Skip("run integration/suborg_fixture.py")
	}
	var conf suborgFixture
	if err := json.Unmarshal([]byte(raw), &conf); err != nil {
		t.Fatal(err)
	}
	return conf
}

func suborgClient(t *testing.T, token string) *Client {
	t.Helper()
	c, err := NewClient(Config{Token: token, BaseURL: os.Getenv("HOT_TEST_BASE_URL"), Timeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func requireDenied(t *testing.T, err error, statuses ...int) {
	t.Helper()
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected API denial, got %v", err)
	}
	for _, status := range statuses {
		if apiErr.StatusCode == status {
			return
		}
	}
	t.Fatalf("unexpected denial: %v", err)
}

func requireKeyScope(t *testing.T, err error, scope string) {
	t.Helper()
	var scopeErr *KeyScopeError
	if !errors.As(err, &scopeErr) || scopeErr.RequiredScope != scope || scopeErr.StatusCode != 403 {
		t.Fatalf("expected key scope error needing %q, got %v", scope, err)
	}
}

func suborgSlug(i int) string {
	return fmt.Sprintf("sdk-go-%d-%d", time.Now().UnixNano()%1e12, i)
}

func TestSuborgIntegration(t *testing.T) {
	conf := suborgConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	orgs := suborgClient(t, conf.OrgToken)
	var children, envs []string
	var keys []map[string]any
	var clients []*Client
	defer func() {
		for _, child := range children {
			if _, err := orgs.Orgs.Update(ctx, child, map[string]any{"state": "archived"}); err != nil {
				t.Error(err)
			}
		}
	}()
	for i := 0; i < 2; i++ {
		slug := suborgSlug(i)
		body := map[string]any{"name": "SDK Customer", "slug": slug, "external_id": slug, "idempotency_key": slug}
		child, err := orgs.Orgs.Create(ctx, body)
		if err != nil {
			t.Fatal(err)
		}
		org := child["org"].(map[string]any)
		childID := org["org_id"].(string)
		children = append(children, childID)
		if child["created"] != true || org["parent_org_id"] != conf.RootOrgID || org["external_id"] != slug {
			t.Fatalf("create: %v", child)
		}
		replay, err := orgs.Orgs.Create(ctx, body)
		if err != nil || replay["created"] != false || replay["org"].(map[string]any)["org_id"] != childID {
			t.Fatalf("idempotency: %v %v", replay, err)
		}
		env, err := orgs.Orgs.CreateEnv(ctx, childID, map[string]any{"name": "production"})
		if err != nil {
			t.Fatal(err)
		}
		again, err := orgs.Orgs.CreateEnv(ctx, childID, map[string]any{"name": "production"})
		if err != nil || again["env_id"] != env["env_id"] {
			t.Fatalf("environment idempotency: %v %v", again, err)
		}
		envID := env["env_id"].(string)
		envs = append(envs, envID)
		if got, err := orgs.Orgs.GetEnv(ctx, childID, envID); err != nil || got["org_id"] != childID {
			t.Fatalf("get env: %v %v", got, err)
		}
		listedEnvs, err := orgs.Orgs.ListEnvs(ctx, childID)
		if err != nil || len(listedEnvs["data"].([]any)) != 2 {
			t.Fatalf("envs (default and production): %v %v", listedEnvs, err)
		}
		key, err := orgs.Orgs.CreateEnvAPIKey(ctx, childID, envID, map[string]any{"description": "sdk", "permissions": map[string]any{"*:*": []string{"*"}}})
		if err != nil {
			t.Fatal(err)
		}
		if key["scope"] != "env" || key["env_id"] != envID || key["token"] == nil {
			t.Fatalf("minted key: %v", key)
		}
		keys = append(keys, key)
		client := suborgClient(t, key["token"].(string))
		clients = append(clients, client)
		info, err := client.Env.Get(ctx)
		if err != nil || info["org_id"] != childID {
			t.Fatalf("scope: %v %v", info, err)
		}
		usage, err := client.Org.Usage(ctx)
		if err != nil || usage["env_id"] != envID || usage["usage_scope"] != "environment" || usage["limits_scope"] != "billing_account" {
			t.Fatalf("usage scope: %v %v", usage, err)
		}
	}

	// Each credential kind calls only its own routes.
	_, err := orgs.Env.Get(ctx)
	requireKeyScope(t, err, "env")
	_, err = clients[0].Orgs.List(ctx, nil)
	requireKeyScope(t, err, "org")
	_, err = suborgClient(t, conf.RootToken).APIKeys.Get(ctx, keys[0]["api_key_id"].(string))
	requireKeyScope(t, err, "org")

	listed, err := orgs.Orgs.List(ctx, nil)
	if err != nil || listed["pagination"].(map[string]any)["total"].(float64) < 2 {
		t.Fatalf("children: %v %v", listed, err)
	}
	if root, err := orgs.Orgs.Get(ctx, conf.RootOrgID); err != nil || root["parent_org_id"] != nil {
		t.Fatalf("root: %v %v", root, err)
	}
	usage, err := orgs.Orgs.Usage(ctx, conf.RootOrgID)
	if err != nil || usage["org_id"] != conf.RootOrgID || usage["usage_scope"] != "account" {
		t.Fatalf("account usage: %v %v", usage, err)
	}
	if orgKeys, err := orgs.Orgs.ListAPIKeys(ctx, conf.RootOrgID, nil); err != nil || len(orgKeys["data"].([]any)) == 0 {
		t.Fatalf("org keys: %v %v", orgKeys, err)
	}
	envKeys, err := orgs.Orgs.ListEnvAPIKeys(ctx, children[0], envs[0], nil)
	if err != nil || len(envKeys["data"].([]any)) != 1 {
		t.Fatalf("env keys: %v %v", envKeys, err)
	}

	// Sibling and parent isolation.
	a, b := clients[0], clients[1]
	pa, err := a.Projects.Create(ctx, map[string]any{"name": "shared-name"})
	if err != nil {
		t.Fatal(err)
	}
	pb, err := b.Projects.Create(ctx, map[string]any{"name": "shared-name"})
	if err != nil {
		t.Fatal(err)
	}
	if pa["project_id"] == pb["project_id"] {
		t.Fatal("same project across siblings")
	}
	for i, c := range clients {
		p, err := c.Projects.Get(ctx, "shared-name")
		if err != nil || p["env_id"] != envs[i] {
			t.Fatalf("slug isolation: %v %v", p, err)
		}
	}
	_, err = b.Projects.Get(ctx, pa["project_id"].(string))
	requireDenied(t, err, 404)
	_, err = suborgClient(t, conf.RootToken).Projects.Get(ctx, pa["project_id"].(string))
	requireDenied(t, err, 404)
	eventBody := map[string]any{"event_type": "sdk:isolation", "event_data": map[string]any{"owner": "a"}}
	event, err := a.Events.Publish(ctx, eventBody)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.Events.Get(ctx, event["event_id"].(string)); err != nil {
		t.Fatal(err)
	}
	_, err = b.Events.Get(ctx, event["event_id"].(string))
	requireDenied(t, err, 404)
	for _, streamErr := range b.Streams.Subscribe(ctx, event["stream_id"].(string), nil) {
		requireDenied(t, streamErr, 403, 404)
		break
	}

	// The org key reaches projects and context in a child environment.
	scoped := orgs.Orgs.Env(children[0], envs[0])
	if _, err := scoped.Projects.Create(ctx, map[string]any{"name": "provisioned"}); err != nil {
		t.Fatal(err)
	}
	projects, err := scoped.Projects.List(ctx, nil)
	if err != nil || len(projects["data"].([]any)) != 2 {
		t.Fatalf("scoped projects: %v %v", projects, err)
	}
	if _, err := scoped.Context.Create(ctx, "provisioned", map[string]any{"key": "sdk.callback", "value": "https://example.test/hot"}); err != nil {
		t.Fatal(err)
	}
	contextVars, err := a.Context.List(ctx, "provisioned")
	if err != nil || len(contextVars["data"].([]any)) != 1 {
		t.Fatalf("context seen by the env key: %v %v", contextVars, err)
	}
	if err := scoped.Context.Delete(ctx, "provisioned", "sdk.callback"); err != nil {
		t.Fatal(err)
	}
	_, err = orgs.Orgs.Env(conf.RootOrgID, conf.RootEnvID).Projects.List(ctx, nil)
	requireDenied(t, err, 403, 404)

	// Restricted keys, service keys and sessions.
	restricted, err := orgs.Orgs.CreateEnvAPIKey(ctx, children[0], envs[0], map[string]any{"description": "event-only", "permissions": map[string]any{"event:sdk:*": []string{"create", "read"}}})
	if err != nil {
		t.Fatal(err)
	}
	limited := suborgClient(t, restricted["token"].(string))
	if _, err = limited.Events.Publish(ctx, eventBody); err != nil {
		t.Fatal(err)
	}
	_, err = limited.Projects.List(ctx, nil)
	requireDenied(t, err, 403)
	permissions := map[string]any{"event:sdk:*": []string{"create", "read"}, "stream:*": []string{"read"}}
	service, err := a.ServiceKeys.Create(ctx, map[string]any{"name": "sdk-service", "permissions": permissions})
	if err != nil {
		t.Fatal(err)
	}
	session, err := a.Sessions.Create(ctx, map[string]any{"permissions": permissions})
	if err != nil {
		t.Fatal(err)
	}
	other, err := b.Events.Publish(ctx, eventBody)
	if err != nil {
		t.Fatal(err)
	}
	for _, credential := range []map[string]any{service, session} {
		client := suborgClient(t, credential["token"].(string))
		if _, err = client.Events.Publish(ctx, eventBody); err != nil {
			t.Fatal(err)
		}
		_, err = client.Events.Get(ctx, other["event_id"].(string))
		requireDenied(t, err, 403, 404)
	}
	if err = a.ServiceKeys.Revoke(ctx, service["service_key_id"].(string)); err != nil {
		t.Fatal(err)
	}
	if err = a.Sessions.Revoke(ctx, session["session_id"].(string)); err != nil {
		t.Fatal(err)
	}
	for _, credential := range []map[string]any{service, session} {
		_, err = suborgClient(t, credential["token"].(string)).Events.Publish(ctx, eventBody)
		requireDenied(t, err, 401)
	}

	// Key lifecycle by ID: read, narrow, rotate, revoke.
	restrictedID := restricted["api_key_id"].(string)
	if got, err := orgs.APIKeys.Get(ctx, restrictedID); err != nil || got["scope"] != "env" || got["token"] != nil {
		t.Fatalf("key metadata: %v %v", got, err)
	}
	if _, err := orgs.APIKeys.Update(ctx, restrictedID, map[string]any{"permissions": map[string]any{"event:*": []string{"read"}}}); err != nil {
		t.Fatal(err)
	}
	_, err = limited.Events.Publish(ctx, eventBody)
	requireDenied(t, err, 403)
	rotated, err := orgs.APIKeys.Rotate(ctx, restrictedID, map[string]any{"overlap_seconds": 0})
	if err != nil || rotated["token"] == nil || rotated["previous_secret_expires_at"] == nil {
		t.Fatalf("rotate: %v %v", rotated, err)
	}
	_, err = limited.Env.Get(ctx)
	requireDenied(t, err, 401)
	renewed := suborgClient(t, rotated["token"].(string))
	if _, err := renewed.Events.Get(ctx, event["event_id"].(string)); err != nil {
		t.Fatal(err)
	}
	if err := orgs.APIKeys.Revoke(ctx, restrictedID); err != nil {
		t.Fatal(err)
	}
	_, err = renewed.Events.Get(ctx, event["event_id"].(string))
	requireDenied(t, err, 401)

	// Lifecycle: a suspended child stops; its sibling does not.
	suspended, err := orgs.Orgs.Update(ctx, children[0], map[string]any{"state": "suspended"})
	if err != nil || suspended["lifecycle_state"] != "suspended" {
		t.Fatalf("suspend: %v %v", suspended, err)
	}
	_, err = a.Env.Get(ctx)
	requireDenied(t, err, 401, 403)
	if _, err = b.Env.Get(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = orgs.Orgs.Update(ctx, children[0], map[string]any{"state": "active"}); err != nil {
		t.Fatal(err)
	}
	if _, err = a.Env.Get(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestSuborgIssuerIntegration(t *testing.T) {
	conf := suborgConfig(t)
	ctx := context.Background()
	event := map[string]any{"event_type": "sdk:isolation", "event_data": map[string]any{}}
	for _, token := range conf.Revoked {
		_, err := suborgClient(t, token).Events.Publish(ctx, event)
		requireDenied(t, err, 401)
	}
	for _, token := range conf.Narrowed {
		client := suborgClient(t, token)
		if _, err := client.Events.Publish(ctx, event); err != nil {
			t.Fatal(err)
		}
		_, err := client.Projects.List(ctx, nil)
		requireDenied(t, err, 403)
		_, err = client.Events.Publish(ctx, map[string]any{"event_type": "other:blocked", "event_data": map[string]any{}})
		requireDenied(t, err, 403)
	}
}
