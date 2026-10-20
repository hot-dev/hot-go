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

type suborgFixture struct {
	RootOrgID       string   `json:"root_org_id"`
	RootToken       string   `json:"root_token"`
	ManagementToken string   `json:"management_token"`
	Revoked         []string `json:"revoked"`
	Narrowed        []string `json:"narrowed"`
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
func suborgRuntime(t *testing.T, token string) *Client {
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
func TestSuborgIntegration(t *testing.T) {
	conf := suborgConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	management, err := NewManagementClient(Config{Token: conf.ManagementToken, BaseURL: os.Getenv("HOT_TEST_BASE_URL"), Timeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var children, envs []string
	var clients []*Client
	defer func() {
		for _, child := range children {
			if err := management.SetChildState(ctx, child, "archived"); err != nil {
				t.Error(err)
			}
		}
	}()
	for i := 0; i < 2; i++ {
		identity := fmt.Sprintf("sdk-go-%d-%d", time.Now().UnixNano(), i)
		body := map[string]any{"name": "SDK Customer", "slug": identity, "external_id": identity, "idempotency_key": identity}
		child, err := management.CreateChild(ctx, body)
		if err != nil {
			t.Fatal(err)
		}
		childID := child["org"].(map[string]any)["org_id"].(string)
		children = append(children, childID)
		replay, err := management.CreateChild(ctx, body)
		if err != nil || replay["created"] != false || replay["org"].(map[string]any)["org_id"] != childID {
			t.Fatalf("idempotency: %v %v", replay, err)
		}
		env, err := management.CreateEnvironment(ctx, childID, map[string]any{"name": "production"})
		if err != nil {
			t.Fatal(err)
		}
		again, err := management.CreateEnvironment(ctx, childID, map[string]any{"name": "production"})
		if err != nil || again["env_id"] != env["env_id"] {
			t.Fatalf("environment idempotency: %v", err)
		}
		envID := env["env_id"].(string)
		envs = append(envs, envID)
		key, err := management.CreateAPIKey(ctx, childID, envID, map[string]any{"description": "sdk", "permissions": map[string]any{"*:*": []string{"*"}}})
		if err != nil {
			t.Fatal(err)
		}
		client := suborgRuntime(t, key["token"].(string))
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
	_, err = suborgRuntime(t, conf.RootToken).Projects.Get(ctx, pa["project_id"].(string))
	requireDenied(t, err, 404)
	eventBody := map[string]any{"event_type": "sdk:isolation", "event_data": map[string]any{"owner": "a"}}
	event, err := a.Events.Publish(ctx, eventBody)
	if err != nil {
		t.Fatal(err)
	}
	_, err = a.Events.Get(ctx, event["event_id"].(string))
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.Events.Get(ctx, event["event_id"].(string))
	requireDenied(t, err, 404)
	for _, streamErr := range b.Streams.Subscribe(ctx, event["stream_id"].(string), nil) {
		requireDenied(t, streamErr, 403, 404)
		break
	}
	badManager, err := NewManagementClient(Config{Token: conf.RootToken, BaseURL: os.Getenv("HOT_TEST_BASE_URL")})
	if err != nil {
		t.Fatal(err)
	}
	_, err = badManager.ListChildren(ctx, nil)
	requireDenied(t, err, 401)
	_, err = suborgRuntime(t, conf.ManagementToken).Env.Get(ctx)
	requireDenied(t, err, 401)
	listed, err := management.ListChildren(ctx, nil)
	if err != nil || listed["total"].(float64) < 2 {
		t.Fatalf("children: %v %v", listed, err)
	}
	usage, err := management.Usage(ctx)
	if err != nil || usage["root_org_id"] != conf.RootOrgID {
		t.Fatalf("account usage: %v %v", usage, err)
	}
	restricted, err := management.CreateAPIKey(ctx, children[0], envs[0], map[string]any{"description": "event-only", "permissions": map[string]any{"event:sdk:*": []string{"create", "read"}}})
	if err != nil {
		t.Fatal(err)
	}
	limited := suborgRuntime(t, restricted["token"].(string))
	_, err = limited.Events.Publish(ctx, eventBody)
	if err != nil {
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
		client := suborgRuntime(t, credential["token"].(string))
		_, err = client.Events.Publish(ctx, eventBody)
		if err != nil {
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
		_, err = suborgRuntime(t, credential["token"].(string)).Events.Publish(ctx, eventBody)
		requireDenied(t, err, 401)
	}
	if err = management.SetChildState(ctx, children[0], "suspended"); err != nil {
		t.Fatal(err)
	}
	_, err = a.Env.Get(ctx)
	requireDenied(t, err, 401, 403)
	if _, err = b.Env.Get(ctx); err != nil {
		t.Fatal(err)
	}
	if err = management.SetChildState(ctx, children[0], "active"); err != nil {
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
		_, err := suborgRuntime(t, token).Events.Publish(ctx, event)
		requireDenied(t, err, 401)
	}
	for _, token := range conf.Narrowed {
		client := suborgRuntime(t, token)
		if _, err := client.Events.Publish(ctx, event); err != nil {
			t.Fatal(err)
		}
		_, err := client.Projects.List(ctx, nil)
		requireDenied(t, err, 403)
		_, err = client.Events.Publish(ctx, map[string]any{"event_type": "other:blocked", "event_data": map[string]any{}})
		requireDenied(t, err, 403)
	}
}
