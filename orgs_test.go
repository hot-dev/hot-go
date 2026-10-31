package hot

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type recordedRequest struct {
	method, uri string
	body        map[string]any
	rawBody     string
}

// recordingServer answers every JSON request with {"data": {"ok": true}} (or
// 204 for DELETE) and records each request.
func recordingServer(t *testing.T) (*Client, func() []recordedRequest) {
	t.Helper()
	var mu sync.Mutex
	var requests []recordedRequest
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		mu.Lock()
		requests = append(requests, recordedRequest{r.Method, r.URL.RequestURI(), body, string(raw)})
		mu.Unlock()
		if r.Method == "DELETE" && !strings.HasSuffix(r.URL.Path, "/calls") {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeJSON(t, w, envelope(map[string]any{"ok": true}))
	}))
	return client, func() []recordedRequest {
		mu.Lock()
		defer mu.Unlock()
		return append([]recordedRequest(nil), requests...)
	}
}

func TestOrgAPIMethodsCallTheirRoutes(t *testing.T) {
	client, recorded := recordingServer(t)
	ctx := context.Background()
	grants := map[string]any{"event:*": []string{"create"}}
	calls := []struct {
		name   string
		call   func() error
		method string
		uri    string
		body   map[string]any
	}{
		{"Orgs.List", func() error { _, err := client.Orgs.List(ctx, url.Values{"limit": {"5"}}); return err }, "GET", "/v1/orgs?limit=5", nil},
		{"Orgs.Create", func() error {
			_, err := client.Orgs.Create(ctx, map[string]any{"name": "Acme", "slug": "acme", "idempotency_key": "k1"})
			return err
		}, "POST", "/v1/orgs", map[string]any{"name": "Acme", "slug": "acme", "idempotency_key": "k1"}},
		{"Orgs.Get", func() error { _, err := client.Orgs.Get(ctx, "o1"); return err }, "GET", "/v1/orgs/o1", nil},
		{"Orgs.Update", func() error {
			_, err := client.Orgs.Update(ctx, "o1", map[string]any{"state": "suspended"})
			return err
		}, "PATCH", "/v1/orgs/o1", map[string]any{"state": "suspended"}},
		{"Orgs.Usage", func() error { _, err := client.Orgs.Usage(ctx, "root"); return err }, "GET", "/v1/orgs/root/usage", nil},
		{"Orgs.PurgeCalls", func() error { _, err := client.Orgs.PurgeCalls(ctx, "o1", true); return err }, "DELETE", "/v1/orgs/o1/calls?include_sub_orgs=true", nil},
		{"Orgs.PurgeCalls own", func() error { _, err := client.Orgs.PurgeCalls(ctx, "o1", false); return err }, "DELETE", "/v1/orgs/o1/calls", nil},
		{"Orgs.ListEnvs", func() error { _, err := client.Orgs.ListEnvs(ctx, "o1"); return err }, "GET", "/v1/orgs/o1/envs", nil},
		{"Orgs.CreateEnv", func() error {
			_, err := client.Orgs.CreateEnv(ctx, "o1", map[string]any{"name": "prod"})
			return err
		}, "POST", "/v1/orgs/o1/envs", map[string]any{"name": "prod"}},
		{"Orgs.GetEnv", func() error { _, err := client.Orgs.GetEnv(ctx, "o1", "e1"); return err }, "GET", "/v1/orgs/o1/envs/e1", nil},
		{"Orgs.ListAPIKeys", func() error { _, err := client.Orgs.ListAPIKeys(ctx, "o1", nil); return err }, "GET", "/v1/orgs/o1/api-keys", nil},
		{"Orgs.CreateAPIKey", func() error {
			_, err := client.Orgs.CreateAPIKey(ctx, "o1", map[string]any{"description": "d", "permissions": map[string]any{"org:*": []string{"read"}}})
			return err
		}, "POST", "/v1/orgs/o1/api-keys", map[string]any{"description": "d", "permissions": map[string]any{"org:*": []any{"read"}}}},
		{"Orgs.ListEnvAPIKeys", func() error { _, err := client.Orgs.ListEnvAPIKeys(ctx, "o1", "e1", nil); return err }, "GET", "/v1/orgs/o1/envs/e1/api-keys", nil},
		{"Orgs.CreateEnvAPIKey", func() error {
			_, err := client.Orgs.CreateEnvAPIKey(ctx, "o1", "e1", map[string]any{"description": "d", "permissions": grants})
			return err
		}, "POST", "/v1/orgs/o1/envs/e1/api-keys", map[string]any{"description": "d", "permissions": map[string]any{"event:*": []any{"create"}}}},
		{"APIKeys.Get", func() error { _, err := client.APIKeys.Get(ctx, "k1"); return err }, "GET", "/v1/api-keys/k1", nil},
		{"APIKeys.Update", func() error {
			_, err := client.APIKeys.Update(ctx, "k1", map[string]any{"permissions": grants})
			return err
		}, "PATCH", "/v1/api-keys/k1", map[string]any{"permissions": map[string]any{"event:*": []any{"create"}}}},
		{"APIKeys.Revoke", func() error { return client.APIKeys.Revoke(ctx, "k1") }, "DELETE", "/v1/api-keys/k1", nil},
		{"APIKeys.Rotate", func() error {
			_, err := client.APIKeys.Rotate(ctx, "k1", map[string]any{"overlap_seconds": 60})
			return err
		}, "POST", "/v1/api-keys/k1/rotate", map[string]any{"overlap_seconds": float64(60)}},
		{"APIKeys.Rotate default", func() error { _, err := client.APIKeys.Rotate(ctx, "k1", nil); return err }, "POST", "/v1/api-keys/k1/rotate", nil},
		{"OrgEnv.Projects.List", func() error { _, err := client.Orgs.Env("o1", "e1").Projects.List(ctx, nil); return err }, "GET", "/v1/orgs/o1/envs/e1/projects", nil},
		{"OrgEnv.Projects.Create", func() error {
			_, err := client.Orgs.Env("o1", "e1").Projects.Create(ctx, map[string]any{"name": "p"})
			return err
		}, "POST", "/v1/orgs/o1/envs/e1/projects", map[string]any{"name": "p"}},
		{"OrgEnv.Builds.Deploy", func() error {
			_, err := client.Orgs.Env("o1", "e1").Builds.Deploy(ctx, "p", "b1")
			return err
		}, "POST", "/v1/orgs/o1/envs/e1/projects/p/builds/b1/deploy", nil},
		{"OrgEnv.Builds.List", func() error { _, err := client.Orgs.Env("o1", "e1").Builds.List(ctx, nil); return err }, "GET", "/v1/orgs/o1/envs/e1/builds", nil},
		{"OrgEnv.Context.Update", func() error {
			_, err := client.Orgs.Env("o1", "e1").Context.Update(ctx, "p", "a.b", map[string]any{"value": "v"})
			return err
		}, "PUT", "/v1/orgs/o1/envs/e1/projects/p/context/a.b", map[string]any{"value": "v"}},
		{"Env.PurgeCalls", func() error { _, err := client.Env.PurgeCalls(ctx); return err }, "DELETE", "/v1/env/calls", nil},
		{"Runs.PurgeCalls", func() error { _, err := client.Runs.PurgeCalls(ctx, "r1"); return err }, "DELETE", "/v1/runs/r1/calls", nil},
		{"Streams.PurgeCalls", func() error { _, err := client.Streams.PurgeCalls(ctx, "s1"); return err }, "DELETE", "/v1/streams/s1/calls", nil},
	}
	for i, c := range calls {
		if err := c.call(); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		got := recorded()[i]
		if got.method != c.method || got.uri != c.uri {
			t.Errorf("%s: %s %s, want %s %s", c.name, got.method, got.uri, c.method, c.uri)
		}
		if c.body == nil {
			if got.rawBody != "" {
				t.Errorf("%s: body = %q, want none", c.name, got.rawBody)
			}
		} else if gotJSON, wantJSON := mustJSON(t, got.body), mustJSON(t, c.body); gotJSON != wantJSON {
			t.Errorf("%s: body = %s, want %s", c.name, gotJSON, wantJSON)
		}
	}
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func TestKeyScopeMismatchIsTyped(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		writeJSON(t, w, map[string]any{"error": map[string]any{"code": "key_scope_mismatch", "message": "wrong key", "request_id": "req"}})
	}))
	ctx := context.Background()
	cases := map[string]struct {
		call  func() error
		scope string
	}{
		"org route":          {func() error { _, err := client.Orgs.List(ctx, nil); return err }, "org"},
		"api-key route":      {func() error { return client.APIKeys.Revoke(ctx, "k1") }, "org"},
		"org env route":      {func() error { _, err := client.Orgs.Env("o", "e").Projects.Get(ctx, "p"); return err }, "org"},
		"runtime route":      {func() error { _, err := client.Env.Get(ctx); return err }, "env"},
		"runtime org usage":  {func() error { _, err := client.Org.Usage(ctx); return err }, "env"},
		"runtime stream":     {func() error { _, err := client.Runs.Wait(ctx, "r1", nil); return err }, "env"},
		"runtime raw upload": {func() error { _, err := client.Files.Upload(ctx, "a.txt", []byte("x"), "text/plain"); return err }, "env"},
		"runtime blob":       {func() error { _, err := client.Blobs.Download(ctx, "b1"); return err }, "env"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			err := c.call()
			var scopeErr *KeyScopeError
			if !errors.As(err, &scopeErr) || scopeErr.RequiredScope != c.scope {
				t.Fatalf("err = %v, want *KeyScopeError needing %q", err, c.scope)
			}
			var apiErr *APIError
			if !errors.As(err, &apiErr) || apiErr.StatusCode != 403 || apiErr.Code != "key_scope_mismatch" || apiErr.RequestID != "req" {
				t.Fatalf("APIError = %+v", apiErr)
			}
			want := "environment API key"
			if c.scope == "org" {
				want = "needs an org API key"
			}
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("message = %q, want it to mention %q", err.Error(), want)
			}
		})
	}
}

func TestOtherForbiddenErrorsStayPlain(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		writeJSON(t, w, map[string]any{"error": map[string]any{"code": "forbidden", "message": "no"}})
	}))
	_, err := client.Orgs.List(context.Background(), nil)
	var scopeErr *KeyScopeError
	var apiErr *APIError
	if errors.As(err, &scopeErr) || !errors.As(err, &apiErr) || apiErr.StatusCode != 403 {
		t.Fatalf("err = %v", err)
	}
}

func TestBodylessDeletesUseTimeoutAndRetry(t *testing.T) {
	ctx := context.Background()
	deletes := map[string]func(c *Client) error{
		"Projects.Delete":    func(c *Client) error { return c.Projects.Delete(ctx, "p") },
		"Context.Delete":     func(c *Client) error { return c.Context.Delete(ctx, "p", "k") },
		"Files.Delete":       func(c *Client) error { return c.Files.Delete(ctx, "f") },
		"Files.AbortUpload":  func(c *Client) error { return c.Files.AbortUpload(ctx, "u") },
		"Domains.Delete":     func(c *Client) error { return c.Domains.Delete(ctx, "d") },
		"ServiceKeys.Revoke": func(c *Client) error { return c.ServiceKeys.Revoke(ctx, "s") },
		"Sessions.Revoke":    func(c *Client) error { return c.Sessions.Revoke(ctx, "s") },
		"APIKeys.Revoke":     func(c *Client) error { return c.APIKeys.Revoke(ctx, "k") },
	}
	for name, del := range deletes {
		t.Run(name+"/timeout", func(t *testing.T) {
			t.Parallel()
			server := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case <-r.Context().Done():
				case <-time.After(2 * time.Second):
				}
			}))
			client, err := NewClient(Config{Token: "t", BaseURL: server.BaseURL(), Timeout: 100 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			err = del(client)
			if err == nil || time.Since(start) > time.Second {
				t.Fatalf("err = %v after %v; Config.Timeout must bound the DELETE", err, time.Since(start))
			}
		})
		t.Run(name+"/429", func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					w.Header().Set("Retry-After", "1")
					w.WriteHeader(http.StatusTooManyRequests)
					return
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			if err := del(client); err != nil || calls.Load() != 2 {
				t.Fatalf("err = %v, calls = %d", err, calls.Load())
			}
		})
	}
}

func TestIdempotentRequestsRetryGatewayErrors(t *testing.T) {
	fastBackoff(t)
	for method, wantCalls := range map[string]int32{"GET": 2, "PUT": 2, "DELETE": 2, "POST": 1, "PATCH": 1} {
		t.Run(method, func(t *testing.T) {
			var calls atomic.Int32
			client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				writeJSON(t, w, envelope(map[string]any{"ok": true}))
			}))
			_, err := client.Request(context.Background(), method, "/thing", nil, nil)
			if calls.Load() != wantCalls || (wantCalls == 2) != (err == nil) {
				t.Fatalf("calls = %d, err = %v", calls.Load(), err)
			}
		})
	}
}

func TestTokenIsTrimmed(t *testing.T) {
	var auth string
	server := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		writeJSON(t, w, envelope(map[string]any{}))
	}))
	client, err := NewClient(Config{Token: "hot_abc\n", BaseURL: server.BaseURL()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Env.Get(context.Background()); err != nil || auth != "Bearer hot_abc" {
		t.Fatalf("auth = %q, err = %v", auth, err)
	}
	if _, err := NewClient(Config{Token: " \n"}); err == nil {
		t.Fatal("blank token accepted")
	}
}
