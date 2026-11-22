package hot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func newRoundTripClient(t *testing.T, baseURL string, fn roundTripFunc) *Client {
	t.Helper()
	if baseURL == "" {
		baseURL = "https://api.hot.test"
	}
	client, err := NewClient(Config{
		Token:      "secret-token",
		BaseURL:    baseURL,
		HTTPClient: &http.Client{Transport: fn},
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func jsonResponse(status int, body any, headers http.Header) *http.Response {
	if headers == nil {
		headers = http.Header{}
	}
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		if err != nil {
			panic(err)
		}
		if headers.Get("Content-Type") == "" {
			headers.Set("Content-Type", "application/json")
		}
	}
	return &http.Response{
		StatusCode: status,
		Header:     headers,
		Body:       io.NopCloser(bytes.NewReader(raw)),
	}
}

func textResponse(status int, body string, headers http.Header) *http.Response {
	if headers == nil {
		headers = http.Header{}
	}
	return &http.Response{
		StatusCode: status,
		Header:     headers,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func sseResponse(events ...string) *http.Response {
	var buf bytes.Buffer
	for _, ev := range events {
		fmt.Fprintf(&buf, "data: %s\n\n", ev)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"text/event-stream"}},
		Body:       io.NopCloser(&buf),
	}
}

func TestUnitPathSegmentValidationRejectsBeforeHTTP(t *testing.T) {
	var hits atomic.Int32
	client := newRoundTripClient(t, "", func(r *http.Request) (*http.Response, error) {
		hits.Add(1)
		return jsonResponse(http.StatusOK, envelope(map[string]any{}), nil), nil
	})
	ctx := context.Background()

	cases := map[string]func() error{
		"Sessions.Revoke dot": func() error {
			return client.Sessions.Revoke(ctx, ".")
		},
		"ServiceKeys.Revoke empty": func() error {
			return client.ServiceKeys.Revoke(ctx, "")
		},
		"Context.Delete dotdot": func() error {
			return client.Context.Delete(ctx, "p1", "..")
		},
		"Files.Upload dotdot": func() error {
			_, err := client.Files.Upload(ctx, "../x", []byte("hi"), "text/plain")
			return err
		},
		"Files.Upload empty": func() error {
			_, err := client.Files.Upload(ctx, "", []byte("hi"), "text/plain")
			return err
		},
		"Files.Upload slash only": func() error {
			_, err := client.Files.Upload(ctx, "///", []byte("hi"), "text/plain")
			return err
		},
		"Files.Upload empty middle segment": func() error {
			_, err := client.Files.Upload(ctx, "a//b", []byte("hi"), "text/plain")
			return err
		},
		"Files.Upload dot segment": func() error {
			_, err := client.Files.Upload(ctx, "a/./b", []byte("hi"), "text/plain")
			return err
		},
		"Files.Upload trailing slash": func() error {
			_, err := client.Files.Upload(ctx, "a/b/", []byte("hi"), "text/plain")
			return err
		},
		"Orgs.Env invalid org": func() error {
			_, err := client.Orgs.Env("..", "e1").Projects.List(ctx, nil)
			return err
		},
		"Orgs.Env invalid env": func() error {
			_, err := client.Orgs.Env("o1", "").Builds.List(ctx, nil)
			return err
		},
		"Runs.Subscribe dotdot": func() error {
			for _, err := range client.Runs.Subscribe(ctx, "..") {
				return err
			}
			return nil
		},
		"Runs.Wait dot": func() error {
			_, err := client.Runs.Wait(ctx, ".", nil)
			return err
		},
		"Tasks.Subscribe empty": func() error {
			for _, err := range client.Tasks.Subscribe(ctx, "") {
				return err
			}
			return nil
		},
		"Tasks.Wait dotdot": func() error {
			_, err := client.Tasks.Wait(ctx, "..", nil)
			return err
		},
		"Streams.Subscribe dot": func() error {
			for _, err := range client.Streams.Subscribe(ctx, ".", nil) {
				return err
			}
			return nil
		},
		"Streams.SubscribePost dotdot": func() error {
			for _, err := range client.Streams.SubscribePost(ctx, "..", nil) {
				return err
			}
			return nil
		},
		"Streams.WaitForRunResult dotdot": func() error {
			_, err := client.Streams.WaitForRunResult(ctx, "..", "e1", nil)
			return err
		},
		"Blobs.Download empty": func() error {
			_, err := client.Blobs.Download(ctx, "")
			return err
		},
		"Builds.Upload dotdot": func() error {
			_, err := client.Builds.Upload(ctx, "..", BuildUpload{File: strings.NewReader("zip"), Hash: "abc"})
			return err
		},
		"Domains.Delete dot": func() error {
			return client.Domains.Delete(ctx, ".")
		},
		"Projects.Delete dotdot": func() error {
			return client.Projects.Delete(ctx, "..")
		},
		"APIKeys.Revoke empty": func() error {
			return client.APIKeys.Revoke(ctx, "")
		},
		"Events.Get dot": func() error {
			_, err := client.Events.Get(ctx, ".")
			return err
		},
	}

	for name, fn := range cases {
		t.Run(name, func(t *testing.T) {
			err := fn()
			if err == nil {
				t.Fatal("expected error for invalid path segment, got nil")
			}
		})
	}
	if got := hits.Load(); got != 0 {
		t.Fatalf("HTTP hits = %d, want 0 (invalid segments must fail before HTTP)", got)
	}
}

func TestUnitFilesUploadStripsLeadingSlashAndEscapesSegments(t *testing.T) {
	var seenURI string
	client := newRoundTripClient(t, "", func(r *http.Request) (*http.Response, error) {
		seenURI = r.URL.RequestURI()
		return jsonResponse(http.StatusOK, envelope(map[string]any{"file_id": "f1"}), nil), nil
	})
	if _, err := client.Files.Upload(context.Background(), "/dir/my file.txt", []byte("data"), "text/plain"); err != nil {
		t.Fatal(err)
	}
	if seenURI != "/v1/files/upload/dir/my%20file.txt" {
		t.Fatalf("URI = %q, want /v1/files/upload/dir/my%%20file.txt", seenURI)
	}
}

func TestUnitNon2xxReturnsAPIError(t *testing.T) {
	// Return 302 without Location (or with ErrUseLastResponse) so http.Client returns 3xx directly.
	client, err := NewClient(Config{
		Token:   "secret-token",
		BaseURL: "https://api.hot.test",
		HTTPClient: &http.Client{
			Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				return textResponse(http.StatusFound, "moved", http.Header{"Location": {"https://api.hot.test/other"}}), nil
			}),
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	t.Run("requestJSON", func(t *testing.T) {
		_, err := client.Projects.List(ctx, nil)
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusFound {
			t.Fatalf("err = %v, want 302 *APIError", err)
		}
	})
	t.Run("requestRaw", func(t *testing.T) {
		resp, err := client.RequestRaw(ctx, "GET", "/files/f1/download", nil, nil, nil)
		var apiErr *APIError
		if resp != nil || !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusFound {
			t.Fatalf("resp = %v, err = %v, want 302 *APIError", resp, err)
		}
	})
	t.Run("stream", func(t *testing.T) {
		var gotErr error
		for _, err := range client.Streams.Subscribe(ctx, "s1", nil) {
			gotErr = err
		}
		var apiErr *APIError
		if !errors.As(gotErr, &apiErr) || apiErr.StatusCode != http.StatusFound {
			t.Fatalf("err = %v, want 302 *APIError", gotErr)
		}
	})
	t.Run("status", func(t *testing.T) {
		_, err := client.Status(ctx)
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusFound {
			t.Fatalf("err = %v, want 302 *APIError", err)
		}
	})
}

func TestUnitCheckRedirectSafety(t *testing.T) {
	ctx := context.Background()

	t.Run("blocks HTTPS to HTTP downgrade", func(t *testing.T) {
		client := newRoundTripClient(t, "https://api.hot.test", func(r *http.Request) (*http.Response, error) {
			return textResponse(http.StatusFound, "", http.Header{"Location": {"http://api.hot.test/v1/env"}}), nil
		})
		_, err := client.Env.Get(ctx)
		if err == nil || !strings.Contains(err.Error(), "downgrade") {
			t.Fatalf("err = %v, want downgrade error", err)
		}
	})

	t.Run("strips Authorization on cross-origin host", func(t *testing.T) {
		var auths []string
		client := newRoundTripClient(t, "https://api.hot.test", func(r *http.Request) (*http.Response, error) {
			auths = append(auths, r.Header.Get("Authorization"))
			if r.URL.Host == "api.hot.test" {
				return textResponse(http.StatusFound, "", http.Header{"Location": {"https://other.example/v1/env"}}), nil
			}
			return jsonResponse(http.StatusOK, envelope(map[string]any{"ok": true}), nil), nil
		})
		if _, err := client.Env.Get(ctx); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(auths, []string{"Bearer secret-token", ""}) {
			t.Fatalf("auths = %#v, want [Bearer secret-token, \"\"]", auths)
		}
	})

	t.Run("strips Authorization on subdomain redirect", func(t *testing.T) {
		var auths []string
		client := newRoundTripClient(t, "https://api.hot.test", func(r *http.Request) (*http.Response, error) {
			auths = append(auths, r.Header.Get("Authorization"))
			if r.URL.Host == "api.hot.test" {
				return textResponse(http.StatusFound, "", http.Header{"Location": {"https://sub.api.hot.test/v1/env"}}), nil
			}
			return jsonResponse(http.StatusOK, envelope(map[string]any{"ok": true}), nil), nil
		})
		if _, err := client.Env.Get(ctx); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(auths, []string{"Bearer secret-token", ""}) {
			t.Fatalf("auths = %#v, want Authorization stripped on subdomain", auths)
		}
	})

	t.Run("strips Authorization on port change", func(t *testing.T) {
		var auths []string
		client := newRoundTripClient(t, "https://api.hot.test", func(r *http.Request) (*http.Response, error) {
			auths = append(auths, r.Header.Get("Authorization"))
			if r.URL.Port() == "" {
				return textResponse(http.StatusFound, "", http.Header{"Location": {"https://api.hot.test:8443/v1/env"}}), nil
			}
			return jsonResponse(http.StatusOK, envelope(map[string]any{"ok": true}), nil), nil
		})
		if _, err := client.Env.Get(ctx); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(auths, []string{"Bearer secret-token", ""}) {
			t.Fatalf("auths = %#v, want Authorization stripped on port change", auths)
		}
	})

	t.Run("preserves Authorization on same origin including explicit default port", func(t *testing.T) {
		var auths []string
		client := newRoundTripClient(t, "https://api.hot.test", func(r *http.Request) (*http.Response, error) {
			auths = append(auths, r.Header.Get("Authorization"))
			if r.URL.Path == "/v1/env" {
				return textResponse(http.StatusFound, "", http.Header{"Location": {"https://api.hot.test:443/v1/env2"}}), nil
			}
			return jsonResponse(http.StatusOK, envelope(map[string]any{"ok": true}), nil), nil
		})
		if _, err := client.Env.Get(ctx); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(auths, []string{"Bearer secret-token", "Bearer secret-token"}) {
			t.Fatalf("auths = %#v, want Authorization preserved on same origin", auths)
		}
	})

	t.Run("strips Authorization on bounce-back after cross-origin hop", func(t *testing.T) {
		var auths []string
		client := newRoundTripClient(t, "https://api.hot.test", func(r *http.Request) (*http.Response, error) {
			auths = append(auths, r.Header.Get("Authorization"))
			switch r.URL.String() {
			case "https://api.hot.test/v1/env":
				return textResponse(http.StatusFound, "", http.Header{"Location": {"https://evil.example/bounce"}}), nil
			case "https://evil.example/bounce":
				return textResponse(http.StatusFound, "", http.Header{"Location": {"https://api.hot.test/v1/final"}}), nil
			default:
				return jsonResponse(http.StatusOK, envelope(map[string]any{"ok": true}), nil), nil
			}
		})
		if _, err := client.Env.Get(ctx); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(auths, []string{"Bearer secret-token", "", ""}) {
			t.Fatalf("auths = %#v, want Authorization stripped on bounce-back", auths)
		}
	})

	t.Run("shallow copies caller HTTPClient without mutating it", func(t *testing.T) {
		caller := &http.Client{
			Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				return textResponse(http.StatusFound, "", http.Header{"Location": {"http://api.hot.test/v1/env"}}), nil
			}),
		}
		client, err := NewClient(Config{Token: "tok", BaseURL: "https://api.hot.test", HTTPClient: caller})
		if err != nil {
			t.Fatal(err)
		}
		if caller.CheckRedirect != nil {
			t.Fatal("caller HTTPClient was mutated")
		}
		if client.transport.httpClient.CheckRedirect == nil {
			t.Fatal("client transport HTTPClient should have default CheckRedirect")
		}
	})
}

func TestUnitClientStatus(t *testing.T) {
	var seenMethod, seenURL, seenAuth, seenUA, seenAccept string
	client := newRoundTripClient(t, "https://api.hot.test/v1/", func(r *http.Request) (*http.Response, error) {
		seenMethod = r.Method
		seenURL = r.URL.String()
		seenAuth = r.Header.Get("Authorization")
		seenUA = r.Header.Get("User-Agent")
		seenAccept = r.Header.Get("Accept")
		return jsonResponse(http.StatusOK, map[string]any{"status": "ok", "version": "3.0.14"}, nil), nil
	})

	status, err := client.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status["status"] != "ok" || status["version"] != "3.0.14" {
		t.Fatalf("status = %#v", status)
	}
	if seenMethod != "GET" || seenURL != "https://api.hot.test/status" {
		t.Fatalf("request = %s %s, want GET https://api.hot.test/status", seenMethod, seenURL)
	}
	if seenAuth != "" {
		t.Fatalf("Authorization = %q, want empty on /status", seenAuth)
	}
	if seenUA != "hot-sdk-go/"+Version || seenAccept != "application/json" {
		t.Fatalf("headers UA=%q Accept=%q", seenUA, seenAccept)
	}
}

func TestUnitEventIDPublishRetryAndConflict(t *testing.T) {
	fastBackoff(t)
	ctx := context.Background()

	t.Run("publishes with caller event_id", func(t *testing.T) {
		var gotBody map[string]any
		client := newRoundTripClient(t, "", func(r *http.Request) (*http.Response, error) {
			_ = json.NewDecoder(r.Body).Decode(&gotBody)
			return jsonResponse(http.StatusOK, envelope(map[string]any{"event_id": "evt-custom", "stream_id": "s1"}), nil), nil
		})
		out, err := client.Events.Publish(ctx, map[string]any{
			"event_id":   "evt-custom",
			"event_type": "order:created",
			"event_data": map[string]any{"id": 1},
		})
		if err != nil {
			t.Fatal(err)
		}
		if out["event_id"] != "evt-custom" || gotBody["event_id"] != "evt-custom" {
			t.Fatalf("out = %v, body = %v", out, gotBody)
		}
	})

	t.Run("503 event_publish_in_progress auto-retries POST", func(t *testing.T) {
		var calls atomic.Int32
		client := newRoundTripClient(t, "", func(r *http.Request) (*http.Response, error) {
			if calls.Add(1) == 1 {
				return jsonResponse(http.StatusServiceUnavailable, map[string]any{
					"error": map[string]any{
						"code":    "event_publish_in_progress",
						"message": "in flight",
					},
				}, nil), nil
			}
			return jsonResponse(http.StatusOK, envelope(map[string]any{"event_id": "evt-1"}), nil), nil
		})
		out, err := client.Events.Publish(ctx, map[string]any{"event_id": "evt-1", "event_type": "x"})
		if err != nil || out["event_id"] != "evt-1" || calls.Load() != 2 {
			t.Fatalf("out = %v, err = %v, calls = %d", out, err, calls.Load())
		}
	})

	t.Run("503 event_publish_in_progress caps RetryAfter at 30s", func(t *testing.T) {
		err := &APIError{StatusCode: http.StatusServiceUnavailable, Code: "event_publish_in_progress", RetryAfter: 120}
		delay, ok := jsonRetryDelay(http.MethodPost, http.StatusServiceUnavailable, err, 0)
		if !ok || delay != 30*time.Second {
			t.Fatalf("delay = %v, ok = %v, want 30s, true", delay, ok)
		}
	})

	t.Run("409 event_id_conflict does not retry", func(t *testing.T) {
		var calls atomic.Int32
		client := newRoundTripClient(t, "", func(r *http.Request) (*http.Response, error) {
			calls.Add(1)
			return jsonResponse(http.StatusConflict, map[string]any{
				"error": map[string]any{
					"code":    "event_id_conflict",
					"message": "different payload for event_id",
				},
			}, nil), nil
		})
		_, err := client.Events.Publish(ctx, map[string]any{"event_id": "evt-1", "event_type": "y"})
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.StatusCode != 409 || apiErr.Code != "event_id_conflict" || calls.Load() != 1 {
			t.Fatalf("err = %v, calls = %d", err, calls.Load())
		}
	})
}

func TestUnitOrgsListEnvsWithQuery(t *testing.T) {
	var uris []string
	client := newRoundTripClient(t, "", func(r *http.Request) (*http.Response, error) {
		uris = append(uris, r.URL.RequestURI())
		return jsonResponse(http.StatusOK, listEnvelope([]any{map[string]any{"env_id": "e1"}}), nil), nil
	})
	ctx := context.Background()
	if _, err := client.Orgs.ListEnvs(ctx, "o1"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Orgs.ListEnvsWithQuery(ctx, "o1", url.Values{"limit": {"10"}, "offset": {"20"}}); err != nil {
		t.Fatal(err)
	}
	want := []string{"/v1/orgs/o1/envs", "/v1/orgs/o1/envs?limit=10&offset=20"}
	if !reflect.DeepEqual(uris, want) {
		t.Fatalf("uris = %v, want %v", uris, want)
	}
}

func TestUnitRunFailRetryRunIDAndSeenTerminal(t *testing.T) {
	t.Run("retry_run_id correlates retry and flushes buffered chunks immediately", func(t *testing.T) {
		var chunks []string
		tracker := newRunTracker("e1", func(text string) { chunks = append(chunks, text) })

		// Chunk for r2 arrives before r1 fails with retry_run_id: r2.
		tracker.handle(StreamEvent{"type": "stream:data", "run_id": "r2", "payload": map[string]any{"text": "early-"}})
		if len(chunks) != 0 {
			t.Fatalf("unexpected early flush: %v", chunks)
		}

		// r1 fails with top-level retry_run_id = "r2" (even without origin_run_id on r2 later).
		if final, ok := tracker.handle(StreamEvent{
			"type":         "run:fail",
			"retry_run_id": "r2",
			"run": map[string]any{
				"run_id":        "r1",
				"event_id":      "e1",
				"run_type":      "call",
				"status":        "pending_retry",
				"retry_attempt": float64(1),
			},
		}); ok || final != nil {
			t.Fatalf("run:fail with retry_run_id must not settle: %v", final)
		}
		// Buffered chunk for r2 should already have flushed!
		if strings.Join(chunks, "") != "early-" {
			t.Fatalf("chunks after retry_run_id = %v, want [early-]", chunks)
		}

		// Live chunk for r2 delivers immediately even before r2 run:start.
		tracker.handle(StreamEvent{"type": "stream:data", "run_id": "r2", "payload": map[string]any{"text": "live"}})
		if strings.Join(chunks, "") != "early-live" {
			t.Fatalf("chunks = %v, want early-live", chunks)
		}

		// r2 fails on its last retry WITHOUT origin_run_id: must settle immediately (not held!).
		final, ok := tracker.handle(StreamEvent{
			"type": "run:fail",
			"run": map[string]any{
				"run_id":        "r2",
				"event_id":      "e2",
				"status":        "failed",
				"retry_attempt": float64(1),
				"result":        map[string]any{"$err": "final boom"},
			},
		})
		if !ok || final == nil || final.RunID() != "r2" || tracker.held != nil {
			t.Fatalf("final = %v, ok = %v, held = %v", final, ok, tracker.held)
		}
	})

	t.Run("SubscribeWithEvent does not drop final failure after pending_retry for same run_id", func(t *testing.T) {
		var calls atomic.Int32
		client := newRoundTripClient(t, "", func(r *http.Request) (*http.Response, error) {
			switch calls.Add(1) {
			case 1:
				return sseResponse(
					publishedEvent,
					`{"type":"run:fail","run":{"run_id":"r1","event_id":"e1","status":"pending_retry","retry_attempt":1}}`,
					`{"type":"stream:complete","stream_id":"s1"}`,
				), nil
			default:
				return sseResponse(
					`{"type":"run:fail","run":{"run_id":"r1","event_id":"e1","status":"failed","retry_attempt":0,"result":{"$err":"really failed"}}}`,
					`{"type":"run:fail","run":{"run_id":"r1","event_id":"e1","status":"failed","retry_attempt":0,"result":{"$err":"really failed"}}}`,
				), nil
			}
		})

		events, err := collectEvents(t, client, nil)
		if err != nil {
			t.Fatal(err)
		}
		var statuses []string
		for _, ev := range events {
			if ev.Type() == "run:fail" {
				statuses = append(statuses, ev.RunStatus())
			}
		}
		if !reflect.DeepEqual(statuses, []string{"pending_retry", "failed"}) {
			t.Fatalf("run:fail statuses = %v, want [pending_retry failed]", statuses)
		}
	})
}

func TestUnitTypedNilBodyOmitted(t *testing.T) {
	type payload struct{ X int }
	var seenBodies []string
	var seenContentTypes []string

	client := newRoundTripClient(t, "", func(r *http.Request) (*http.Response, error) {
		var raw []byte
		if r.Body != nil {
			raw, _ = io.ReadAll(r.Body)
		}
		seenBodies = append(seenBodies, string(raw))
		seenContentTypes = append(seenContentTypes, r.Header.Get("Content-Type"))
		if strings.Contains(r.URL.Path, "subscribe-with-event") {
			return sseResponse(`{"type":"stream:complete","stream_id":"s1"}`), nil
		}
		return jsonResponse(http.StatusOK, envelope(map[string]any{}), nil), nil
	})
	ctx := context.Background()

	var nilMap map[string]any
	var nilSlice []any
	var nilPtr *payload

	if _, err := client.Request(ctx, "POST", "/a", nilMap, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Request(ctx, "POST", "/b", nilSlice, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Request(ctx, "POST", "/c", nilPtr, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Projects.Update(ctx, "p1", nil); err != nil {
		t.Fatal(err)
	}
	for _, err := range client.Streams.SubscribeWithEvent(ctx, nil, &SubscribeWithEventOptions{DisableReconnect: true}) {
		if err != nil {
			t.Fatal(err)
		}
	}
	// Non-nil empty map MUST still serialize as "{}".
	if _, err := client.Request(ctx, "POST", "/d", map[string]any{}, nil); err != nil {
		t.Fatal(err)
	}

	wantBodies := []string{"", "", "", "", "", "{}"}
	wantCTs := []string{"", "", "", "", "", "application/json"}
	if !reflect.DeepEqual(seenBodies, wantBodies) {
		t.Fatalf("bodies = %#v, want %#v", seenBodies, wantBodies)
	}
	if !reflect.DeepEqual(seenContentTypes, wantCTs) {
		t.Fatalf("Content-Types = %#v, want %#v", seenContentTypes, wantCTs)
	}
}

func TestUnitBuildsUploadValidationAndNoPipeLeak(t *testing.T) {
	ctx := context.Background()

	t.Run("requires File and Hash before touching HTTP", func(t *testing.T) {
		var hits atomic.Int32
		client := newRoundTripClient(t, "", func(r *http.Request) (*http.Response, error) {
			hits.Add(1)
			return jsonResponse(http.StatusOK, envelope(map[string]any{}), nil), nil
		})
		if _, err := client.Builds.Upload(ctx, "p1", BuildUpload{Hash: "abc"}); err == nil {
			t.Fatal("expected error for nil File")
		}
		if _, err := client.Builds.Upload(ctx, "p1", BuildUpload{File: strings.NewReader("zip"), Hash: ""}); err == nil {
			t.Fatal("expected error for empty Hash")
		}
		if _, err := client.Builds.Upload(ctx, "p1", BuildUpload{File: strings.NewReader("zip"), Hash: "   "}); err == nil {
			t.Fatal("expected error for whitespace Hash")
		}
		if hits.Load() != 0 {
			t.Fatalf("hits = %d, want 0", hits.Load())
		}
	})

	t.Run("encodes multipart upload fields and file", func(t *testing.T) {
		var gotHash, gotBuildID, gotFilename, gotContent string
		client := newRoundTripClient(t, "", func(r *http.Request) (*http.Response, error) {
			if r.Method != "POST" || r.URL.Path != "/v1/projects/p1/builds" {
				t.Errorf("request = %s %s", r.Method, r.URL.Path)
			}
			mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
			if err != nil || mediaType != "multipart/form-data" {
				t.Fatalf("Content-Type = %q (%v)", r.Header.Get("Content-Type"), err)
			}
			mr := multipart.NewReader(r.Body, params["boundary"])
			for {
				part, err := mr.NextPart()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				data, _ := io.ReadAll(part)
				switch part.FormName() {
				case "hash":
					gotHash = string(data)
				case "build_id":
					gotBuildID = string(data)
				case "file":
					gotFilename = part.FileName()
					gotContent = string(data)
				}
			}
			return jsonResponse(http.StatusOK, envelope(map[string]any{"build_id": "b1"}), nil), nil
		})
		out, err := client.Builds.Upload(ctx, "p1", BuildUpload{
			File:     strings.NewReader("archive-bytes"),
			Filename: "app.tar.gz",
			Hash:     "sha256:1234",
			BuildID:  "b1",
		})
		if err != nil {
			t.Fatal(err)
		}
		if out["build_id"] != "b1" || gotHash != "sha256:1234" || gotBuildID != "b1" || gotFilename != "app.tar.gz" || gotContent != "archive-bytes" {
			t.Fatalf("out=%v hash=%q buildID=%q filename=%q content=%q", out, gotHash, gotBuildID, gotFilename, gotContent)
		}
	})

	t.Run("does not leak pipe writer goroutine on early transport error", func(t *testing.T) {
		client := newRoundTripClient(t, "", func(r *http.Request) (*http.Response, error) {
			// Return an error immediately without reading r.Body.
			return nil, errors.New("connection refused")
		})
		runtime.GC()
		baseline := runtime.NumGoroutine()
		const n = 20
		for range n {
			if _, err := client.Builds.Upload(ctx, "p1", BuildUpload{
				File: strings.NewReader("archive-bytes"),
				Hash: "sha256:abc",
			}); err == nil {
				t.Fatal("expected transport error")
			}
		}
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			runtime.Gosched()
			if runtime.NumGoroutine() <= baseline+4 {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("goroutines leaked: baseline=%d, now=%d (after %d failed uploads)", baseline, runtime.NumGoroutine(), n)
	})
}

func TestUnitWaitForPublishedRunPreservesCancelAndPermanent4xx(t *testing.T) {
	shortGrace(t, 5*time.Second)

	t.Run("preserves context.Canceled while failure is held", func(t *testing.T) {
		pr, pw := io.Pipe()
		ctx, cancel := context.WithCancel(context.Background())
		client := newRoundTripClient(t, "", func(r *http.Request) (*http.Response, error) {
			go func() {
				fmt.Fprintf(pw, "data: %s\n\n", replayFailRetried)
				<-r.Context().Done()
				pw.CloseWithError(r.Context().Err())
			}()
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": {"text/event-stream"}},
				Body:       pr,
			}, nil
		})
		time.AfterFunc(40*time.Millisecond, cancel)
		_, err := client.Streams.WaitForRunResult(ctx, "s1", "e1", &WaitForRunResultOptions{Timeout: 5 * time.Second})
		var runErr *RunWaitError
		if errors.As(err, &runErr) || !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled (not *RunWaitError)", err)
		}
	})

	t.Run("preserves permanent 4xx on reconnect while failure is held", func(t *testing.T) {
		fastBackoff(t)
		var calls atomic.Int32
		client := newRoundTripClient(t, "", func(r *http.Request) (*http.Response, error) {
			if calls.Add(1) == 1 {
				return sseResponse(replayFailRetried, `{"type":"stream:complete","stream_id":"s1"}`), nil
			}
			return jsonResponse(http.StatusForbidden, map[string]any{
				"error": map[string]any{"code": "forbidden", "message": "revoked"},
			}, nil), nil
		})
		_, err := client.Streams.WaitForRunResult(context.Background(), "s1", "e1", &WaitForRunResultOptions{Timeout: 5 * time.Second})
		var apiErr *APIError
		var runErr *RunWaitError
		if errors.As(err, &runErr) || !errors.As(err, &apiErr) || apiErr.StatusCode != 403 {
			t.Fatalf("err = %v, want 403 *APIError (not *RunWaitError)", err)
		}
	})

	t.Run("still returns held *RunWaitError on timeout deadline", func(t *testing.T) {
		pr, pw := io.Pipe()
		client := newRoundTripClient(t, "", func(r *http.Request) (*http.Response, error) {
			go func() {
				fmt.Fprintf(pw, "data: %s\n\n", replayFailRetried)
				<-r.Context().Done()
				pw.CloseWithError(r.Context().Err())
			}()
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": {"text/event-stream"}},
				Body:       pr,
			}, nil
		})
		_, err := client.Streams.WaitForRunResult(context.Background(), "s1", "e1", &WaitForRunResultOptions{Timeout: 80 * time.Millisecond})
		var runErr *RunWaitError
		if !errors.As(err, &runErr) || runErr.Run["run_id"] != "r1" {
			t.Fatalf("err = %v, want held *RunWaitError on deadline", err)
		}
	})
}

func TestUnitGraceWatchResetsOnTargetChange(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	tracker := newRunTracker("e1", nil)
	watch := &graceWatch{cancel: cancel}
	defer watch.stop()

	start := time.Now()
	tracker.held = &heldFailure{runID: "r1", until: start.Add(40 * time.Millisecond)}
	watch.sync(tracker)

	// Switch held target to r2 with a later deadline before r1's timer fires.
	tracker.held = &heldFailure{runID: "r2", until: start.Add(180 * time.Millisecond)}
	watch.sync(tracker)

	select {
	case <-ctx.Done():
		t.Fatalf("old timer fired early at %v", time.Since(start))
	case <-time.After(90 * time.Millisecond):
	}

	select {
	case <-ctx.Done():
		if !errors.Is(context.Cause(ctx), errRetryGrace) {
			t.Fatalf("cause = %v, want %v", context.Cause(ctx), errRetryGrace)
		}
	case <-time.After(time.Second):
		t.Fatal("new timer did not fire")
	}
}

func TestUnitReconnectorCapsRetryAfterAndPreventsEOFSpinLoop(t *testing.T) {
	t.Run("caps waiter RetryAfter at 30s", func(t *testing.T) {
		r := newReconnector("run", 5)
		// With a 35s deadline and RetryAfter=3600:
		// - Without the 30s cap, time.Until(deadline) < 3600s immediately returns DeadlineExceeded.
		// - With the 30s cap, delay is 30s <= 35s, so it waits until our 25ms cancel fires!
		ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
		defer cancel()
		time.AfterFunc(25*time.Millisecond, cancel)

		err := r.next(ctx, connection{
			err: &APIError{StatusCode: http.StatusTooManyRequests, RetryAfter: 3600},
		}, "ended")
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled (RetryAfter should be capped at 30s)", err)
		}
	})

	t.Run("bounds and backs off repeated premature clean EOFs without stream:complete", func(t *testing.T) {
		fastBackoff(t)
		var calls atomic.Int32
		client := newRoundTripClient(t, "", func(r *http.Request) (*http.Response, error) {
			calls.Add(1)
			// Delivers a non-terminal event and closes cleanly without stream:complete.
			return sseResponse(runUpdate("running", "")), nil
		})
		start := time.Now()
		_, err := client.Runs.Wait(context.Background(), "r1", &WaitForRunOptions{MaxAttempts: 3})
		elapsed := time.Since(start)
		if err == nil || !strings.Contains(err.Error(), "ended before the run completed") {
			t.Fatalf("err = %v, want ended error", err)
		}
		if got := calls.Load(); got != 4 {
			t.Fatalf("calls = %d, want 4 (1 initial + 3 bounded reconnects)", got)
		}
		// Backoff sums to at least 1+2+4 = 7ms with fastBackoff(2ms base).
		if elapsed < 5*time.Millisecond {
			t.Fatalf("elapsed = %v, expected non-zero backoff between premature EOFs", elapsed)
		}
	})
}

func TestUnitRequestRawReplacesHeaders(t *testing.T) {
	var gotAuth, gotUA, gotMulti []string
	client := newRoundTripClient(t, "", func(r *http.Request) (*http.Response, error) {
		gotAuth = append([]string(nil), r.Header.Values("Authorization")...)
		gotUA = append([]string(nil), r.Header.Values("User-Agent")...)
		gotMulti = append([]string(nil), r.Header.Values("X-Custom")...)
		return textResponse(http.StatusOK, "ok", nil), nil
	})
	headers := http.Header{
		"authorization": {"Bearer override"},
		"User-Agent":    {"custom-agent/1.0"},
		"X-Custom":      {"v1", "v2"},
	}
	resp, err := client.RequestRaw(context.Background(), "GET", "/files/f1/download", nil, headers, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if !reflect.DeepEqual(gotAuth, []string{"Bearer override"}) {
		t.Fatalf("Authorization = %v, want single replaced header", gotAuth)
	}
	if !reflect.DeepEqual(gotUA, []string{"custom-agent/1.0"}) {
		t.Fatalf("User-Agent = %v, want single replaced header", gotUA)
	}
	if !reflect.DeepEqual(gotMulti, []string{"v1", "v2"}) {
		t.Fatalf("X-Custom = %v, want [v1 v2]", gotMulti)
	}
}

func TestUnitBaseURLAndPathQueryNormalization(t *testing.T) {
	cases := map[string]string{
		"":                                "https://api.hot.dev",
		"https://api.hot.dev/":            "https://api.hot.dev",
		"https://api.hot.dev/v1":          "https://api.hot.dev",
		"https://api.hot.dev/v1/":         "https://api.hot.dev",
		"  http://localhost:4681/v1///  ": "http://localhost:4681",
		"https://proxy.example/hot/v1/":   "https://proxy.example/hot",
		"https://api.hot.dev/v10/":        "https://api.hot.dev/v10",
	}
	for input, want := range cases {
		c, err := NewClient(Config{Token: "tok", BaseURL: input})
		if err != nil {
			t.Fatalf("BaseURL %q: %v", input, err)
		}
		if got := c.BaseURL(); got != want {
			t.Errorf("BaseURL(%q) = %q, want %q", input, got, want)
		}
	}

	var seenURLs []string
	client := newRoundTripClient(t, "https://api.hot.test/v1/", func(r *http.Request) (*http.Response, error) {
		seenURLs = append(seenURLs, r.URL.String())
		return jsonResponse(http.StatusOK, envelope(map[string]any{}), nil), nil
	})
	ctx := context.Background()
	if _, err := client.Request(ctx, "GET", "projects", nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Request(ctx, "GET", "/runs?status=running", nil, url.Values{"limit": {"5"}}); err != nil {
		t.Fatal(err)
	}
	wantURLs := []string{
		"https://api.hot.test/v1/projects",
		"https://api.hot.test/v1/runs?status=running&limit=5",
	}
	if !reflect.DeepEqual(seenURLs, wantURLs) {
		t.Fatalf("URLs = %v, want %v", seenURLs, wantURLs)
	}
}

type failingReader struct{ err error }

func (f failingReader) Read([]byte) (int, error) { return 0, f.err }
func (f failingReader) Close() error             { return nil }

func TestUnitErrorBodyCapAndReadError(t *testing.T) {
	ctx := context.Background()

	t.Run("caps error body at 1 MiB across requestJSON, requestRaw, and stream", func(t *testing.T) {
		huge := strings.Repeat("x", maxErrorBodyBytes+4096)
		client := newRoundTripClient(t, "", func(r *http.Request) (*http.Response, error) {
			return textResponse(http.StatusBadRequest, huge, nil), nil
		})

		_, err := client.Env.Get(ctx)
		var apiErr *APIError
		if !errors.As(err, &apiErr) || len(apiErr.Message) != maxErrorBodyBytes {
			t.Fatalf("requestJSON len(Message) = %d, want %d (err=%v)", len(apiErr.Message), maxErrorBodyBytes, err)
		}

		_, err = client.RequestRaw(ctx, "GET", "/env", nil, nil, nil)
		apiErr = nil
		if !errors.As(err, &apiErr) || len(apiErr.Message) != maxErrorBodyBytes {
			t.Fatalf("requestRaw len(Message) = %d, want %d", len(apiErr.Message), maxErrorBodyBytes)
		}

		var streamErr error
		for _, err := range client.Streams.Subscribe(ctx, "s1", nil) {
			streamErr = err
		}
		apiErr = nil
		if !errors.As(streamErr, &apiErr) || len(apiErr.Message) != maxErrorBodyBytes {
			t.Fatalf("stream len(Message) = %d, want %d", len(apiErr.Message), maxErrorBodyBytes)
		}
	})

	t.Run("does not discard error body read failures", func(t *testing.T) {
		boom := errors.New("connection reset reading error body")
		client := newRoundTripClient(t, "", func(r *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Header:     http.Header{},
				Body:       failingReader{err: boom},
			}, nil
		})

		if _, err := client.RequestRaw(ctx, "GET", "/env", nil, nil, nil); !errors.Is(err, boom) {
			t.Fatalf("requestRaw err = %v, want %v", err, boom)
		}
		var streamErr error
		for _, err := range client.Streams.Subscribe(ctx, "s1", nil) {
			streamErr = err
		}
		if !errors.Is(streamErr, boom) {
			t.Fatalf("stream err = %v, want %v", streamErr, boom)
		}
	})
}

func TestUnitConfigStringRedactsToken(t *testing.T) {
	cfg := Config{Token: "hot_super_secret_123", BaseURL: "https://api.hot.dev", Timeout: 5 * time.Second}
	for _, formatted := range []string{cfg.String(), fmt.Sprint(cfg), fmt.Sprintf("%+v", cfg), fmt.Sprint(&cfg)} {
		if strings.Contains(formatted, "hot_super_secret_123") {
			t.Fatalf("formatted Config leaked token: %q", formatted)
		}
		if !strings.Contains(formatted, "[REDACTED]") {
			t.Fatalf("formatted Config missing [REDACTED]: %q", formatted)
		}
	}
	empty := Config{BaseURL: "https://api.hot.dev"}
	if strings.Contains(empty.String(), "[REDACTED]") {
		t.Fatalf("empty token should not show [REDACTED]: %q", empty.String())
	}
}

func TestUnitRetryAfterParsing(t *testing.T) {
	fixedNow := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	prevNow := nowFunc
	nowFunc = func() time.Time { return fixedNow }
	t.Cleanup(func() { nowFunc = prevNow })

	cases := []struct {
		name   string
		value  any
		header string
		want   int
	}{
		{"json integer", float64(7), "", 7},
		{"json sub-second float rounds up", float64(0.25), "", 1},
		{"json zero falls back to header", float64(0), "5", 5},
		{"json zero without header", float64(0), "", 0},
		{"json negative clamped", float64(-10), "", 0},
		{"header integer", nil, "12", 12},
		{"header zero", nil, "0", 0},
		{"header negative clamped", nil, "-5", 0},
		{"header RFC 9110 future date", nil, fixedNow.Add(15 * time.Second).Format(http.TimeFormat), 15},
		{"header RFC 9110 past date clamped", nil, fixedNow.Add(-10 * time.Second).Format(http.TimeFormat), 0},
		{"header invalid string", nil, "not-a-date", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := http.Header{}
			if tc.header != "" {
				h.Set("Retry-After", tc.header)
			}
			if got := retryAfterSeconds(tc.value, h); got != tc.want {
				t.Errorf("retryAfterSeconds(%v, %q) = %d, want %d", tc.value, tc.header, got, tc.want)
			}
		})
	}
}

func TestUnitRequestJSONDeadlineJitterPrefixAnd502_504(t *testing.T) {
	t.Run("fails fast returning APIError when ctx deadline is shorter than Retry-After", func(t *testing.T) {
		var calls atomic.Int32
		client := newRoundTripClient(t, "", func(r *http.Request) (*http.Response, error) {
			calls.Add(1)
			return jsonResponse(http.StatusTooManyRequests, map[string]any{
				"error": map[string]any{"code": "rate_limited", "message": "slow down", "retry_after": 30},
			}, nil), nil
		})
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()
		start := time.Now()
		_, err := client.Runs.Get(ctx, "r1")
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.StatusCode != 429 || calls.Load() != 1 {
			t.Fatalf("err = %v, calls = %d, want immediate 429 *APIError", err, calls.Load())
		}
		if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
			t.Fatalf("took %v, expected immediate fail-fast before deadline", elapsed)
		}
	})

	t.Run("429 retry delay adds bounded jitter", func(t *testing.T) {
		err := &APIError{StatusCode: http.StatusTooManyRequests, RetryAfter: 2}
		for range 50 {
			d, ok := jsonRetryDelay(http.MethodGet, http.StatusTooManyRequests, err, 0)
			if !ok || d < 2*time.Second || d > 2*time.Second+250*time.Millisecond {
				t.Fatalf("delay = %v, ok = %v", d, ok)
			}
		}
		cappedErr := &APIError{StatusCode: http.StatusTooManyRequests, RetryAfter: 90}
		d, ok := jsonRetryDelay(http.MethodGet, http.StatusTooManyRequests, cappedErr, 0)
		if !ok || d != 30*time.Second {
			t.Fatalf("capped delay = %v, ok = %v, want 30s", d, ok)
		}
	})

	t.Run("cancelled retry error includes pathPrefix", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		client := newRoundTripClient(t, "", func(r *http.Request) (*http.Response, error) {
			time.AfterFunc(20*time.Millisecond, cancel)
			return jsonResponse(http.StatusTooManyRequests, map[string]any{
				"error": map[string]any{"code": "rate_limited", "message": "slow", "retry_after": 5},
			}, nil), nil
		})
		_, err := client.Orgs.Env("o1", "e1").Projects.List(ctx, nil)
		if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "/orgs/o1/envs/e1/projects") {
			t.Fatalf("err = %v, want context.Canceled with pathPrefix", err)
		}
	})

	t.Run("502 and 504 retry idempotent methods only", func(t *testing.T) {
		fastBackoff(t)
		for _, status := range []int{http.StatusBadGateway, http.StatusGatewayTimeout} {
			for method, want := range map[string]int32{"GET": 2, "PUT": 2, "DELETE": 2, "POST": 1, "PATCH": 1} {
				var calls atomic.Int32
				client := newRoundTripClient(t, "", func(r *http.Request) (*http.Response, error) {
					if calls.Add(1) == 1 {
						return textResponse(status, "gateway", nil), nil
					}
					return jsonResponse(http.StatusOK, envelope(map[string]any{"ok": true}), nil), nil
				})
				_, err := client.Request(context.Background(), method, "/test", nil, nil)
				if calls.Load() != want || (want == 2) != (err == nil) {
					t.Errorf("status=%d method=%s: calls=%d want=%d err=%v", status, method, calls.Load(), want, err)
				}
			}
		}
	})
}

func TestUnitSSEStreamEOFDiscardAndBufferCap(t *testing.T) {
	ctx := context.Background()

	t.Run("does not flush unterminated trailing SSE bytes on EOF", func(t *testing.T) {
		client := newRoundTripClient(t, "", func(r *http.Request) (*http.Response, error) {
			return textResponse(http.StatusOK,
				"data: {\"type\":\"ok\"}\n\ndata: {\"type\":\"unterminated\"}",
				http.Header{"Content-Type": {"text/event-stream"}},
			), nil
		})
		var types []string
		for ev, err := range client.Streams.Subscribe(ctx, "s1", nil) {
			if err != nil {
				t.Fatal(err)
			}
			types = append(types, ev.Type())
		}
		if !reflect.DeepEqual(types, []string{"ok"}) {
			t.Fatalf("types = %v, want [ok] (unterminated trailing event must be discarded)", types)
		}
	})

	t.Run("stream returns errSSEBufferOverflow when cap exceeded", func(t *testing.T) {
		prev := maxSSEBufferBytes
		maxSSEBufferBytes = 64
		t.Cleanup(func() { maxSSEBufferBytes = prev })

		client := newRoundTripClient(t, "", func(r *http.Request) (*http.Response, error) {
			return textResponse(http.StatusOK,
				"data: {\"type\":\"ok\"}\n\ndata: "+strings.Repeat("z", 128)+"\n\n",
				http.Header{"Content-Type": {"text/event-stream"}},
			), nil
		})
		var types []string
		var gotErr error
		for ev, err := range client.Streams.Subscribe(ctx, "s1", nil) {
			if err != nil {
				gotErr = err
				break
			}
			types = append(types, ev.Type())
		}
		if !reflect.DeepEqual(types, []string{"ok"}) || !errors.Is(gotErr, errSSEBufferOverflow) {
			t.Fatalf("types = %v, err = %v, want [ok] and errSSEBufferOverflow", types, gotErr)
		}
	})
}

func TestUnitMemoryCapsAndEdgeCases(t *testing.T) {
	t.Run("runTracker caps orphans and cleans buffered chunks for task orphans", func(t *testing.T) {
		prevOrphans := maxOrphanEvents
		maxOrphanEvents = 3
		t.Cleanup(func() { maxOrphanEvents = prevOrphans })

		var chunks []string
		tracker := newRunTracker("e1", func(text string) { chunks = append(chunks, text) })

		// Buffer chunk for a task run whose origin is r1, and finish task before r1 correlates.
		tracker.handle(StreamEvent{"type": "stream:data", "run_id": "task1", "payload": map[string]any{"text": "t"}})
		tracker.handle(StreamEvent{
			"type": "run:stop",
			"run":  map[string]any{"run_id": "task1", "event_id": "e9", "origin_run_id": "r1", "run_type": "task", "status": "succeeded"},
		})
		// Fill orphan cap with unrelated orphans.
		for i := range 10 {
			id := fmt.Sprintf("u%d", i)
			tracker.handle(StreamEvent{"type": "stream:data", "run_id": id, "payload": map[string]any{"text": "x"}})
			tracker.handle(StreamEvent{
				"type": "run:stop",
				"run":  map[string]any{"run_id": id, "event_id": "other", "origin_run_id": fmt.Sprintf("orig%d", i), "status": "succeeded"},
			})
		}
		if tracker.orphanCount > maxOrphanEvents {
			t.Fatalf("orphanCount = %d, exceeds cap %d", tracker.orphanCount, maxOrphanEvents)
		}
		// Now r1 arrives: task1's orphan is rejected as non-retry AND task1's buffered chunk must be cleaned up.
		tracker.handle(StreamEvent{
			"type": "run:start",
			"run":  map[string]any{"run_id": "r1", "event_id": "e1", "run_type": "call", "status": "running"},
		})
		if _, leaked := tracker.buffered["task1"]; leaked {
			t.Fatal("task1 buffered chunks were leaked after r1 correlated")
		}
		if len(chunks) != 0 {
			t.Fatalf("unexpected chunks delivered: %v", chunks)
		}
	})

	t.Run("boundedSet caps seenStart and seenTerminal", func(t *testing.T) {
		s := newBoundedSet(3)
		s.add("r1")
		s.add("r2")
		s.add("r3")
		s.add("r4") // evicts r1
		if s.has("r1") {
			t.Fatal("r1 should have been evicted")
		}
		if !s.has("r2") || !s.has("r3") || !s.has("r4") || len(s.items) != 3 {
			t.Fatalf("items = %v", s.items)
		}
	})

	t.Run("decodeDataResponse handles empty 204 body", func(t *testing.T) {
		resp := textResponse(http.StatusNoContent, "", nil)
		data, err := decodeDataResponse(resp)
		if err != nil || data != nil {
			t.Fatalf("data = %v, err = %v, want nil, nil", data, err)
		}
	})

	t.Run("event:published empty fields do not clobber valid stream_id or event_id", func(t *testing.T) {
		var calls atomic.Int32
		client := newRoundTripClient(t, "", func(r *http.Request) (*http.Response, error) {
			if calls.Add(1) == 1 {
				return sseResponse(
					publishedEvent,
					`{"type":"event:published","stream_id":"","event_id":""}`,
					`{"type":"stream:complete","stream_id":"s1"}`,
				), nil
			}
			if r.URL.Path != "/v1/streams/s1/subscribe" {
				t.Errorf("reconnect path = %s, want /v1/streams/s1/subscribe", r.URL.Path)
			}
			return sseResponse(`{"type":"run:stop","run":{"run_id":"r1","event_id":"e1","status":"succeeded","result":"ok"}}`), nil
		})
		out, err := client.Events.CallHot(context.Background(), "fn", nil, nil)
		if err != nil || out != "ok" {
			t.Fatalf("out = %v, err = %v", out, err)
		}
	})

	t.Run("negative waiter Timeout defaults to 60s instead of expiring immediately", func(t *testing.T) {
		client := newRoundTripClient(t, "", func(r *http.Request) (*http.Response, error) {
			switch {
			case strings.Contains(r.URL.Path, "/runs/"):
				return sseResponse(runUpdate("succeeded", "ok")), nil
			case strings.Contains(r.URL.Path, "/tasks/"):
				return sseResponse(taskUpdate("completed", "ok")), nil
			default:
				return sseResponse(runStop), nil
			}
		})
		ctx := context.Background()
		if _, err := client.Runs.Wait(ctx, "r1", &WaitForRunOptions{Timeout: -1 * time.Second}); err != nil {
			t.Fatalf("Runs.Wait with negative timeout failed: %v", err)
		}
		if _, err := client.Tasks.Wait(ctx, "t1", &WaitForTaskOptions{Timeout: -1 * time.Second}); err != nil {
			t.Fatalf("Tasks.Wait with negative timeout failed: %v", err)
		}
		if _, err := client.Streams.WaitForRunResult(ctx, "s1", "e1", &WaitForRunResultOptions{Timeout: -1 * time.Second}); err != nil {
			t.Fatalf("WaitForRunResult with negative timeout failed: %v", err)
		}
	})

	t.Run("stream idle timeout fires on silent stream and ignores slow consumers", func(t *testing.T) {
		prev := streamIdleTimeout
		streamIdleTimeout = 50 * time.Millisecond
		t.Cleanup(func() { streamIdleTimeout = prev })

		// Silent stream should return errStreamIdle.
		silentClient := newRoundTripClient(t, "", func(r *http.Request) (*http.Response, error) {
			pr, pw := io.Pipe()
			go func() {
				<-r.Context().Done()
				pw.CloseWithError(r.Context().Err())
			}()
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": {"text/event-stream"}},
				Body:       pr,
			}, nil
		})
		var gotErr error
		for _, err := range silentClient.Streams.Subscribe(context.Background(), "s1", nil) {
			gotErr = err
		}
		if !errors.Is(gotErr, errStreamIdle) {
			t.Fatalf("silent stream err = %v, want %v", gotErr, errStreamIdle)
		}

		// Slow consumer taking longer than idle timeout must not trigger idle timeout.
		slowClient := newRoundTripClient(t, "", func(r *http.Request) (*http.Response, error) {
			pr, pw := io.Pipe()
			go func() {
				fmt.Fprint(pw, "data: {\"type\":\"a\"}\n\n")
				fmt.Fprint(pw, "data: {\"type\":\"b\"}\n\n")
				pw.Close()
			}()
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": {"text/event-stream"}},
				Body:       pr,
			}, nil
		})
		var types []string
		for ev, err := range slowClient.Streams.Subscribe(context.Background(), "s1", nil) {
			if err != nil {
				t.Fatalf("slow consumer got unexpected error: %v", err)
			}
			types = append(types, ev.Type())
			time.Sleep(80 * time.Millisecond)
		}
		if !reflect.DeepEqual(types, []string{"a", "b"}) {
			t.Fatalf("slow consumer types = %v, want [a b]", types)
		}
	})
}

func TestUnitResourcesRoundTrip(t *testing.T) {
	var mu sync.Mutex
	var calls []string

	client := newRoundTripClient(t, "", func(r *http.Request) (*http.Response, error) {
		mu.Lock()
		calls = append(calls, r.Method+" "+r.URL.RequestURI())
		mu.Unlock()

		if strings.HasSuffix(r.URL.Path, "/subscribe") {
			return sseResponse(`{"type":"stream:complete","stream_id":"s1"}`), nil
		}
		if strings.HasSuffix(r.URL.Path, "/download") {
			return textResponse(http.StatusOK, "raw-bytes", http.Header{"Content-Type": {"application/octet-stream"}}), nil
		}
		if r.Method == "DELETE" && !strings.HasSuffix(r.URL.Path, "/calls") && r.URL.Path != "/v1/sessions" && r.URL.Path != "/v1/service-keys" {
			return textResponse(http.StatusNoContent, "", nil), nil
		}
		return jsonResponse(http.StatusOK, envelope(map[string]any{"ok": true}), nil), nil
	})
	ctx := context.Background()

	must := func(name string, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	mustResp := func(name string, resp *http.Response, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(b) != "raw-bytes" {
			t.Fatalf("%s: body = %q, want raw-bytes", name, string(b))
		}
	}

	// Builds
	_, err := client.Builds.List(ctx, url.Values{"limit": {"5"}})
	must("Builds.List", err)
	_, err = client.Builds.ListForProject(ctx, "p1", nil)
	must("Builds.ListForProject", err)
	_, err = client.Builds.Get(ctx, "p1", "b1")
	must("Builds.Get", err)
	_, err = client.Builds.Deployed(ctx, "p1")
	must("Builds.Deployed", err)
	_, err = client.Builds.Live(ctx, "p1")
	must("Builds.Live", err)
	resp, err := client.Builds.Download(ctx, "p1", "b1")
	mustResp("Builds.Download", resp, err)
	_, err = client.Builds.Deploy(ctx, "p1", "b1")
	must("Builds.Deploy", err)

	// Files
	_, err = client.Files.List(ctx, url.Values{"prefix": {"a/"}})
	must("Files.List", err)
	_, err = client.Files.Get(ctx, "f1")
	must("Files.Get", err)
	must("Files.Delete", client.Files.Delete(ctx, "f1"))
	resp, err = client.Files.Download(ctx, "f1")
	mustResp("Files.Download", resp, err)
	_, err = client.Files.Upload(ctx, "dir/a.txt", []byte("hi"), "text/plain")
	must("Files.Upload", err)
	_, err = client.Files.InitiateUpload(ctx, map[string]any{"path": "dir/big.bin"})
	must("Files.InitiateUpload", err)
	_, err = client.Files.UploadPart(ctx, "u1", 1, []byte("part1"))
	must("Files.UploadPart", err)
	_, err = client.Files.CompleteUpload(ctx, "u1")
	must("Files.CompleteUpload", err)
	must("Files.AbortUpload", client.Files.AbortUpload(ctx, "u1"))

	// Domains
	_, err = client.Domains.Create(ctx, map[string]any{"domain": "app.example.com"})
	must("Domains.Create", err)
	_, err = client.Domains.List(ctx)
	must("Domains.List", err)
	_, err = client.Domains.Get(ctx, "d1")
	must("Domains.Get", err)
	_, err = client.Domains.Verify(ctx, "d1")
	must("Domains.Verify", err)
	must("Domains.Delete", client.Domains.Delete(ctx, "d1"))

	// ServiceKeys
	_, err = client.ServiceKeys.Create(ctx, map[string]any{"description": "sk"})
	must("ServiceKeys.Create", err)
	_, err = client.ServiceKeys.List(ctx, nil)
	must("ServiceKeys.List", err)
	_, err = client.ServiceKeys.Get(ctx, "sk1")
	must("ServiceKeys.Get", err)
	_, err = client.ServiceKeys.Update(ctx, "sk1", map[string]any{"description": "sk2"})
	must("ServiceKeys.Update", err)
	must("ServiceKeys.Revoke", client.ServiceKeys.Revoke(ctx, "sk1"))
	_, err = client.ServiceKeys.RevokeAll(ctx)
	must("ServiceKeys.RevokeAll", err)

	// Sessions
	_, err = client.Sessions.Create(ctx, map[string]any{"permissions": map[string]any{}})
	must("Sessions.Create", err)
	_, err = client.Sessions.List(ctx, nil)
	must("Sessions.List", err)
	must("Sessions.Revoke", client.Sessions.Revoke(ctx, "ses1"))
	_, err = client.Sessions.RevokeAll(ctx)
	must("Sessions.RevokeAll", err)

	// Projects
	_, err = client.Projects.List(ctx, nil)
	must("Projects.List", err)
	_, err = client.Projects.Create(ctx, map[string]any{"name": "p1"})
	must("Projects.Create", err)
	_, err = client.Projects.Get(ctx, "p1")
	must("Projects.Get", err)
	_, err = client.Projects.Update(ctx, "p1", map[string]any{"name": "p2"})
	must("Projects.Update", err)
	_, err = client.Projects.Activate(ctx, "p1")
	must("Projects.Activate", err)
	_, err = client.Projects.Deactivate(ctx, "p1")
	must("Projects.Deactivate", err)
	_, err = client.Projects.EventHandlers(ctx, "p1")
	must("Projects.EventHandlers", err)
	_, err = client.Projects.Schedules(ctx, "p1")
	must("Projects.Schedules", err)
	must("Projects.Delete", client.Projects.Delete(ctx, "p1"))

	// Context
	_, err = client.Context.List(ctx, "p1")
	must("Context.List", err)
	_, err = client.Context.Create(ctx, "p1", map[string]any{"key": "k1", "value": "v1"})
	must("Context.Create", err)
	_, err = client.Context.Update(ctx, "p1", "k1", map[string]any{"value": "v2"})
	must("Context.Update", err)
	must("Context.Delete", client.Context.Delete(ctx, "p1", "k1"))

	// Events, Runs, Tasks, Env, Org
	_, err = client.Events.List(ctx, nil)
	must("Events.List", err)
	_, err = client.Events.Get(ctx, "e1")
	must("Events.Get", err)
	_, err = client.Events.GetRuns(ctx, "e1")
	must("Events.GetRuns", err)

	_, err = client.Runs.Stats(ctx)
	must("Runs.Stats", err)
	for _, err := range client.Runs.Subscribe(ctx, "r1") {
		must("Runs.Subscribe", err)
	}
	_, err = client.Tasks.Get(ctx, "t1")
	must("Tasks.Get", err)
	for _, err := range client.Tasks.Subscribe(ctx, "t1") {
		must("Tasks.Subscribe", err)
	}

	_, err = client.Env.Get(ctx)
	must("Env.Get", err)
	for _, err := range client.Env.Subscribe(ctx) {
		must("Env.Subscribe", err)
	}
	_, err = client.Env.PurgeCalls(ctx)
	must("Env.PurgeCalls", err)

	_, err = client.Org.Usage(ctx)
	must("Org.Usage", err)

	want := []string{
		"GET /v1/builds?limit=5",
		"GET /v1/projects/p1/builds",
		"GET /v1/projects/p1/builds/b1",
		"GET /v1/projects/p1/builds/deployed",
		"GET /v1/projects/p1/builds/live",
		"GET /v1/projects/p1/builds/b1/download",
		"POST /v1/projects/p1/builds/b1/deploy",
		"GET /v1/files?prefix=a%2F",
		"GET /v1/files/f1",
		"DELETE /v1/files/f1",
		"GET /v1/files/f1/download",
		"PUT /v1/files/upload/dir/a.txt",
		"POST /v1/files/uploads",
		"PUT /v1/files/uploads/u1/1",
		"POST /v1/files/uploads/u1/complete",
		"DELETE /v1/files/uploads/u1",
		"POST /v1/domains",
		"GET /v1/domains",
		"GET /v1/domains/d1",
		"POST /v1/domains/d1/verify",
		"DELETE /v1/domains/d1",
		"POST /v1/service-keys",
		"GET /v1/service-keys",
		"GET /v1/service-keys/sk1",
		"PATCH /v1/service-keys/sk1",
		"DELETE /v1/service-keys/sk1",
		"DELETE /v1/service-keys",
		"POST /v1/sessions",
		"GET /v1/sessions",
		"DELETE /v1/sessions/ses1",
		"DELETE /v1/sessions",
		"GET /v1/projects",
		"POST /v1/projects",
		"GET /v1/projects/p1",
		"PATCH /v1/projects/p1",
		"POST /v1/projects/p1/activate",
		"POST /v1/projects/p1/deactivate",
		"GET /v1/projects/p1/event-handlers",
		"GET /v1/projects/p1/schedules",
		"DELETE /v1/projects/p1",
		"GET /v1/projects/p1/context",
		"POST /v1/projects/p1/context",
		"PUT /v1/projects/p1/context/k1",
		"DELETE /v1/projects/p1/context/k1",
		"GET /v1/events",
		"GET /v1/events/e1",
		"GET /v1/events/e1/runs",
		"GET /v1/runs/stats",
		"GET /v1/runs/r1/subscribe",
		"GET /v1/tasks/t1",
		"GET /v1/tasks/t1/subscribe",
		"GET /v1/env",
		"GET /v1/env/subscribe",
		"DELETE /v1/env/calls",
		"GET /v1/org/usage",
	}
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls mismatch:\ngot:  %v\nwant: %v", calls, want)
	}
}
