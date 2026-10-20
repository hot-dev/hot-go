package hot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"
)

func TestManagementContract(t *testing.T) {
	var seen []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer hm_test" {
			t.Error("missing management auth")
		}
		seen = append(seen, r.Method+" "+r.URL.RequestURI())
		if r.Method == "PATCH" {
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body["state"] != "suspended" {
				t.Errorf("state body: %v %v", body, err)
			}
			w.WriteHeader(204)
			return
		}
		if r.Method == "POST" {
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body["name"] != "sdk" {
				t.Errorf("request body: %v %v", body, err)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data":{"marker":"response"},"meta":{}}`)
	}))
	defer server.Close()
	client, err := NewManagementClient(Config{Token: "hm_test", BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	body := map[string]any{"name": "sdk"}
	data, err := client.ListChildren(ctx, url.Values{"limit": {"7"}, "offset": {"3"}})
	if err != nil || data["marker"] != "response" {
		t.Fatalf("list: %v %v", data, err)
	}
	if _, err = client.CreateChild(ctx, body); err != nil {
		t.Fatal(err)
	}
	if err = client.SetChildState(ctx, "child/one", "suspended"); err != nil {
		t.Fatal(err)
	}
	if _, err = client.CreateEnvironment(ctx, "child/one", body); err != nil {
		t.Fatal(err)
	}
	if _, err = client.CreateAPIKey(ctx, "child/one", "env/two", body); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Usage(ctx); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"GET /v1/management/children?limit=7&offset=3",
		"POST /v1/management/children",
		"PATCH /v1/management/children/child%2Fone/state",
		"POST /v1/management/children/child%2Fone/environments",
		"POST /v1/management/children/child%2Fone/environments/env%2Ftwo/api-keys",
		"GET /v1/management/usage",
	}
	if !reflect.DeepEqual(seen, want) {
		t.Fatalf("calls = %v", seen)
	}
}

func TestAuthorizationFailuresStopReconnects(t *testing.T) {
	for _, status := range []int{401, 403} {
		for _, kind := range []string{"stream", "run", "task", "call", "result"} {
			t.Run(fmt.Sprintf("%s-%d", kind, status), func(t *testing.T) {
				calls := 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls++
					if kind == "stream" && calls == 1 {
						w.Header().Set("Content-Type", "text/event-stream")
						fmt.Fprint(w, "data: {\"type\":\"event:published\",\"stream_id\":\"s\",\"event_id\":\"e\"}\n\n")
						return
					}
					w.WriteHeader(status)
					fmt.Fprint(w, `{"error":{"message":"revoked"}}`)
				}))
				defer server.Close()
				client, err := NewClient(Config{Token: "test", BaseURL: server.URL})
				if err != nil {
					t.Fatal(err)
				}
				ctx := context.Background()
				switch kind {
				case "stream":
					for _, streamErr := range client.Streams.SubscribeWithEvent(ctx, map[string]any{}, nil) {
						if streamErr != nil {
							err = streamErr
						}
					}
				case "run":
					_, err = client.Runs.Wait(ctx, "r", nil)
				case "task":
					_, err = client.Tasks.Wait(ctx, "t", nil)
				case "call":
					_, err = client.Events.CallHot(ctx, "f", nil, nil)
				case "result":
					_, err = client.Streams.WaitForRunResult(ctx, "s", "e", nil)
				}
				var apiErr *APIError
				if !errors.As(err, &apiErr) || apiErr.StatusCode != status {
					t.Fatalf("error = %v", err)
				}
				want := 1
				if kind == "stream" {
					want = 2
				}
				if calls != want {
					t.Fatalf("calls = %d, want %d", calls, want)
				}
			})
		}
	}
}
