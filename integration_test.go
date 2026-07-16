package hot

// Integration tests run against a live `hot dev` instance serving the fixture
// project in integration/fixture. Start one and run this file with
// integration/run.sh; without HOT_TEST_API_KEY set, every test skips.

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func integrationClient(t *testing.T) *Client {
	t.Helper()
	key := os.Getenv("HOT_TEST_API_KEY")
	if key == "" {
		t.Skip("HOT_TEST_API_KEY not set; run integration/run.sh")
	}
	baseURL := os.Getenv("HOT_TEST_BASE_URL")
	if baseURL == "" {
		baseURL = "http://localhost:4681"
	}
	client, err := NewClient(Config{Token: key, BaseURL: baseURL})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestIntegrationEnvAndProjects(t *testing.T) {
	client := integrationClient(t)
	ctx := context.Background()

	env, err := client.Env.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if env["name"] != "development" {
		t.Errorf("env name = %v, want development", env["name"])
	}

	projects, err := client.Projects.List(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	if data, ok := projects["data"].([]any); ok {
		for _, project := range data {
			if p, ok := project.(map[string]any); ok && p["name"] == "sdk-fixture" {
				found = true
			}
		}
	}
	if !found {
		t.Errorf("projects = %v, want one named sdk-fixture", projects["data"])
	}
}

func TestIntegrationSubscribeWithEventEcho(t *testing.T) {
	client := integrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var result map[string]any
	for event, err := range client.Streams.SubscribeWithEvent(ctx, map[string]any{
		"event_type": "sdk:echo",
		"event_data": map[string]any{"text": "hello"},
	}, nil) {
		if err != nil {
			t.Fatal(err)
		}
		switch event.Type() {
		case "run:fail", "run:cancel":
			t.Fatalf("run did not complete: %v", event.Run())
		case "run:stop":
			result, _ = event.Run()["result"].(map[string]any)
		}
		if result != nil {
			break
		}
	}

	echoed, _ := result["echoed"].(map[string]any)
	if echoed["text"] != "hello" {
		t.Errorf("run result = %v, want echoed.text=hello", result)
	}
}

func TestIntegrationCallHot(t *testing.T) {
	client := integrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := client.Events.CallHot(ctx, "::fixture::sdk/add-nums", []any{2, 3}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result != float64(5) {
		t.Errorf("add-nums result = %v, want 5", result)
	}
}

func TestIntegrationRunFailureMessage(t *testing.T) {
	client := integrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	published, err := client.Events.Publish(ctx, map[string]any{
		"event_type": "sdk:fail",
		"event_data": map[string]any{},
	})
	if err != nil {
		t.Fatal(err)
	}

	streamID, _ := published["stream_id"].(string)
	eventID, _ := published["event_id"].(string)
	_, err = client.Streams.WaitForRunResult(ctx, streamID, eventID, nil)
	if err == nil || !strings.Contains(err.Error(), "sdk fixture failure") {
		t.Fatalf("err = %v, want message containing %q", err, "sdk fixture failure")
	}
}
