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

func TestIntegrationWaitForTaskStartedByEventHandler(t *testing.T) {
	client := integrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	var taskID, handlerRunID, streamID string
	for event, err := range client.Streams.SubscribeWithEvent(ctx, map[string]any{
		"event_type": "sdk:task",
		"event_data": map[string]any{"value": "hello"},
	}, nil) {
		if err != nil {
			t.Fatal(err)
		}
		switch event.Type() {
		case "run:fail", "run:cancel":
			t.Fatalf("run did not complete: %v", event.Run())
		case "run:stop":
			run := event.Run()
			result, _ := run["result"].(map[string]any)
			work, _ := result["work"].(map[string]any)
			taskID, _ = work["task"].(string)
			handlerRunID, _ = run["run_id"].(string)
			streamID, _ = run["stream_id"].(string)
		}
		if taskID != "" {
			break
		}
	}
	if taskID == "" {
		t.Fatal("run result did not contain work.task")
	}
	handlerRun, err := client.Runs.Wait(ctx, handlerRunID, &WaitForRunOptions{Timeout: 60 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if handlerRun["status"] != "succeeded" {
		t.Fatalf("handler run status = %v, want succeeded", handlerRun["status"])
	}

	var streamedTask map[string]any
	for event, err := range client.Streams.Subscribe(ctx, streamID, nil) {
		if err != nil {
			t.Fatal(err)
		}
		if event.Type() != "task:update" {
			continue
		}
		task, _ := event["task"].(map[string]any)
		if task["task_id"] != taskID {
			continue
		}
		switch task["status"] {
		case "failed", "cancelled", "timed_out":
			t.Fatalf("task did not complete: %v", task)
		case "completed":
			streamedTask = task
		}
		if streamedTask != nil {
			break
		}
	}
	if streamedTask == nil {
		t.Fatal("stream ended before task completed")
	}

	task, err := client.Tasks.Wait(ctx, taskID, &WaitForTaskOptions{Timeout: 60 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if task["status"] != "completed" {
		t.Fatalf("task status = %v, want completed", task["status"])
	}
	result, _ := task["result"].(map[string]any)
	if result["processed"] != "hello" {
		t.Errorf("task result = %v, want processed=hello", result)
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

func TestIntegrationCallHotDeliversEarlyChunks(t *testing.T) {
	client := integrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// The server may send these chunks before run:start; none may be dropped.
	var chunks []string
	result, err := client.Events.CallHot(ctx, "::fixture::sdk/stream-chunks", nil, &CallHotOptions{
		OnChunk: func(text string) { chunks = append(chunks, text) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != "done" || strings.Join(chunks, ",") != "a,b,c" {
		t.Fatalf("result = %v, chunks = %v, want done and a,b,c", result, chunks)
	}
}

func TestIntegrationWaitersFollowRetriedRuns(t *testing.T) {
	client := integrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// sdk:flaky fails its first attempt (run:fail, status pending_retry) and
	// succeeds on the retry, a new run linked by origin_run_id.
	published, err := client.Events.Publish(ctx, map[string]any{"event_type": "sdk:flaky", "event_data": map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	run, err := client.Streams.WaitForRunResult(ctx, published["stream_id"].(string), published["event_id"].(string), nil)
	if err != nil {
		t.Fatal(err)
	}
	result, _ := run["result"].(map[string]any)
	if run["status"] != "succeeded" || result["recovered"] != true || run["origin_run_id"] == nil {
		t.Fatalf("run = %v, want the succeeded retry run", run)
	}

	var final StreamEvent
	for event, err := range client.Streams.SubscribeWithEvent(ctx, map[string]any{"event_type": "sdk:flaky", "event_data": map[string]any{}}, nil) {
		if err != nil {
			t.Fatal(err)
		}
		final = event
	}
	if final.Type() != "run:stop" || final.RunStatus() != "succeeded" {
		t.Fatalf("last event = %v, want the retry run's run:stop", final)
	}
}
