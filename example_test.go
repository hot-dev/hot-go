package hot_test

// Compile-checked examples rendered on pkg.go.dev. None declare an Output
// comment, so `go test` builds them without executing network calls. They
// mirror the README and the Go snippets in hot's SDK guide
// (resources/docs/api/sdks.md); keep them in sync.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	hot "github.com/hot-dev/hot-go"
)

func ExampleNewClient() {
	client, err := hot.NewClient(hot.Config{Token: os.Getenv("HOT_API_KEY")})
	if err != nil {
		log.Fatal(err)
	}

	env, err := client.Env.Get(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(env["name"])
}

func ExampleStreamsResource_SubscribeWithEvent() {
	client, err := hot.NewClient(hot.Config{Token: os.Getenv("HOT_API_KEY")})
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	for event, err := range client.Streams.SubscribeWithEvent(ctx, map[string]any{
		"event_type": "team-agent:ask",
		"event_data": map[string]any{"question": "what is blocking launch?"},
	}, nil) {
		if err != nil {
			log.Fatal(err)
		}

		if event.Type() == "stream:data" {
			fmt.Println(event.DataType(), event.Payload())
		}

		// The iterator ends after the published event's final run event.
		if event.Type() == "run:stop" {
			fmt.Println(event.Run()["result"])
		}
	}
}

func ExampleEventsResource_CallHot() {
	client, err := hot.NewClient(hot.Config{Token: os.Getenv("HOT_API_KEY")})
	if err != nil {
		log.Fatal(err)
	}

	result, err := client.Events.CallHot(
		context.Background(), "::myapp::math/add-nums", []any{2, 3}, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result)
}

func ExampleAPIError() {
	client, err := hot.NewClient(hot.Config{Token: os.Getenv("HOT_API_KEY")})
	if err != nil {
		log.Fatal(err)
	}

	_, err = client.Projects.Get(context.Background(), "missing-project")
	var apiErr *hot.APIError
	if errors.As(err, &apiErr) {
		fmt.Println(apiErr.StatusCode, apiErr.Code, apiErr.RequestID, apiErr.RetryAfter)
	}
}

func ExampleEventsResource_CallHot_onChunk() {
	client, err := hot.NewClient(hot.Config{Token: os.Getenv("HOT_API_KEY")})
	if err != nil {
		log.Fatal(err)
	}

	result, err := client.Events.CallHot(context.Background(), "::myapp::agent/answer", []any{"hello"}, &hot.CallHotOptions{
		Timeout: 30 * time.Minute,
		OnChunk: func(text string) { fmt.Print(text) },
	})
	var runErr *hot.RunWaitError
	if errors.As(err, &runErr) {
		log.Fatalf("run %v failed: %v", runErr.Run["run_id"], runErr)
	} else if errors.Is(err, context.DeadlineExceeded) {
		log.Fatal("no result within 30 minutes")
	} else if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result)
}

func ExampleRunsResource_Wait() {
	client, err := hot.NewClient(hot.Config{Token: os.Getenv("HOT_API_KEY")})
	if err != nil {
		log.Fatal(err)
	}

	run, err := client.Runs.Wait(context.Background(), "run-id", &hot.WaitForRunOptions{
		Timeout: 5 * time.Minute,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(run["status"], run["result"])
}

func ExampleTasksResource_Wait() {
	client, err := hot.NewClient(hot.Config{Token: os.Getenv("HOT_API_KEY")})
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	taskID := "task-id"

	task, err := client.Tasks.Wait(ctx, taskID, &hot.WaitForTaskOptions{
		Timeout: 5 * time.Minute,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(task["result"])
}

func ExampleOrgsResource() {
	ctx := context.Background()
	orgs, err := hot.NewClient(hot.Config{Token: os.Getenv("HOT_ORG_API_KEY")})
	if err != nil {
		log.Fatal(err)
	}
	child, err := orgs.Orgs.Create(ctx, map[string]any{
		"name": "Customer", "slug": "customer-123",
		"external_id": "customer-123", "idempotency_key": "customer-123",
	})
	if err != nil {
		log.Fatal(err)
	}
	orgID := child["org"].(map[string]any)["org_id"].(string)
	envID := child["env_id"].(string) // the child's default environment

	// Deploy-side work in the child, with the org key's grants for that environment.
	if _, err := orgs.Orgs.Env(orgID, envID).Projects.Create(ctx, map[string]any{"name": "backend"}); err != nil {
		log.Fatal(err)
	}

	// Runtime work in the child, with an environment key.
	key, err := orgs.Orgs.CreateEnvAPIKey(ctx, orgID, envID, map[string]any{
		"description": "Customer backend",
		"permissions": map[string]any{"event:*": []string{"create"}, "run:*": []string{"read"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	customer, err := hot.NewClient(hot.Config{Token: key["token"].(string)})
	if err != nil {
		log.Fatal(err)
	}
	_, err = customer.Events.Publish(ctx, map[string]any{"event_type": "customer:signup", "event_data": map[string]any{}})
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleAPIKeysResource_Rotate() {
	orgs, err := hot.NewClient(hot.Config{Token: os.Getenv("HOT_ORG_API_KEY")})
	if err != nil {
		log.Fatal(err)
	}

	// The previous secret keeps working for an hour.
	key, err := orgs.APIKeys.Rotate(context.Background(), "api-key-id", map[string]any{"overlap_seconds": 3600})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(key["token"], key["previous_secret_expires_at"])
}

func ExampleKeyScopeError() {
	// An environment key cannot call the org API, and an org key cannot call
	// runtime routes.
	client, err := hot.NewClient(hot.Config{Token: os.Getenv("HOT_API_KEY")})
	if err != nil {
		log.Fatal(err)
	}

	_, err = client.Orgs.List(context.Background(), nil)
	var scopeErr *hot.KeyScopeError
	if errors.As(err, &scopeErr) {
		fmt.Println("this call needs a", scopeErr.RequiredScope, "API key")
	}
}

func ExampleConfig_httpClient() {
	client, err := hot.NewClient(hot.Config{
		Token:      os.Getenv("HOT_API_KEY"),
		HTTPClient: &http.Client{Transport: http.DefaultTransport},
		Timeout:    30 * time.Second, // JSON requests only; streams use the waiters' Timeout
	})
	if err != nil {
		log.Fatal(err)
	}
	_ = client
}
