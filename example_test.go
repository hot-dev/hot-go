package hot_test

// Compile-checked examples rendered on pkg.go.dev. None declare an Output
// comment, so `go test` builds them without executing network calls.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

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
			fmt.Println(event["data_type"], event["payload"])
		}

		// Break on the terminal event; the server holds the stream open.
		if event.Type() == "run:stop" {
			fmt.Println(event.Run()["result"])
			break
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
