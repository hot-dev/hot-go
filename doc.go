// Package hot is the official Go SDK for the Hot Dev API (https://hot.dev).
//
// The client is a thin mirror of the Hot API v1 resources. Request and
// response payloads use the Hot API wire format (event_type, stream_id,
// event_data, ...) as map[string]any values; the SDK never transforms
// user-owned payloads such as event_data. SDK-only options use Go-idiomatic
// names (BaseURL, Timeout).
//
//	client, err := hot.NewClient(hot.Config{Token: os.Getenv("HOT_API_KEY")})
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	for event, err := range client.Streams.SubscribeWithEvent(ctx, map[string]any{
//		"event_type": "team-agent:ask",
//		"event_data": map[string]any{"question": "what is blocking launch?"},
//	}, nil) {
//		if err != nil {
//			log.Fatal(err)
//		}
//		if event.Type() == "run:stop" {
//			break
//		}
//	}
//
// Authenticated clients should run server-side; browser or untrusted clients
// should call your own backend instead of embedding a Hot API key.
package hot
