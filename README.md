# hot-go

Official Go SDK for the [Hot Dev](https://hot.dev) API.

- Module: `github.com/hot-dev/hot-go`
- Package: `hot`
- Requires Go 1.23+
- Zero runtime dependencies (net/http only)

## Install

```bash
go get github.com/hot-dev/hot-go
```

## Quick Start

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	hot "github.com/hot-dev/hot-go"
)

func main() {
	client, err := hot.NewClient(hot.Config{Token: os.Getenv("HOT_API_KEY")})
	if err != nil {
		log.Fatal(err)
	}

	// BaseURL defaults to https://api.hot.dev.
	// For local development with `hot dev`, set BaseURL: "http://localhost:4681".

	ctx := context.Background()
	for event, err := range client.Streams.SubscribeWithEvent(ctx, map[string]any{
		"event_type": "team-agent:ask",
		"event_data": map[string]any{
			"session_id": "web:chat:demo",
			"user_id":    "web:user:demo",
			"user_name":  "Demo User",
			"question":   "what is blocking launch?",
		},
	}, nil) {
		if err != nil {
			log.Fatal(err)
		}

		if event.Type() == "stream:data" {
			fmt.Println(event["data_type"], event["payload"])
		}

		if event.Type() == "run:stop" {
			fmt.Println(event.Run()["result"])
			break
		}
	}
}
```

Authenticated clients should run server-side. Browser apps and untrusted
clients should call your own backend route instead of exposing a Hot API key.
Administrative runtime endpoints require an API key with appropriate grants. Sessions and service keys are
permission-scoped and are mainly for event publishing and stream reads.

When code has a run id but does not need the full stream, wait on its durable
terminal snapshot:

```go
run, err := client.Runs.Wait(ctx, runID, &hot.WaitForRunOptions{
	Timeout: 5 * time.Minute,
})
```

When a run returns a background task id, wait through the durable task resource:

```go
task, err := client.Tasks.Wait(ctx, taskID, &hot.WaitForTaskOptions{
	Timeout: 5 * time.Minute,
})
```

The task waiter receives the latest persisted state first and reconnects, so it
cannot miss a task that completed before subscription. A failed, cancelled, or
timed-out task returns `*hot.TaskWaitError` with the final task record.
The task's parent stream also emits durable `task:update` events, which is
useful when one subscription needs to coordinate several tasks.

## Errors

Non-2xx API responses return `*hot.APIError` with structured fields:

```go
_, err := client.Projects.Get(ctx, "missing-project")
var apiErr *hot.APIError
if errors.As(err, &apiErr) {
	fmt.Println(apiErr.StatusCode, apiErr.Code, apiErr.RequestID, apiErr.RetryAfter)
}
```

### Retries

JSON requests are retried automatically (at most twice) when the API responds
429 with a `retry_after`; other errors are returned as-is. Streaming and raw
requests are never retried.

## Resources

`Client` mirrors the Hot API v1 resources:

- `client.Events` — publish, list, get, inspect event runs, and call Hot functions with `CallHot(ctx, fn, args, opts)`
- `client.Streams` — subscribe to run and task updates, wait for run results, and publish events atomically (reconnects automatically across the 5-minute SSE timeout; set `DisableReconnect` to opt out)
- `client.Runs` — list, inspect, subscribe to, and wait for durable runs
- `client.Tasks` — get, subscribe to, and wait for durable background tasks
- `client.Files` — upload, download, list, and delete files (including multipart uploads)
- `client.Projects` — create, list, update, activate, deactivate, and delete projects
- `client.Builds` — upload, download, deploy, and look up live/deployed builds
- `client.Context` — manage encrypted project context variables
- `client.Domains` — register, verify, list, and delete custom domains
- `client.Sessions` — create and revoke scoped sessions
- `client.ServiceKeys` — create and revoke scoped service keys
- `client.Org` — view usage and limits
- `client.Env` — read environment info and subscribe to environment events

Use `client.Request(...)` or `client.RequestRaw(...)` as an escape hatch for
API endpoints that do not yet have a resource helper.

`client.Env.Subscribe(ctx)` requires API key credentials and a live API
pub/sub backend; local API servers without pub/sub return a 503.
`SubscribeWithEvent` reconnects across the API's 5-minute SSE timeout and
stops after the terminal run correlated to the event it published. Unrelated
runs on the same stream do not end the iterator.

## Streaming

Stream methods return Go 1.23 iterators (`iter.Seq2[hot.StreamEvent, error]`):
range over them and stop any time with `break`. Cancel the passed context to
abort the underlying connection.

## Customizing the HTTP Client

Pass your own `*http.Client` for proxies or custom transports:

```go
client, err := hot.NewClient(hot.Config{
	Token:      os.Getenv("HOT_API_KEY"),
	HTTPClient: &http.Client{Transport: myTransport},
})
```

Do not set `http.Client.Timeout` on a client used for SSE subscriptions — it
bounds the whole request, including the stream. Use `Config.Timeout` (JSON
requests only) and context deadlines instead.

## Casing Policy

Core API request and response payloads use the Hot API wire format:
`event_type`, `event_data`, `stream_id`, and so on. SDK-only options use Go
style names such as `BaseURL` and `Timeout`.

The SDK never transforms user-owned payloads such as `event_data`.

## Local Development

```bash
gofmt -l .
go vet ./...
go test -race ./...
```

## Related

- [Hot Dev documentation](https://hot.dev/docs)
- [SDK docs](https://hot.dev/docs/api/sdks)
- [JavaScript / TypeScript SDK](https://www.npmjs.com/package/@hot-dev/sdk)
- [Python SDK](https://pypi.org/project/hot-dev/)

## License

Apache-2.0

## Sub-organization management

Requires a Hot server release containing the sub-org management API. Use the
separate `HotManagementClient` with an `hm_` credential issued by a root admin.
Runtime API keys, service keys, and sessions cannot call management endpoints;
management credentials cannot call runtime endpoints. Create a runtime client
per environment credential. A root environment key cannot access its children,
and a child key cannot access siblings. There is no tenant or org selector on
runtime methods. Each child owns its environments and deployments.

```go
management, err := hot.NewManagementClient(hot.Config{Token: managementToken})
if err != nil { log.Fatal(err) }
child, err := management.CreateChild(ctx, map[string]any{
    "name": "Customer", "slug": "customer-unique-slug",
    "idempotency_key": "customer-provisioning-123",
})
if err != nil { log.Fatal(err) }
org := child["org"].(map[string]any)
key, err := management.CreateAPIKey(ctx, org["org_id"].(string), child["env_id"].(string), map[string]any{
    "description": "Customer backend", "permissions": map[string]any{"*:*": []string{"*"}},
})
if err != nil { log.Fatal(err) }
customer, err := hot.NewClient(hot.Config{Token: key["token"].(string)})
if err != nil { log.Fatal(err) }
_ = customer // runtime operations are bound to this child environment
```

| Management method | Purpose |
| --- | --- |
| `ListChildren(ctx, query)` | List authorized children (returns `children` and `total`) |
| `CreateChild(ctx, body)` | Provision a child and default environment |
| `SetChildState(ctx, childID, state)` | Set `active`, `suspended`, or `archived` (no response body) |
| `CreateEnvironment(ctx, childID, body)` | Provision a child environment by name |
| `CreateAPIKey(ctx, childID, envID, body)` | Issue a child API key within the delegation ceiling |
| `Usage(ctx)` | Read account-wide usage |

Child creation requires a stable `idempotency_key`; reuse it for retries of the
same request. A different payload with that key is rejected. Environment
creation is idempotent within a child by exact name. Listing accepts `limit`
(maximum 500) and `offset`. Account usage requires `usage:read` and explicit
`all_children` authority. Management credentials cannot issue other management
credentials or grant human access. Service keys and sessions are issued with the
child environment's runtime API key and inherit its management issuer chain.
Issuer expiry, revocation, or permission narrowing restrict existing descendants;
expansion never expands their stored grants.

Administrative runtime operations require an API key **and** the appropriate
resource/action grants. For example, project listing requires `project:*` with
`read`, and environment info and usage require `env:{env_id}` with `read`.
Event-only keys receive `403` for these operations; API-key status alone does
not grant administration. Full-access `{"*:*": ["*"]}` keys retain access.
See the server's [permission contract](https://hot.dev/docs/api/management).

`org.usage` reports only the credential's **environment consumption**, with
`env_id`, `usage_scope: "environment"`, and `limits_scope: "billing_account"`.
Its limits and plan come from the billing root; percentages compare this one
environment against shared limits and do not describe remaining account quota.
`usage.team_members` is zero because seats belong to the account. Use management
`usage` for the account aggregate. Existing runtime endpoints and request bodies
keep their shapes. JSON requests retain bounded 429/Retry-After handling;
streaming reconnects and waiters stop on permanent `401`/`403` authorization errors.

### Sub-org integration tests

See [the validation record](integration/VALIDATION.md) for the tested server revision and results.

The fixture creates an owned disposable database and loopback API server, runs
the SDK tests, and removes only its own resources. The canonical fixture lives
in hot-python and is synchronized across all five SDKs. No deployed credentials
are used. Requires Python 3 and a Hot CLI built from the matching server branch.
For PostgreSQL, also install Docker and `psql` and provide the public hot checkout
(default: sibling `hot`). Migrations use a non-superuser schema owner; the API
uses a distinct non-owner `NOSUPERUSER NOBYPASSRLS` login with RLS enforcement.
The fixture's administrator seeds credentials only; SDK calls always use HTTP.

```bash
python3 integration/suborg_fixture.py --hot ../hot/target/debug/hot --backend postgres -- \
  go test -race -run Suborg -v ./...
# For SQLite, use --backend sqlite.
```

Coverage includes provisioning and environment idempotency, identical project
names in siblings, foreign-ID and stream denial, parent runtime isolation,
management/runtime token separation, usage scope, restricted API-key grants,
service/session issuance and revocation, pre-existing descendant issuer
revocation/narrowing, and child suspension/resumption.
