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
			fmt.Println(event.DataType(), event.Payload())
		}

		// The iterator ends after the published event's final run event.
		if event.Type() == "run:stop" {
			fmt.Println(event.Run()["result"])
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
The task's parent stream also emits durable `task:update` events, but a plain
`Streams.Subscribe` ends at the API's 5-minute subscription limit. To wait for
several tasks, call `Tasks.Wait` for each one (for example in goroutines).

### Waiting, reconnects and timeouts

`Runs.Wait`, `Tasks.Wait`, `Streams.SubscribeWithEvent`,
`Streams.WaitForRunResult` and `Events.CallHot` share one reconnect policy:

- They reconnect across the API's 5-minute subscription limit and across
  transient failures: transport errors, 408, 429 and 5xx. Reconnects back off
  exponentially with jitter (the first wait is 0.25-0.5 s, doubling to at
  most 5-10 s) and wait at least the server's `retry_after`.
- Other 4xx responses end the wait at once: 401 and 403 (including
  `*hot.KeyScopeError`), 404, 410, 422 and the rest.
- `MaxAttempts` (default 5) caps **consecutive** failed connections. A
  connection that delivered events or ended with the server's `stream:complete`
  resets the count, so a run can outlast any number of 5-minute windows.
- `Timeout` (default 60 s) bounds the whole wait, reconnects and backoff
  included. A timeout error wraps `context.DeadlineExceeded`; cancelling `ctx`
  returns an error wrapping its cause, so `errors.Is(err, context.Canceled)`
  works.
- A stream that sends nothing, not even the server's 15-second keepalive, for
  45 seconds counts as a dead connection and reconnects.
- A run whose handler will retry it ends with `run:fail` and status
  `pending_retry`. That is not final: the waiters follow the retry run, which
  links back through `origin_run_id`. The API can also replay that first
  attempt as `run:stop` with status `pending_retry`, or as `run:fail` with
  status `failed` and a raised `retry_attempt` before the retry run exists.
  The first is never final either; the second is held for up to 5 seconds
  (or until the wait's timeout) and reported only if no retry run appears.
  Failures of handlers without retries, and of last retries, are reported
  at once. `OnChunk` receives every chunk of the
  matched run in order, including chunks the API sends before `run:start`.
  A final failure returns `*hot.RunWaitError` with the run.

`SubscribeWithEvent` sends its publishing request again only when the server
cannot have published the event (a 429 or 503 response, or a connection that
never opened), so a retry never publishes twice.

## Errors

Non-2xx API responses return `*hot.APIError` with structured fields:

```go
_, err := client.Projects.Get(ctx, "missing-project")
var apiErr *hot.APIError
if errors.As(err, &apiErr) {
	fmt.Println(apiErr.StatusCode, apiErr.Code, apiErr.RequestID, apiErr.RetryAfter)
}
```

A 403 `key_scope_mismatch` is a `*hot.KeyScopeError`: the call needs a
different kind of key. Its `RequiredScope` is `"org"` for `client.Orgs` and
`client.APIKeys` (org API keys) and `"env"` for everything else (environment
API keys, sessions and service keys). It wraps the `*hot.APIError`.

```go
var scopeErr *hot.KeyScopeError
if errors.As(err, &scopeErr) {
	fmt.Println("this call needs a", scopeErr.RequiredScope, "key")
}
```

### Retries

JSON requests, body-less `DELETE`s included, are retried automatically (at
most twice) when the API responds 429 with a `retry_after`, and idempotent
ones (`GET`, `PUT`, `DELETE`) also on 502, 503 and 504. Other errors are
returned as-is. `Config.Timeout` bounds each JSON request. Uploads,
downloads and other raw requests are never retried; streams follow the
waiter policy above.

## Resources

`Client` mirrors the Hot API v1 resources:

- `client.Events` — publish, list, get, inspect event runs, and call Hot functions with `CallHot(ctx, fn, args, opts)`
- `client.Streams` — subscribe to run and task updates, wait for run results, publish events atomically (reconnects automatically across the 5-minute subscription limit; set `DisableReconnect` to opt out), and purge a stream's call traces
- `client.Runs` — list, inspect, subscribe to, wait for, and purge call traces of durable runs
- `client.Tasks` — get, subscribe to, and wait for durable background tasks
- `client.Files` — upload, download, list, and delete files (including multipart uploads)
- `client.Projects` — create, list, update, activate, deactivate, and delete projects
- `client.Builds` — upload, download, deploy, and look up live/deployed builds
- `client.Context` — manage encrypted project context variables
- `client.Domains` — register, verify, list, and delete custom domains
- `client.Sessions` — create and revoke scoped sessions
- `client.ServiceKeys` — create and revoke scoped service keys
- `client.Org` — view this environment's usage against the account's limits
- `client.Env` — read environment info, subscribe to environment events, and purge call traces
- `client.Orgs` — with an org API key: provision child organizations and environments, mint org and environment API keys, read account usage, purge call traces, and manage projects, builds and context in an environment with `client.Orgs.Env(orgID, envID)`
- `client.APIKeys` — with an org API key: read, narrow, revoke and rotate API keys by ID

Use `client.Request(...)` or `client.RequestRaw(...)` as an escape hatch for
API endpoints that do not yet have a resource helper.

`client.Env.Subscribe(ctx)` requires API key credentials and a live API
pub/sub backend; local API servers without pub/sub return a 503.
`SubscribeWithEvent` reconnects across the API's 5-minute subscription limit
and stops after the final run event correlated to the event it published.
Unrelated runs on the same stream do not end the iterator.

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
requests only), the waiters' `Timeout` options and context deadlines instead.

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
HOT_TEST_PORT=4721 ./integration/run.sh   # needs the hot CLI on PATH
```

## Related

- [Hot Dev documentation](https://hot.dev/docs)
- [SDK docs](https://hot.dev/docs/api/sdks)
- [JavaScript / TypeScript SDK](https://www.npmjs.com/package/@hot-dev/sdk)
- [Python SDK](https://pypi.org/project/hot-dev/)

## License

Apache-2.0

## Organizations and org API keys

Requires Hot 3.0.1 or later. An **org API key** belongs to a root
organization and calls only the org API: `client.Orgs` and `client.APIKeys`.
A root administrator creates the first one on the organization's
**Children** tab in the Hot App. Runtime resources
(events, streams, runs, projects, ...) need an **environment API key**,
session or service key instead, and each kind gets a `*hot.KeyScopeError` on
the other's routes. Mint environment keys with the org key and use a separate
`Client` per environment.

```go
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
```

| Method | Route |
| --- | --- |
| `Orgs.List(ctx, query)` | `GET /v1/orgs` (list envelope; `limit`, `offset`) |
| `Orgs.Create(ctx, body)` | `POST /v1/orgs` → `{org, env_id, created}` |
| `Orgs.Get(ctx, orgID)` / `Orgs.Update(ctx, orgID, {"state": ...})` | `GET`/`PATCH /v1/orgs/{org_id}` |
| `Orgs.Usage(ctx, rootOrgID)` | `GET /v1/orgs/{org_id}/usage` (account usage; root org only) |
| `Orgs.PurgeCalls(ctx, orgID, includeSubOrgs)` | `DELETE /v1/orgs/{org_id}/calls` |
| `Orgs.ListEnvs` / `CreateEnv` / `GetEnv` | `/v1/orgs/{org_id}/envs[/{env_id}]` |
| `Orgs.ListAPIKeys` / `CreateAPIKey` | `/v1/orgs/{org_id}/api-keys` (org keys) |
| `Orgs.ListEnvAPIKeys` / `CreateEnvAPIKey` | `/v1/orgs/{org_id}/envs/{env_id}/api-keys` (environment keys) |
| `Orgs.Env(orgID, envID).Projects` / `.Builds` / `.Context` | `/v1/orgs/{org_id}/envs/{env_id}/projects/...`, `/builds` |
| `APIKeys.Get` / `Update` / `Revoke` / `Rotate` | `/v1/api-keys/{key_id}[/rotate]` |

Child creation needs a stable `idempotency_key`: a retry with the same key
returns the same child with `created: false`. Slugs are at most 32
characters. Environment creation is idempotent by exact name. Minted keys
must fit inside the org key's own grants, and their `token` is returned only
once (also by `Rotate`, whose `overlap_seconds` keeps the previous secret
working for a while). Revoking or narrowing a key restricts everything it
issued, including service keys and sessions created with an environment key it
minted.

`client.Org.Usage` with an environment credential reports only that
environment's consumption (`usage_scope: "environment"`) against the
account's shared limits. Use `Orgs.Usage` for the account aggregate.
See the server's [sub-organization documentation](https://hot.dev/docs/api/management).

### Sub-org integration tests

See [the validation record](integration/VALIDATION.md) for the tested server
revision. The fixture owns a disposable database and loopback API server,
seeds an org API key, runs the SDK tests, and removes only its own resources.
The canonical fixture lives in hot-python and is synchronized across all five
SDKs. Requires Python 3 and a Hot CLI built from the revision in
`integration/hot-server.ref`. For PostgreSQL, also install Docker and `psql`
and provide the hot checkout (default: sibling `hot`). Migrations use a
non-superuser schema owner; the API uses a distinct non-owner
`NOSUPERUSER NOBYPASSRLS` login with RLS enforcement.

```bash
python3 integration/suborg_fixture.py --hot ../hot/target/debug/hot --backend postgres -- \
  go test -race -run '^TestSuborg' -v ./...
# For SQLite, use --backend sqlite.
```

Coverage includes provisioning and environment idempotency, identical project
names in siblings, foreign-ID and stream denial, parent runtime isolation,
org/runtime key separation (`*hot.KeyScopeError` both ways), account and
environment usage, org-scoped projects and context, restricted environment
keys, service key and session issuance and revocation, key narrowing,
rotation and revocation, issuer revocation and narrowing for existing
descendants, and child suspension and resumption.
