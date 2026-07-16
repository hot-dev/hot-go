# hot-go — Implementation Plan

Official Go SDK for the Hot Dev API. Mirrors `hot-python` (Layer 1 only: client,
resources, SSE streaming, errors) and follows the shared SDK conventions.

## Package identity

- Module path: **`github.com/hot-dev/hot-go`** — in Go the module path *is* the
  package identity, so the repo must live (publicly) at exactly that URL.
- Package name: `hot` (import `"github.com/hot-dev/hot-go"`, use as
  `hot.NewClient(...)` — same pattern as `yaml.v3` etc.).
- License: Apache-2.0 (a recognized license is required for pkg.go.dev to render docs).
- Go version: require **go 1.23+** (for `iter.Seq2` streaming iterators).
- Versioning: lockstep with the other SDKs; tags `v<version>`.
  ⚠️ **Go-specific caveat**: if the SDK family ever moves to 2.x, Go requires the
  module path to become `github.com/hot-dev/hot-go/v2`. Plan for it; it's a
  mechanical but breaking change.

## Tech choices

- **Zero runtime dependencies**: `net/http`, `encoding/json`, `bufio` for SSE.
  This is a major selling point for a Go SDK; nothing in the API surface needs more.
- **Context everywhere**: every API method takes `context.Context` as its first
  argument. No separate sync/async split — goroutines are the async story.
- **Config**: `hot.NewClient(hot.Config{Token, BaseURL, HTTPClient, Timeout})`.
  `BaseURL` defaults to `https://api.hot.dev`; custom `*http.Client` is the
  retry/proxy escape hatch (mirrors the `httpx.Client` injection in Python).
- **Typing policy**: hybrid. Thin typed structs for stable envelope fields
  (`StreamID`, `RunID`, `EventType`, error body), `json.RawMessage` /
  `map[string]any` for user-owned payloads (`event_data`, run results) — the SDK
  never transforms user-owned payloads. Full typed models from OpenAPI are a
  later release, same as Python. Untyped-everything (Python-style) reads badly
  in Go; fully-typed-everything breaks version lockstep economics. Hybrid is the
  right middle.
- **Errors**: `*hot.APIError` implementing `error`, with `StatusCode`, `Code`,
  `RequestID`, `RetryAfter` fields; callers use `errors.As`.

## Architecture

```
client.go       — Client struct, NewClient, Request()/RequestRaw() escape hatches
transport.go    — auth header, JSON round-trip, error mapping
errors.go
sse.go          — SSE parser over bufio.Scanner
streams.go      — Subscribe / SubscribeWithEvent returning iter.Seq2[StreamEvent, error]
                  (plus a lower-level *Subscription with Next() for pre-rangefunc users
                  if we want; start with iter.Seq2 only), WaitForRunResult
builds.go, contextvars.go, domains.go, env.go, events.go, files.go, org.go,
projects.go, runs.go, servicekeys.go, sessions.go   — 12 resource namespaces,
                  ~59 operations, exposed as client.Events.Publish(ctx, ...) etc.
```

Behavioral contract to preserve:
- Auto-reconnect across the API's 5-minute SSE timeout (opt-out), and
  `SubscribeWithEvent` stops after the first terminal `run:*` event.
- Casing policy: wire format in payload maps (`event_type`, `stream_id`);
  Go-idiomatic names for SDK options (`BaseURL`, `Timeout`).
- `files` multipart upload/download; `builds` upload/download as `io.Reader`.

## Implementation phases

1. Scaffold: `go mod init github.com/hot-dev/hot-go`, CI, LICENSE, README, AGENTS.md.
2. Transport + errors + `Request`/`RequestRaw`.
3. Resource namespaces (port from `hot-python/src/hot/resources.py`).
4. SSE parser + reconnect + `SubscribeWithEvent` + `WaitForRunResult`.
5. Docs: package-level doc comment with the quick-start example, `Example*`
   test functions (these render on pkg.go.dev), README mirroring JS/Python.
6. OpenAPI coverage test + fixture.

## Testing

- **Unit/integration**: `net/http/httptest.Server` — no mocking library needed.
  Cover JSON endpoints, error mapping, and SSE reconnect using a handler that
  streams events and drops the connection (mirror `test_reconnect.py`).
  Run with `-race`.
- **OpenAPI coverage**: check in `testdata/openapi-operations.json` (same format
  as the other SDKs) + a small `go run ./internal/update-openapi` tool fetching
  `http://localhost:4681/openapi.json`. Test asserts every operation has a
  resource method.
- **Live smoke test** (optional): behind a build tag or `HOT_API_KEY` env guard.
- CI: `gofmt -l`, `go vet`, `golangci-lint`, `go test -race ./...` on the two
  most recent Go versions, pushes/PRs to `main`.

## Publishing (no registry — the repo is the release)

There is nothing to configure with any registry. One-time setup (Curt):
1. Create the **public** GitHub repo `hot-dev/hot-go`. Public visibility is the
   publish switch — proxy.golang.org can't see private repos.
2. That's it. No tokens, no accounts, no signing.

Release flow:
- Bump nothing in-code (Go has no version file) — commit, tag `v<version>`, push.
- `proxy.golang.org` indexes the tag lazily on first `go get`; to make it
  immediate, run
  `GOPROXY=proxy.golang.org go list -m github.com/hot-dev/hot-go@v<version>`
  (worth putting in the release workflow as a post-tag step).
- pkg.go.dev picks up docs from the proxy shortly after.
- ⚠️ Published versions are **immutable and cached forever** by the module
  proxy — never delete/re-tag a version; ship a patch release instead. A
  `retract` directive in go.mod is the recall mechanism.
- Optional release workflow: on `v*` tag, run the full test suite and the
  `go list` warm-up. No artifacts to upload.

## Open questions

- Whether to also expose a channel-based subscription API alongside `iter.Seq2`
  (recommend no — one way to do it).
- Struct-vs-map boundary per endpoint: decide during the resources port, biasing
  toward `map[string]any` wherever Python uses bare dicts today.
