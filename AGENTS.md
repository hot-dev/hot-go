# AGENTS.md - hot-go

This repo contains the Go SDK for the Hot Dev API.

## Package Shape

- Module path: `github.com/hot-dev/hot-go`
- Package name: `hot`
- Zero runtime dependencies; Go 1.23+ (iter.Seq2 streaming iterators)
- Payloads are `map[string]any` in the Hot API wire format (`event_type`,
  `stream_id`); the SDK never transforms user-owned payloads such as
  `event_data`. SDK-only options use Go-style names (`BaseURL`, `Timeout`).
- Versioning follows the public API package version, in lockstep with
  `@hot-dev/sdk` (npm) and `hot-dev` (PyPI).

## Commands

```bash
gofmt -l .
go vet ./...
go test -race ./...
go run ./internal/update-openapi   # refresh testdata/openapi-operations.json from a local `hot dev`
```

## Layout

- `client.go` / `transport.go` — Config, Client, JSON + raw + SSE transport
  (429 and idempotent 5xx retries, the 45 s stream idle timeout)
- `reconnect.go` — the shared waiter reconnect policy (permanent 4xx stop,
  jittered backoff, consecutive-failure budget, deadlines) and the run
  tracker (pending_retry follow-up, early stream:data buffering)
- `streams.go` — SubscribeWithEvent, WaitForRunResult, CallHot's wait
- `orgs.go` — the org API (`Orgs`, `Orgs.Env`, `APIKeys`), org API keys only
- one file per resource (mirroring hot-python's resources.py)
- `openapi_coverage_test.go` — every OpenAPI operation must map to an SDK
  method or carry an explicit ignore; fixture lives in `testdata/`

## CI and Releases

- CI runs on pushes and pull requests to `main`: gofmt, vet, `go test -race`
  on Go 1.23 (the go.mod minimum), oldstable and stable; the `hot dev`
  integration suite against the released CLI; and the org API isolation job,
  which builds hot at `integration/hot-server.ref` and runs the PostgreSQL
  sub-org fixture. A weekly scheduled run builds hot `main` instead, to catch
  API drift. Dependabot watches gomod and GitHub Actions.
- There is no registry: publishing is pushing a `v<version>` tag to the public
  repo. proxy.golang.org and pkg.go.dev index it from there. The release
  workflow runs the tests and warms the module proxy.
- Published versions are immutable and cached forever by the module proxy.
  Never delete or re-tag a version; ship a patch release (and a go.mod
  `retract` directive if needed).
- If the SDK family ever moves to 2.x, Go requires the module path to become
  `github.com/hot-dev/hot-go/v2`.

Release flow:

```bash
# Bump the Version constant in client.go.
git add client.go
git commit -m "release v<version>"
git tag v<version>
git push origin main v<version>
```

Always update the SDK version in lockstep with new API surface area added to
the matching `@hot-dev/sdk` JavaScript release when possible.

## Integration Tests

`integration/run.sh` starts a scratch `hot dev` (requires the `hot` CLI and
python3 on PATH) serving the Hot fixture project in `integration/fixture`
(project `sdk-fixture`: an echo event handler, an always-failing handler and
function, and hot:call targets). It binds 127.0.0.1 on HOT_TEST_PORT (default
4721), mints a full-access API key directly in the fixture's sqlite database,
exports HOT_TEST_BASE_URL / HOT_TEST_API_KEY, and runs `go test -run Integration`.
The integration tests skip when HOT_TEST_API_KEY is unset, so regular test
runs are unaffected. The fixture project also has a retrying handler
(`sdk:flaky`) and a streaming function (`::fixture::sdk/stream-chunks`).

`integration/suborg_fixture.py` runs the `TestSuborg*` tests against a
disposable server built from `integration/hot-server.ref` (SQLite by default,
`--backend postgres` for restricted-role PostgreSQL). When the API changes,
bump the SHA there and in ci.yml together, regenerate the OpenAPI fixture,
and reconcile `openapi_coverage_test.go`. The fixture pins the API to 127.0.0.1 because binding
"localhost" can yield an IPv6-only listener that JDK-style clients (which
dial the first resolved address only) cannot reach.

## Lockstep Releases

All five SDK repos (hot-js, hot-python, hot-go, hot-rust, hot-java) release in
lockstep. Checklist for a coordinated release:

1. Bump every SDK's version: js `npm version` (syncs src/version.ts via the
   `version` script), python pyproject.toml, rust Cargo.toml, java
   gradle.properties + README snippets, go `Version` constant in client.go.
2. Regenerate each repo's openapi-operations fixture from a current `hot dev`
   and reconcile the coverage tables.
3. Run every repo's `integration/run.sh`.
4. Commit, tag `v<version>`, push in each repo.

The integration fixture (integration/fixture, mint_key.py, run.sh) is
duplicated across all five repos; the canonical copy lives in **hot-python** —
change it there first, then sync the others.
