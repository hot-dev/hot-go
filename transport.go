package hot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"math/rand/v2"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"
)

const defaultBaseURL = "https://api.hot.dev"

// JSON requests retry on 429 when the server supplies a retry delay, on 503
// event_publish_in_progress for POST event publishes, and idempotent JSON
// requests also retry on 502, 503 and 504.
const (
	maxRetries           = 2
	maxRetryAfterSeconds = 30
	maxErrorBodyBytes    = 1 << 20 // 1 MiB
)

// streamIdleTimeout ends a stream read that receives nothing at all. Hot sends
// a keepalive comment every 15 seconds, so silence this long means a dead
// connection; waiters treat it as a transient error and reconnect.
var streamIdleTimeout = 45 * time.Second

var errStreamIdle = errors.New("hot: the stream sent nothing, not even a keepalive, within the idle timeout; the connection looks dead")

type transport struct {
	token      string
	baseURL    string
	apiBaseURL string
	// pathPrefix scopes every request below /v1, e.g. /orgs/{org}/envs/{env}.
	pathPrefix string
	// scopeErr records an invalid path segment passed to Orgs.Env so every
	// call on the returned OrgEnv fails before touching HTTP.
	scopeErr   error
	httpClient *http.Client
	timeout    time.Duration
}

func newTransport(cfg Config) (*transport, error) {
	token := strings.TrimSpace(cfg.Token)
	if token == "" {
		return nil, fmt.Errorf("hot: Config.Token is required")
	}
	baseURL := normalizeBaseURL(cfg.BaseURL)

	var httpClient *http.Client
	if cfg.HTTPClient == nil {
		httpClient = &http.Client{CheckRedirect: checkRedirect}
	} else {
		copied := *cfg.HTTPClient
		if copied.CheckRedirect == nil {
			copied.CheckRedirect = checkRedirect
		}
		httpClient = &copied
	}

	return &transport{
		token:      token,
		baseURL:    baseURL,
		apiBaseURL: baseURL + "/v1",
		httpClient: httpClient,
		timeout:    cfg.Timeout,
	}, nil
}

func normalizeBaseURL(raw string) string {
	baseURL := strings.TrimSpace(raw)
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	baseURL = strings.TrimRight(baseURL, "/")
	if strings.HasSuffix(baseURL, "/v1") {
		baseURL = strings.TrimRight(strings.TrimSuffix(baseURL, "/v1"), "/")
	}
	return baseURL
}

func effectivePort(u *url.URL) string {
	if port := u.Port(); port != "" {
		return port
	}
	switch strings.ToLower(u.Scheme) {
	case "http":
		return "80"
	case "https":
		return "443"
	default:
		return ""
	}
}

func sameOrigin(a, b *url.URL) bool {
	if a == nil || b == nil {
		return false
	}
	return strings.EqualFold(a.Scheme, b.Scheme) &&
		strings.EqualFold(a.Hostname(), b.Hostname()) &&
		effectivePort(a) == effectivePort(b)
}

// checkRedirect enforces safe redirect handling: at most 10 redirects, no
// HTTPS-to-HTTP downgrades, and stripping Authorization whenever scheme,
// hostname, or port changes across any hop.
func checkRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return errors.New("hot: stopped after 10 redirects")
	}
	for _, prev := range via {
		if prev == nil || prev.URL == nil {
			continue
		}
		if strings.EqualFold(prev.URL.Scheme, "https") && !strings.EqualFold(req.URL.Scheme, "https") {
			return fmt.Errorf("hot: refusing redirect downgrade from %s to %s", prev.URL.Scheme, req.URL.Scheme)
		}
		if !sameOrigin(prev.URL, req.URL) {
			req.Header.Del("Authorization")
		}
	}
	return nil
}

// scoped returns a transport whose paths sit below prefix.
func (t *transport) scoped(prefix string, err error) *transport {
	scoped := *t
	if scoped.scopeErr == nil {
		scoped.scopeErr = err
	}
	scoped.pathPrefix = t.pathPrefix + prefix
	return &scoped
}

func normalizePath(path string) string {
	if path != "" && !strings.HasPrefix(path, "/") && !strings.HasPrefix(path, "?") {
		return "/" + path
	}
	return path
}

func appendQuery(rawURL string, query url.Values) string {
	if len(query) == 0 {
		return rawURL
	}
	encoded := query.Encode()
	if encoded == "" {
		return rawURL
	}
	if strings.Contains(rawURL, "?") {
		if strings.HasSuffix(rawURL, "?") || strings.HasSuffix(rawURL, "&") {
			return rawURL + encoded
		}
		return rawURL + "&" + encoded
	}
	return rawURL + "?" + encoded
}

func isNilBody(body any) bool {
	if body == nil {
		return true
	}
	v := reflect.ValueOf(body)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func isNon2xx(status int) bool {
	return status < 200 || status >= 300
}

func readErrorBody(r io.Reader) ([]byte, error) {
	return io.ReadAll(io.LimitReader(r, maxErrorBodyBytes))
}

func errorStream(err error) iter.Seq2[StreamEvent, error] {
	return func(yield func(StreamEvent, error) bool) {
		yield(nil, err)
	}
}

func (t *transport) newRequestWithURL(ctx context.Context, method, requestURL string, body io.Reader, sendAuth bool) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, requestURL, body)
	if err != nil {
		return nil, err
	}
	if sendAuth {
		req.Header.Set("Authorization", "Bearer "+t.token)
	}
	req.Header.Set("User-Agent", "hot-sdk-go/"+Version)
	return req, nil
}

func (t *transport) newRequest(ctx context.Context, method, path string, body io.Reader, query url.Values) (*http.Request, error) {
	normPath := normalizePath(path)
	requestURL := appendQuery(t.apiBaseURL+t.pathPrefix+normPath, query)
	return t.newRequestWithURL(ctx, method, requestURL, body, true)
}

// requestJSON calls a JSON endpoint and returns the response envelope. Empty
// bodies become {"data": nil, "meta": {}}; non-object JSON is wrapped the
// same way.
func (t *transport) requestJSON(ctx context.Context, method, path string, body any, query url.Values) (map[string]any, error) {
	if t.scopeErr != nil {
		return nil, t.scopeErr
	}
	normPath := normalizePath(path)
	fullPath := t.pathPrefix + normPath
	requestURL := appendQuery(t.apiBaseURL+fullPath, query)
	payload, err := t.doJSON(ctx, method, requestURL, fullPath, body, true)
	if err != nil {
		return nil, err
	}
	if len(payload) == 0 {
		return map[string]any{"data": nil, "meta": map[string]any{}}, nil
	}
	var parsed any
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return nil, fmt.Errorf("hot: invalid JSON response: %w", err)
	}
	if envelope, ok := parsed.(map[string]any); ok {
		return envelope, nil
	}
	return map[string]any{"data": parsed, "meta": map[string]any{}}, nil
}

// requestStatus calls GET {baseURL}/status without Authorization and returns
// the unwrapped JSON object.
func (t *transport) requestStatus(ctx context.Context) (map[string]any, error) {
	requestURL := t.baseURL + "/status"
	payload, err := t.doJSON(ctx, http.MethodGet, requestURL, "/status", nil, false)
	if err != nil {
		return nil, err
	}
	if len(payload) == 0 {
		return map[string]any{}, nil
	}
	var parsed any
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return nil, fmt.Errorf("hot: invalid JSON response: %w", err)
	}
	if object, ok := parsed.(map[string]any); ok {
		return object, nil
	}
	return nil, fmt.Errorf("hot: invalid JSON response: expected object")
}

func (t *transport) doJSON(ctx context.Context, method, requestURL, fullPath string, body any, sendAuth bool) ([]byte, error) {
	if t.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t.timeout)
		defer cancel()
	}

	var encoded []byte
	hasBody := !isNilBody(body)
	if hasBody {
		var err error
		encoded, err = json.Marshal(body)
		if err != nil {
			return nil, err
		}
	}

	for attempt := 0; ; attempt++ {
		var reader io.Reader
		if encoded != nil {
			reader = bytes.NewReader(encoded)
		}
		req, err := t.newRequestWithURL(ctx, method, requestURL, reader, sendAuth)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		if hasBody {
			req.Header.Set("Content-Type", "application/json")
		}

		resp, err := t.httpClient.Do(req)
		if err != nil {
			return nil, err
		}
		if isNon2xx(resp.StatusCode) {
			errPayload, readErr := readErrorBody(resp.Body)
			resp.Body.Close()
			if readErr != nil {
				return nil, readErr
			}
			err := apiError(fullPath, resp.StatusCode, errPayload, resp.Header)
			if delay, ok := jsonRetryDelay(method, resp.StatusCode, err, attempt); ok {
				if deadline, hasDeadline := ctx.Deadline(); hasDeadline && time.Until(deadline) < delay {
					return nil, err
				}
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return nil, fmt.Errorf("hot: %s %s: %w", method, fullPath, context.Cause(ctx))
				case <-timer.C:
				}
				continue
			}
			return nil, err
		}
		payload, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}
		return payload, nil
	}
}

func jitterRetryAfter(retryAfter time.Duration) time.Duration {
	maxDelay := time.Duration(maxRetryAfterSeconds) * time.Second
	if retryAfter >= maxDelay {
		return maxDelay
	}
	jitterCap := min(retryAfter/4, 250*time.Millisecond, maxDelay-retryAfter)
	if jitterCap <= 0 {
		return retryAfter
	}
	return retryAfter + rand.N(jitterCap+1)
}

// jsonRetryDelay decides whether a failed JSON request is retried, and after
// how long. A 429 is retried when the server supplies a retry delay (with small
// jitter). A 503 with code event_publish_in_progress is retried for POST
// requests. A 502, 503 or 504 is retried for idempotent methods, after the
// server's delay or a short backoff.
func jsonRetryDelay(method string, status int, err error, attempt int) (time.Duration, bool) {
	if attempt >= maxRetries {
		return 0, false
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return 0, false
	}
	retryAfter := time.Duration(min(max(apiErr.RetryAfter, 0), maxRetryAfterSeconds)) * time.Second
	switch status {
	case http.StatusTooManyRequests:
		if retryAfter <= 0 {
			return 0, false
		}
		return jitterRetryAfter(retryAfter), true
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		if method == http.MethodPost && status == http.StatusServiceUnavailable && apiErr.Code == "event_publish_in_progress" {
			if retryAfter > 0 {
				return jitterRetryAfter(retryAfter), true
			}
			return backoffDelay(attempt + 1), true
		}
		switch method {
		case http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete:
			return max(retryAfter, backoffDelay(attempt+1)), true
		}
	}
	return 0, false
}

// requestRaw performs a request and returns the raw response. The caller must
// close the response body. Non-2xx responses are returned as *APIError.
// Config.Timeout is intentionally not applied here: raw requests include
// downloads and streams, which a total-request timeout would sever.
func (t *transport) requestRaw(ctx context.Context, method, path string, body io.Reader, headers http.Header, query url.Values) (*http.Response, error) {
	if t.scopeErr != nil {
		return nil, t.scopeErr
	}
	normPath := normalizePath(path)
	req, err := t.newRequest(ctx, method, normPath, body, query)
	if err != nil {
		return nil, err
	}
	for key, values := range headers {
		req.Header.Del(key)
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if isNon2xx(resp.StatusCode) {
		payload, readErr := readErrorBody(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		return nil, apiError(t.pathPrefix+normPath, resp.StatusCode, payload, resp.Header)
	}
	return resp, nil
}

// delete calls a body-less DELETE through the JSON path, so Config.Timeout
// and the 429 retry apply to it like any other control call.
func (t *transport) delete(ctx context.Context, path string, query url.Values) error {
	_, err := t.requestJSON(ctx, http.MethodDelete, path, nil, query)
	return err
}

// stream opens an SSE request and yields decoded events. Iteration stops
// after yielding a non-nil error. A connection that delivers no bytes for
// streamIdleTimeout (keepalives included) ends with a transient error.
func (t *transport) stream(ctx context.Context, method, path string, body any, query url.Values) iter.Seq2[StreamEvent, error] {
	if t.scopeErr != nil {
		return errorStream(t.scopeErr)
	}
	return func(yield func(StreamEvent, error) bool) {
		var reader io.Reader
		hasBody := !isNilBody(body)
		if hasBody {
			encoded, err := json.Marshal(body)
			if err != nil {
				yield(nil, err)
				return
			}
			reader = bytes.NewReader(encoded)
		}

		streamCtx, cancel := context.WithCancelCause(ctx)
		defer cancel(nil)

		var idleMu sync.Mutex
		var idleGen uint64
		var idleTimer *time.Timer
		startIdle := func() {
			idleMu.Lock()
			if idleTimer != nil {
				idleTimer.Stop()
			}
			idleGen++
			gen := idleGen
			idleTimer = time.AfterFunc(streamIdleTimeout, func() {
				idleMu.Lock()
				fire := idleGen == gen
				idleMu.Unlock()
				if fire {
					cancel(errStreamIdle)
				}
			})
			idleMu.Unlock()
		}
		stopIdle := func() {
			idleMu.Lock()
			idleGen++
			if idleTimer != nil {
				idleTimer.Stop()
				idleTimer = nil
			}
			idleMu.Unlock()
		}
		defer stopIdle()

		// failed reports the idle timeout in place of the cancellation it caused.
		failed := func(err error) error {
			if ctx.Err() == nil && errors.Is(context.Cause(streamCtx), errStreamIdle) {
				return errStreamIdle
			}
			return err
		}

		normPath := normalizePath(path)
		req, err := t.newRequest(streamCtx, method, normPath, reader, query)
		if err != nil {
			yield(nil, err)
			return
		}
		req.Header.Set("Accept", "text/event-stream")
		if hasBody {
			req.Header.Set("Content-Type", "application/json")
		}

		startIdle()
		resp, err := t.httpClient.Do(req)
		stopIdle()
		if err != nil {
			yield(nil, failed(err))
			return
		}
		defer resp.Body.Close()
		if isNon2xx(resp.StatusCode) {
			startIdle()
			payload, readErr := readErrorBody(resp.Body)
			stopIdle()
			if readErr != nil {
				yield(nil, failed(readErr))
				return
			}
			yield(nil, apiError(t.pathPrefix+normPath, resp.StatusCode, payload, resp.Header))
			return
		}

		parser := newSSEParser(maxSSEBufferBytes)
		chunk := make([]byte, 4096)
		for {
			// Time only the server's silence, not the caller's work between events.
			startIdle()
			n, readErr := resp.Body.Read(chunk)
			stopIdle()
			if n > 0 {
				events, parseErr := parser.feed(chunk[:n])
				for _, event := range events {
					if !yield(event, nil) {
						return
					}
				}
				if parseErr != nil {
					yield(nil, parseErr)
					return
				}
			}
			if readErr == io.EOF {
				// Per WHATWG SSE §9.2.5, discard any unterminated trailing data on EOF.
				break
			}
			if readErr != nil {
				yield(nil, failed(readErr))
				return
			}
		}
	}
}

// dataObject extracts the envelope's "data" member when it is an object.
func dataObject(envelope map[string]any) map[string]any {
	object, _ := envelope["data"].(map[string]any)
	return object
}

func decodeDataResponse(resp *http.Response) (map[string]any, error) {
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if len(payload) == 0 {
		return nil, nil
	}
	var envelope map[string]any
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return nil, fmt.Errorf("hot: invalid JSON response: %w", err)
	}
	return dataObject(envelope), nil
}
