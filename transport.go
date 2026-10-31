package hot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const defaultBaseURL = "https://api.hot.dev"

// JSON requests retry on 429 when the server supplies a retry delay, and
// idempotent JSON requests also retry on 502, 503 and 504.
const (
	maxRetries           = 2
	maxRetryAfterSeconds = 30
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
	httpClient *http.Client
	timeout    time.Duration
}

func newTransport(cfg Config) (*transport, error) {
	token := strings.TrimSpace(cfg.Token)
	if token == "" {
		return nil, fmt.Errorf("hot: Config.Token is required")
	}
	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	baseURL = strings.TrimRight(baseURL, "/")
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	return &transport{
		token:      token,
		baseURL:    baseURL,
		apiBaseURL: baseURL + "/v1",
		httpClient: httpClient,
		timeout:    cfg.Timeout,
	}, nil
}

// scoped returns a transport whose paths sit below prefix.
func (t *transport) scoped(prefix string) *transport {
	scoped := *t
	scoped.pathPrefix = t.pathPrefix + prefix
	return &scoped
}

func (t *transport) newRequest(ctx context.Context, method, path string, body io.Reader, query url.Values) (*http.Request, error) {
	requestURL := t.apiBaseURL + t.pathPrefix + path
	if len(query) > 0 {
		requestURL += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, requestURL, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+t.token)
	req.Header.Set("User-Agent", "hot-sdk-go/"+Version)
	return req, nil
}

// requestJSON calls a JSON endpoint and returns the response envelope. Empty
// bodies become {"data": nil, "meta": {}}; non-object JSON is wrapped the
// same way.
func (t *transport) requestJSON(ctx context.Context, method, path string, body any, query url.Values) (map[string]any, error) {
	if t.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t.timeout)
		defer cancel()
	}

	var encoded []byte
	if body != nil {
		var err error
		encoded, err = json.Marshal(body)
		if err != nil {
			return nil, err
		}
	}

	var payload []byte
	for attempt := 0; ; attempt++ {
		var reader io.Reader
		if encoded != nil {
			reader = bytes.NewReader(encoded)
		}
		req, err := t.newRequest(ctx, method, path, reader, query)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}

		resp, err := t.httpClient.Do(req)
		if err != nil {
			return nil, err
		}
		payload, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}
		if resp.StatusCode >= 400 {
			err := apiError(t.pathPrefix+path, resp.StatusCode, payload, resp.Header)
			if delay, ok := jsonRetryDelay(method, resp.StatusCode, err, attempt); ok {
				select {
				case <-ctx.Done():
					return nil, fmt.Errorf("hot: %s %s: %w", method, path, context.Cause(ctx))
				case <-time.After(delay):
				}
				continue
			}
			return nil, err
		}
		break
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

// jsonRetryDelay decides whether a failed JSON request is retried, and after
// how long. A 429 is retried when the server supplies a retry delay. A 502,
// 503 or 504 is retried for idempotent methods only, after the server's
// delay or a short backoff.
func jsonRetryDelay(method string, status int, err error, attempt int) (time.Duration, bool) {
	if attempt >= maxRetries {
		return 0, false
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return 0, false
	}
	retryAfter := time.Duration(min(apiErr.RetryAfter, maxRetryAfterSeconds)) * time.Second
	switch status {
	case http.StatusTooManyRequests:
		return retryAfter, retryAfter > 0
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
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
	req, err := t.newRequest(ctx, method, path, body, query)
	if err != nil {
		return nil, err
	}
	for key, values := range headers {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		payload, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, apiError(t.pathPrefix+path, resp.StatusCode, payload, resp.Header)
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
	return func(yield func(StreamEvent, error) bool) {
		var reader io.Reader
		if body != nil {
			encoded, err := json.Marshal(body)
			if err != nil {
				yield(nil, err)
				return
			}
			reader = bytes.NewReader(encoded)
		}

		streamCtx, cancel := context.WithCancelCause(ctx)
		defer cancel(nil)
		idle := time.AfterFunc(streamIdleTimeout, func() { cancel(errStreamIdle) })
		defer idle.Stop()
		// failed reports the idle timeout in place of the cancellation it caused.
		failed := func(err error) error {
			if ctx.Err() == nil && errors.Is(context.Cause(streamCtx), errStreamIdle) {
				return errStreamIdle
			}
			return err
		}

		req, err := t.newRequest(streamCtx, method, path, reader, query)
		if err != nil {
			yield(nil, err)
			return
		}
		req.Header.Set("Accept", "text/event-stream")
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}

		resp, err := t.httpClient.Do(req)
		if err != nil {
			yield(nil, failed(err))
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 400 {
			payload, _ := io.ReadAll(resp.Body)
			yield(nil, apiError(t.pathPrefix+path, resp.StatusCode, payload, resp.Header))
			return
		}

		buffer := ""
		chunk := make([]byte, 4096)
		for {
			// Time only the server's silence, not the caller's work between events.
			idle.Reset(streamIdleTimeout)
			n, readErr := resp.Body.Read(chunk)
			idle.Stop()
			if n > 0 {
				buffer += string(chunk[:n])
				var events []StreamEvent
				events, buffer = consumeSSEBlocks(buffer)
				for _, event := range events {
					if !yield(event, nil) {
						return
					}
				}
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				yield(nil, failed(readErr))
				return
			}
		}

		events, _ := consumeSSEBlocks(buffer + "\n\n")
		for _, event := range events {
			if !yield(event, nil) {
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
	var envelope map[string]any
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return nil, fmt.Errorf("hot: invalid JSON response: %w", err)
	}
	return dataObject(envelope), nil
}
