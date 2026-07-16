package hot

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const defaultBaseURL = "https://api.hot.dev"

// JSON requests retry on 429 when the server supplies a retry delay.
const (
	maxRetries           = 2
	maxRetryAfterSeconds = 30
)

type transport struct {
	token      string
	baseURL    string
	apiBaseURL string
	httpClient *http.Client
	timeout    time.Duration
}

func newTransport(cfg Config) (*transport, error) {
	if cfg.Token == "" {
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
		token:      cfg.Token,
		baseURL:    baseURL,
		apiBaseURL: baseURL + "/v1",
		httpClient: httpClient,
		timeout:    cfg.Timeout,
	}, nil
}

func (t *transport) newRequest(ctx context.Context, method, path string, body io.Reader, query url.Values) (*http.Request, error) {
	requestURL := t.apiBaseURL + path
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
			apiErr := parseAPIError(resp.StatusCode, payload, resp.Header)
			if resp.StatusCode == http.StatusTooManyRequests &&
				attempt < maxRetries && apiErr.RetryAfter > 0 {
				delay := min(apiErr.RetryAfter, maxRetryAfterSeconds)
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(time.Duration(delay) * time.Second):
				}
				continue
			}
			return nil, apiErr
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
		return nil, parseAPIError(resp.StatusCode, payload, resp.Header)
	}
	return resp, nil
}

// stream opens an SSE request and yields decoded events. Iteration stops
// after yielding a non-nil error.
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

		req, err := t.newRequest(ctx, method, path, reader, query)
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
			yield(nil, err)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 400 {
			payload, _ := io.ReadAll(resp.Body)
			yield(nil, parseAPIError(resp.StatusCode, payload, resp.Header))
			return
		}

		buffer := ""
		chunk := make([]byte, 4096)
		for {
			n, readErr := resp.Body.Read(chunk)
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
				yield(nil, readErr)
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
