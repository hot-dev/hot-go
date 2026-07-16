package hot

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"time"
)

// Version is the SDK version, kept in lockstep with the other Hot SDKs and
// bumped as part of the release flow (Go has no version manifest to read).
const Version = "1.1.3"

// Config configures a Client.
type Config struct {
	// Token is the Hot API key, session token, or service key. Required.
	Token string
	// BaseURL defaults to https://api.hot.dev. For local development with
	// `hot dev`, use "http://localhost:4681".
	BaseURL string
	// Timeout bounds each JSON request when set. It is not applied to
	// streaming or raw requests; use context deadlines for those.
	Timeout time.Duration
	// HTTPClient overrides the underlying *http.Client, e.g. to configure
	// proxies or transports. Do not set http.Client.Timeout on a client used
	// for SSE subscriptions — it would sever long-lived streams.
	HTTPClient *http.Client
}

// Client is a Hot API v1 client. Construct it with NewClient; the resource
// fields mirror the Hot API resources.
type Client struct {
	transport *transport

	Builds      *BuildsResource
	Context     *ContextResource
	Domains     *DomainsResource
	Env         *EnvResource
	Events      *EventsResource
	Files       *FilesResource
	Org         *OrgResource
	Projects    *ProjectsResource
	Runs        *RunsResource
	ServiceKeys *ServiceKeysResource
	Sessions    *SessionsResource
	Streams     *StreamsResource
}

// NewClient returns a Client for the Hot API.
func NewClient(cfg Config) (*Client, error) {
	t, err := newTransport(cfg)
	if err != nil {
		return nil, err
	}
	return &Client{
		transport:   t,
		Builds:      &BuildsResource{t},
		Context:     &ContextResource{t},
		Domains:     &DomainsResource{t},
		Env:         &EnvResource{t},
		Events:      &EventsResource{t},
		Files:       &FilesResource{t},
		Org:         &OrgResource{t},
		Projects:    &ProjectsResource{t},
		Runs:        &RunsResource{t},
		ServiceKeys: &ServiceKeysResource{t},
		Sessions:    &SessionsResource{t},
		Streams:     &StreamsResource{t},
	}, nil
}

// BaseURL returns the configured base URL without the /v1 suffix.
func (c *Client) BaseURL() string { return c.transport.baseURL }

// Request calls a JSON API endpoint that does not yet have a resource helper.
// path is relative to /v1 and the returned map is the response envelope.
func (c *Client) Request(ctx context.Context, method, path string, body any, query url.Values) (map[string]any, error) {
	return c.transport.requestJSON(ctx, method, path, body, query)
}

// RequestRaw calls an API endpoint and returns the raw HTTP response. The
// caller must close the response body. Non-2xx responses are returned as
// *APIError.
func (c *Client) RequestRaw(ctx context.Context, method, path string, body io.Reader, headers http.Header, query url.Values) (*http.Response, error) {
	return c.transport.requestRaw(ctx, method, path, body, headers, query)
}
