package hot

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// APIError is the structured error returned for non-2xx Hot API responses.
type APIError struct {
	StatusCode int
	Message    string
	Code       string
	RequestID  string
	// RetryAfter is the server-suggested retry delay in seconds, from the
	// error body or Retry-After header. Zero when the server sent none.
	RetryAfter int
	Headers    http.Header
	// ErrorBody is the raw "error" object from the response body, if any.
	ErrorBody map[string]any
}

func (e *APIError) Error() string {
	return fmt.Sprintf("hot: %s (status %d)", e.Message, e.StatusCode)
}

func parseAPIError(statusCode int, body []byte, headers http.Header) *APIError {
	var parsed map[string]any
	_ = json.Unmarshal(body, &parsed)

	errObj, ok := parsed["error"].(map[string]any)
	if !ok {
		message := string(body)
		if message == "" {
			message = fmt.Sprintf("Hot API error (%d)", statusCode)
		}
		return &APIError{
			StatusCode: statusCode,
			Message:    message,
			Headers:    headers,
			RetryAfter: retryAfterSeconds(nil, headers),
		}
	}

	message, _ := errObj["message"].(string)
	if message == "" {
		message = fmt.Sprintf("Hot API error (%d)", statusCode)
	}
	apiErr := &APIError{
		StatusCode: statusCode,
		Message:    message,
		Headers:    headers,
		ErrorBody:  errObj,
		RetryAfter: retryAfterSeconds(errObj["retry_after"], headers),
	}
	if code := errObj["code"]; code != nil {
		apiErr.Code = fmt.Sprint(code)
	}
	if requestID := errObj["request_id"]; requestID != nil {
		apiErr.RequestID = fmt.Sprint(requestID)
	}
	return apiErr
}

func retryAfterSeconds(value any, headers http.Header) int {
	if seconds, ok := value.(float64); ok {
		return int(seconds)
	}
	if header := headers.Get("Retry-After"); header != "" {
		if seconds, err := strconv.Atoi(header); err == nil {
			return seconds
		}
	}
	return 0
}

// KeyScopeError is returned for 403 key_scope_mismatch: the credential is
// the wrong kind of key for the route. Org API keys call only the org API
// (/v1/orgs and /v1/api-keys); environment API keys, sessions and service
// keys call every other route. It wraps the *APIError, so errors.As with an
// *APIError also matches.
type KeyScopeError struct {
	*APIError
	// RequiredScope is "org" when the call needs an org API key and "env"
	// when it needs an environment API key, session or service key.
	RequiredScope string
}

func (e *KeyScopeError) Error() string {
	if e.RequiredScope == "org" {
		return "hot: this call needs an org API key; environment API keys, sessions and service keys only call runtime routes (status 403, key_scope_mismatch)"
	}
	return "hot: this call needs an environment API key, session or service key; org API keys only call /v1/orgs and /v1/api-keys (status 403, key_scope_mismatch)"
}

// Unwrap returns the underlying *APIError.
func (e *KeyScopeError) Unwrap() error { return e.APIError }

// requiredScope names the kind of key a /v1-relative path needs.
func requiredScope(path string) string {
	for _, prefix := range []string{"/orgs", "/api-keys"} {
		if path == prefix || strings.HasPrefix(path, prefix+"/") || strings.HasPrefix(path, prefix+"?") {
			return "org"
		}
	}
	return "env"
}

// apiError builds the error for a non-2xx response to a /v1-relative path.
func apiError(path string, statusCode int, body []byte, headers http.Header) error {
	apiErr := parseAPIError(statusCode, body, headers)
	if statusCode == http.StatusForbidden && apiErr.Code == "key_scope_mismatch" {
		return &KeyScopeError{APIError: apiErr, RequiredScope: requiredScope(path)}
	}
	return apiErr
}

// isPermanentError reports whether err must end a durable wait instead of
// reconnecting: any 4xx response except 408 and 429 (bad request,
// authorization including key_scope_mismatch, not found, gone, ...).
func isPermanentError(err error) bool {
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	status := apiErr.StatusCode
	return status >= 400 && status < 500 &&
		status != http.StatusRequestTimeout && status != http.StatusTooManyRequests
}
