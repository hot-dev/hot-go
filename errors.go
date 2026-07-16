package hot

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
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
