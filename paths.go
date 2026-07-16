package hot

import (
	"net/url"
	"strings"
)

// enc escapes a single path segment, including any "/".
func enc(segment string) string {
	return url.PathEscape(segment)
}

// encPath escapes each segment of a slash-separated path, keeping the slashes.
func encPath(path string) string {
	parts := strings.Split(path, "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return strings.Join(parts, "/")
}
