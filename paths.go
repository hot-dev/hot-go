package hot

import (
	"fmt"
	"net/url"
	"strings"
)

type invalidPathError struct {
	msg string
}

func (e *invalidPathError) Error() string { return e.msg }

// encSegment validates and escapes a single URL path segment, including any "/".
// Empty segments and dot segments ("." and "..") are rejected before any HTTP
// request is sent.
func encSegment(name, segment string) (string, error) {
	if segment == "" || segment == "." || segment == ".." {
		return "", &invalidPathError{msg: fmt.Sprintf("hot: invalid %s path segment %q", name, segment)}
	}
	return url.PathEscape(segment), nil
}

// encPath validates and escapes each segment of a slash-separated path, keeping
// the slashes. Leading slashes are stripped; empty paths and empty or dot
// segments ("", ".", "..") are rejected before any HTTP request is sent.
func encPath(path string) (string, error) {
	trimmed := strings.TrimLeft(path, "/")
	if trimmed == "" {
		return "", &invalidPathError{msg: fmt.Sprintf("hot: invalid path %q", path)}
	}
	parts := strings.Split(trimmed, "/")
	for i, part := range parts {
		if part == "" || part == "." || part == ".." {
			return "", &invalidPathError{msg: fmt.Sprintf("hot: invalid path %q: invalid segment %q", path, part)}
		}
		parts[i] = url.PathEscape(part)
	}
	return strings.Join(parts, "/"), nil
}
