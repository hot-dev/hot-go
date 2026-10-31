package hot

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
)

func TestBlobsDownloadStreamsRawBytes(t *testing.T) {
	// Not valid UTF-8, so the test fails if the bytes are decoded as text.
	want := []byte{0x00, 0xff, 0xfe, 'h', 'o', 't', '\n', 0x80}
	var mu sync.Mutex
	var gotMethod, gotURI, gotAuth string
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotMethod, gotURI, gotAuth = r.Method, r.RequestURI, r.Header.Get("Authorization")
		mu.Unlock()
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(want)
	}))

	resp, err := client.Blobs.Download(context.Background(), "blob/ref 1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if gotMethod != "GET" || gotURI != "/v1/blobs/blob%2Fref%201/download" {
		t.Errorf("request = %s %s, want GET /v1/blobs/blob%%2Fref%%201/download", gotMethod, gotURI)
	}
	if gotAuth != "Bearer test-token" {
		t.Errorf("Authorization = %q", gotAuth)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("body = %v, want %v", got, want)
	}
}

func TestBlobsDownloadReturnsAPIErrors(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		writeJSON(t, w, map[string]any{"error": map[string]any{"code": "not_found", "message": "no such blob", "request_id": "req"}})
	}))

	resp, err := client.Blobs.Download(context.Background(), "missing")
	var apiErr *APIError
	if resp != nil || !errors.As(err, &apiErr) || apiErr.StatusCode != 404 || apiErr.Code != "not_found" {
		t.Fatalf("resp = %v, err = %v, want a 404 *APIError", resp, err)
	}
}
