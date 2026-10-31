package hot

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
)

// FilesResource uploads, downloads, lists, and deletes files.
type FilesResource struct{ transport *transport }

// List returns the file list envelope (data + pagination).
func (r *FilesResource) List(ctx context.Context, query url.Values) (map[string]any, error) {
	return r.transport.requestJSON(ctx, "GET", "/files", nil, query)
}

// Get returns a single file's metadata.
func (r *FilesResource) Get(ctx context.Context, fileID string) (map[string]any, error) {
	envelope, err := r.transport.requestJSON(ctx, "GET", "/files/"+enc(fileID), nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// Delete deletes a file.
func (r *FilesResource) Delete(ctx context.Context, fileID string) error {
	return r.transport.delete(ctx, "/files/"+enc(fileID), nil)
}

// Download returns the raw HTTP response streaming the file content. The
// caller must close the response body.
func (r *FilesResource) Download(ctx context.Context, fileID string) (*http.Response, error) {
	return r.transport.requestRaw(ctx, "GET", "/files/"+enc(fileID)+"/download", nil, nil, nil)
}

// Upload uploads content to a slash-separated file path. contentType may be
// "" to let the server infer it.
func (r *FilesResource) Upload(ctx context.Context, path string, content []byte, contentType string) (map[string]any, error) {
	headers := http.Header{"Accept": {"application/json"}}
	if contentType != "" {
		headers.Set("Content-Type", contentType)
	}
	resp, err := r.transport.requestRaw(ctx, "PUT", "/files/upload/"+encPath(path), bytes.NewReader(content), headers, nil)
	if err != nil {
		return nil, err
	}
	return decodeDataResponse(resp)
}

// InitiateUpload starts a multipart upload.
func (r *FilesResource) InitiateUpload(ctx context.Context, body map[string]any) (map[string]any, error) {
	envelope, err := r.transport.requestJSON(ctx, "POST", "/files/uploads", body, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// UploadPart uploads one part of a multipart upload.
func (r *FilesResource) UploadPart(ctx context.Context, uploadID string, partNumber int, content []byte) (map[string]any, error) {
	headers := http.Header{"Accept": {"application/json"}}
	path := fmt.Sprintf("/files/uploads/%s/%d", enc(uploadID), partNumber)
	resp, err := r.transport.requestRaw(ctx, "PUT", path, bytes.NewReader(content), headers, nil)
	if err != nil {
		return nil, err
	}
	return decodeDataResponse(resp)
}

// CompleteUpload finishes a multipart upload.
func (r *FilesResource) CompleteUpload(ctx context.Context, uploadID string) (map[string]any, error) {
	envelope, err := r.transport.requestJSON(ctx, "POST", "/files/uploads/"+enc(uploadID)+"/complete", nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// AbortUpload cancels a multipart upload.
func (r *FilesResource) AbortUpload(ctx context.Context, uploadID string) error {
	return r.transport.delete(ctx, "/files/uploads/"+enc(uploadID), nil)
}
