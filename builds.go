package hot

import (
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
)

// BuildsResource uploads, deploys, and inspects builds.
type BuildsResource struct{ transport *transport }

// BuildUpload describes a build archive to upload.
type BuildUpload struct {
	// File is the build archive content. Required.
	File io.Reader
	// Filename names the multipart file part. Defaults to "build".
	Filename string
	// Hash is the build content hash. Required.
	Hash string
	// BuildID optionally pins the build id.
	BuildID string
}

// List returns the build list envelope across all projects.
func (r *BuildsResource) List(ctx context.Context, query url.Values) (map[string]any, error) {
	return r.transport.requestJSON(ctx, "GET", "/builds", nil, query)
}

// ListForProject returns the build list envelope for one project.
func (r *BuildsResource) ListForProject(ctx context.Context, project string, query url.Values) (map[string]any, error) {
	projectSeg, err := encSegment("project", project)
	if err != nil {
		return nil, err
	}
	return r.transport.requestJSON(ctx, "GET", "/projects/"+projectSeg+"/builds", nil, query)
}

// Get returns a single build.
func (r *BuildsResource) Get(ctx context.Context, project, buildID string) (map[string]any, error) {
	projectSeg, err := encSegment("project", project)
	if err != nil {
		return nil, err
	}
	buildSeg, err := encSegment("build_id", buildID)
	if err != nil {
		return nil, err
	}
	envelope, err := r.transport.requestJSON(ctx, "GET", "/projects/"+projectSeg+"/builds/"+buildSeg, nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// Deployed returns the project's deployed build.
func (r *BuildsResource) Deployed(ctx context.Context, project string) (map[string]any, error) {
	projectSeg, err := encSegment("project", project)
	if err != nil {
		return nil, err
	}
	envelope, err := r.transport.requestJSON(ctx, "GET", "/projects/"+projectSeg+"/builds/deployed", nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// Live returns the project's live build.
func (r *BuildsResource) Live(ctx context.Context, project string) (map[string]any, error) {
	projectSeg, err := encSegment("project", project)
	if err != nil {
		return nil, err
	}
	envelope, err := r.transport.requestJSON(ctx, "GET", "/projects/"+projectSeg+"/builds/live", nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// Upload uploads a build archive as multipart form data.
func (r *BuildsResource) Upload(ctx context.Context, project string, upload BuildUpload) (map[string]any, error) {
	projectSeg, err := encSegment("project", project)
	if err != nil {
		return nil, err
	}
	if upload.File == nil {
		return nil, fmt.Errorf("hot: BuildUpload.File is required")
	}
	if strings.TrimSpace(upload.Hash) == "" {
		return nil, fmt.Errorf("hot: BuildUpload.Hash is required")
	}

	body, contentType, err := encodeBuildUpload(upload)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	headers := http.Header{
		"Accept":       {"application/json"},
		"Content-Type": {contentType},
	}
	resp, err := r.transport.requestRaw(ctx, "POST", "/projects/"+projectSeg+"/builds", body, headers, nil)
	if err != nil {
		return nil, err
	}
	return decodeDataResponse(resp)
}

// Download returns the raw HTTP response streaming the build archive. The
// caller must close the response body.
func (r *BuildsResource) Download(ctx context.Context, project, buildID string) (*http.Response, error) {
	projectSeg, err := encSegment("project", project)
	if err != nil {
		return nil, err
	}
	buildSeg, err := encSegment("build_id", buildID)
	if err != nil {
		return nil, err
	}
	return r.transport.requestRaw(ctx, "GET", "/projects/"+projectSeg+"/builds/"+buildSeg+"/download", nil, nil, nil)
}

// Deploy deploys a build.
func (r *BuildsResource) Deploy(ctx context.Context, project, buildID string) (map[string]any, error) {
	projectSeg, err := encSegment("project", project)
	if err != nil {
		return nil, err
	}
	buildSeg, err := encSegment("build_id", buildID)
	if err != nil {
		return nil, err
	}
	envelope, err := r.transport.requestJSON(ctx, "POST", "/projects/"+projectSeg+"/builds/"+buildSeg+"/deploy", nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

func encodeBuildUpload(upload BuildUpload) (*io.PipeReader, string, error) {
	reader, writer := io.Pipe()
	form := multipart.NewWriter(writer)

	go func() {
		err := func() error {
			if err := form.WriteField("hash", upload.Hash); err != nil {
				return err
			}
			if upload.BuildID != "" {
				if err := form.WriteField("build_id", upload.BuildID); err != nil {
					return err
				}
			}
			filename := upload.Filename
			if filename == "" {
				filename = "build"
			}
			part, err := form.CreateFormFile("file", filename)
			if err != nil {
				return err
			}
			if _, err := io.Copy(part, upload.File); err != nil {
				return err
			}
			return form.Close()
		}()
		writer.CloseWithError(err)
	}()

	return reader, form.FormDataContentType(), nil
}
