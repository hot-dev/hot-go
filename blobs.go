package hot

import (
	"context"
	"net/http"
)

// BlobsResource reads large payloads that the server spilled to blob
// storage. It needs an environment API key with read access to the blob
// (blob:{blob_ref_id}); it is not available on the org-scoped environment
// view returned by Orgs.Env.
type BlobsResource struct{ transport *transport }

// Download returns the raw HTTP response streaming the blob's bytes from the
// caller's environment. The caller must close the response body. String
// payloads are served as UTF-8 text.
func (r *BlobsResource) Download(ctx context.Context, blobRefID string) (*http.Response, error) {
	return r.transport.requestRaw(ctx, "GET", "/blobs/"+enc(blobRefID)+"/download", nil, nil, nil)
}
