package hot

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
)

// coverage maps every OpenAPI operation to the SDK method covering it, or ""
// for operations intentionally left out. Regenerate the fixture with:
//
//	go run ./internal/update-openapi
var coverage = map[string]string{
	// Public health endpoint, not part of authenticated SDK resources.
	"GET /status": "",

	"GET /v1/projects":                                     "Projects.List",
	"POST /v1/projects":                                    "Projects.Create",
	"GET /v1/projects/{project_id_or_slug}":                "Projects.Get",
	"PATCH /v1/projects/{project_id_or_slug}":              "Projects.Update",
	"DELETE /v1/projects/{project_id_or_slug}":             "Projects.Delete",
	"POST /v1/projects/{project_id_or_slug}/activate":      "Projects.Activate",
	"POST /v1/projects/{project_id_or_slug}/deactivate":    "Projects.Deactivate",
	"GET /v1/projects/{project_id_or_slug}/event-handlers": "Projects.EventHandlers",
	"GET /v1/projects/{project_id_or_slug}/schedules":      "Projects.Schedules",

	"GET /v1/builds": "Builds.List",
	"GET /v1/projects/{project_id_or_slug}/builds":                     "Builds.ListForProject",
	"POST /v1/projects/{project_id_or_slug}/builds":                    "Builds.Upload",
	"GET /v1/projects/{project_id_or_slug}/builds/deployed":            "Builds.Deployed",
	"GET /v1/projects/{project_id_or_slug}/builds/live":                "Builds.Live",
	"GET /v1/projects/{project_id_or_slug}/builds/{build_id}":          "Builds.Get",
	"GET /v1/projects/{project_id_or_slug}/builds/{build_id}/download": "Builds.Download",
	"POST /v1/projects/{project_id_or_slug}/builds/{build_id}/deploy":  "Builds.Deploy",

	"GET /v1/projects/{project_id_or_slug}/context":          "Context.List",
	"POST /v1/projects/{project_id_or_slug}/context":         "Context.Create",
	"PUT /v1/projects/{project_id_or_slug}/context/{key}":    "Context.Update",
	"DELETE /v1/projects/{project_id_or_slug}/context/{key}": "Context.Delete",

	"GET /v1/runs":          "Runs.List",
	"GET /v1/runs/stats":    "Runs.Stats",
	"GET /v1/runs/{run_id}": "Runs.Get",

	"POST /v1/events":                "Events.Publish",
	"GET /v1/events":                 "Events.List",
	"GET /v1/events/{event_id}":      "Events.Get",
	"GET /v1/events/{event_id}/runs": "Events.GetRuns",

	"POST /v1/sessions":                "Sessions.Create",
	"GET /v1/sessions":                 "Sessions.List",
	"DELETE /v1/sessions":              "Sessions.RevokeAll",
	"DELETE /v1/sessions/{session_id}": "Sessions.Revoke",

	"POST /v1/service-keys":                    "ServiceKeys.Create",
	"GET /v1/service-keys":                     "ServiceKeys.List",
	"DELETE /v1/service-keys":                  "ServiceKeys.RevokeAll",
	"GET /v1/service-keys/{service_key_id}":    "ServiceKeys.Get",
	"PATCH /v1/service-keys/{service_key_id}":  "ServiceKeys.Update",
	"DELETE /v1/service-keys/{service_key_id}": "ServiceKeys.Revoke",

	"POST /v1/domains":                    "Domains.Create",
	"GET /v1/domains":                     "Domains.List",
	"GET /v1/domains/{domain_id}":         "Domains.Get",
	"DELETE /v1/domains/{domain_id}":      "Domains.Delete",
	"POST /v1/domains/{domain_id}/verify": "Domains.Verify",

	"GET /v1/env":           "Env.Get",
	"GET /v1/env/subscribe": "Env.Subscribe",

	"GET /v1/org/usage": "Org.Usage",

	"GET /v1/streams/{stream_id}/subscribe":  "Streams.Subscribe",
	"POST /v1/streams/{stream_id}/subscribe": "Streams.SubscribePost",
	"POST /v1/streams/subscribe-with-event":  "Streams.SubscribeWithEvent",

	"GET /v1/files":                                   "Files.List",
	"GET /v1/files/{file_id}":                         "Files.Get",
	"DELETE /v1/files/{file_id}":                      "Files.Delete",
	"GET /v1/files/{file_id}/download":                "Files.Download",
	"PUT /v1/files/upload/{path}":                     "Files.Upload",
	"POST /v1/files/uploads":                          "Files.InitiateUpload",
	"PUT /v1/files/uploads/{upload_id}/{part_number}": "Files.UploadPart",
	"POST /v1/files/uploads/{upload_id}/complete":     "Files.CompleteUpload",
	"DELETE /v1/files/uploads/{upload_id}":            "Files.AbortUpload",
}

func TestOpenAPIOperationsHaveSDKCoverageOrExplicitIgnore(t *testing.T) {
	payload, err := os.ReadFile("testdata/openapi-operations.json")
	if err != nil {
		t.Fatal(err)
	}
	var operations []struct {
		Method string `json:"method"`
		Path   string `json:"path"`
	}
	if err := json.Unmarshal(payload, &operations); err != nil {
		t.Fatal(err)
	}

	operationKeys := map[string]bool{}
	for _, operation := range operations {
		operationKeys[fmt.Sprintf("%s %s", operation.Method, operation.Path)] = true
	}

	for key := range coverage {
		if !operationKeys[key] {
			t.Errorf("coverage entry %q is not in the OpenAPI fixture", key)
		}
	}
	for key := range operationKeys {
		if _, ok := coverage[key]; !ok {
			t.Errorf("OpenAPI operation %q has no coverage entry", key)
		}
	}
}

func TestCoveredOperationsPointToExistingSDKMethods(t *testing.T) {
	client, err := NewClient(Config{Token: "test-token"})
	if err != nil {
		t.Fatal(err)
	}

	for key, sdk := range coverage {
		if sdk == "" {
			continue
		}
		parts := strings.SplitN(sdk, ".", 2)
		field := reflect.ValueOf(client).Elem().FieldByName(parts[0])
		if !field.IsValid() {
			t.Errorf("%s -> %s: no resource %q on Client", key, sdk, parts[0])
			continue
		}
		if !field.MethodByName(parts[1]).IsValid() {
			t.Errorf("%s -> %s: no method %q on %s", key, sdk, parts[1], parts[0])
		}
	}
}
