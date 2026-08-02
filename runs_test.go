package hot

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

func runUpdate(status, result string) string {
	return `{"type":"run:update","run":{"run_id":"r1","status":"` + status + `","result":"` + result + `"}}`
}

func TestRunWaitReconnectsAndReturnsSucceededSnapshot(t *testing.T) {
	handler := &sseSequenceHandler{t: t, responses: [][]string{
		{runUpdate("running", "")},
		{runUpdate("succeeded", "done")},
	}}
	client := newTestClient(t, handler)
	run, err := client.Runs.Wait(context.Background(), "r1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if run["status"] != "succeeded" || run["result"] != "done" {
		t.Fatalf("run = %#v", run)
	}
}

func TestRunWaitReturnsStructuredFailure(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeSSE(t, w, runUpdate("failed", "boom"))
	}))
	_, err := client.Runs.Wait(context.Background(), "r1", nil)
	var runErr *RunWaitError
	if !errors.As(err, &runErr) {
		t.Fatalf("err = %v, want *RunWaitError", err)
	}
	if runErr.Run["status"] != "failed" || runErr.Error() != "hot: boom" {
		t.Fatalf("run error = %#v (%v)", runErr.Run, runErr)
	}
}
