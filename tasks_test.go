package hot

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

func taskUpdate(status, result string) string {
	return `{"type":"task:update","task":{"task_id":"t1","status":"` + status + `","result":"` + result + `"}}`
}

func TestTaskWaitReconnectsAndReturnsCompletedSnapshot(t *testing.T) {
	handler := &sseSequenceHandler{t: t, responses: [][]string{
		{taskUpdate("running", "")},
		{taskUpdate("completed", "done")},
	}}
	client := newTestClient(t, handler)
	task, err := client.Tasks.Wait(context.Background(), "t1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if task["status"] != "completed" || task["result"] != "done" {
		t.Fatalf("task = %#v", task)
	}
}

func TestTaskWaitReturnsStructuredFailure(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeSSE(t, w, taskUpdate("failed", "boom"))
	}))
	_, err := client.Tasks.Wait(context.Background(), "t1", nil)
	var taskErr *TaskWaitError
	if !errors.As(err, &taskErr) {
		t.Fatalf("err = %v, want *TaskWaitError", err)
	}
	if taskErr.Task["status"] != "failed" || taskErr.Error() != "hot: boom" {
		t.Fatalf("task error = %#v (%v)", taskErr.Task, taskErr)
	}
}
