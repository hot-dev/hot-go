package hot

import (
	"context"
	"fmt"
	"iter"
	"time"
)

// TasksResource reads and waits for durable asynchronous tasks.
type TasksResource struct{ transport *transport }

// WaitForTaskOptions configures task waiting.
type WaitForTaskOptions struct {
	// Timeout bounds the whole wait, reconnects included. Defaults to 60
	// seconds.
	Timeout time.Duration
	// MaxAttempts caps consecutive failed connections. Defaults to 5.
	MaxAttempts int
}

// TaskWaitError is returned when a task fails, is cancelled, or times out.
type TaskWaitError struct {
	Task map[string]any
}

func (e *TaskWaitError) Error() string {
	if message := resultMessage(e.Task["result"]); message != "" {
		return "hot: " + message
	}
	return fmt.Sprintf("hot: task %v", e.Task["status"])
}

// Get returns the latest durable task snapshot.
func (r *TasksResource) Get(ctx context.Context, taskID string) (map[string]any, error) {
	envelope, err := r.transport.requestJSON(ctx, "GET", "/tasks/"+enc(taskID), nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// Subscribe yields durable task:update snapshots until the task is terminal.
func (r *TasksResource) Subscribe(ctx context.Context, taskID string) iter.Seq2[StreamEvent, error] {
	return r.transport.stream(ctx, "GET", "/tasks/"+enc(taskID)+"/subscribe", nil, nil)
}

// Wait returns a completed task snapshot. Non-success terminal states return
// a *TaskWaitError containing the final durable snapshot. Reconnects,
// timeouts and cancellation behave as for Runs.Wait.
func (r *TasksResource) Wait(ctx context.Context, taskID string, opts *WaitForTaskOptions) (map[string]any, error) {
	options := WaitForTaskOptions{}
	if opts != nil {
		options = *opts
	}
	if options.Timeout == 0 {
		options.Timeout = 60 * time.Second
	}
	ctx, cancel := withWaitTimeout(ctx, options.Timeout)
	defer cancel()

	reconnect := newReconnector("task", options.MaxAttempts)
	for {
		var conn connection
		for event, err := range r.Subscribe(ctx, taskID) {
			if err != nil {
				conn.err = err
				break
			}
			conn.delivered = true
			switch event.Type() {
			case "stream:complete":
				conn.complete = true
			case "task:update":
				task, _ := event["task"].(map[string]any)
				status, _ := task["status"].(string)
				switch status {
				case "completed":
					return task, nil
				case "failed", "cancelled", "timed_out":
					return nil, &TaskWaitError{Task: task}
				}
			}
		}
		if err := reconnect.next(ctx, conn, "hot: task subscription ended before the task completed"); err != nil {
			return nil, err
		}
	}
}
