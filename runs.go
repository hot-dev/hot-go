package hot

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"net/url"
	"time"
)

// RunsResource lists and inspects runs.
type RunsResource struct{ transport *transport }

// WaitForRunOptions configures run waiting.
type WaitForRunOptions struct {
	// Timeout bounds the wait. Defaults to 60 seconds.
	Timeout time.Duration
	// MaxAttempts caps reconnection attempts. Defaults to 5.
	MaxAttempts int
}

// RunWaitError is returned when a run fails or is cancelled.
type RunWaitError struct {
	Run map[string]any
}

func (e *RunWaitError) Error() string {
	if message := resultMessage(e.Run["result"]); message != "" {
		return "hot: " + message
	}
	return fmt.Sprintf("hot: run %v", e.Run["status"])
}

// List returns the run list envelope (data + pagination).
func (r *RunsResource) List(ctx context.Context, query url.Values) (map[string]any, error) {
	return r.transport.requestJSON(ctx, "GET", "/runs", nil, query)
}

// Stats returns aggregate run statistics.
func (r *RunsResource) Stats(ctx context.Context) (map[string]any, error) {
	envelope, err := r.transport.requestJSON(ctx, "GET", "/runs/stats", nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// Get returns a single run.
func (r *RunsResource) Get(ctx context.Context, runID string) (map[string]any, error) {
	envelope, err := r.transport.requestJSON(ctx, "GET", "/runs/"+enc(runID), nil, nil)
	if err != nil {
		return nil, err
	}
	return dataObject(envelope), nil
}

// Subscribe yields durable run:update snapshots until the run is terminal.
func (r *RunsResource) Subscribe(ctx context.Context, runID string) iter.Seq2[StreamEvent, error] {
	return r.transport.stream(ctx, "GET", "/runs/"+enc(runID)+"/subscribe", nil, nil)
}

// Wait returns a succeeded run snapshot. Failed and cancelled runs return a
// *RunWaitError containing the final durable snapshot.
func (r *RunsResource) Wait(ctx context.Context, runID string, opts *WaitForRunOptions) (map[string]any, error) {
	options := WaitForRunOptions{}
	if opts != nil {
		options = *opts
	}
	if options.Timeout == 0 {
		options.Timeout = 60 * time.Second
	}
	if options.MaxAttempts == 0 {
		options.MaxAttempts = 5
	}
	ctx, cancel := context.WithTimeout(ctx, options.Timeout)
	defer cancel()

	for attempts := 0; ; attempts++ {
		for event, err := range r.Subscribe(ctx, runID) {
			if err != nil {
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					return nil, errors.New("hot: timeout waiting for run")
				}
				if isAuthorizationError(err) || attempts >= options.MaxAttempts {
					return nil, err
				}
				break
			}
			if event.Type() != "run:update" {
				continue
			}
			run, _ := event["run"].(map[string]any)
			status, _ := run["status"].(string)
			switch status {
			case "succeeded":
				return run, nil
			case "failed", "cancelled":
				return nil, &RunWaitError{Run: run}
			}
		}
		if ctx.Err() != nil {
			return nil, errors.New("hot: timeout waiting for run")
		}
		if attempts >= options.MaxAttempts {
			return nil, errors.New("hot: run subscription ended before the run completed")
		}
	}
}
