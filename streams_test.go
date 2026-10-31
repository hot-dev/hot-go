package hot

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
)

const (
	callPublished = `{"type":"event:published","event_id":"e1","stream_id":"s1"}`
	chunkA        = `{"type":"stream:data","run_id":"r1","data_type":"chunk","payload":{"text":"a"}}`
	chunkB        = `{"type":"stream:data","run_id":"r1","data_type":"chunk","payload":{"text":"b"}}`
	otherChunk    = `{"type":"stream:data","run_id":"other","data_type":"chunk","payload":{"text":"x"}}`
	runStart      = `{"type":"run:start","run":{"run_id":"r1","event_id":"e1","status":"running"}}`
	runStop       = `{"type":"run:stop","run":{"run_id":"r1","event_id":"e1","status":"succeeded","result":"done"}}`
	otherStop     = `{"type":"run:stop","run":{"run_id":"other","event_id":"e9","status":"succeeded","result":"no"}}`
	// The first attempt fails and will be retried; the retry run has a new
	// event_id and links back by origin_run_id, as hot sends it.
	retryPending = `{"type":"run:fail","run":{"run_id":"r1","event_id":"e1","status":"pending_retry","result":{"$err":"first attempt"}}}`
	retryStop    = `{"type":"run:stop","run":{"run_id":"r2","event_id":"e2","origin_run_id":"r1","status":"succeeded","result":"recovered"}}`
	retryFail    = `{"type":"run:fail","run":{"run_id":"r2","event_id":"e2","origin_run_id":"r1","status":"failed","result":{"$err":"second attempt"}}}`
)

func callWithChunks(t *testing.T, events ...string) (any, []string, error) {
	t.Helper()
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeSSE(t, w, append([]string{callPublished}, events...)...)
	}))
	var chunks []string
	result, err := client.Events.CallHot(context.Background(), "f", nil, &CallHotOptions{
		OnChunk: func(text string) { chunks = append(chunks, text) },
	})
	return result, chunks, err
}

func TestOnChunkDeliversChunksInEitherOrder(t *testing.T) {
	orders := map[string][]string{
		"run:start first":            {runStart, chunkA, chunkB, runStop},
		"chunks before run:start":    {chunkA, chunkB, runStart, runStop},
		"chunks and stop before run": {chunkA, chunkB, runStop, runStart},
		"unrelated run interleaved":  {otherChunk, chunkA, otherStop, chunkB, runStop},
	}
	for name, events := range orders {
		t.Run(name, func(t *testing.T) {
			result, chunks, err := callWithChunks(t, events...)
			if err != nil || result != "done" || strings.Join(chunks, "") != "ab" {
				t.Fatalf("result = %v, chunks = %v, err = %v", result, chunks, err)
			}
		})
	}
}

func TestWaitForRunResultBuffersEarlyChunks(t *testing.T) {
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeSSE(t, w, chunkA, chunkB, runStop, runStart)
	}))
	var chunks []string
	run, err := client.Streams.WaitForRunResult(context.Background(), "s1", "e1", &WaitForRunResultOptions{
		OnChunk: func(text string) { chunks = append(chunks, text) },
	})
	if err != nil || run["result"] != "done" || strings.Join(chunks, ",") != "a,b" {
		t.Fatalf("run = %v, chunks = %v, err = %v", run, chunks, err)
	}
}

func TestWaitersFollowPendingRetryRuns(t *testing.T) {
	t.Run("call", func(t *testing.T) {
		result, _, err := callWithChunks(t, retryPending, retryStop)
		if err != nil || result != "recovered" {
			t.Fatalf("result = %v, err = %v", result, err)
		}
	})
	t.Run("result", func(t *testing.T) {
		client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeSSE(t, w, retryPending, retryStop)
		}))
		run, err := client.Streams.WaitForRunResult(context.Background(), "s1", "e1", nil)
		if err != nil || run["run_id"] != "r2" {
			t.Fatalf("run = %v, err = %v", run, err)
		}
	})
	t.Run("final failure", func(t *testing.T) {
		_, _, err := callWithChunks(t, retryPending, retryFail)
		var runErr *RunWaitError
		if !errors.As(err, &runErr) || runErr.Run["run_id"] != "r2" || err.Error() != "hot: second attempt" {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("stream", func(t *testing.T) {
		client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeSSE(t, w, publishedEvent, retryPending, retryStop, otherStop)
		}))
		events, err := collectEvents(t, client, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(eventTypes(events), ","); got != "event:published,run:fail,run:stop" || events[2].RunID() != "r2" {
			t.Fatalf("events = %v", got)
		}
	})
}

func TestRunFailureIsRunWaitError(t *testing.T) {
	_, _, err := callWithChunks(t, `{"type":"run:fail","run":{"run_id":"r1","event_id":"e1","status":"failed","result":{"$err":"boom"}}}`)
	var runErr *RunWaitError
	if !errors.As(err, &runErr) || runErr.Run["status"] != "failed" || err.Error() != "hot: boom" {
		t.Fatalf("err = %v", err)
	}
}
