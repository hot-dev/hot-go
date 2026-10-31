package hot

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
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

// shortGrace shrinks the retry grace for the rest of the test.
func shortGrace(t *testing.T, grace time.Duration) {
	t.Helper()
	previous := retryGrace
	retryGrace = grace
	t.Cleanup(func() { retryGrace = previous })
}

// quietAfter writes events on the first request, then holds the stream open
// with no further data; later requests get the same events.
func quietAfter(t *testing.T, events ...string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeSSE(t, w, events...)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	})
}

// Replays of a retried first attempt run A, as hot sends them before and after
// the retry run B exists (see the SDK notes on lossy retry replays).
const (
	// Case 2: a poll replay before the retry is queued maps pending_retry to run:stop.
	replayStopPending = `{"type":"run:stop","run":{"run_id":"r1","event_id":"e1","status":"pending_retry","retry_attempt":1,"result":{"$err":"first attempt"}}}`
	// Case 3: once the retry is queued, A reads back as failed with a raised retry_attempt.
	replayFailRetried = `{"type":"run:fail","run":{"run_id":"r1","event_id":"e1","status":"failed","retry_attempt":1,"result":{"$err":"first attempt"}}}`
	retryStopAttempt  = `{"type":"run:stop","run":{"run_id":"r2","event_id":"e2","origin_run_id":"r1","status":"succeeded","retry_attempt":1,"result":"recovered"}}`
	retryFailAttempt  = `{"type":"run:fail","run":{"run_id":"r2","event_id":"e2","origin_run_id":"r1","status":"failed","retry_attempt":1,"result":{"$err":"second attempt"}}}`
	// The documented case, as hot sends it live: retry_attempt is already raised.
	retryPendingAttempt = `{"type":"run:fail","run":{"run_id":"r1","event_id":"e1","status":"pending_retry","retry_attempt":1,"result":{"$err":"first attempt"}}}`
	plainFailAttempt    = `{"type":"run:fail","run":{"run_id":"r1","event_id":"e1","status":"failed","retry_attempt":0,"result":{"$err":"no retries"}}}`
)

func waitResult(t *testing.T, handler http.Handler, timeout time.Duration) (map[string]any, error, time.Duration) {
	t.Helper()
	client := newTestClient(t, handler)
	start := time.Now()
	run, err := client.Streams.WaitForRunResult(context.Background(), "s1", "e1", &WaitForRunResultOptions{Timeout: timeout})
	return run, err, time.Since(start)
}

func TestRetryReplayPendingRetryRunStopIsNotFinal(t *testing.T) {
	shortGrace(t, time.Second)
	run, err, _ := waitResult(t, quietAfter(t, replayStopPending, retryStopAttempt), 10*time.Second)
	if err != nil || run["run_id"] != "r2" {
		t.Fatalf("run = %v, err = %v; a pending_retry run:stop is not the result", run, err)
	}
}

func TestRetryReplayRaisedAttemptFailureWaitsForTheRetry(t *testing.T) {
	shortGrace(t, 2*time.Second)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeSSE(t, w, replayFailRetried)
		w.(http.Flusher).Flush()
		time.Sleep(200 * time.Millisecond) // the retry run starts later
		writeSSE(t, w, retryStopAttempt)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	})
	run, err, _ := waitResult(t, handler, 10*time.Second)
	if err != nil || run["run_id"] != "r2" {
		t.Fatalf("run = %v, err = %v; the held failure must give way to its retry", run, err)
	}
}

func TestRetryReplayHeldFailureStandsAfterGrace(t *testing.T) {
	shortGrace(t, 300*time.Millisecond)
	_, err, elapsed := waitResult(t, quietAfter(t, replayFailRetried), 10*time.Second)
	var runErr *RunWaitError
	if !errors.As(err, &runErr) || runErr.Run["run_id"] != "r1" || err.Error() != "hot: first attempt" {
		t.Fatalf("err = %v", err)
	}
	if elapsed < 300*time.Millisecond || elapsed > 5*time.Second {
		t.Fatalf("held failure reported after %v, want after the 300ms grace", elapsed)
	}
}

func TestRetryReplayHeldFailureStandsAtTheDeadline(t *testing.T) {
	shortGrace(t, 10*time.Second)
	_, err, elapsed := waitResult(t, quietAfter(t, replayFailRetried), 300*time.Millisecond)
	var runErr *RunWaitError
	if !errors.As(err, &runErr) || errors.Is(err, context.DeadlineExceeded) || elapsed > 5*time.Second {
		t.Fatalf("err = %v after %v, want the held failure rather than a timeout", err, elapsed)
	}
}

func TestRetryReplayOrphanedRetryArrivesFirst(t *testing.T) {
	shortGrace(t, 10*time.Second)
	// The retry run B is on the stream before any event for A ties it.
	run, err, elapsed := waitResult(t, quietAfter(t, retryStopAttempt, replayFailRetried), 10*time.Second)
	if err != nil || run["run_id"] != "r2" || elapsed > 5*time.Second {
		t.Fatalf("run = %v, err = %v after %v", run, err, elapsed)
	}
}

func TestFailuresWithoutRetryAreReportedAtOnce(t *testing.T) {
	shortGrace(t, 10*time.Second)
	cases := map[string][]string{
		"no retries":      {plainFailAttempt},
		"last retry":      {retryPendingAttempt, retryFailAttempt},
		"last retry, raw": {retryPending, retryFail},
	}
	for name, events := range cases {
		t.Run(name, func(t *testing.T) {
			_, err, elapsed := waitResult(t, quietAfter(t, events...), 30*time.Second)
			var runErr *RunWaitError
			if !errors.As(err, &runErr) || elapsed > 2*time.Second {
				t.Fatalf("err = %v after %v, want an immediate *RunWaitError", err, elapsed)
			}
		})
	}
}

func TestSubscribeWithEventFollowsRetryReplays(t *testing.T) {
	t.Run("pending_retry run:stop", func(t *testing.T) {
		shortGrace(t, 10*time.Second)
		client := newTestClient(t, quietAfter(t, publishedEvent, replayStopPending, retryStopAttempt))
		events, err := collectEvents(t, client, nil)
		if err != nil || len(events) != 3 || events[2].RunID() != "r2" {
			t.Fatalf("events = %v, err = %v", eventTypes(events), err)
		}
	})
	t.Run("held failure ends after grace", func(t *testing.T) {
		shortGrace(t, 300*time.Millisecond)
		client := newTestClient(t, quietAfter(t, publishedEvent, replayFailRetried))
		start := time.Now()
		events, err := collectEvents(t, client, nil)
		if err != nil || len(events) != 2 || events[1].RunStatus() != "failed" || time.Since(start) < 300*time.Millisecond {
			t.Fatalf("events = %v, err = %v after %v", eventTypes(events), err, time.Since(start))
		}
	})
}

// A task run started by our run names it as origin_run_id on the same
// stream, but it is not a retry: only a run of its origin's run_type is.
const (
	callRunStop = `{"type":"run:stop","run":{"run_id":"r1","event_id":"e1","run_type":"call","status":"succeeded","retry_attempt":0,"result":"call result"}}`
	callRunFail = `{"type":"run:fail","run":{"run_id":"r1","event_id":"e1","run_type":"call","status":"failed","retry_attempt":0,"result":{"$err":"call failed"}}}`
	callStart   = `{"type":"run:start","run":{"run_id":"r1","event_id":"e1","run_type":"call","status":"running","retry_attempt":0}}`
	taskStop    = `{"type":"run:stop","run":{"run_id":"t1","event_id":"e7","origin_run_id":"r1","run_type":"task","status":"succeeded","retry_attempt":0,"result":"task result"}}`
	taskChunk   = `{"type":"stream:data","run_id":"t1","data_type":"chunk","payload":{"text":"task"}}`
)

func TestTaskRunsAreNotRetries(t *testing.T) {
	shortGrace(t, 10*time.Second)
	orders := map[string][]string{
		"task finishes after run:start": {callStart, taskChunk, taskStop, callRunStop},
		"task finishes first (orphan)":  {taskChunk, taskStop, callStart, callRunStop},
	}
	for name, events := range orders {
		t.Run(name, func(t *testing.T) {
			result, chunks, err := callWithChunks(t, events...)
			if err != nil || result != "call result" || len(chunks) != 0 {
				t.Fatalf("result = %v, chunks = %v, err = %v; the task run must not end or alter the call", result, chunks, err)
			}
			client := newTestClient(t, quietAfter(t, append([]string{publishedEvent}, events...)...))
			streamed, err := collectEvents(t, client, nil)
			if err != nil || streamed[len(streamed)-1].RunID() != "r1" {
				t.Fatalf("SubscribeWithEvent ended on %v (err %v)", streamed[len(streamed)-1], err)
			}
		})
	}
	t.Run("plain failure still fails at once", func(t *testing.T) {
		for _, events := range [][]string{{callStart, taskStop, callRunFail}, {taskStop, callRunFail}} {
			_, err, elapsed := waitResult(t, quietAfter(t, events...), 30*time.Second)
			var runErr *RunWaitError
			if !errors.As(err, &runErr) || runErr.Run["run_id"] != "r1" || elapsed > 2*time.Second {
				t.Fatalf("err = %v after %v, want r1's failure at once", err, elapsed)
			}
		}
	})
}
