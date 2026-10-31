package hot

import (
	"encoding/json"
	"strings"
)

// StreamEvent is one event from a Hot run stream, in the Hot API wire format
// ("type", "data_type", "payload", "run", ...).
type StreamEvent map[string]any

// Type returns the event's "type" field, or "" when absent.
func (e StreamEvent) Type() string {
	eventType, _ := e["type"].(string)
	return eventType
}

// Run returns the event's "run" object, or nil when absent.
func (e StreamEvent) Run() map[string]any {
	run, _ := e["run"].(map[string]any)
	return run
}

// RunID returns run.run_id, or the top-level run_id of a stream:data event,
// or "" when absent.
func (e StreamEvent) RunID() string {
	if runID, ok := e.Run()["run_id"].(string); ok {
		return runID
	}
	runID, _ := e["run_id"].(string)
	return runID
}

// RunStatus returns run.status (for example "succeeded", "failed" or
// "pending_retry"), or "" when absent.
func (e StreamEvent) RunStatus() string {
	status, _ := e.Run()["status"].(string)
	return status
}

// StreamID returns the event's stream_id (event:published,
// stream:complete), falling back to run.stream_id, or "" when absent.
func (e StreamEvent) StreamID() string {
	if streamID, ok := e["stream_id"].(string); ok {
		return streamID
	}
	streamID, _ := e.Run()["stream_id"].(string)
	return streamID
}

// EventID returns the event:published event_id, falling back to
// run.event_id, or "" when absent.
func (e StreamEvent) EventID() string {
	if eventID, ok := e["event_id"].(string); ok {
		return eventID
	}
	return eventIDOfRun(e.Run())
}

// DataType returns a stream:data event's data_type, or "" when absent.
func (e StreamEvent) DataType() string {
	dataType, _ := e["data_type"].(string)
	return dataType
}

// Payload returns a stream:data event's payload object, or nil when absent.
func (e StreamEvent) Payload() map[string]any {
	payload, _ := e["payload"].(map[string]any)
	return payload
}

// Task returns a task:update event's task object, or nil when absent.
func (e StreamEvent) Task() map[string]any {
	task, _ := e["task"].(map[string]any)
	return task
}

// consumeSSEBlocks parses the complete SSE blocks in buffer and returns the
// decoded events plus the unconsumed remainder.
func consumeSSEBlocks(buffer string) ([]StreamEvent, string) {
	var events []StreamEvent
	remaining := buffer

	for {
		block, rest, found := strings.Cut(remaining, "\n\n")
		if !found {
			break
		}
		remaining = rest

		var data strings.Builder
		hasData := false
		for _, line := range strings.Split(block, "\n") {
			line = strings.TrimSuffix(line, "\r")
			if payload, ok := strings.CutPrefix(line, "data: "); ok {
				data.WriteString(payload)
				hasData = true
			} else if payload, ok := strings.CutPrefix(line, "data:"); ok {
				data.WriteString(payload)
				hasData = true
			}
		}
		if !hasData {
			continue
		}

		var parsed any
		if err := json.Unmarshal([]byte(data.String()), &parsed); err != nil {
			continue
		}
		if object, ok := parsed.(map[string]any); ok {
			if _, ok := object["type"].(string); ok {
				events = append(events, StreamEvent(object))
			}
		}
	}

	return events, remaining
}
