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

// RunID returns run.run_id, or "" when absent.
func (e StreamEvent) RunID() string {
	runID, _ := e.Run()["run_id"].(string)
	return runID
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
