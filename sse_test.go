package hot

import (
	"reflect"
	"testing"
)

func TestConsumeSSEBlocksKeepsPartialBlock(t *testing.T) {
	events, remaining := consumeSSEBlocks(`data: {"type":"a"}` + "\n\n" + `data: {"ty`)
	if len(events) != 1 || events[0].Type() != "a" {
		t.Errorf("events = %v", events)
	}
	if remaining != `data: {"ty` {
		t.Errorf("remaining = %q", remaining)
	}
}

func TestConsumeSSEBlocksHandlesDataWithoutSpace(t *testing.T) {
	events, _ := consumeSSEBlocks("data:{\"type\":\"a\"}\n\n")
	if len(events) != 1 || events[0].Type() != "a" {
		t.Errorf("events = %v", events)
	}
}

func TestConsumeSSEBlocksJoinsMultilineData(t *testing.T) {
	events, _ := consumeSSEBlocks("data: {\"type\":\ndata: \"a\"}\n\n")
	if len(events) != 1 || events[0].Type() != "a" {
		t.Errorf("events = %v", events)
	}
}

func TestConsumeSSEBlocksSkipsInvalidAndUntypedPayloads(t *testing.T) {
	events, _ := consumeSSEBlocks(
		"data: not json\n\n" +
			"data: {\"no_type\":true}\n\n" +
			": comment\n\n" +
			"data: {\"type\":\"ok\"}\n\n",
	)
	if len(events) != 1 || events[0].Type() != "ok" {
		t.Errorf("events = %v", events)
	}
}

func TestConsumeSSEBlocksHandlesCRLF(t *testing.T) {
	events, _ := consumeSSEBlocks("data: {\"type\":\"a\"}\r\n\n")
	if len(events) != 1 || events[0].Type() != "a" {
		t.Errorf("events = %v", events)
	}
}

func TestStreamEventAccessors(t *testing.T) {
	event := StreamEvent{
		"type": "run:start",
		"run":  map[string]any{"run_id": "r1"},
	}
	if event.Type() != "run:start" || event.RunID() != "r1" {
		t.Errorf("accessors = %q %q", event.Type(), event.RunID())
	}
	if !reflect.DeepEqual(event.Run(), map[string]any{"run_id": "r1"}) {
		t.Errorf("Run() = %v", event.Run())
	}

	empty := StreamEvent{}
	if empty.Type() != "" || empty.RunID() != "" || empty.Run() != nil {
		t.Error("empty event accessors should return zero values")
	}
}
