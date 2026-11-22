package hot

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
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

func TestConsumeSSEBlocksJoinsMultilineDataWithNewline(t *testing.T) {
	// Valid multi-line JSON works when joined with "\n".
	events, remaining := consumeSSEBlocks("data: {\"type\":\ndata: \"a\"}\n\n")
	if len(events) != 1 || events[0].Type() != "a" || remaining != "" {
		t.Errorf("events = %v, remaining = %q", events, remaining)
	}

	// Prove data: lines are joined with "\n" (not concatenated with "") per WHATWG §9.2.6:
	// "1" + "\n" + "2" is invalid JSON, whereas "1" + "" + "2" would parse as 12.
	invalidSplit, _ := consumeSSEBlocks("data: {\"type\":\"a\",\"n\":1\ndata: 2}\n\n")
	if len(invalidSplit) != 0 {
		t.Fatalf("expected invalid split number to fail JSON parsing when joined with newline, got %v", invalidSplit)
	}

	// Also verify raw accumulated buffer directly on sseParser.
	p := newSSEParser(defaultMaxSSEBufferBytes)
	if _, err := p.feed([]byte("data: first\r\ndata: second\r\n")); err != nil {
		t.Fatal(err)
	}
	if got := p.data.String(); got != "first\nsecond" {
		t.Fatalf("accumulated data = %q, want %q", got, "first\\nsecond")
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
	events, remaining := consumeSSEBlocks("data: {\"type\":\"a\"}\r\n\r\ndata: {\"type\":\"b\"}\r\n\r\n")
	if len(events) != 2 || events[0].Type() != "a" || events[1].Type() != "b" || remaining != "" {
		t.Errorf("events = %v, remaining = %q", events, remaining)
	}
}

func TestConsumeSSEBlocksHandlesCROnly(t *testing.T) {
	events, remaining := consumeSSEBlocks("data: {\"type\":\"a\"}\r\rdata: {\"type\":\"b\"}\r\r")
	if len(events) != 2 || events[0].Type() != "a" || events[1].Type() != "b" || remaining != "" {
		t.Errorf("events = %v, remaining = %q", events, remaining)
	}
}

func TestConsumeSSEBlocksStripsBOM(t *testing.T) {
	events, remaining := consumeSSEBlocks("\uFEFFdata: {\"type\":\"a\"}\r\n\r\n")
	if len(events) != 1 || events[0].Type() != "a" || remaining != "" {
		t.Errorf("events = %v, remaining = %q", events, remaining)
	}
}

func TestSSEParserChunkBoundariesBOMAndCRLF(t *testing.T) {
	raw := []byte("\xEF\xBB\xBFdata: {\"type\":\r\ndata: \"a\"}\r\n\r\n: keepalive\r\r" +
		"data: {\"type\":\"b\"}\r\rdata: {\"type\":\"c\"}\n\n" +
		"data: {\"type\":\"unterminated\"}")
	// Feed 1 byte at a time to exercise split BOM and split \r\n boundaries.
	p := newSSEParser(defaultMaxSSEBufferBytes)
	var events []StreamEvent
	for i := range raw {
		got, err := p.feed(raw[i : i+1])
		if err != nil {
			t.Fatalf("byte %d: %v", i, err)
		}
		events = append(events, got...)
	}
	want := []string{"a", "b", "c"}
	if len(events) != len(want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	for i, w := range want {
		if events[i].Type() != w {
			t.Errorf("events[%d].Type() = %q, want %q", i, events[i].Type(), w)
		}
	}
}

func TestSSEParserEnforcesBufferCap(t *testing.T) {
	t.Run("oversized single line", func(t *testing.T) {
		p := newSSEParser(64)
		_, err := p.feed(bytes.Repeat([]byte("a"), 65))
		if !errors.Is(err, errSSEBufferOverflow) {
			t.Fatalf("err = %v, want %v", err, errSSEBufferOverflow)
		}
	})
	t.Run("oversized multiline event", func(t *testing.T) {
		p := newSSEParser(64)
		line := []byte("data: " + strings.Repeat("x", 25) + "\n")
		if _, err := p.feed(line); err != nil {
			t.Fatalf("first line err = %v", err)
		}
		if _, err := p.feed(line); err != nil {
			t.Fatalf("second line err = %v", err)
		}
		_, err := p.feed(line)
		if !errors.Is(err, errSSEBufferOverflow) {
			t.Fatalf("third line err = %v, want %v", err, errSSEBufferOverflow)
		}
	})
}

func TestStreamEventAccessors(t *testing.T) {
	event := StreamEvent{
		"type":         "run:fail",
		"retry_run_id": "r2",
		"run":          map[string]any{"run_id": "r1", "status": "pending_retry"},
	}
	if event.Type() != "run:fail" || event.RunID() != "r1" || event.RetryRunID() != "r2" || event.RunStatus() != "pending_retry" {
		t.Errorf("accessors = %q %q %q %q", event.Type(), event.RunID(), event.RetryRunID(), event.RunStatus())
	}
	if !reflect.DeepEqual(event.Run(), map[string]any{"run_id": "r1", "status": "pending_retry"}) {
		t.Errorf("Run() = %v", event.Run())
	}

	nestedRetry := StreamEvent{
		"type": "run:fail",
		"run":  map[string]any{"run_id": "r1", "retry_run_id": "r3"},
	}
	if nestedRetry.RetryRunID() != "r3" {
		t.Errorf("nested RetryRunID() = %q, want r3", nestedRetry.RetryRunID())
	}

	empty := StreamEvent{}
	if empty.Type() != "" || empty.RunID() != "" || empty.RetryRunID() != "" || empty.Run() != nil {
		t.Error("empty event accessors should return zero values")
	}
}
