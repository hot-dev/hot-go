package hot

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
)

const defaultMaxSSEBufferBytes = 16 * 1024 * 1024 // 16 MiB

// maxSSEBufferBytes caps buffered bytes in a single SSE line/event so a
// malformed or unbounded stream cannot exhaust memory. Overridden in tests.
var maxSSEBufferBytes = defaultMaxSSEBufferBytes

// errSSEBufferOverflow is returned when a single SSE line or event exceeds
// maxSSEBufferBytes.
var errSSEBufferOverflow = errors.New("hot: SSE event exceeded maximum buffer size")

var utf8BOM = [3]byte{0xEF, 0xBB, 0xBF}

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

// RetryRunID returns the top-level retry_run_id on a run:fail event (falling
// back to run.retry_run_id), or "" when absent.
func (e StreamEvent) RetryRunID() string {
	if retryRunID, ok := e["retry_run_id"].(string); ok {
		return retryRunID
	}
	retryRunID, _ := e.Run()["retry_run_id"].(string)
	return retryRunID
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

// sseParser incrementally parses a WHATWG Server-Sent Events byte stream
// without quadratic buffer reallocation. It handles \r\n, \n, and \r line
// endings (including \r\n split across chunks), strips a leading UTF-8 BOM,
// joins multi-line data fields with "\n", and enforces a maximum buffered size.
type sseParser struct {
	maxBytes   int
	bomChecked bool
	bomBuf     [3]byte
	bomLen     int
	skipNextLF bool
	lineBuf    []byte
	data       bytes.Buffer
	hasData    bool
}

func newSSEParser(maxBytes int) *sseParser {
	if maxBytes <= 0 {
		maxBytes = defaultMaxSSEBufferBytes
	}
	return &sseParser{maxBytes: maxBytes}
}

func (p *sseParser) bufferedSize() int {
	return len(p.lineBuf) + p.data.Len()
}

func (p *sseParser) consumeBOM(chunk []byte) []byte {
	if p.bomLen == 0 {
		if len(chunk) == 0 {
			return nil
		}
		if chunk[0] != utf8BOM[0] {
			p.bomChecked = true
			return chunk
		}
		if len(chunk) >= 3 && chunk[0] == utf8BOM[0] && chunk[1] == utf8BOM[1] && chunk[2] == utf8BOM[2] {
			p.bomChecked = true
			return chunk[3:]
		}
	}
	for len(chunk) > 0 && !p.bomChecked {
		if chunk[0] == utf8BOM[p.bomLen] {
			p.bomBuf[p.bomLen] = chunk[0]
			p.bomLen++
			chunk = chunk[1:]
			if p.bomLen == len(utf8BOM) {
				p.bomLen = 0
				p.bomChecked = true
				return chunk
			}
		} else {
			merged := make([]byte, p.bomLen+len(chunk))
			copy(merged, p.bomBuf[:p.bomLen])
			copy(merged[p.bomLen:], chunk)
			p.bomLen = 0
			p.bomChecked = true
			return merged
		}
	}
	return nil
}

// feed parses chunk and returns any completed events. Per WHATWG SSE §9.2.5,
// incomplete trailing data is retained until a blank line terminates the event
// (and discarded if the stream reaches EOF before that blank line).
func (p *sseParser) feed(chunk []byte) ([]StreamEvent, error) {
	if !p.bomChecked {
		chunk = p.consumeBOM(chunk)
		if len(chunk) == 0 {
			return nil, nil
		}
	}

	var events []StreamEvent
	for len(chunk) > 0 {
		if p.skipNextLF {
			p.skipNextLF = false
			if chunk[0] == '\n' {
				chunk = chunk[1:]
				continue
			}
		}

		idx := bytes.IndexAny(chunk, "\r\n")
		if idx < 0 {
			if p.bufferedSize()+len(chunk) > p.maxBytes {
				return events, errSSEBufferOverflow
			}
			p.lineBuf = append(p.lineBuf, chunk...)
			break
		}

		if p.bufferedSize()+idx > p.maxBytes {
			return events, errSSEBufferOverflow
		}
		var line []byte
		if len(p.lineBuf) == 0 {
			line = chunk[:idx]
		} else {
			p.lineBuf = append(p.lineBuf, chunk[:idx]...)
			line = p.lineBuf
		}

		sep := chunk[idx]
		chunk = chunk[idx+1:]
		if sep == '\r' {
			p.skipNextLF = true
		}

		event, err := p.processLine(line)
		p.lineBuf = p.lineBuf[:0]
		if err != nil {
			return events, err
		}
		if event != nil {
			events = append(events, event)
		}
	}

	return events, nil
}

func (p *sseParser) processLine(line []byte) (StreamEvent, error) {
	if len(line) == 0 {
		return p.dispatch(), nil
	}
	if line[0] == ':' {
		return nil, nil
	}
	field, value, hasColon := bytes.Cut(line, []byte{':'})
	if !hasColon {
		return nil, nil
	}
	if len(value) > 0 && value[0] == ' ' {
		value = value[1:]
	}
	if string(field) != "data" {
		return nil, nil
	}
	add := len(value)
	if p.hasData {
		add++ // "\n" separator per WHATWG SSE §9.2.6
	}
	if p.data.Len()+add > p.maxBytes {
		return nil, errSSEBufferOverflow
	}
	if p.hasData {
		p.data.WriteByte('\n')
	}
	p.data.Write(value)
	p.hasData = true
	return nil, nil
}

func (p *sseParser) dispatch() StreamEvent {
	if !p.hasData {
		return nil
	}
	event := decodeSSEData(p.data.Bytes())
	p.data.Reset()
	p.hasData = false
	return event
}

func decodeSSEData(data []byte) StreamEvent {
	var parsed any
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil
	}
	object, ok := parsed.(map[string]any)
	if !ok {
		return nil
	}
	if _, ok := object["type"].(string); !ok {
		return nil
	}
	return StreamEvent(object)
}

// consumeSSEBlocks parses the complete SSE blocks in buffer and returns the
// decoded events plus the unconsumed remainder. It supports \r\n, \n, and \r
// line endings, strips a leading UTF-8 BOM, and joins multi-line data fields
// with "\n" per WHATWG SSE §9.2.6.
func consumeSSEBlocks(buffer string) ([]StreamEvent, string) {
	buffer = strings.TrimPrefix(buffer, "\uFEFF")

	var events []StreamEvent
	var data strings.Builder
	hasData := false
	blockStart := 0
	pos := 0

	for pos < len(buffer) {
		rel := strings.IndexAny(buffer[pos:], "\r\n")
		if rel < 0 {
			break
		}
		sepPos := pos + rel
		line := buffer[pos:sepPos]
		termLen := 1
		if buffer[sepPos] == '\r' && sepPos+1 < len(buffer) && buffer[sepPos+1] == '\n' {
			termLen = 2
		}
		pos = sepPos + termLen

		if len(line) == 0 {
			if hasData {
				if event := decodeSSEData([]byte(data.String())); event != nil {
					events = append(events, event)
				}
				data.Reset()
				hasData = false
			}
			blockStart = pos
			continue
		}
		if line[0] == ':' {
			continue
		}
		field, value, hasColon := strings.Cut(line, ":")
		if !hasColon || field != "data" {
			continue
		}
		value = strings.TrimPrefix(value, " ")
		if hasData {
			data.WriteByte('\n')
		}
		data.WriteString(value)
		hasData = true
	}

	return events, buffer[blockStart:]
}
