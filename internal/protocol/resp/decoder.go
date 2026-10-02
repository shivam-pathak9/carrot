// decoder.go handles bounded RESP framing, including nested arrays and inline commands.
package resp

import (
	"bufio"
	"fmt"
	"strings"
)

// Decoder reads one RESP value at a time while enforcing message and nesting limits.
type Decoder struct {
	reader          *bufio.Reader
	maxMessageBytes int
	messageBytes    int
	depth           int
}

// NewDecoder creates a decoder with the default maximum message size.
func NewDecoder(reader *bufio.Reader) *Decoder {
	return NewDecoderWithLimit(reader, 2<<20)
}

// NewDecoderWithLimit creates a decoder with a per-message byte limit.
func NewDecoderWithLimit(reader *bufio.Reader, maxMessageBytes int) *Decoder {
	return &Decoder{
		reader:          reader,
		maxMessageBytes: maxMessageBytes,
	}
}

// Decode reads one RESP value. Array elements are decoded recursively and count
// toward the same message-size and nesting-depth limits.
func (d *Decoder) Decode() (Value, error) {
	if d.depth == 0 {
		d.messageBytes = 0
	}
	d.depth++
	defer func() { d.depth-- }()
	if d.depth > maxRESPDepth {
		return Value{}, fmt.Errorf("RESP nesting exceeds max allowed %d", maxRESPDepth)
	}

	// Decode reads the next RESP value from the underlying reader
	// by inspecting the leading type byte and dispatching to the
	// appropriate helper. It returns a typed Value on success.
	//
	// Implementation notes:
	// - Callers may rely on `Decode()` being re-entrant in the
	//   sense that decoding arrays will recursively call `Decode`
	//   for each element. This keeps the parsing logic simple and
	//   consistent for nested structures.
	// - We deliberately read exactly the bytes required for each
	//   type: e.g., bulk strings use a length header + `io.ReadFull`
	//   (see `decodeBulkString`) to avoid ambiguity when payloads
	//   contain CR or LF bytes.
	// - Any IO or protocol error is returned immediately so the
	//   caller (server) can decide how to handle client disconnects
	//   or protocol violations.
	// - Inline commands (plain text, no RESP framing) are supported
	//   for tools like redis-benchmark that send "PING\r\n" before
	//   switching to full RESP. We detect them when the first byte
	//   is not a known RESP prefix and fall back to decodeInline().
	prefix, err := d.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}
	if err := d.consume(1); err != nil {
		return Value{}, err
	}

	switch Type(prefix) {
	case SimpleString:
		return d.decodeSimpleString()

	case Error:
		return d.decodeError()

	case Integer:
		return d.decodeInteger()

	case BulkString:
		return d.decodeBulkString()

	case Array:
		return d.decodeArray()

	default:
		// Not a RESP framed message — treat as an inline command.
		// redis-benchmark (and redis-cli) send plain-text commands
		// like "PING\r\n" during handshake / pipeline warm-up.
		// We unread the byte so decodeInline can read the full line.
		if err := d.reader.UnreadByte(); err != nil {
			return Value{}, fmt.Errorf("failed to unread byte: %w", err)
		}
		d.messageBytes--
		return d.decodeInline()
	}
}

// decodeInline parses an inline command (plain text, CRLF-terminated).
// Redis protocol allows clients to send commands as plain text lines,
// e.g. "PING\r\n" or "SET key value\r\n". We convert the whitespace-
// separated tokens into a RESP Array of BulkStrings so the rest of
// the pipeline (Parser → Executor) needs no changes.
func (d *Decoder) decodeInline() (Value, error) {
	line, err := d.readLine()
	if err != nil {
		return Value{}, fmt.Errorf("inline command read error: %w", err)
	}

	tokens := strings.Fields(line)
	if len(tokens) == 0 {
		return Value{}, fmt.Errorf("empty inline command")
	}

	values := make([]Value, len(tokens))
	for i, tok := range tokens {
		values[i] = NewBulkString(tok)
	}

	return NewArray(values...), nil
}
