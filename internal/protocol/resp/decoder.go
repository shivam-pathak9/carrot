package resp

import (
	"bufio"
	"fmt"
)

type Decoder struct {
	reader *bufio.Reader
}

func NewDecoder(reader *bufio.Reader) *Decoder {
	// NewDecoder creates a RESP decoder that reads values from
	// the provided buffered reader.
	return &Decoder{
		reader: reader,
	}
}

func (d *Decoder) Decode() (Value, error) {
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
	prefix, err := d.reader.ReadByte()
	if err != nil {
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
		return Value{}, fmt.Errorf("unsupported RESP type %q", prefix)
	}
}
