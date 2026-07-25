package resp

import (
	"bufio"
	"fmt"
)

type Encoder struct {
	writer *bufio.Writer
}

func NewEncoder(writer *bufio.Writer) *Encoder {
	// NewEncoder returns a RESP encoder that writes encoded
	// RESP values to the provided buffered writer.
	return &Encoder{
		writer: writer,
	}
}

func (e *Encoder) Encode(v Value) error {
	// Encode writes the provided `Value` to the underlying writer
	// using the appropriate RESP encoding. It does not flush the
	// buffer — callers should call `Flush()` on the client when
	// immediate delivery is required.
	//
	// Implementation notes:
	// - Encoding an Array will recursively encode each element via
	//   `encodeArray` -> `Encode`, allowing nested arrays to be
	//   produced naturally.
	// - We intentionally separate encoding from flushing so callers
	//   can batch multiple encodings into a single network write
	//   (improving throughput) and only flush when responses must
	//   be sent immediately.

	switch v.Type {

	case SimpleString:
		return e.encodeSimpleString(v)

	case Error:
		return e.encodeError(v)

	case Integer:
		return e.encodeInteger(v)

	case BulkString:
		return e.encodeBulkString(v)

	case Array:
		return e.encodeArray(v)

	default:
		return fmt.Errorf("unsupported RESP type")
	}
}
