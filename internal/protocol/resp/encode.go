package resp

import (
	"bufio"
	"fmt"
)

type Encoder struct {
	writer *bufio.Writer
}

func NewEncoder(writer *bufio.Writer) *Encoder {
	return &Encoder{
		writer: writer,
	}
}

func (e *Encoder) Encode(v Value) error {

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
