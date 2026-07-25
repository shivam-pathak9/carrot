package resp

import (
	"bufio"
	"fmt"
)

type Decoder struct {
	reader *bufio.Reader
}

func NewDecoder(reader *bufio.Reader) *Decoder {
	return &Decoder{
		reader: reader,
	}
}

func (d *Decoder) Decode() (Value, error) {
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