package resp

import (
	"fmt"
)

func (e *Encoder) encodeSimpleString(v Value) error {

	// encodeSimpleString writes a RESP Simple String: `+<string>\r\n`.
	_, err := fmt.Fprintf(
		e.writer,
		"+%s\r\n",
		v.String,
	)

	return err
}

func (e *Encoder) encodeError(v Value) error {

	// encodeError writes a RESP Error: `-<message>\r\n`.
	_, err := fmt.Fprintf(
		e.writer,
		"-%s\r\n",
		v.String,
	)

	return err
}

func (e *Encoder) encodeInteger(v Value) error {

	// encodeInteger writes a RESP Integer: `:<number>\r\n`.
	_, err := fmt.Fprintf(
		e.writer,
		":%d\r\n",
		v.Integer,
	)

	return err
}

func (e *Encoder) encodeBulkString(v Value) error {

	// encodeBulkString writes a RESP Bulk String header and payload
	// `$<length>\r\n<payload>\r\n`.
	_, err := fmt.Fprintf(
		e.writer,
		"$%d\r\n%s\r\n",
		len(v.String),
		v.String,
	)

	return err
}

func (e *Encoder) encodeArray(v Value) error {

	// encodeArray writes a RESP Array header `*<count>\r\n` and
	// then encodes each element in sequence. This is recursive in
	// that each element may itself be an Array, BulkString, etc.,
	// and will be encoded via the top-level `Encode` dispatch.
	_, err := fmt.Fprintf(
		e.writer,
		"*%d\r\n",
		len(v.Array),
	)

	if err != nil {
		return err
	}

	for _, value := range v.Array {

		if err := e.Encode(value); err != nil {
			return err
		}
	}

	return nil
}
