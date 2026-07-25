package resp

import (
	"fmt"
)

func (e *Encoder) encodeSimpleString(v Value) error {

	_, err := fmt.Fprintf(
		e.writer,
		"+%s\r\n",
		v.String,
	)

	return err
}

func (e *Encoder) encodeError(v Value) error {

	_, err := fmt.Fprintf(
		e.writer,
		"-%s\r\n",
		v.String,
	)

	return err
}

func (e *Encoder) encodeInteger(v Value) error {

	_, err := fmt.Fprintf(
		e.writer,
		":%d\r\n",
		v.Integer,
	)

	return err
}

func (e *Encoder) encodeBulkString(v Value) error {

	_, err := fmt.Fprintf(
		e.writer,
		"$%d\r\n%s\r\n",
		len(v.String),
		v.String,
	)

	return err
}

func (e *Encoder) encodeArray(v Value) error {

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
