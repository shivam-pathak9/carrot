package resp

import (
	"fmt"
	"strconv"
)

func (d *Decoder) decodeInteger() (Value, error) {
	// decodeInteger parses a RESP integer line (`:<number>`) and
	// returns a Value wrapping the parsed int64.
	line, err := d.readLine()
	if err != nil {
		return Value{}, err
	}

	value, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return Value{}, fmt.Errorf("invalid integer %q: %w", line, err)
	}

	return NewInteger(value), nil
}
