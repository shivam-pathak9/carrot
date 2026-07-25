package resp

import (
	"fmt"
	"strconv"
)

func (d *Decoder) decodeArray() (Value, error) {

	line, err := d.readLine()
	if err != nil {
		return Value{}, err
	}

	count, err := strconv.Atoi(line)
	if err != nil {
		return Value{}, fmt.Errorf("invalid array length %q: %w", line, err)
	}

	// RESP Null Array
	if count == -1 {
		return Value{
			Type: Array,
		}, nil
	}

	if count < -1 {
		return Value{}, fmt.Errorf("invalid array length %d", count)
	}

	values := make([]Value, 0, count)

	for i := 0; i < count; i++ {

		value, err := d.Decode()
		if err != nil {
			return Value{}, err
		}

		values = append(values, value)
	}

	return NewArray(values...), nil
}
