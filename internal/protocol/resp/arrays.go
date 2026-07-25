package resp

import (
	"fmt"
	"strconv"
)

func (d *Decoder) decodeArray() (Value, error) {
	// decodeArray reads a RESP Array header (`*<count>\r\n`) and
	// then decodes `count` subsequent RESP values. Important notes:
	//
	// - The decoder is recursive: each element is decoded by
	//   calling `Decode()`, which inspects the next type byte and
	//   dispatches to the appropriate reader. This allows nested
	//   arrays and mixed-type payloads to be decoded naturally.
	// - A count of `-1` represents a NULL array. We represent a
	//   NULL array as a Value with Type==Array and a nil/empty
	//   slice.
	// - We validate that the count is not less than -1 to avoid
	//   integer/logic errors.
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
