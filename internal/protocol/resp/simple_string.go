package resp

func (d *Decoder) decodeSimpleString() (Value, error) {
	// decodeSimpleString reads a simple string line (`+<text>`) and
	// returns a Value wrapping the text payload.
	line, err := d.readLine()
	if err != nil {
		return Value{}, err
	}

	return NewSimpleString(line), nil
}
