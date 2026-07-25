package resp

func (d *Decoder) decodeSimpleString() (Value, error) {
	line, err := d.readLine()
	if err != nil {
		return Value{}, err
	}

	return NewSimpleString(line), nil
}
