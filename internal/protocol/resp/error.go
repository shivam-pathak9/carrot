package resp

func (d *Decoder) decodeError() (Value, error) {
	line, err := d.readLine()
	if err != nil {
		return Value{}, err
	}

	return NewError(line), nil
}