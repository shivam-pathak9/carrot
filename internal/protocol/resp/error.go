package resp

func (d *Decoder) decodeError() (Value, error) {
	// decodeError reads an error line (`-<message>`) and returns
	// a Value of type Error containing the message.
	line, err := d.readLine()
	if err != nil {
		return Value{}, err
	}

	return NewError(line), nil
}
