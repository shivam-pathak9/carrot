package resp

// These all are the constructors for the RESP values. They are used to create new RESP values of different types.

func NewSimpleString(value string) Value {
	return Value{
		Type:   SimpleString,
		String: value,
	}
}

func NewError(value string) Value {
	return Value{
		Type:   Error,
		String: value,
	}
}

func NewInteger(value int64) Value {
	return Value{
		Type:    Integer,
		Integer: value,
	}
}

func NewBulkString(value string) Value {
	return Value{
		Type:   BulkString,
		String: value,
	}
}

func NewArray(values ...Value) Value {
	return Value{
		Type:  Array,
		Array: values,
	}
}
