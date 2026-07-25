package resp
import "fmt"

// Type represents the RESP data type.
// The value of each constant matches the first byte of a RESP message.
//
// + Simple String
// - Error
// : Integer
// $ Bulk String
// * Array
type Type byte

const (
	SimpleString Type = '+'
	Error        Type = '-'
	Integer      Type = ':'
	BulkString   Type = '$'
	Array         Type = '*'
)

// Value represents a single RESP value.
//
// Examples:
//
// +OK
// :100
// $5\r\nhello\r\n
// *2\r\n$4\r\nPING\r\n$4\r\nPONG\r\n

type Value struct {
	Type Type

	// Used for Simple String, Bulk String and Error
	String string

	// Used for Integer
	Integer int64

	// Used for Array
	Array []Value
}



// String returns the string representation of the Type.
func (t Type) String() string {
	switch t {
	case SimpleString:
		return "SimpleString"
	case Error:
		return "Error"
	case Integer:
		return "Integer"
	case BulkString:
		return "BulkString"
	case Array:
		return "Array"
	default:
		return fmt.Sprintf("Unknown(%q)", byte(t))
	}
}