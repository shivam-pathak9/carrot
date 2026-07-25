package command

// Command represents a parsed Redis command.
//
// Example:
//
//	PING
//	-> Name: "PING"
//	-> Args: []
//
//	PING hello
//	-> Name: "PING"
//	-> Args: ["hello"]
//
//	SET name shivam
//	-> Name: "SET"
//	-> Args: ["name", "shivam"]
type Command struct {
	Name string
	Args []string
}
