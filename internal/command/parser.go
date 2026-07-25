package command

import (
	"fmt"
	"strings"

	"github.com/shivampathak/carrot/internal/protocol/resp"
)

type Parser struct{}

func NewParser() *Parser {
	return &Parser{}
}

func (p *Parser) Parse(value resp.Value) (Command, error) {

	// Redis commands always arrive as RESP Arrays.
	if value.Type != resp.Array {
		return Command{}, fmt.Errorf("ERR protocol error: expected array")
	}

	// Array must contain at least the command name.
	if len(value.Array) == 0 {
		return Command{}, fmt.Errorf("ERR protocol error: empty command")
	}

	commandValue := value.Array[0]

	// Command name must be a Bulk String.
	if commandValue.Type != resp.BulkString {
		return Command{}, fmt.Errorf("ERR protocol error: command must be bulk string")
	}

	cmd := Command{
		Name: strings.ToUpper(commandValue.String),
		Args: make([]string, 0, len(value.Array)-1),
	}

	for i := 1; i < len(value.Array); i++ {

		arg := value.Array[i]

		if arg.Type != resp.BulkString {
			return Command{}, fmt.Errorf("ERR protocol error: arguments must be bulk strings")
		}

		cmd.Args = append(cmd.Args, arg.String)
	}

	return cmd, nil
}
