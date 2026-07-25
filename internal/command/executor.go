package command

import (
	"fmt"
	"github.com/shivampathak/carrot/internal/protocol/resp"
	"strings"
)

type Executor struct{}

func NewExecutor() *Executor {
	// NewExecutor constructs a command executor. The executor is
	// responsible for implementing the server-side behavior of
	// supported commands (PING, SET, GET, etc.). Currently it is
	// stateless and only implements a small subset.
	return &Executor{}
}

func (e *Executor) Execute(cmd Command) (resp.Value, error) {

	// Execute runs the given `Command` and returns a RESP `Value`
	// that represents the response to send to the client. For
	// unknown or invalid usage the function returns an appropriate
	// RESP Error value.

	switch cmd.Name {

	case "PING":

		switch len(cmd.Args) {

		case 0:
			return resp.NewSimpleString("PONG"), nil

		case 1:
			return resp.NewBulkString(cmd.Args[0]), nil

		default:
			return resp.NewError(
				"ERR wrong number of arguments for 'ping' command",
			), nil
		}

	default:
		return resp.NewError(
			fmt.Sprintf("ERR unknown command '%s'", strings.ToLower(cmd.Name)),
		), nil
	}
}
