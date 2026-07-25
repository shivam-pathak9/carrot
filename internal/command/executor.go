package command

import (
	"fmt"
	"github.com/shivampathak/carrot/internal/protocol/resp"
	"strings"
)

type Executor struct{}

func NewExecutor() *Executor {
	return &Executor{}
}

func (e *Executor) Execute(cmd Command) (resp.Value, error) {

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
