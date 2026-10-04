package command

import "github.com/shivam-pathak9/carrot/internal/protocol/resp"

func handlePing(args []string) resp.Value {
	switch len(args) {
	case 0:
		return resp.NewSimpleString("PONG")
	case 1:
		return resp.NewBulkString(args[0])
	default:
		return resp.NewError("ERR wrong number of arguments for 'ping' command")
	}
}
