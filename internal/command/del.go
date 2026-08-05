package command

import (
	"github.com/shivampathak/carrot/internal/protocol/resp"
	"github.com/shivampathak/carrot/internal/storage"
)

// handleDel executes the DEL command.
//
// Syntax: DEL key [key ...]
//
// Returns:
//   - Integer: the number of keys that were removed.
//     Keys that do not exist are ignored.
func handleDel(store *storage.Store, args []string) (resp.Value, error) {
	if len(args) == 0 {
		return resp.NewError("ERR wrong number of arguments for 'del' command"), nil
	}

	var countDeleted int64
	for _, key := range args {
		if store.Del(key) {
			countDeleted++
		}
	}

	return resp.NewInteger(countDeleted), nil
}
