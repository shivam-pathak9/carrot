// list.go validates list-command arguments and maps storage results to RESP.
package command

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/shivam-pathak9/carrot/internal/protocol/resp"
	"github.com/shivam-pathak9/carrot/internal/storage"
)

func listError(err error) resp.Value {
	switch {
	case errors.Is(err, storage.ErrWrongType):
		return resp.NewError(storage.ErrWrongType.Error())
	case errors.Is(err, storage.ErrNoSuchKey):
		return resp.NewError("ERR no such key")
	case errors.Is(err, storage.ErrListIndexRange):
		return resp.NewError("ERR index out of range")
	default:
		return resp.NewError(fmt.Sprintf("ERR %v", err))
	}
}

func parseListInteger(value string) (int64, error) {
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, errors.New("ERR value is not an integer or out of range")
	}
	return n, nil
}

func handleListCommand(name string, store *storage.Store, args []string) (resp.Value, error) {
	switch name {
	case "LPUSH", "RPUSH", "LPUSHX", "RPUSHX":
		return handleListPush(name, store, args)
	case "LPOP", "RPOP":
		return handleListPop(name, store, args)
	case "LLEN":
		if len(args) != 1 {
			return resp.NewError("ERR wrong number of arguments for '" + strings.ToLower(name) + "' command"), nil
		}
		length, err := store.ListLen(args[0])
		if err != nil {
			return listError(err), nil
		}
		return resp.NewInteger(length), nil
	case "LRANGE":
		if len(args) != 3 {
			return resp.NewError("ERR wrong number of arguments for 'lrange' command"), nil
		}
		start, err := parseListInteger(args[1])
		if err != nil {
			return resp.NewError(err.Error()), nil
		}
		stop, err := parseListInteger(args[2])
		if err != nil {
			return resp.NewError(err.Error()), nil
		}
		values, err := store.ListRange(args[0], start, stop)
		if err != nil {
			return listError(err), nil
		}
		return stringArray(values), nil
	case "LINDEX":
		if len(args) != 2 {
			return resp.NewError("ERR wrong number of arguments for 'lindex' command"), nil
		}
		index, err := parseListInteger(args[1])
		if err != nil {
			return resp.NewError(err.Error()), nil
		}
		value, found, err := store.ListIndex(args[0], index)
		if err != nil {
			return listError(err), nil
		}
		if !found {
			return resp.NewNullBulkString(), nil
		}
		return resp.NewBulkString(value), nil
	case "LSET":
		if len(args) != 3 {
			return resp.NewError("ERR wrong number of arguments for 'lset' command"), nil
		}
		index, err := parseListInteger(args[1])
		if err != nil {
			return resp.NewError(err.Error()), nil
		}
		if err := store.ListSet(args[0], index, args[2]); err != nil {
			return listError(err), nil
		}
		return resp.NewSimpleString("OK"), nil
	case "LTRIM":
		if len(args) != 3 {
			return resp.NewError("ERR wrong number of arguments for 'ltrim' command"), nil
		}
		start, err := parseListInteger(args[1])
		if err != nil {
			return resp.NewError(err.Error()), nil
		}
		stop, err := parseListInteger(args[2])
		if err != nil {
			return resp.NewError(err.Error()), nil
		}
		if err := store.ListTrim(args[0], start, stop); err != nil {
			return listError(err), nil
		}
		return resp.NewSimpleString("OK"), nil
	case "LREM":
		if len(args) != 3 {
			return resp.NewError("ERR wrong number of arguments for 'lrem' command"), nil
		}
		count, err := parseListInteger(args[1])
		if err != nil {
			return resp.NewError(err.Error()), nil
		}
		removed, err := store.ListRem(args[0], count, args[2])
		if err != nil {
			return listError(err), nil
		}
		return resp.NewInteger(removed), nil
	case "LINSERT":
		if len(args) != 4 {
			return resp.NewError("ERR wrong number of arguments for 'linsert' command"), nil
		}
		before := strings.EqualFold(args[1], "BEFORE")
		if !before && !strings.EqualFold(args[1], "AFTER") {
			return resp.NewError("ERR syntax error"), nil
		}
		length, err := store.ListInsert(args[0], args[2], args[3], before)
		if err != nil {
			return listError(err), nil
		}
		return resp.NewInteger(length), nil
	case "LMOVE":
		if len(args) != 4 {
			return resp.NewError("ERR wrong number of arguments for 'lmove' command"), nil
		}
		fromLeft, err := parseListDirection(args[2])
		if err != nil {
			return resp.NewError("ERR syntax error"), nil
		}
		toLeft, err := parseListDirection(args[3])
		if err != nil {
			return resp.NewError("ERR syntax error"), nil
		}
		value, found, err := store.ListMove(args[0], args[1], fromLeft, toLeft)
		if err != nil {
			return listError(err), nil
		}
		if !found {
			return resp.NewNullBulkString(), nil
		}
		return resp.NewBulkString(value), nil
	case "RPOPLPUSH":
		if len(args) != 2 {
			return resp.NewError("ERR wrong number of arguments for 'rpoplpush' command"), nil
		}
		value, found, err := store.ListMove(args[0], args[1], false, true)
		if err != nil {
			return listError(err), nil
		}
		if !found {
			return resp.NewNullBulkString(), nil
		}
		return resp.NewBulkString(value), nil
	case "LPOS":
		return handleListPosition(store, args)
	default:
		return resp.NewError(fmt.Sprintf("ERR unknown list command %q", name)), nil
	}
}

func handleListPush(name string, store *storage.Store, args []string) (resp.Value, error) {
	if len(args) < 2 {
		return resp.NewError("ERR wrong number of arguments for '" + strings.ToLower(name) + "' command"), nil
	}
	left := name == "LPUSH" || name == "LPUSHX"
	onlyExisting := name == "LPUSHX" || name == "RPUSHX"
	length, err := store.ListPush(args[0], args[1:], left, onlyExisting)
	if err != nil {
		return listError(err), nil
	}
	return resp.NewInteger(length), nil
}

func handleListPop(name string, store *storage.Store, args []string) (resp.Value, error) {
	if len(args) != 1 && len(args) != 2 {
		return resp.NewError("ERR wrong number of arguments for '" + strings.ToLower(name) + "' command"), nil
	}
	count := int64(1)
	withCount := len(args) == 2
	if withCount {
		parsed, err := parseListInteger(args[1])
		if err != nil {
			return resp.NewError(err.Error()), nil
		}
		if parsed < 0 {
			return resp.NewError("ERR count must be a positive integer"), nil
		}
		count = parsed
	}
	values, err := store.ListPop(args[0], name == "LPOP", count)
	if err != nil {
		return listError(err), nil
	}
	if withCount {
		return stringArray(values), nil
	}
	if len(values) == 0 {
		return resp.NewNullBulkString(), nil
	}
	return resp.NewBulkString(values[0]), nil
}

// handleListPosition parses the optional LPOS RANK, COUNT, and MAXLEN arguments.
func handleListPosition(store *storage.Store, args []string) (resp.Value, error) {
	if len(args) < 2 {
		return resp.NewError("ERR wrong number of arguments for 'lpos' command"), nil
	}
	var rank, count, maxLen int64 = 1, 1, 0
	countSpecified := false
	for i := 2; i < len(args); {
		option := strings.ToUpper(args[i])
		if i+1 >= len(args) {
			return resp.NewError("ERR syntax error"), nil
		}
		value, err := parseListInteger(args[i+1])
		if err != nil {
			return resp.NewError(err.Error()), nil
		}
		switch option {
		case "RANK":
			if value == 0 || value == math.MinInt64 {
				return resp.NewError("ERR RANK can't be zero"), nil
			}
			rank = value
		case "COUNT":
			if value < 0 {
				return resp.NewError("ERR COUNT can't be negative"), nil
			}
			count = value
			countSpecified = true
		case "MAXLEN":
			if value < 0 {
				return resp.NewError("ERR MAXLEN can't be negative"), nil
			}
			maxLen = value
		default:
			return resp.NewError("ERR syntax error"), nil
		}
		i += 2
	}
	positions, err := store.ListPosition(args[0], args[1], rank, count, maxLen)
	if err != nil {
		return listError(err), nil
	}
	if !countSpecified || count == 1 {
		if len(positions) == 0 {
			return resp.NewNullBulkString(), nil
		}
		return resp.NewInteger(positions[0]), nil
	}
	values := make([]resp.Value, len(positions))
	for i, position := range positions {
		values[i] = resp.NewInteger(position)
	}
	return resp.NewArray(values...), nil
}

// parseListDirection converts the Redis LEFT/RIGHT token to the storage flag.
func parseListDirection(value string) (bool, error) {
	switch strings.ToUpper(value) {
	case "LEFT":
		return true, nil
	case "RIGHT":
		return false, nil
	default:
		return false, errors.New("invalid list direction")
	}
}

func stringArray(values []string) resp.Value {
	items := make([]resp.Value, len(values))
	for i, value := range values {
		items[i] = resp.NewBulkString(value)
	}
	return resp.NewArray(items...)
}
