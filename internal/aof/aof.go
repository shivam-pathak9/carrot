// Package aof implements RESP-based append-only persistence and recovery.
//
// The write path is Log.Append, called by command.Executor before a mutation
// changes memory. Open locks the file, replays its records into the store, and
// returns the log for new writes. The implementation currently supports Linux.
package aof

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shivam-pathak9/carrot/internal/command"
	"github.com/shivam-pathak9/carrot/internal/protocol/resp"
	"github.com/shivam-pathak9/carrot/internal/storage"
)

// maxRecordBytes limits the size of one decoded AOF command during recovery.
const maxRecordBytes = 4 << 20

// Log is the append boundary consumed by the command executor. Its mutex
// serializes appends, periodic syncs, and shutdown; failed records prevent any
// later mutation from proceeding through this log.
type Log struct {
	mu        sync.Mutex
	path      string
	file      *os.File
	writer    *bufio.Writer
	policy    string
	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	closed    bool
	failed    error
	closeErr  error
}

// Open locks path, replays its commands into store, and returns an append-ready
// log. It creates the file with owner-only permissions when necessary. A
// partial final RESP command is truncated; corruption in a complete record
// aborts startup rather than silently discarding potentially valid history.
// Linux is currently the only supported platform because file locking and
// directory syncing use Linux filesystem operations.
func Open(path, policy string, store *storage.Store) (*Log, error) {
	if path == "" {
		return nil, errors.New("AOF path must not be empty")
	}
	if store == nil {
		return nil, errors.New("storage store must not be nil")
	}
	if policy != "always" && policy != "everysec" && policy != "no" {
		return nil, fmt.Errorf("unsupported AOF sync policy %q", policy)
	}
	if err := ensurePlatform(); err != nil {
		return nil, err
	}
	_, statErr := os.Stat(path)
	created := errors.Is(statErr, os.ErrNotExist)
	if statErr != nil && !created {
		return nil, fmt.Errorf("inspect AOF %q: %w", path, statErr)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open AOF %q: %w", path, err)
	}
	if err := lockFile(file); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("lock AOF %q (another process may be using it): %w", path, err)
	}
	closeOnError := func(err error) (*Log, error) {
		unlockErr := unlockFile(file)
		closeErr := file.Close()
		return nil, errors.Join(err, unlockErr, closeErr)
	}

	if err := replay(file, store); err != nil {
		return closeOnError(fmt.Errorf("recover AOF %q: %w", path, err))
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return closeOnError(fmt.Errorf("seek AOF %q: %w", path, err))
	}
	if created {
		if err := syncDirectory(path); err != nil {
			return closeOnError(fmt.Errorf("sync AOF directory: %w", err))
		}
	}

	logFile := &Log{
		path:   path,
		file:   file,
		writer: bufio.NewWriter(file),
		policy: policy,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	if policy == "everysec" {
		go logFile.syncLoop()
	} else {
		close(logFile.done)
	}
	return logFile, nil
}

// Rewrite replaces the command history with commands that reconstruct the
// current store. Call it through Executor.RewriteAOF so all mutation commands
// wait until the new file is active. The replacement is written and synced to
// a temporary file in the same directory, then atomically renamed over the
// current AOF. The temporary file is locked before rename so another process
// cannot open the replacement during the handoff.
func (l *Log) Rewrite(store *storage.Store) (resultErr error) {
	if store == nil {
		return errors.New("storage store must not be nil")
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errors.New("AOF is closed")
	}
	if l.failed != nil {
		return fmt.Errorf("AOF is unhealthy: %w", l.failed)
	}

	directory := filepath.Dir(l.path)
	tempFile, err := os.CreateTemp(directory, "."+filepath.Base(l.path)+".rewrite-*")
	if err != nil {
		return fmt.Errorf("create AOF rewrite file: %w", err)
	}
	tempPath := tempFile.Name()
	renamed := false
	tempLocked := false
	defer func() {
		if renamed {
			return
		}
		var unlockErr error
		if tempLocked {
			unlockErr = unlockFile(tempFile)
		}
		closeErr := tempFile.Close()
		removeErr := os.Remove(tempPath)
		resultErr = errors.Join(resultErr, unlockErr, closeErr, removeErr)
	}()

	if err := tempFile.Chmod(0o600); err != nil {
		return fmt.Errorf("set AOF rewrite file permissions: %w", err)
	}
	if err := lockFile(tempFile); err != nil {
		return fmt.Errorf("lock AOF rewrite file: %w", err)
	}
	tempLocked = true

	writer := bufio.NewWriterSize(tempFile, 64<<10)
	if err := store.ForEachSnapshot(func(entry storage.SnapshotEntry) error {
		return writeSnapshotEntry(writer, entry)
	}); err != nil {
		return fmt.Errorf("write AOF rewrite snapshot: %w", err)
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush AOF rewrite file: %w", err)
	}
	if err := tempFile.Sync(); err != nil {
		return fmt.Errorf("sync AOF rewrite file: %w", err)
	}
	if err := l.writer.Flush(); err != nil {
		l.failed = fmt.Errorf("flush current AOF before replacement: %w", err)
		return l.failed
	}
	if err := os.Rename(tempPath, l.path); err != nil {
		return fmt.Errorf("atomically replace AOF: %w", err)
	}

	// The path now names the synced replacement, whose lock was acquired
	// before rename. Switch future appends to it before releasing the old lock.
	oldFile := l.file
	l.file = tempFile
	l.writer = bufio.NewWriter(tempFile)
	renamed = true

	directorySyncErr := syncDirectory(l.path)
	if directorySyncErr != nil {
		// The rename has happened, but without a durable directory entry we
		// cannot safely acknowledge later writes against the replacement path.
		l.failed = fmt.Errorf("sync AOF directory after replacement: %w", directorySyncErr)
		log.Printf("AOF rewrite directory sync failed: %v", directorySyncErr)
	}
	unlockErr := unlockFile(oldFile)
	closeErr := oldFile.Close()
	return errors.Join(
		wrapIfError(directorySyncErr, "sync AOF directory after replacement"),
		wrapIfError(unlockErr, "unlock replaced AOF"),
		wrapIfError(closeErr, "close replaced AOF"),
	)
}

// writeSnapshotEntry serializes one current key as ordinary replayable
// commands. Lists are emitted one element at a time so no generated record
// exceeds the decoder's per-record size limit; absolute expiry preserves the
// key's original deadline rather than starting a fresh TTL.
func writeSnapshotEntry(writer *bufio.Writer, entry storage.SnapshotEntry) error {
	switch entry.Kind {
	case storage.StringKind:
		args := []string{entry.Key, entry.Value}
		if !entry.ExpiresAt.IsZero() {
			args = append(args, "PXAT", strconv.FormatInt(entry.ExpiresAt.UnixMilli(), 10))
		}
		return writeRecord(writer, command.Command{Name: "SET", Args: args})
	case storage.ListKind:
		for _, value := range entry.List {
			if err := writeRecord(writer, command.Command{
				Name: "RPUSH",
				Args: []string{entry.Key, value},
			}); err != nil {
				return err
			}
		}
		if !entry.ExpiresAt.IsZero() {
			return writeRecord(writer, command.Command{
				Name: "PEXPIREAT",
				Args: []string{entry.Key, strconv.FormatInt(entry.ExpiresAt.UnixMilli(), 10)},
			})
		}
		return nil
	default:
		return fmt.Errorf("cannot rewrite unsupported value kind %d for key %q", entry.Kind, entry.Key)
	}
}

// writeRecord encodes one command and refuses to emit a record that recovery
// would reject as too large. Buffering is per record, not per AOF file.
func writeRecord(writer *bufio.Writer, cmd command.Command) error {
	args := make([]resp.Value, 1, len(cmd.Args)+1)
	args[0] = resp.NewBulkString(cmd.Name)
	for _, arg := range cmd.Args {
		args = append(args, resp.NewBulkString(arg))
	}
	var record limitedRecordBuffer
	recordWriter := bufio.NewWriter(&record)
	if err := resp.NewEncoder(recordWriter).Encode(resp.NewArray(args...)); err != nil {
		return fmt.Errorf("encode snapshot command %s: %w", cmd.Name, err)
	}
	if err := recordWriter.Flush(); err != nil {
		return fmt.Errorf("flush encoded snapshot command %s: %w", cmd.Name, err)
	}
	if _, err := writer.Write(record.Bytes()); err != nil {
		return fmt.Errorf("write snapshot command %s: %w", cmd.Name, err)
	}
	return nil
}

// limitedRecordBuffer prevents a generated record from consuming more memory
// than recovery allows for decoding one record.
type limitedRecordBuffer struct {
	bytes.Buffer
}

func (b *limitedRecordBuffer) Write(p []byte) (int, error) {
	if len(p) > maxRecordBytes-b.Len() {
		return 0, fmt.Errorf("snapshot command exceeds AOF record limit of %d bytes", maxRecordBytes)
	}
	return b.Buffer.Write(p)
}

func wrapIfError(err error, operation string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", operation, err)
}

// Append writes a command as one RESP array and returns the exact command that
// the executor should apply. Relative expirations are converted to absolute
// deadlines so replay never renews their lifetime. A valid persisted command
// is flushed to the OS before it is applied in memory; "always" additionally
// syncs the file before returning. Commands that cannot be canonicalized are
// passed through without being journaled, allowing the command executor to
// produce its normal command-level response without recording malformed input.
func (l *Log) Append(cmd command.Command) (command.Command, error) {
	persisted, ok := canonicalCommand(cmd, time.Now())
	if !ok {
		return cmd, nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return command.Command{}, errors.New("AOF is closed")
	}
	if l.failed != nil {
		return command.Command{}, fmt.Errorf("AOF is unhealthy: %w", l.failed)
	}
	offset, err := l.file.Seek(0, io.SeekCurrent)
	if err != nil {
		l.failed = err
		return command.Command{}, fmt.Errorf("get AOF append offset: %w", err)
	}

	args := make([]resp.Value, 1, len(persisted.Args)+1)
	args[0] = resp.NewBulkString(persisted.Name)
	for _, arg := range persisted.Args {
		args = append(args, resp.NewBulkString(arg))
	}
	if err := resp.NewEncoder(l.writer).Encode(resp.NewArray(args...)); err != nil {
		return command.Command{}, l.rollback(offset, fmt.Errorf("encode AOF command: %w", err))
	}
	if err := l.writer.Flush(); err != nil {
		return command.Command{}, l.rollback(offset, fmt.Errorf("append AOF command: %w", err))
	}
	if l.policy == "always" {
		if err := l.file.Sync(); err != nil {
			return command.Command{}, l.rollback(offset, fmt.Errorf("sync AOF command: %w", err))
		}
	}
	return persisted, nil
}

// rollback removes a failed append so recovery cannot mistake its partial
// bytes for a valid record. If truncation, repositioning, or syncing the
// rollback fails, the log is marked unhealthy and rejects future appends.
func (l *Log) rollback(offset int64, cause error) error {
	if err := l.file.Truncate(offset); err != nil {
		l.failed = errors.Join(cause, fmt.Errorf("truncate failed AOF append: %w", err))
		return l.failed
	}
	if _, err := l.file.Seek(offset, io.SeekStart); err != nil {
		l.failed = errors.Join(cause, fmt.Errorf("seek after failed AOF append: %w", err))
		return l.failed
	}
	l.writer.Reset(l.file)
	if err := l.file.Sync(); err != nil {
		l.failed = errors.Join(cause, fmt.Errorf("sync AOF rollback: %w", err))
		return l.failed
	}
	l.failed = cause
	return cause
}

// Close stops periodic syncing, flushes and syncs pending data, releases the
// exclusive file lock, and closes the file. It is safe to call more than once.
func (l *Log) Close() error {
	l.closeOnce.Do(func() {
		if l.policy == "everysec" {
			close(l.stop)
			<-l.done
		}
		l.mu.Lock()
		defer l.mu.Unlock()
		l.closed = true
		l.closeErr = l.failed
		if err := l.writer.Flush(); err != nil {
			l.closeErr = errors.Join(l.closeErr, fmt.Errorf("flush AOF: %w", err))
		} else if err := l.file.Sync(); err != nil {
			l.closeErr = errors.Join(l.closeErr, fmt.Errorf("sync AOF on close: %w", err))
		}
		if err := unlockFile(l.file); err != nil {
			l.closeErr = errors.Join(l.closeErr, fmt.Errorf("unlock AOF: %w", err))
		}
		if err := l.file.Close(); err != nil {
			l.closeErr = errors.Join(l.closeErr, fmt.Errorf("close AOF: %w", err))
		}
	})
	return l.closeErr
}

// syncLoop implements the everysec policy. A sync failure is retained on Log
// and logged; Append then rejects future writes rather than acknowledging
// mutations that cannot meet the configured persistence contract.
func (l *Log) syncLoop() {
	defer close(l.done)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			l.mu.Lock()
			if !l.closed && l.failed == nil {
				if err := l.file.Sync(); err != nil {
					l.failed = err
					log.Printf("AOF periodic sync failed: %v", err)
				}
			}
			l.mu.Unlock()
		case <-l.stop:
			return
		}
	}
}

// countingReader tracks bytes fetched by bufio so replay can calculate the
// exact consumed record offset without seeking the file after every command.
type countingReader struct {
	reader io.Reader
	read   int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.read += int64(n)
	return n, err
}

// replay streams complete mutation records into store in file order. Memory
// use is bounded by the decoder buffer and one record instead of the full AOF
// size. It truncates only an incomplete final record; malformed complete
// records, read-only commands, and decoder errors fail recovery.
func replay(file *os.File, store *storage.Store) error {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek AOF to start: %w", err)
	}
	source := &countingReader{reader: file}
	reader := bufio.NewReaderSize(source, 4096)
	decoder := resp.NewDecoderWithLimit(reader, maxRecordBytes)
	executor := command.NewExecutor(store)
	parser := command.NewParser()
	var goodOffset int64

	for {
		value, decodeErr := decoder.Decode()
		// Subtract bytes still buffered from bytes fetched from disk to get the
		// exact logical file offset after this decode.
		offset := source.read - int64(reader.Buffered())
		if decodeErr != nil {
			if errors.Is(decodeErr, io.EOF) || errors.Is(decodeErr, io.ErrUnexpectedEOF) {
				if offset == goodOffset {
					break
				}
				if err := file.Truncate(goodOffset); err != nil {
					return fmt.Errorf("truncate incomplete AOF tail: %w", err)
				}
				log.Printf("AOF recovery discarded an incomplete final command at byte %d", goodOffset)
				break
			}
			return fmt.Errorf("decode command at byte %d: %w", goodOffset, decodeErr)
		}
		if offset <= goodOffset {
			return fmt.Errorf("decoder made no progress at byte %d", goodOffset)
		}
		parsed, err := parser.Parse(value)
		if err != nil {
			return fmt.Errorf("parse command at byte %d: %w", goodOffset, err)
		}
		if !command.IsMutation(parsed.Name) {
			return fmt.Errorf("unexpected non-mutation %q in AOF at byte %d", parsed.Name, goodOffset)
		}
		if _, err := executor.Execute(parsed); err != nil {
			return fmt.Errorf("replay command at byte %d: %w", goodOffset, err)
		}
		goodOffset = offset
	}
	return nil
}

// canonicalCommand makes a copy of a mutation suitable for stable replay.
// Relative SET and EXPIRE durations become absolute millisecond deadlines.
// It returns false for malformed or unsupported forms, which must not be
// journaled because they cannot be safely normalized for recovery.
func canonicalCommand(cmd command.Command, now time.Time) (command.Command, bool) {
	copyCommand := command.Command{Name: cmd.Name, Args: append([]string(nil), cmd.Args...)}
	switch cmd.Name {
	case "SET":
		if len(copyCommand.Args) < 2 {
			return command.Command{}, false
		}
		for i := 2; i < len(copyCommand.Args); {
			if i+1 >= len(copyCommand.Args) {
				return command.Command{}, false
			}
			option := strings.ToUpper(copyCommand.Args[i])
			switch option {
			case "EX":
				seconds, err := strconv.ParseInt(copyCommand.Args[i+1], 10, 64)
				if err != nil || seconds <= 0 || seconds > int64((1<<63-1)/int64(time.Second)) {
					return command.Command{}, false
				}
				copyCommand.Args[i], copyCommand.Args[i+1] = "PXAT", strconv.FormatInt(now.Add(time.Duration(seconds)*time.Second).UnixMilli(), 10)
			case "PX":
				milliseconds, err := strconv.ParseInt(copyCommand.Args[i+1], 10, 64)
				if err != nil || milliseconds <= 0 || milliseconds > int64((1<<63-1)/int64(time.Millisecond)) {
					return command.Command{}, false
				}
				copyCommand.Args[i], copyCommand.Args[i+1] = "PXAT", strconv.FormatInt(now.Add(time.Duration(milliseconds)*time.Millisecond).UnixMilli(), 10)
			case "PXAT":
				if _, err := strconv.ParseInt(copyCommand.Args[i+1], 10, 64); err != nil {
					return command.Command{}, false
				}
			default:
				return command.Command{}, false
			}
			i += 2
		}
	case "EXPIRE":
		if len(copyCommand.Args) != 2 {
			return command.Command{}, false
		}
		seconds, err := strconv.ParseInt(copyCommand.Args[1], 10, 64)
		if err != nil || seconds > int64((1<<63-1)/int64(time.Second)) {
			return command.Command{}, false
		}
		deadline := now
		if seconds > 0 {
			deadline = now.Add(time.Duration(seconds) * time.Second)
		}
		copyCommand.Name = "PEXPIREAT"
		copyCommand.Args[1] = strconv.FormatInt(deadline.UnixMilli(), 10)
	case "DEL":
		if len(copyCommand.Args) == 0 {
			return command.Command{}, false
		}
	default:
		if !command.IsMutation(copyCommand.Name) {
			return command.Command{}, false
		}
		// Current mutations other than SET and EXPIRE have no relative time
		// arguments. Add explicit normalization above if a future mutation
		// introduces a relative expiration.
	}
	return copyCommand, true
}
