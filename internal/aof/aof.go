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
	closing   bool
	failed    error
	closeErr  error
	rewrite   *rewriteJob
}

type rewriteJob struct {
	tempPath    string
	tempFile    *os.File
	tempWriter  *bufio.Writer
	deltaPath   string
	deltaFile   *os.File
	deltaWriter *bufio.Writer
	ready       chan error
	done        chan struct{}
	deltaErr    error
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

// StartRewrite prepares an asynchronously written snapshot. Mutations are
// captured in a sidecar delta while the snapshot is written; the executor
// installs the rewrite under its mutation barrier after ready reports success.
func (l *Log) StartRewrite(entries []storage.SnapshotEntry) (<-chan error, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.closing {
		return nil, errors.New("AOF is closed")
	}
	if l.failed != nil {
		return nil, fmt.Errorf("AOF is unhealthy: %w", l.failed)
	}
	if l.rewrite != nil {
		return nil, errors.New("AOF rewrite already in progress")
	}

	directory := filepath.Dir(l.path)
	tempFile, err := os.CreateTemp(directory, "."+filepath.Base(l.path)+".rewrite-*")
	if err != nil {
		return nil, fmt.Errorf("create AOF rewrite file: %w", err)
	}
	cleanupTemp := func() error {
		return errors.Join(tempFile.Close(), os.Remove(tempFile.Name()))
	}
	if err := tempFile.Chmod(0o600); err != nil {
		return nil, errors.Join(fmt.Errorf("set AOF rewrite file permissions: %w", err), cleanupTemp())
	}
	if err := lockFile(tempFile); err != nil {
		return nil, errors.Join(fmt.Errorf("lock AOF rewrite file: %w", err), cleanupTemp())
	}
	deltaFile, err := os.CreateTemp(directory, "."+filepath.Base(l.path)+".delta-*")
	if err != nil {
		_ = unlockFile(tempFile)
		return nil, errors.Join(fmt.Errorf("create AOF rewrite delta: %w", err), cleanupTemp())
	}
	if err := deltaFile.Chmod(0o600); err != nil {
		_ = deltaFile.Close()
		_ = os.Remove(deltaFile.Name())
		_ = unlockFile(tempFile)
		return nil, errors.Join(fmt.Errorf("set AOF rewrite delta permissions: %w", err), cleanupTemp())
	}

	job := &rewriteJob{
		tempPath:    tempFile.Name(),
		tempFile:    tempFile,
		tempWriter:  bufio.NewWriterSize(tempFile, 64<<10),
		deltaPath:   deltaFile.Name(),
		deltaFile:   deltaFile,
		deltaWriter: bufio.NewWriterSize(deltaFile, 64<<10),
		ready:       make(chan error, 1),
		done:        make(chan struct{}),
	}
	l.rewrite = job
	go func() {
		var snapshotErr error
		for _, entry := range entries {
			if err := writeSnapshotEntry(job.tempWriter, entry); err != nil {
				snapshotErr = fmt.Errorf("write AOF rewrite snapshot: %w", err)
				break
			}
		}
		if snapshotErr == nil {
			if err := job.tempWriter.Flush(); err != nil {
				snapshotErr = fmt.Errorf("flush AOF rewrite snapshot: %w", err)
			}
		}
		job.ready <- snapshotErr
	}()
	return job.ready, nil
}

// CompleteRewrite appends the captured mutation delta and atomically installs
// the replacement. Call only after the snapshot worker is ready and while the
// executor mutation barrier is held.
func (l *Log) CompleteRewrite() (resultErr error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	job := l.rewrite
	if job == nil {
		return errors.New("AOF rewrite is not active")
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, l.cleanupRewriteLocked(job))
		}
		l.rewrite = nil
		close(job.done)
	}()
	if l.closed {
		return errors.New("AOF is closed")
	}
	if l.failed != nil {
		return fmt.Errorf("AOF is unhealthy: %w", l.failed)
	}
	if job.deltaErr != nil {
		return fmt.Errorf("record AOF rewrite delta: %w", job.deltaErr)
	}
	if err := l.writer.Flush(); err != nil {
		l.failed = fmt.Errorf("flush current AOF before replacement: %w", err)
		return l.failed
	}
	if err := job.deltaWriter.Flush(); err != nil {
		return fmt.Errorf("flush AOF rewrite delta: %w", err)
	}
	if _, err := job.deltaFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek AOF rewrite delta: %w", err)
	}
	if _, err := io.Copy(job.tempWriter, job.deltaFile); err != nil {
		return fmt.Errorf("append AOF rewrite delta: %w", err)
	}
	if err := job.tempWriter.Flush(); err != nil {
		return fmt.Errorf("flush completed AOF rewrite: %w", err)
	}
	if err := job.tempFile.Sync(); err != nil {
		return fmt.Errorf("sync completed AOF rewrite: %w", err)
	}
	if err := os.Rename(job.tempPath, l.path); err != nil {
		return fmt.Errorf("atomically replace AOF: %w", err)
	}

	oldFile := l.file
	l.file = job.tempFile
	l.writer = bufio.NewWriter(job.tempFile)
	job.tempFile = nil
	directorySyncErr := syncDirectory(l.path)
	if directorySyncErr != nil {
		l.failed = fmt.Errorf("sync AOF directory after replacement: %w", directorySyncErr)
		log.Printf("AOF rewrite directory sync failed: %v", directorySyncErr)
	}
	unlockErr := unlockFile(oldFile)
	closeErr := oldFile.Close()
	deltaCloseErr := job.deltaFile.Close()
	deltaRemoveErr := os.Remove(job.deltaPath)
	job.deltaFile = nil
	return errors.Join(
		wrapIfError(directorySyncErr, "sync AOF directory after replacement"),
		wrapIfError(unlockErr, "unlock replaced AOF"),
		wrapIfError(closeErr, "close replaced AOF"),
		wrapIfError(deltaCloseErr, "close AOF rewrite delta"),
		wrapIfError(deltaRemoveErr, "remove AOF rewrite delta"),
	)
}

// AbortRewrite removes an unfinished replacement while leaving the active AOF
// untouched.
func (l *Log) AbortRewrite(cause error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.rewrite == nil {
		return nil
	}
	job := l.rewrite
	l.rewrite = nil
	cleanupErr := l.cleanupRewriteLocked(job)
	close(job.done)
	return errors.Join(cause, cleanupErr)
}

func (l *Log) cleanupRewriteLocked(job *rewriteJob) error {
	var errs []error
	if job.tempFile != nil {
		errs = append(errs, unlockFile(job.tempFile), job.tempFile.Close())
	}
	if job.deltaFile != nil {
		errs = append(errs, job.deltaFile.Close())
	}
	errs = append(errs, removeRewriteFile(job.tempPath), removeRewriteFile(job.deltaPath))
	return errors.Join(errs...)
}

func removeRewriteFile(path string) error {
	err := os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// Rewrite keeps the synchronous journal API for callers that need to wait for
// completion. Server commands use StartRewrite and report status instead.
func (l *Log) Rewrite(store *storage.Store) error {
	entries, err := store.SnapshotEntries()
	if err != nil {
		return err
	}
	ready, err := l.StartRewrite(entries)
	if err != nil {
		return err
	}
	if err := <-ready; err != nil {
		return l.AbortRewrite(err)
	}
	return l.CompleteRewrite()
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

// Append writes one command through the batch append path.
func (l *Log) Append(cmd command.Command) (command.Command, error) {
	results, err := l.AppendBatch([]command.Command{cmd})
	if err != nil {
		return command.Command{}, err
	}
	return results[0], nil
}

// GroupCommitDelay allows concurrent always-sync writes to join a bounded
// 250-microsecond commit window. Other policies batch requests already queued
// without adding an intentional wait.
func (l *Log) GroupCommitDelay() time.Duration {
	if l.policy == "always" {
		return 250 * time.Microsecond
	}
	return 0
}

// AppendBatch canonicalizes, appends, and syncs a batch as one journal
// transaction. Any append or required sync failure rolls the complete batch
// back before the executor applies any mutation.
func (l *Log) AppendBatch(commands []command.Command) ([]command.Command, error) {
	persisted := make([]command.Command, len(commands))
	valid := make([]bool, len(commands))
	for i, cmd := range commands {
		canonical, ok := canonicalCommand(cmd, time.Now())
		if ok {
			persisted[i] = canonical
			valid[i] = true
		} else {
			persisted[i] = cmd
		}
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.closing {
		return nil, errors.New("AOF is closed")
	}
	if l.failed != nil {
		return nil, fmt.Errorf("AOF is unhealthy: %w", l.failed)
	}
	if len(commands) == 0 {
		return persisted, nil
	}
	offset, err := l.file.Seek(0, io.SeekCurrent)
	if err != nil {
		l.failed = err
		return nil, fmt.Errorf("get AOF append offset: %w", err)
	}
	written := 0
	for i, cmd := range persisted {
		if !valid[i] {
			continue
		}
		if err := writeRecord(l.writer, cmd); err != nil {
			return nil, l.rollback(offset, fmt.Errorf("encode AOF command %s: %w", cmd.Name, err))
		}
		written++
	}
	if written > 0 {
		if err := l.writer.Flush(); err != nil {
			return nil, l.rollback(offset, fmt.Errorf("append AOF command batch: %w", err))
		}
	}
	if written > 0 && l.policy == "always" {
		if err := l.file.Sync(); err != nil {
			return nil, l.rollback(offset, fmt.Errorf("sync AOF command batch: %w", err))
		}
	}
	if job := l.rewrite; job != nil && job.deltaErr == nil {
		for i, cmd := range persisted {
			if valid[i] {
				if err := writeRecord(job.deltaWriter, cmd); err != nil {
					job.deltaErr = err
					break
				}
			}
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
		l.closing = true
		rewrite := l.rewrite
		l.mu.Unlock()
		if rewrite != nil {
			<-rewrite.done
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
