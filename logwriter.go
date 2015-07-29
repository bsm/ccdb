package ccdb

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

// LogWriters append to log files. WARNING: log writers are not thread-safe,
// use a single writer thread or protect concurrent writes with a mutex.
type LogWriter struct {
	header *fileHeader
	file   *os.File
	buffer *bufio.Writer

	mutex     sync.Mutex // write mutex
	seekToPos bool       // out-of-position

	tbuf []byte // temporary buffers
}

func newLogWriter(header *fileHeader, file *os.File) *LogWriter {
	return &LogWriter{
		header: header,
		file:   file,
		buffer: bufio.NewWriterSize(file, 1024*1024),
		tbuf:   make([]byte, binary.MaxVarintLen64),
	}
}

// CreateLog creates a new log file
func CreateLog(fname string) (*LogWriter, error) {
	file, err := os.Create(fname)
	if err != nil {
		return nil, err
	}

	header := newFileHeader()
	if _, err = header.WriteTo(file); err != nil {
		file.Close()
		return nil, err
	}

	return newLogWriter(header, file), nil
}

// AppendLog opens an existing log file, to append new data
func AppendLog(fname string) (*LogWriter, error) {
	file, err := os.OpenFile(fname, os.O_RDWR, 0664)
	if err != nil {
		return nil, err
	}

	header, err := readFileHeader(file)
	if err != nil {
		file.Close()
		return nil, err
	}

	if _, err = file.Seek(header.pos, os.SEEK_SET); err != nil {
		file.Close()
		return nil, err
	}

	return newLogWriter(header, file), nil
}

// Flush flushes all buffers, rewrites header and issues an fsync()
func (w *LogWriter) Flush() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	err := w.buffer.Flush()
	if err != nil {
		return err
	}

	w.seekToPos = true

	if _, err = w.file.Seek(0, os.SEEK_SET); err != nil {
		return err
	}
	if _, err = w.header.WriteTo(w.file); err != nil {
		return err
	}

	return w.file.Sync()
}

// Close flushes all buffers and closes the underlying file
func (w *LogWriter) Close() error {
	err := w.Flush()
	if e := w.file.Close(); e != nil {
		err = e
	}
	return err
}

// Put inserts a new key/value pair to the log
func (w *LogWriter) Put(key, val []byte) error {
	if len(key) == 0 {
		return errBlankKey
	} else if len(val) == 0 {
		return errBlankValue
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.seekToPos {
		if _, err := w.file.Seek(w.header.pos, os.SEEK_SET); err != nil {
			return err
		}
		w.seekToPos = false
	}

	n := binary.PutUvarint(w.tbuf, uint64(len(key)))
	if _, err := w.buffer.Write(w.tbuf[:n]); err != nil {
		return err
	}
	w.header.pos += int64(n)

	n = binary.PutUvarint(w.tbuf, uint64(len(val)))
	if _, err := w.buffer.Write(w.tbuf[:n]); err != nil {
		return err
	}
	w.header.pos += int64(n)

	n, err := w.buffer.Write(key)
	if err != nil {
		return err
	}
	w.header.pos += int64(n)

	n, err = w.buffer.Write(val)
	if err != nil {
		return err
	}
	w.header.pos += int64(n)

	return nil
}

// WriteIndex writes an index for the current log into the target file path
func (w *LogWriter) WriteIndex(fname string) error {
	if err := w.Flush(); err != nil {
		return err
	}
	return WriteIndex(fname, w.file.Name())
}
