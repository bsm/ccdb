package ccdb

import (
	"encoding/binary"
	"io"
)

// LogReader can lookup key/value pairs by offset
type LogReader struct {
	*fileReader
}

// OpenLog opens a log file for reading. Example:
//     ccdb.OpenLog("/path/to/my/db.ccl")
func OpenLog(fname string) (*LogReader, error) {
	reader, err := openFileReader(fname)
	if err != nil {
		return nil, err
	}

	return &LogReader{reader}, nil
}

// GetReader returns a key and a value reader
func (r *LogReader) GetReader(offset int64) ([]byte, *io.SectionReader, error) {
	if offset < fileHeaderLen || offset >= r.header.pos {
		return nil, nil, errInvalidOffset
	}

	buf := make([]byte, 20)
	if _, err := r.file.ReadAt(buf, offset); err != nil {
		return nil, nil, err
	}

	klen, n := binary.Uvarint(buf)
	vlen, m := binary.Uvarint(buf[n:])

	min := offset + int64(n+m)
	key := make([]byte, klen)
	if _, err := r.file.ReadAt(key, min); err != nil {
		return nil, nil, err
	}
	return key, io.NewSectionReader(r.file, min+int64(klen), int64(vlen)), nil
}

// Get returns a key/value pair at an offset
func (r *LogReader) Get(offset int64) ([]byte, []byte, error) {
	key, sr, err := r.GetReader(offset)
	if err != nil {
		return nil, nil, err
	}

	val, err := readSection(sr)
	return key, val, err
}

func (r *LogReader) iterator() *logIterator {
	return &logIterator{
		src:  r.file,
		pos:  fileHeaderLen,
		tbuf: make([]byte, 1),
	}
}

// --------------------------------------------------------------------

type logIterator struct {
	src io.Reader
	err error

	pos int64
	cur logEntry

	tbuf []byte
}

func (i *logIterator) ReadByte() (byte, error) {
	_, err := i.src.Read(i.tbuf)
	if err == nil {
		i.pos++
	}
	return i.tbuf[0], err
}

func (i *logIterator) Read(p []byte) (int, error) {
	n, err := i.src.Read(p)
	if err == nil {
		i.pos += int64(n)
	}
	return n, err
}

func (i *logIterator) Next() bool {
	if i.err != nil {
		return false
	}

	i.cur.Pos = i.pos
	kn, err := binary.ReadUvarint(i)
	if err != nil {
		i.err = err
		return false
	}

	vn, err := binary.ReadUvarint(i)
	if err != nil {
		i.err = err
		return false
	}

	i.cur.Key = make([]byte, int(kn))
	if _, i.err = i.Read(i.cur.Key); i.err != nil {
		return false
	}
	i.cur.Val = make([]byte, int(vn))
	if _, i.err = i.Read(i.cur.Val); i.err != nil {
		return false
	}
	return true
}

func (i *logIterator) Entry() *logEntry { return &i.cur }
func (i *logIterator) Error() error {
	if i.err == io.EOF {
		return nil
	}
	return i.err
}
