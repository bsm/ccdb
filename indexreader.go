package ccdb

import (
	"encoding/binary"
	"io"
)

// IndexReader can search index files for log offsets
type IndexReader struct {
	*fileReader
}

// OpenIndex opens an index file for reading/searching. Example:
//     ccdb.OpenIndex("/path/to/my/db.cci")
func OpenIndex(fname string) (*IndexReader, error) {
	reader, err := openFileReader(fname)
	if err != nil {
		return nil, err
	}

	return &IndexReader{reader}, nil
}

// Seek returns an log-offset iterator
func (i *IndexReader) Seek(key []byte) (*IndexIterator, error) {
	cksum := checksum(key)
	tbuf := make([]byte, 8)

	offset, nslots, err := i.seekBucket(cksum.Bucket(), tbuf)
	if err != nil {
		return nil, err
	}

	iter := &IndexIterator{
		src:    i.file,
		cksum:  cksum,
		offset: offset,
		nslots: nslots,
		tbuf:   tbuf,
	}
	if nslots > 0 {
		iter.cursor = cksum.Slot() % nslots
	}
	return iter, nil
}

func (i *IndexReader) seekBucket(n int, tbuf []byte) (int64, int, error) {
	_, err := i.file.ReadAt(tbuf, fileHeaderLen+int64(n*8))
	if err != nil {
		return 0, 0, err
	}

	return int64(binary.LittleEndian.Uint32(tbuf[0:])),
		int(binary.LittleEndian.Uint32(tbuf[4:])),
		nil
}

// --------------------------------------------------------------------

// IndexIterator allows index readers to iterate over matching offsets
type IndexIterator struct {
	src    io.ReaderAt
	cksum  csum32
	nslots int
	offset int64

	current slot
	cursor  int
	steps   int

	err  error
	tbuf []byte
}

// Value returns the current log offset
func (i *IndexIterator) Value() int64 { return i.current.lpos }

// Error returns an error if one occurred during iteration
func (i *IndexIterator) Error() error { return i.err }

// Next advances to the next matching slot, returns true if successful
func (i *IndexIterator) Next() bool {
	for i.err == nil && i.steps < i.nslots {
		slot, err := i.readCurrent()

		if err != nil {
			i.err = err
			break
		}

		if slot.lpos == 0 {
			i.steps = i.nslots
			break
		}

		i.steps++
		if i.cursor++; i.cursor == i.nslots {
			i.cursor = 0
		}

		if slot.cksum == i.cksum {
			i.current = slot
			return true
		}
	}
	return false
}

func (i *IndexIterator) readCurrent() (s slot, err error) {
	if _, err = i.src.ReadAt(i.tbuf, i.offset+int64(i.cursor*8)); err != nil {
		return
	}

	s.cksum = csum32(binary.LittleEndian.Uint32(i.tbuf[0:]))
	s.lpos = int64(binary.LittleEndian.Uint32(i.tbuf[4:]))
	return
}
