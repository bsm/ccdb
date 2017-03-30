package ccdb

import (
	"bytes"
	"io"
)

// DB is a read-only abstraction of an index and a log-file combination
type DB struct {
	index *IndexReader
	log   *LogReader
}

// Open opens a DB for read-only access
func Open(indexFileName, logFileName string) (*DB, error) {
	index, err := OpenIndex(indexFileName)
	if err != nil {
		return nil, err
	}

	log, err := OpenLog(logFileName)
	if err != nil {
		index.Close()
		return nil, err
	}

	if log.header.id != log.header.id {
		index.Close()
		log.Close()
		return nil, errHeaderDifferent
	}

	return &DB{index: index, log: log}, nil
}

// Close closed the database
func (db *DB) Close() error {
	err := db.index.Close()
	if e := db.log.Close(); e != nil {
		err = e
	}
	return err
}

// Get retrieves a key and returns a value iterator
func (db *DB) Get(key []byte) (*Iterator, error) {
	ii, err := db.index.Seek(key)
	if err != nil {
		return nil, err
	}

	return &Iterator{ii: ii, key: key, log: db.log}, nil
}

// --------------------------------------------------------------------

// Iterator allows to iterate over values, associated with a key
type Iterator struct {
	ii  *IndexIterator
	log *LogReader
	key []byte

	cur *io.SectionReader
	err error
}

// All returns all values
func (i *Iterator) All() ([][]byte, error) {
	var vals [][]byte
	for i.Next() {
		val, err := i.Value()
		if err != nil {
			return nil, err
		}
		vals = append(vals, val)
	}
	return vals, i.Error()
}

// Next advances to the next item, returns true if successful
func (i *Iterator) Next() bool {
	if i.err != nil {
		return false
	}

	for i.ii.Next() {
		key, reader, err := i.log.GetReader(i.ii.Value())
		if err != nil {
			i.err = err
			return false
		} else if bytes.Equal(i.key, key) {
			i.cur = reader
			return true
		}
	}
	i.err = i.ii.Error()
	return false
}

// Value returns the value
func (i *Iterator) Value() ([]byte, error) {
	if i.cur == nil {
		return nil, nil
	}
	return readSection(i.cur)
}

// Section returns a redable section
func (i *Iterator) Section() *io.SectionReader { return i.cur }

// Error returns errors if any occurred
func (i *Iterator) Error() error { return i.err }
