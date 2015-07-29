package ccdb

import (
	"errors"
	"fmt"
	"io"
)

const (
	magicNumber  uint16 = 0xCCDB
	majorVersion uint16 = 1
	minorVersion uint16 = 0

	numBuckets = 256

	checksumInit csum32 = 5381 // Initial checksum value
)

var (
	errBadMagic                = errors.New("ccdb: bad magic number")
	errWrongMajorVersion       = errors.New("ccdb: wrong major version")
	errUnsupportedMinorVersion = errors.New("ccdb: unsupported minor version")
	errHeaderCorrupt           = errors.New("ccdb: header corrupt")
	errHeaderDifferent         = errors.New("ccdb: file headers differ")
	errBadFileID               = errors.New("ccdb: bad file ID")
	errInvalidOffset           = errors.New("ccdb: invalid offset")
	errBlankKey                = errors.New("ccdb: keys must not be blank")
	errBlankValue              = errors.New("ccdb: values must not be blank")
)

type version struct {
	major, minor uint16
}

type slot struct {
	cksum csum32
	lpos  int64 // log position
}

// --------------------------------------------------------------------

type logEntry struct {
	Pos      int64
	Key, Val []byte
}

func (e *logEntry) Checksum() csum32 { return checksum(e.Key) }
func (e *logEntry) String() string   { return fmt.Sprintf("%010d: %s %s", e.Pos, e.Key, e.Val) }

// --------------------------------------------------------------------

type csum32 uint32

func (n csum32) Bucket() int { return int(n) % numBuckets }
func (n csum32) Slot() int   { return int(n) / numBuckets }

func checksum(data []byte) csum32 {
	h := checksumInit
	for _, b := range data {
		h = ((h << 5) + h) ^ csum32(b)
	}
	return h
}

func readSection(rd *io.SectionReader) ([]byte, error) {
	val := make([]byte, rd.Size())
	if _, err := rd.Read(val); err != nil {
		return nil, err
	}
	return val, nil
}
