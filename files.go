package ccdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
)

const fileHeaderLen = 128

// --------------------------------------------------------------------

type fileReader struct {
	file *os.File

	header *fileHeader
	closer io.Closer
}

func openFileReader(fname string) (*fileReader, error) {
	file, err := os.Open(fname)
	if err != nil {
		return nil, err
	}

	reader, err := newFileReader(file)
	if err != nil {
		file.Close()
		return nil, err
	}

	reader.closer = file
	return reader, nil
}

func newFileReader(file *os.File) (*fileReader, error) {
	header, err := readFileHeader(file)
	if err != nil {
		return nil, err
	}
	return &fileReader{file: file, header: header}, nil
}

// Close closes the reader
func (r *fileReader) Close() error {
	if r.closer != nil {
		return r.closer.Close()
	}
	return nil
}

// --------------------------------------------------------------------

type fileHeader struct {
	version
	id  uint32
	pos int64
}

func newFileHeader() *fileHeader {
	id := rand.Uint32()
	for id == 0 {
		id = rand.Uint32()
	}
	return &fileHeader{
		id:      id,
		version: version{majorVersion, minorVersion},
		pos:     fileHeaderLen,
	}
}

func readFileHeader(r io.Reader) (*fileHeader, error) {
	buf := make([]byte, fileHeaderLen)
	if _, err := r.Read(buf); err == io.EOF {
		return nil, errHeaderCorrupt
	} else if err != nil {
		return nil, err
	}

	h := fileHeader{}
	if magic := binary.LittleEndian.Uint16(buf[0:]); magic != magicNumber {
		return nil, errBadMagic
	} else if h.major = binary.LittleEndian.Uint16(buf[2:]); h.major != majorVersion {
		return nil, errWrongMajorVersion
	} else if h.minor = binary.LittleEndian.Uint16(buf[4:]); h.minor < minorVersion {
		return nil, errWrongMajorVersion
	} else if h.id = binary.LittleEndian.Uint32(buf[6:]); h.id == 0 {
		return nil, errBadFileID
	} else if h.pos = int64(binary.LittleEndian.Uint32(buf[10:])); h.pos < fileHeaderLen {
		return nil, errHeaderCorrupt
	}
	return &h, nil
}

func (h *fileHeader) String() string {
	return fmt.Sprintf("Version %d.%d\nIdentifier: %08x\nSize: %d\n", h.major, h.minor, h.id, h.pos)
}

func (h *fileHeader) WriteTo(w io.Writer) (int64, error) {
	buf := make([]byte, fileHeaderLen)
	binary.LittleEndian.PutUint16(buf[0:], magicNumber)
	binary.LittleEndian.PutUint16(buf[2:], majorVersion)
	binary.LittleEndian.PutUint16(buf[4:], minorVersion)
	binary.LittleEndian.PutUint32(buf[6:], h.id)
	binary.LittleEndian.PutUint32(buf[10:], uint32(h.pos))

	n, err := w.Write(buf)
	return int64(n), err
}
