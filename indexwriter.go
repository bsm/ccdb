package ccdb

import (
	"encoding/binary"
	"io"
	"os"
)

// WriteIndex iterates over log file and (over-)writes an index file
func WriteIndex(indexFileName, logFileName string) error {
	reader, err := OpenLog(logFileName)
	if err != nil {
		return err
	}
	defer reader.Close()

	dst, err := os.Create(indexFileName)
	if err != nil {
		return err
	}
	defer dst.Close()

	return writeIndex(reader, dst)
}

// writeIndex iterates over source log and writes an index
func writeIndex(reader *LogReader, dst io.Writer) error {

	// Accumulate bucket information
	iter := reader.iterator()
	buckets := make([][]slot, numBuckets)
	for iter.Next() {
		entry := iter.Entry()
		cksum := entry.Checksum()
		bucket := cksum.Bucket()

		buckets[bucket] = append(buckets[bucket], slot{cksum, entry.Pos})
	}

	// Stop on errors
	err := iter.Error()
	if err != nil {
		return err
	}

	// Create writer, write header, buckets index
	writer := newIndexWriter(dst)
	if err = writer.WriteHeader(reader.header); err != nil {
		return err
	} else if err = writer.WriteBuckets(buckets); err != nil {
		return err
	}

	// Create a temporary slots cache to avoid allocations
	maxSlots := 0
	for _, slots := range buckets {
		if size := len(slots); maxSlots < size {
			maxSlots = size
		}
	}
	cache := make([]slot, maxSlots*2)

	// Write slot info, 1-by-1
	for _, dense := range buckets {
		if err = writer.WriteSlots(dense, cache); err != nil {
			return err
		}
	}
	return nil
}

// --------------------------------------------------------------------

type indexWriter struct {
	dst io.Writer
	buf []byte // reusable buffer
}

func newIndexWriter(dst io.Writer) *indexWriter {
	return &indexWriter{
		dst: dst,
		buf: make([]byte, numBuckets*8),
	}
}

// WriteHeader writes the file header
func (w *indexWriter) WriteHeader(header *fileHeader) error {
	_, err := header.WriteTo(w.dst)
	return err
}

// WriteBuckets writes bucket index
func (w *indexWriter) WriteBuckets(buckets [][]slot) error {
	ipos := len(w.buf) + fileHeaderLen
	for i, slots := range buckets {
		nslots := len(slots) * 2
		binary.LittleEndian.PutUint32(w.buf[i*8:], uint32(ipos))
		binary.LittleEndian.PutUint32(w.buf[i*8+4:], uint32(nslots))
		ipos += 8 * nslots
	}

	_, err := w.dst.Write(w.buf)
	return err
}

// WriteSlots writes slot info
func (w *indexWriter) WriteSlots(dense []slot, slots []slot) error {
	if len(dense) == 0 {
		return nil
	}

	nslots := len(dense) * 2
	slots = slots[:nslots]

	// Reset slots
	for i := 0; i < len(slots); i++ {
		slots[i].lpos = 0
		slots[i].cksum = 0
	}

	// Populate slots
	for _, slot := range dense {
		n := slot.cksum.Slot() % nslots
		for slots[n].lpos != 0 {
			if n++; n == nslots {
				n = 0
			}
		}
		slots[n] = slot
	}

	for _, slot := range slots {
		binary.LittleEndian.PutUint32(w.buf[0:], uint32(slot.cksum))
		binary.LittleEndian.PutUint32(w.buf[4:], uint32(slot.lpos))
		if _, err := w.dst.Write(w.buf[:8]); err != nil {
			return err
		}
	}
	return nil
}
