package ccdb

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("LogWriter", func() {
	var subject *LogWriter
	var dir, fname string

	var doWrite = func(sfx string) {
		err := subject.Put([]byte("key"+sfx), []byte("value"+sfx))
		Expect(err).NotTo(HaveOccurred())

		err = subject.Put([]byte("longerkey"+sfx), []byte("v"+sfx))
		Expect(err).NotTo(HaveOccurred())
	}

	BeforeEach(func() {
		dir = mkTemp()
		fname = filepath.Join(dir, "data.ccl")

		var err error
		subject, err = CreateLog(fname)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		subject.Close()
		os.RemoveAll(dir)
	})

	It("should create", func() {
		Expect(subject.header.pos).To(Equal(int64(fileHeaderLen)))
	})

	It("should put/del", func() {
		doWrite("1")
		Expect(subject.header.pos).To(Equal(int64(154)))
	})

	It("should open, append and reopen", func() {
		// Write something
		doWrite("1")
		Expect(subject.Close()).NotTo(HaveOccurred())

		var err error
		subject, err = AppendLog(fname)
		Expect(err).NotTo(HaveOccurred())
		Expect(subject.header.pos).To(Equal(int64(154)))

		// Write more
		doWrite("2")
		Expect(subject.Flush()).NotTo(HaveOccurred())
		Expect(subject.header.pos).To(Equal(int64(180)))

		// Open iterator
		reader, err := OpenLog(fname)
		Expect(err).NotTo(HaveOccurred())
		defer reader.Close()
		iter := reader.iterator()

		// Write even more
		doWrite("3")

		var acc []logEntry
		for iter.Next() {
			acc = append(acc, *iter.Entry())
		}
		Expect(iter.Error()).NotTo(HaveOccurred())
		Expect(acc).To(Equal([]logEntry{
			{Pos: 128, Key: []byte("key1"), Val: []byte("value1")},
			{Pos: 140, Key: []byte("longerkey1"), Val: []byte("v1")},
			{Pos: 154, Key: []byte("key2"), Val: []byte("value2")},
			{Pos: 166, Key: []byte("longerkey2"), Val: []byte("v2")},
		}))
	})

	It("should index", func() {
		iname := filepath.Join(dir, "data.cci")
		doWrite("1")
		Expect(subject.WriteIndex(iname)).NotTo(HaveOccurred())

		info, err := os.Stat(iname)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Size()).To(Equal(int64(2208)))
	})

})
