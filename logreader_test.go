package ccdb

import (
	"io"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("LogReader", func() {
	var subject *LogReader
	var dir string

	BeforeEach(func() {
		dir = mkTemp()
		fname, err := writeTestLog(dir, 2)
		Expect(err).NotTo(HaveOccurred())

		subject, err = OpenLog(fname)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		subject.Close()
		os.RemoveAll(dir)
	})

	It("should open log files", func() {
		Expect(subject.header).NotTo(BeNil())
	})

	It("should error on bad offsets", func() {
		_, _, err := subject.Get(125)
		Expect(err).To(Equal(errInvalidOffset))

		_, _, err = subject.Get(1250000)
		Expect(err).To(Equal(errInvalidOffset))
	})

	It("should get k/v pairs", func() {
		key, val, err := subject.Get(128)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(key)).To(Equal("key.0000"))
		Expect(string(val)).To(Equal("val.0000.00"))

		_, _, err = subject.Get(129)
		Expect(err).To(Equal(io.EOF))

		key, val, err = subject.Get(149)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(key)).To(Equal("key.0001"))
		Expect(string(val)).To(Equal("val.0001.00"))
	})

	It("should get k/v readers", func() {
		key, reader, err := subject.GetReader(128)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(key)).To(Equal("key.0000"))
		Expect(reader.Size()).To(Equal(int64(11)))

		buf := make([]byte, 100)
		n, err := reader.Read(buf)
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(11))
		Expect(string(buf[:n])).To(Equal("val.0000.00"))
	})

	It("should retrieve small k/v pairs", func() {
		fname := filepath.Join(dir, "test2.ccl")
		writer, err := CreateLog(fname)
		Expect(err).NotTo(HaveOccurred())
		Expect(writer.Put([]byte{'a'}, []byte{'b'})).NotTo(HaveOccurred())
		Expect(writer.Close()).NotTo(HaveOccurred())

		reader, err := OpenLog(fname)
		Expect(err).NotTo(HaveOccurred())

		key, val, err := reader.Get(128)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(key)).To(Equal("a"))
		Expect(string(val)).To(Equal("b"))
	})

})

var _ = Describe("logIterator", func() {
	var subject *logIterator
	var reader *LogReader
	var dir string

	BeforeEach(func() {
		dir = mkTemp()

		fname, err := writeTestLog(dir, 500)
		Expect(err).NotTo(HaveOccurred())

		reader, err = OpenLog(fname)
		Expect(err).NotTo(HaveOccurred())

		subject = reader.iterator()
	})

	AfterEach(func() {
		reader.Close()
		os.RemoveAll(dir)
	})

	It("should iterate", func() {
		var acc []logEntry
		for subject.Next() {
			acc = append(acc, *subject.Entry())
		}
		Expect(subject.Error()).NotTo(HaveOccurred())
		Expect(acc).To(HaveLen(1390))

		Expect(acc[585]).To(Equal(logEntry{Pos: 12413, Key: []byte("key.0306"), Val: []byte("val.0306.00")}))
		Expect(acc[1025]).To(Equal(logEntry{Pos: 21653, Key: []byte("key.0422"), Val: []byte("val.0422.03")}))
	})
})
