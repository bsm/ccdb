package ccdb

import (
	"bytes"
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("fileReader", func() {

	It("should open files", func() {
		dir := mkTemp()
		defer os.RemoveAll(dir)

		fname, err := writeTestLog(dir, 1)
		Expect(err).NotTo(HaveOccurred())

		reader, err := openFileReader(fname)
		Expect(err).NotTo(HaveOccurred())
		defer reader.Close()

		Expect(reader.header).NotTo(BeNil())
	})

})

var _ = Describe("fileHeader", func() {
	var subject *fileHeader

	BeforeEach(func() {
		subject = newFileHeader()
		subject.id = 74682
		subject.pos = 8096
	})

	It("should dump and load", func() {
		buf := &bytes.Buffer{}
		n, err := subject.WriteTo(buf)
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(fileHeaderLen)))

		read, err := readFileHeader(bytes.NewReader(buf.Bytes()))
		Expect(err).NotTo(HaveOccurred())
		Expect(read).To(Equal(&fileHeader{
			version: version{majorVersion, minorVersion},
			id:      74682,
			pos:     8096,
		}))
	})

})
