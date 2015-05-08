package ccdb

import (
	"bytes"
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("WriteIndex", func() {

	It("should write index", func() {
		dir := mkTemp()
		defer os.RemoveAll(dir)

		fname, err := writeTestLog(dir, 50)
		Expect(err).NotTo(HaveOccurred())

		reader, err := OpenLog(fname)
		Expect(err).NotTo(HaveOccurred())
		defer reader.Close()

		out := &bytes.Buffer{}
		Expect(writeIndex(reader, out)).NotTo(HaveOccurred())
		Expect(out.Len()).To(Equal(2976))
	})

})
