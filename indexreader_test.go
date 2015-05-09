package ccdb

import (
	"fmt"
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("IndexReader", func() {

	var setup = func(dir string) (*IndexReader, error) {
		_, iname, err := writeTestLogAndIndex(dir, 500)
		if err != nil {
			return nil, err
		}

		return OpenIndex(iname)
	}

	It("should seek", func() {
		dir := mkTemp()
		defer os.RemoveAll(dir)

		reader, err := setup(dir)
		Expect(err).NotTo(HaveOccurred())
		defer reader.Close()

		for i := 0; i < 500; i++ {
			key := fmt.Sprintf("key.%04d", i)
			_, err := reader.Seek([]byte(key))
			Expect(err).NotTo(HaveOccurred(), "for %s", key)
		}
	})

	It("should iterate over log offsets", func() {
		tests := []struct {
			key  string
			offs []int64
		}{
			{"", nil},
			{"NOT FOUND", nil},
			{"MAYBE NOT", nil},

			{"key.0000", []int64{128}},
			{"key.0001", []int64{149}},
			{"key.0011", []int64{359}},
			{"key.0110", []int64{2438}},
			{"key.0111", []int64{2459, 2480}},
			{"key.0200", []int64{6197, 6218}},
			{"key.0300", []int64{12035, 12056, 12077}},
			{"key.0306", []int64{12413, 12434, 12455}},
			{"key.0400", []int64{19742, 19763, 19784, 19805}},
			{"key.0460", []int64{25118, 25139, 25160, 25181, 25202}},
		}

		dir := mkTemp()
		defer os.RemoveAll(dir)

		reader, err := setup(dir)
		Expect(err).NotTo(HaveOccurred())
		defer reader.Close()

		for _, test := range tests {
			iter, err := reader.Seek([]byte(test.key))
			Expect(err).NotTo(HaveOccurred(), "for %s", test.key)

			var offs []int64
			for iter.Next() {
				offs = append(offs, iter.Value())
			}
			Expect(offs).To(Equal(test.offs), "for %s", test.key)
			Expect(iter.Error()).NotTo(HaveOccurred(), "for %s", test.key)
		}
	})

	It("cannot resolve collisions", func() {
		dir := mkTemp()
		defer os.RemoveAll(dir)

		_, iname, err := writeTestWithCollisions(dir, 20)
		reader, err := OpenIndex(iname)
		Expect(err).NotTo(HaveOccurred())
		defer reader.Close()

		for _, key := range []string{"key.4985194", "key.5405800"} {
			var n int
			bkey := []byte(key)
			Expect(checksum(bkey)).To(Equal(csum32(1954791040)))

			iter, err := reader.Seek(bkey)
			Expect(err).NotTo(HaveOccurred())
			for iter.Next() {
				n++
			}
			Expect(iter.Error()).NotTo(HaveOccurred())
			Expect(n).To(Equal(40))
		}
	})

})
