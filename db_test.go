package ccdb

import (
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("DB", func() {

	It("should iterate over log offsets", func() {
		tests := []struct {
			key  string
			vals []string
		}{
			{"", []string{}},
			{"NOT FOUND", []string{}},

			{"key.0000", []string{"val.0000.00"}},
			{"key.0001", []string{"val.0001.00"}},
			{"key.0011", []string{"val.0011.00"}},
			{"key.0110", []string{"val.0110.00"}},
			{"key.0111", []string{"val.0111.00", "val.0111.01"}},
			{"key.0200", []string{"val.0200.00", "val.0200.01"}},
			{"key.0300", []string{"val.0300.00", "val.0300.01", "val.0300.02"}},
			{"key.0306", []string{"val.0306.00", "val.0306.01", "val.0306.02"}},
			{"key.0400", []string{"val.0400.00", "val.0400.01", "val.0400.02", "val.0400.03"}},
			{"key.0460", []string{"val.0460.00", "val.0460.01", "val.0460.02", "val.0460.03", "val.0460.04"}},
		}

		dir := mkTemp()
		defer os.RemoveAll(dir)

		lname, iname, err := writeTestLogAndIndex(dir, 500)
		Expect(err).NotTo(HaveOccurred())

		subject, err := Open(iname, lname)
		Expect(err).NotTo(HaveOccurred())
		defer subject.Close()

		for _, test := range tests {
			iter, err := subject.Get([]byte(test.key))
			Expect(err).NotTo(HaveOccurred(), "for %s", test.key)

			vals, err := iter.All()
			Expect(err).NotTo(HaveOccurred(), "for %s", test.key)

			strs := make([]string, len(vals))
			for i, val := range vals {
				strs[i] = string(val)
			}
			Expect(strs).To(Equal(test.vals), "for %s", test.key)
		}
	})

	It("should resolve key collisions", func() {
		dir := mkTemp()
		defer os.RemoveAll(dir)

		lname, iname, err := writeTestWithCollisions(dir, 20)
		subject, err := Open(iname, lname)
		Expect(err).NotTo(HaveOccurred())
		defer subject.Close()

		for _, key := range []string{"key.4985194", "key.5405800"} {
			bkey := []byte(key)
			Expect(checksum(bkey)).To(Equal(csum32(1954791040)))

			iter, err := subject.Get(bkey)
			Expect(err).NotTo(HaveOccurred())
			vals, err := iter.All()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(HaveLen(20))
		}
	})

})
