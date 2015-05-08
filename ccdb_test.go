package ccdb

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("csum32", func() {

	It("should calculate checksums", func() {
		Expect(checksum([]byte("one"))).To(Equal(csum32(193420161)))
		Expect(checksum([]byte("two"))).To(Equal(csum32(193421353)))
		Expect(checksum([]byte("three"))).To(Equal(csum32(183191147)))
	})

	It("should extract slot and bucket information", func() {
		cs := checksum([]byte("one"))
		Expect(cs.Bucket()).To(Equal(129))
		Expect(cs.Slot()).To(Equal(755547))
	})

})

// --------------------------------------------------------------------

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ccdb")
}

// --------------------------------------------------------------------

func writeTestLog(dir string, size int) (string, error) {
	fullname := filepath.Join(dir, "test.ccl")
	writer, err := CreateLog(fullname)
	if err != nil {
		return fullname, err
	}
	defer writer.Close()

	for i := 0; i < size; i++ {
		key := []byte(fmt.Sprintf("key.%04d", i))

		for j := 0; j <= i/111; j++ {
			val := fmt.Sprintf("val.%04d.%02d", i, j)
			err := writer.Put(key, []byte(val))
			if err != nil {
				return "", err
			}
		}
	}
	return fullname, nil
}

func writeTestLogAndIndex(dir string, size int) (string, string, error) {
	iname := filepath.Join(dir, "test.cci")
	lname, err := writeTestLog(dir, size)
	if err != nil {
		return lname, iname, err
	}
	return lname, iname, WriteIndex(iname, lname)
}

func writeTestWithCollisions(dir string, size int) (string, string, error) {
	lname, iname := filepath.Join(dir, "coll.ccl"), filepath.Join(dir, "coll.cci")
	writer, err := CreateLog(lname)
	if err != nil {
		return lname, iname, err
	}
	defer writer.Close()

	key4, key5 := []byte("key.4985194"), []byte("key.5405800")
	for i := 0; i < size; i++ {
		if err = writer.Put(key4, []byte(fmt.Sprintf("va4.%04d", i))); err != nil {
			return lname, iname, err
		}
		if err = writer.Put(key5, []byte(fmt.Sprintf("va5.%04d", i))); err != nil {
			return lname, iname, err
		}
	}
	return lname, iname, writer.WriteIndex(iname)
}

// --------------------------------------------------------------------

func mkTemp() string {
	dir, err := ioutil.TempDir("", "ccdb-test")
	Expect(err).NotTo(HaveOccurred())
	return dir
}

func benchTempDir(b *testing.B) string {
	dir, err := ioutil.TempDir("", "ccdb-bench")
	if err != nil {
		b.Fatal(err)
	}
	return dir
}

func BenchmarkWrite(b *testing.B) {
	dir := benchTempDir(b)
	defer os.RemoveAll(dir)

	writer, err := CreateLog(filepath.Join(dir, "bench.ccl"))
	if err != nil {
		b.Fatal(err)
	}
	defer writer.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		val := []byte(fmt.Sprintf("data.%04d", i))
		err := writer.Put(val, val)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRead(b *testing.B) {
	dir := benchTempDir(b)
	defer os.RemoveAll(dir)

	writer, err := CreateLog(filepath.Join(dir, "bench.ccl"))
	if err != nil {
		b.Fatal(err)
	}
	defer writer.Close()

	for i := 0; i < 200000; i++ {
		val := []byte(fmt.Sprintf("data.%04d", i*2+1))
		if err := writer.Put(val, val); err != nil {
			b.Fatal(err)
		}
	}
	if err := writer.WriteIndex(filepath.Join(dir, "bench.cci")); err != nil {
		b.Fatal(err)
	}

	reader, err := Open(filepath.Join(dir, "bench.cci"), filepath.Join(dir, "bench.ccl"))
	if err != nil {
		b.Fatal(err)
	}
	defer reader.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter, err := reader.Get([]byte(fmt.Sprintf("data.%04d", i)))
		if err != nil {
			b.Fatal(err)
		} else if _, err := iter.All(); err != nil {
			b.Fatal(err)
		}
	}
}
