package ccdb_test

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/bsm/ccdb"
)

func handleError(err error) {
	if err != nil {
		panic(err.Error())
	}
}

func ExampleLogWriter() {
	err := os.MkdirAll("testdata/example", 0755)
	handleError(err)
	defer os.RemoveAll("testdata/example")

	// Open a new log file
	log, err := ccdb.CreateLog("testdata/example/db.ccl")
	handleError(err)

	// Append data
	err = log.Put([]byte("foo"), []byte("value1"))
	handleError(err)
	err = log.Put([]byte("foo"), []byte("value2"))
	handleError(err)

	// Close log
	err = log.Close()
	handleError(err)

	// Re-open a log file, append more data
	log, err = ccdb.AppendLog("testdata/example/db.ccl")
	handleError(err)
	defer log.Close()

	err = log.Put([]byte("bar"), []byte("othervalue"))
	handleError(err)

	// Create an index spapshot
	err = log.WriteIndex("testdata/example/db.cci")
	handleError(err)

	// Close log again
	err = log.Close()
	handleError(err)

	// Read entries
	entries, err := filepath.Glob("testdata/example/*")
	handleError(err)
	fmt.Println(entries)

	// Output:
	// [testdata/example/db.cci testdata/example/db.ccl]
}

func ExampleDB() {
	// Open a database for reading
	db, err := ccdb.Open("testdata/data.cci", "testdata/data.ccl")
	handleError(err)
	defer db.Close()

	// Iterate over the values of a key
	iter, err := db.Get([]byte("foo"))
	handleError(err)

	fmt.Println("\nKEY: foo")
	for iter.Next() {
		val, _ := iter.Value()
		fmt.Println(string(val))
	}
	fmt.Println("ERROR:", iter.Error())

	// Or, read them all
	iter, err = db.Get([]byte("bar"))
	handleError(err)

	fmt.Println("\nKEY: bar")
	vals, err := iter.All()
	handleError(err)
	for _, val := range vals {
		fmt.Println(string(val))
	}
	fmt.Println("ERROR:", iter.Error())

	// Output:
	//
	// KEY: foo
	// value1
	// value2
	// ERROR: <nil>
	//
	// KEY: bar
	// othervalue
	// ERROR: <nil>
}
