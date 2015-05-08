/*
Package ccdb is a pure Go library to read and write ccdb ("continuous constant database") databases.
It is an adaptation of D. J. Bernstein's http://cr.yp.to/cdb.html design, inspired by ideas taken
from https://github.com/spotify/sparkey.

Examples & Workflow:

Open a new log file, append data & close:

    log, err := CreateLog("data.ccl")
    ...
    defer log.Close()

    err = log.Put([]byte("foo"), []byte("value1"))
    ...
    err = log.Put([]byte("foo"), []byte("value2"))
    ...

Re-open a log file, append more data:

    log, err := AppendLog("data.ccl")
    ...
    defer log.Close()

    err = log.Put([]byte("bar"), []byte("othervalue"))
    ...

Create an index spapshot:

    err := log.WriteIndex("data.cci")

Open a database for reading:

    db, err := Open("data.cci", "data.ccl")
    ...
    defer db.Close()

Iterate over the values of a key:

    iter, err := db.Get("foo")
    ...
    for iter.Next() {
        value, err := iter.Value()
        ...
    }
    if err := iter.Error(); err != nil {
        ...
    }

Or, read them all:

    iter, err := db.Get("foo")
    ...
    vals, err := iter.All()
    ...

*/
package ccdb
