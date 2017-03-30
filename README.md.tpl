# ccdb

[![Build Status](https://travis-ci.org/bsm/ccdb.png)](https://travis-ci.org/bsm/ccdb)
[![GoDoc](https://godoc.org/github.com/bsm/ccdb?status.png)](http://godoc.org/github.com/bsm/ccdb)
[![Go Report Card](https://goreportcard.com/badge/github.com/bsm/ccdb)](https://goreportcard.com/report/github.com/bsm/ccdb)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

ccdb is a pure Go library to read and write ccdb ("continuous constant database") databases.
It is an adaptation of D. J. Bernstein's [cdb](http://cr.yp.to/cdb.html) design, inspired by
ideas taken from [sparkey](https://github.com/spotify/sparkey).

## Features

* Written in pure [Go](http://golang.org), no dependencies beyond stdlib.
* All the features of [cdb](http://cr.yp.to/cdb.html) fast & simple.
* Multiple values per key.
* Databases are thread-safe.
* Support for multiple, concurrent readers.
* Data is always appended and never replaced.
* Closed databases can be re-opened and appended to.
* Values can be streamed (`io.Reader`).
* Log and index are stored in separate files as proposed by [sparkey](https://github.com/spotify/sparkey#design): "The advantages of having two files instead of just one is that it's trivial to mlock one of the files and not the other. It also enables us to append more data to existing log files, even after it's already in use."

## Documentation

Check out the full API on [godoc.org](http://godoc.org/github.com/bsm/ccdb).

## Workflow

First, write/append your data to a log. You can index your logs:

```go
import(
  "fmt"
  "os"
  "path/filepath"

  "github.com/bsm/ccdb"
)

func main() {{ "ExampleLogWriter" | code }}
```

Open the DB for reading:

```go
import (
  "fmt"

  "github.com/bsm/ccdb"
)

func main() {{ "ExampleDB" | code }}
```
