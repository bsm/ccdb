# ccdb [![Build Status](https://travis-ci.org/bsm/ccdb.png)](https://travis-ci.org/bsm/ccdb)

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

## Licence (MIT)

```
Copyright (c) 2015 Black Square Media

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```
