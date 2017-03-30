default: test test-race

deps:
	go get -t ./...

test:
	go test ./...

test-race:
	go test ./... -race -cpu=1,2,4

bench:
	go test ./... -bench=. -benchmem -run=NONE

README.md: README.md.tpl $(wildcard *.go)
	becca -package $(subst $(GOPATH)/src/,,$(PWD))
