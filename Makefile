default: testshort

deps:
	go get -t ./...

test: testlong testrace

testlong:
	go test ./...

testshort:
	go test ./... -short

testrace:
	go test ./... -race -short -cpu=1,2,4

bench:
	go test ./... -bench=. -benchmem -run=NONE

