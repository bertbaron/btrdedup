all: clean linux

VERSION=`git describe --tags --always --dirty)`
BUILD_TIME=`date +%FT%T%z`
LDFLAGS=-ldflags "-X main.version=${VERSION} -X main.buildTime=${BUILD_TIME}"

init:
	go get ./...

linux: init
	GOOS=linux GOARCH=amd64 go build ${LDFLAGS} -o btrdedup .

clean:
	-rm -f btrdedup
