all: clean linux

init:
	go get ./...

linux: init
	GOOS=linux GOARCH=amd64 go build -o btrdedup .

clean:
	-rm -f btrdedup
