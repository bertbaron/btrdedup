package storage

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

type FileBased struct {
	outfile    *os.File
	writer     *bufio.Writer
	infilename string
}

// Creates a file based storage instance
func NewFileBased() *FileBased {
	return new(FileBased)
}

// ** PASS 1 **
func (state *FileBased) StartPass1() {
	initWriter(state)
}

func (state *FileBased) AddFile(file FileInformation) {
	prefix := strconv.FormatInt(int64(file.PhysicalOffset()), 36)
	writeFileInfo(prefix, file, state.writer)

}

func (state *FileBased) EndPass1() {
	closeWriterAndSaveFilename(state)
}

// ** PASS 2 **

func (state *FileBased) StartPass2() {
	initWriter(state)
}

func (state *FileBased) PartitionOnOffset(receiver func(files []*FileInformation) bool) {
	partitionFile(state.infilename, func(files []*FileInformation) {
		if receiver(files) {
			for _, file := range files {
				prefix := base64.StdEncoding.EncodeToString(file.Csum[:])
				writeFileInfo(prefix, *file, state.writer)
			}
		}
	})

}

func (state *FileBased) EndPass2() {
	closeWriterAndSaveFilename(state)
}

// ** PASS 3 **

func (state *FileBased) StartPass3() {}

func (state *FileBased) PartitionOnHash(receiver func(files []*FileInformation)) {
	partitionFile(state.infilename, receiver)
}

func (state *FileBased) EndPass3() {}

// ** private functions **

func initWriter(state *FileBased) {
	var err error
	state.outfile, err = ioutil.TempFile("", "btrdedup")
	if err != nil {
		log.Fatalf("Unable to create temprary file")
	}

	log.Printf("Writing to %s", state.outfile.Name())
	state.writer = bufio.NewWriter(state.outfile)
}

func closeWriterAndSaveFilename(state *FileBased) {
	state.writer.Flush()
	state.outfile.Close()
	state.infilename = state.outfile.Name()
}

func serialize(fileInfo FileInformation) string {
	buffer := new(bytes.Buffer)
	enc := gob.NewEncoder(buffer)
	err := enc.Encode(fileInfo)
	if err != nil {
		log.Fatalf("Could not encode file information: %v", err)
	}
	return base64.StdEncoding.EncodeToString(buffer.Bytes())
}

func deserialize(line string) (*FileInformation, error) {
	data, err := base64.StdEncoding.DecodeString(line)
	if err != nil {
		return nil, err
	}
	buffer := bytes.NewReader(data)
	dec := gob.NewDecoder(buffer)
	fileInfo := new(FileInformation)
	err = dec.Decode(fileInfo)
	if err != nil {
		return nil, err
	}
	return fileInfo, nil
}

func writeFileInfo(prefix string, fileInfo FileInformation, outfile *bufio.Writer) {
	outfile.WriteString(prefix)
	outfile.WriteByte(' ')
	outfile.WriteString(serialize(fileInfo))
	outfile.WriteByte('\n')
}

func partitionFile(fileName string, receiver func([]*FileInformation)) {
	sortFile(fileName)
	infile, err := os.Open(fileName)
	if err != nil {
		log.Fatal("Failed to open %s", fileName)
	}
	defer infile.Close()
	scanner := bufio.NewScanner(infile)
	lastPrefix := ""
	files := make([]*FileInformation, 0)
	for scanner.Scan() {
		line := scanner.Text()
		idx := strings.Index(line, " ")
		prefix := line[:idx]
		fileInfo, err := deserialize(line[idx+1:])
		if err != nil {
			log.Fatalf("Failed to parse %s, %v", line, err)
		}
		if prefix != lastPrefix {
			if len(files) != 0 {
				receiver(files)
			}
			files = files[0:0]
			lastPrefix = prefix
		}
		files = append(files, fileInfo)
	}
	if len(files) != 0 {
		receiver(files)
	}
}

func sortFile(file string) {
	log.Printf("Sorting %s", file)
	command := exec.Command("sort", file, "-o", file)
	err := command.Run()
	if err != nil {
		log.Fatal("Failed to sort %s", file)
	}
	log.Printf("Sorted %s", file)
}
