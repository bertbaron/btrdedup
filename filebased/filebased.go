package filebased

import (
	"github.com/bertbaron/btrdedup/api"
	"os"
	"io/ioutil"
	"log"
	"bufio"
	"os/exec"
	"strconv"
	"bytes"
	"encoding/gob"
	"encoding/base64"
	"strings"
)

type FileBased struct {
	outfile    *os.File
	writer     *bufio.Writer
	infilename string
}

func NewInterface() *FileBased {
	return new(FileBased)
}

// ** PASS 1
func (state *FileBased) StartPass1() {
	var err error
	state.outfile, err = ioutil.TempFile("", "btrdedup")
	if err != nil {
		log.Fatalf("Unable to create temprary file")
	}

	log.Printf("Pass 1, writing to %s", state.outfile.Name())
	state.writer = bufio.NewWriter(state.outfile)
}

func (state *FileBased) AddFile(file api.FileInformation) {
	prefix := strconv.FormatInt(int64(file.PhysicalOffset), 36)
	writeFileInfo(prefix, file, state.writer)

}

func (state *FileBased) EndPass1() {
	state.writer.Flush()
	state.outfile.Close()
	state.infilename = state.outfile.Name()
	sort(state.infilename)
}

// ** PASS 2

// TODO seems like we can share some code with StartPass1
func (state *FileBased) StartPass2() {
	var err error
	state.outfile, err = ioutil.TempFile("", "btrdedup")
	if err != nil {
		log.Fatalf("Unable to create temprary file")
	}

	log.Printf("Pass 1, writing to %s", state.outfile.Name())
	state.writer = bufio.NewWriter(state.outfile)
}

func (state *FileBased) PartitionOnOffset(receiver func(files []api.FileInformation)) {
	partitionFile(state.infilename, receiver)
}

func (state *FileBased) ChecksumUpdated(files []api.FileInformation) {
	for _, file := range files {
		prefix := base64.StdEncoding.EncodeToString(file.Csum[:])
		writeFileInfo(prefix, file, state.writer)
	}
}

// TODO seems like we can share some code with EndPass1
func (state *FileBased) EndPass2() {
	state.writer.Flush()
	state.outfile.Close()
	state.infilename = state.outfile.Name()
	sort(state.infilename)
}

// ** PASS 3

func (state *FileBased) StartPass3() {
	//
}

func (state *FileBased) PartitionOnHash(receiver func(files []api.FileInformation)) {
	partitionFile(state.infilename, receiver)
}

func (state *FileBased) EndPass3() {
	//
}


// ** private functions
func sort(file string) {
	log.Printf("Sorting %s", file)
	command := exec.Command("sort", file, "-o", file)
	err := command.Run()
	if err != nil {
		log.Fatal("Failed to sort %s", file)
	}
	log.Printf("Sorted %s", file)
}

func serialize(fileInfo api.FileInformation) string {
	buffer := new(bytes.Buffer)
	enc := gob.NewEncoder(buffer)
	err := enc.Encode(fileInfo)
	if err != nil {
		log.Fatalf("Could not encode file information: %v", err)
	}
	return base64.StdEncoding.EncodeToString(buffer.Bytes())
}

func deserialize(line string) (*api.FileInformation, error) {
	data, err := base64.StdEncoding.DecodeString(line)
	if err != nil {
		return nil, err
	}
	buffer := bytes.NewReader(data)
	dec := gob.NewDecoder(buffer)
	fileInfo := new(api.FileInformation)
	err = dec.Decode(fileInfo)
	if err != nil {
		return nil, err
	}
	return fileInfo, nil
}

func writeFileInfo(prefix string, fileInfo api.FileInformation, outfile *bufio.Writer) {
	outfile.WriteString(prefix)
	outfile.WriteByte(' ')
	outfile.WriteString(serialize(fileInfo))
	outfile.WriteByte('\n')
}

func partitionFile(fileName string, receiver func([]api.FileInformation)) {
	infile, err := os.Open(fileName)
	if err != nil {
		log.Fatal("Failed to open %s", fileName)
	}
	defer infile.Close()
	scanner := bufio.NewScanner(infile)
	lastPrefix := ""
	files := make([]api.FileInformation, 0)
	for scanner.Scan() {
		line := scanner.Text()
		idx := strings.Index(line, " ")
		prefix := line[:idx]
		fileInfo, err := deserialize(line[idx + 1:])
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
		files = append(files, *fileInfo)
	}
	if len(files) != 0 {
		receiver(files)
	}
}
