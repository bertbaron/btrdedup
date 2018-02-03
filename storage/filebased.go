package storage

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"fmt"
	"github.com/bertbaron/btrdedup/sys"
	"github.com/pkg/errors"
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
	sortFile(state.infilename)
}

// ** PASS 2 **

func (state *FileBased) StartPass2() {
	initWriter(state)
}

func (state *FileBased) PartitionOnOffset(receiver func(files []*FileInformation) bool) {
	partitionFile(state.infilename, false, func(files []*FileInformation) {
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
	sortFile(state.infilename)
}

// ** PASS 3 **

func (state *FileBased) StartPass3() {}

func (state *FileBased) PartitionOnHash(receiver func(files []*FileInformation)) {
	partitionFile(state.infilename, true, receiver)
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

func bool2int(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func writeInt(buff *bytes.Buffer, value int64, size int) {
	for i := 0; i < size; i++ {
		buff.WriteByte(byte(value & 0xFF))
		value >>= 8
	}
}

func readInt(buff *bytes.Buffer, size int) int64 {
	value := int64(0)
	for i := 0; i < size; i++ {
		byte, err := buff.ReadByte()
		if err != nil {
			panic(err)
		}
		value = value | (int64(byte) << (uint(i) * 8))
	}
	return value
}

func serialize(fileInfo FileInformation) string {
	buf := new(bytes.Buffer)
	writeInt(buf, int64(fileInfo.Path), 4)
	writeInt(buf, bool2int(fileInfo.Error), 1)
	writeInt(buf, fileInfo.Size, 8)
	writeInt(buf, int64(len(fileInfo.Fragments)), 4)
	for _, frag := range fileInfo.Fragments {
		writeInt(buf, int64(frag.Start), 8)
		writeInt(buf, int64(frag.Length), 8)
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

func deserialize(line string) (fileInfo *FileInformation, err error) {
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				err = fmt.Errorf("pkg: %v", r)
			}
			err = errors.Wrapf(err, "Failed to parse %v", line)
		}
	}()

	data, err := base64.StdEncoding.DecodeString(line)
	if err != nil {
		return nil, err
	}
	buff := bytes.NewBuffer(data)
	fileInfo = new(FileInformation)
	fileInfo.Path = int32(readInt(buff, 4))
	fileInfo.Error = readInt(buff, 1) != 0
	fileInfo.Size = readInt(buff, 8)
	frags := int(readInt(buff, 4))
	fileInfo.Fragments = make([]sys.Fragment, frags)
	for i := 0; i < frags; i++ {
		start := uint64(readInt(buff, 8))
		end := uint64(readInt(buff, 8))
		fileInfo.Fragments[i] = sys.Fragment{start, end}
	}

	return fileInfo, nil
}

func writeFileInfo(prefix string, fileInfo FileInformation, outfile *bufio.Writer) {
	outfile.WriteString(prefix)
	outfile.WriteByte(' ')
	outfile.WriteString(serialize(fileInfo))
	outfile.WriteByte('\n')
}

func parseHash(s string) (hash [HashSize]byte, err error) {
	bytes, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return
	}
	copy(hash[:], bytes)
	return
}

// TODO using panic/recover for the whole parse stack would probably be more readable
func partitionFile(fileName string, prefixIsHash bool, receiver func([]*FileInformation)) {
	infile, err := os.Open(fileName)
	if err != nil {
		log.Fatal("Failed to open %s", fileName)
	}
	defer infile.Close()
	scanner := bufio.NewScanner(infile)
	scanner.Buffer(make([]byte, 4096), 10*1024*1024)
	lastPrefix := ""
	files := make([]*FileInformation, 0)
	for scanner.Scan() {
		line := scanner.Text()
		idx := strings.Index(line, " ")
		prefix := line[:idx]
		fileInfo, err := deserialize(line[idx + 1:])
		if err != nil {
			log.Fatalf("Failed to parse %s, %v", line, err)
		}
		if prefixIsHash {
			fileInfo.Csum, err = parseHash(prefix)
			if err != nil {
				log.Fatalf("Failed to parse %s, %v", line, err)
			}
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
	if err:=scanner.Err(); err != nil {
		log.Fatalf("Failed to parse %s, %v", fileName, err)
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
