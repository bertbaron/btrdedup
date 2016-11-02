package main

import (
	"bufio"
	"encoding/base64"
	"flag"
	"github.com/bertbaron/btrdedup/btrfs"
	"github.com/pkg/errors"
	"crypto/md5"
	"io/ioutil"
	"log"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/pprof"
	"syscall"
	"fmt"
	"bytes"
	"encoding/gob"
	"strconv"
	"strings"
)

const (
	minSize int64 = 4 * 1024
)

type FileInformation struct {
	Path           string
	PhysicalOffset uint64
	Size           int64
	Csum           *[16]byte
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

// readDirNames reads the directory named by dirname
func readDirNames(dirname string) ([]string, error) {
	f, err := os.Open(dirname)
	if err != nil {
		return nil, errors.Wrap(err, "open dir failed")
	}
	names, err := f.Readdirnames(-1)
	f.Close()
	return names, errors.Wrap(err, "reading dir names failed")
}

func readFileMeta(path string) (*FileInformation, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "open file failed")
	}
	defer f.Close()

	fragments, err := btrfs.Fragments(f)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to read fragments for file")
	}
	physicalOffset := fragments[0].Start
	return &FileInformation{path, physicalOffset, 0, nil}, nil
}

func makeChecksum(data []byte) [16]byte {
	return md5.Sum(data)
}

func readChecksum(path string) (*[16]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "open file failed")
	}
	defer f.Close()
	buffer := make([]byte, 4096)
	n1, err := f.Read(buffer)
	if err != nil {
		return nil, errors.Wrap(err, "reading from file")
	}
	if n1 < 4096 {
		// TODO we should probably need to repeat reading, but for now we assume that the full buffer is read at once
		return nil, errors.New("Less than 4k read, skipping block")
	}
	csum := makeChecksum(buffer)
	return &csum, nil
}

func writeFileInfo(prefix string, fileInfo FileInformation, outfile *bufio.Writer) {
	outfile.WriteString(prefix)
	outfile.WriteByte(' ')
	outfile.WriteString(serialize(fileInfo))
	outfile.WriteByte('\n')
}

// PRE: all files start at the same offset and files is not empty
func dumpChecksums(files []FileInformation, outfile *bufio.Writer) {
	path := files[0].Path
	csum, err := readChecksum(path)
	if err != nil {
		log.Printf("Error creating checksum for first block of file %s, %v", path, err)
		return
	}
	for _, file := range files {
		file.Csum = csum
		prefix := base64.StdEncoding.EncodeToString(csum[:])
		writeFileInfo(prefix, file, outfile)
	}
}

// todo: use filepath.Walk
func collectFileInformation(path string, outfile *bufio.Writer) {
	fi, err := os.Lstat(path)
	if err != nil {
		log.Printf("Error using os.Lstat on file %s: %v", path, err)
		return
	}

	if (fi.Mode() & (os.ModeSymlink | os.ModeNamedPipe)) != 0 {
		return
	}

	switch mode := fi.Mode(); {
	case mode.IsDir():
		elements, err := readDirNames(path)
		if err != nil {
			log.Printf("Error while reading the contents of directory %s: %v", path, err)
			return
		}
		for _, e := range elements {
			collectFileInformation(filepath.Join(path, e), outfile)
		}
	case mode.IsRegular():
		size := fi.Size()
		if size > minSize {
			fileInformation, err := readFileMeta(path)
			if err != nil {
				log.Printf("Error while trying to get the physical offset of file %s: %v", path, err)
				return
			}
			fileInformation.Size = size
			prefix := strconv.FormatInt(int64(fileInformation.PhysicalOffset), 36)
			writeFileInfo(prefix, *fileInformation, outfile)
		}
	}
}

// Submits the files for deduplication. Only if duplication seems to make sense the will actually be deduplicated
func submitForDedup(files []FileInformation, noact bool) {
	if len(files) < 2 || files[0].Csum == nil {
		return
	}

	// currently we assume that the files are equal up to the size of the smallest file
	var size int64 = math.MaxInt64
	for _, file := range files {
		if file.Size < size {
			size = file.Size
		}
	}

	filenames := make([]string, len(files))
	sameOffset := true
	physicalOffset := files[0].PhysicalOffset
	for i, file := range files {
		if file.PhysicalOffset != physicalOffset {
			sameOffset = false
		}
		filenames[i] = file.Path
	}
	if sameOffset {
		log.Printf("Skipping %s and %d other files, they all have the same physical offset", filenames[0], len(files) - 1)
		return
	}
	if !noact {
		log.Printf("Offering for deduplication: %s and %d other files\n", filenames[0], len(files) - 1)
		Dedup(filenames, 0, uint64(size))
	} else {
		log.Printf("Candidate for deduplication: %s and %d other files\n", filenames[0], len(files) - 1)
	}
}

// Increase open file limit if possible, currently simply to the limit. We may want to make an option for this...
func updateOpenFileLimit() {
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		log.Printf("Error Getting Rlimit ", err)
	}
	log.Printf("Current open file limit: %v", rLimit.Cur)
	if rLimit.Cur < rLimit.Max {
		rLimit.Cur = rLimit.Max
		err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
		if err != nil {
			log.Println("Error Setting Rlimit ", err)
		}
		err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
		if err != nil {
			log.Println("Error Getting Rlimit ", err)
		}
		log.Println("Open file limit increased to ", rLimit.Cur)
	}
}

func sort(file string) {
	log.Printf("Sorting %s", file)
	command := exec.Command("sort", file, "-o", file)
	err := command.Run()
	if err != nil {
		log.Fatal("Failed to sort %s", file)
	}
	log.Printf("Sorted %s", file)
}

func pass1(filenames []string) string {
	outfile, err := ioutil.TempFile("", "btrdedup")
	if err != nil {
		log.Fatalf("Unable to create temprary file")
	}
	log.Printf("Pass 1, writing to %s", outfile.Name())
	writer := bufio.NewWriter(outfile)
	for _, filename := range filenames {
		collectFileInformation(filename, writer)
	}
	writer.Flush()
	outfile.Close()

	sort(outfile.Name())
	return outfile.Name()
}

func partitionFile(fileName string, receiver func([]FileInformation)) {
	infile, err := os.Open(fileName)
	if err != nil {
		log.Fatal("Failed to open %s", fileName)
	}
	defer infile.Close()
	scanner := bufio.NewScanner(infile)
	lastPrefix := ""
	files := make([]FileInformation, 0)
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

func pass2(infileName string) string {
	outfile, err := ioutil.TempFile("", "btrdedup")
	if err != nil {
		log.Fatalf("Unable to create temprary file")
	}
	log.Printf("Pass 2, writing to %s", outfile.Name())
	writer := bufio.NewWriter(outfile)

	partitionFile(infileName, func(files []FileInformation) {
		dumpChecksums(files, writer)
	})

	writer.Flush()
	outfile.Close()

	sort(outfile.Name())
	return outfile.Name()
}

func pass3(infileName string, noact bool) {
	log.Printf("Pass 3, deduplucating files")

	partitionFile(infileName, func(files []FileInformation) {
		submitForDedup(files, noact)
	})
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [OPTION]... [FILE-OR-DIR]...\n", os.Args[0])
		flag.PrintDefaults()
	}
	noact := flag.Bool("noact", false, "if provided or true, the tool will only scan and log results, but not actually deduplicate")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	filenames := flag.Args()

	updateOpenFileLimit()

	tmpfile1 := pass1(filenames)

	tmpfile2 := pass2(tmpfile1)

	pass3(tmpfile2, *noact)

	log.Println("Done")
}
