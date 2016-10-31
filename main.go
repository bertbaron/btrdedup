package main

import (
	"bufio"
	"encoding/hex"
	"encoding/base64"
	"flag"
	"github.com/bertbaron/btrdedup/btrfs"
	"github.com/pkg/errors"
	"github.com/spaolacci/murmur3"
	//"crypto/md5"
	//"hash/fnv"
	"io/ioutil"
	"log"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"strings"
	"syscall"
	"fmt"
)

const (
	minSize int64 = 4 * 1024
)

type FileInformation struct {
	Path           string
	physicalOffset uint64
	Size           int64
	csum           *[16]byte
}

func serialize(fileInfo FileInformation, withCsum bool) string {
	//log.Printf("fileInfo: %v", fileInfo)
	offset := strconv.FormatInt(int64(fileInfo.physicalOffset), 16)
	path := base64.StdEncoding.EncodeToString([]byte(fileInfo.Path))
	size := strconv.FormatInt(fileInfo.Size, 16)
	encoded := offset + " " + path + " " + size
	if withCsum {
		encoded = hex.EncodeToString(fileInfo.csum[:]) + " " + encoded
	}
	return encoded
}

func deserialize(line string) (*FileInformation, error) {
	//fields := strings.Fields(line)
	fields := strings.Split(line, " ")
	if len(fields) < 3 {
		return nil, errors.Errorf("Too few fields in line: %s", line)
	}
	var fileInfo FileInformation
	if len(fields) >= 4 {
		csumbytes, err := hex.DecodeString(fields[0])
		if err != nil {
			return nil, errors.Wrapf(err, "Error parsing checksum from %s", fields[0])
		}
		var csum [16]byte
		copy(csum[:], csumbytes)
		fileInfo.csum = &csum
		fields = fields[1:]
	}

	offset, err := strconv.ParseInt(fields[0], 16, 64)
	if err != nil {
		return nil, errors.Wrapf(err, "Error parsing offset from %s", fields[0])
	}
	fileInfo.physicalOffset = uint64(offset)
	fields = fields[1:]

	bytes, err := base64.StdEncoding.DecodeString(fields[0])
	if err != nil {
		return nil, errors.Wrapf(err, "Error parsing path from %s", fields[0])
	}
	fileInfo.Path = string(bytes)
	fields = fields[1:]

	filesize, err := strconv.ParseInt(fields[0], 16, 64)
	if (err != nil) {
		return nil, errors.Wrapf(err, "Error parsing file size from %s", fields[0])
	}
	fileInfo.Size = filesize
	return &fileInfo, nil
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
	physicalOffset, err := btrfs.PhysicalOffset(f)
	if err != nil {
		return nil, errors.Wrap(err, "Faild to read physical offset of file")
	}
	// We also need to ensure the first block is at least 4k, even though this will probably always be the case
	return &FileInformation{path, physicalOffset, 0, nil}, nil
}

// Most simple translation of uint64 into a byte slice. Yes I know its reversed, but that doesn't matter for us
func putInt(data []byte, value uint64) {
	for i := 0; i < 8; i++ {
		data[i] = byte(value & 0xFF)
		value >>= 8
	}
}

func makeChecksum(data []byte) [16]byte {
	//hasher := fnv.New64()
	//hasher.Write(data)
	//csum1 := hasher.Sum64()
	//return md5.Sum(data)
	csum1, csum2 := murmur3.Sum128(data)
	var bytes [16]byte
	putInt(bytes[0:8], csum1)
	putInt(bytes[8:16], csum2)
	return bytes
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

// PRE: all files start at the same offset
func dumpChecksums(files []FileInformation, outfile *bufio.Writer) {
	if len(files) == 0 {
		return
	}

	path := files[0].Path
	csum, err := readChecksum(path)
	if err != nil {
		log.Printf("Error creating checksum for first block of file %s, %v", path, err)
		return
	}
	for _, file := range files {
		file.csum = csum
		outfile.WriteString(serialize(file, true))
		outfile.WriteByte('\n')
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
			outfile.WriteString(serialize(*fileInformation, false))
			outfile.WriteByte('\n')
			//dumpAsJson(strconv.FormatInt(int64(fileInformation.physicalOffset), 10), fileInformation, outfile)
		}
	}
}

// Submits the files for deduplication. Only if duplication seems to make sense the will actually be deduplicated
func submitForDedup(files []FileInformation, noact bool) {
	//log.Println("Dedup:")
	//for _, file := range files {
	//	log.Printf("  %+v: %v\n", file, file.csum)
	//}
	if len(files) < 2 {
		return
	}
	if files[0].csum == nil {
		return // for now we have to deel with nil here...
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
	physicalOffset := files[0].physicalOffset
	for i, file := range files {
		if file.physicalOffset != physicalOffset {
			sameOffset = false
		}
		filenames[i] = file.Path
	}
	if sameOffset {
		log.Printf("Skipping %s and %d other files, they all have the same physical offset", filenames[0], len(files)-1)
		return
	}
	if !noact {
		log.Printf("Offering for deduplication: %s and %d other files\n", filenames[0], len(files)-1)
		Dedup(filenames, 0, uint64(size))
	} else {
		log.Printf("Candidate for deduplication: %s and %d other files\n", filenames[0], len(files)-1)
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

func pass2(infileName string) string {
	infile, err := os.Open(infileName)
	if err != nil {
		log.Fatal("Failed to open %s", infileName)
	}
	defer infile.Close()
	outfile, err := ioutil.TempFile("", "btrdedup")
	if err != nil {
		log.Fatalf("Unable to create temprary file")
	}
	log.Printf("Pass 2, writing to %s", outfile.Name())
	writer := bufio.NewWriter(outfile)

	scanner := bufio.NewScanner(infile)
	lastOffset := uint64(0)
	files := make([]FileInformation, 0)
	for scanner.Scan() {
		line := scanner.Text()
		//idx := strings.Index(line, " ")
		//offset := line[:idx]
		fileInfo, err := deserialize(line)
		if err != nil {
			log.Fatalf("Failed to parse %s, %v", line, err)
		}
		if fileInfo.physicalOffset != lastOffset {
			dumpChecksums(files, writer)
			files = files[0:0]
			lastOffset = fileInfo.physicalOffset
		}
		files = append(files, *fileInfo)
	}
	dumpChecksums(files, writer)

	writer.Flush()
	outfile.Close()

	sort(outfile.Name())
	return outfile.Name()
}

func pass3(infileName string, noact bool) {
	infile, err := os.Open(infileName)
	if err != nil {
		log.Fatal("Failed to open %s", infileName)
	}
	defer infile.Close()
	log.Printf("Pass 3, deduplucating files")

	scanner := bufio.NewScanner(infile)
	var lastHash [16]byte
	files := make([]FileInformation, 0)
	for scanner.Scan() {
		line := scanner.Text()
		fileInfo, err := deserialize(line)
		if err != nil {
			log.Fatalf("Failed to parse %s, %v", line, err)
		}
		var hash [16]byte = *fileInfo.csum
		if hash != lastHash {
			submitForDedup(files, noact)
			files = files[0:0]
			lastHash = hash
		}
		files = append(files, *fileInfo)
	}
	submitForDedup(files, noact)
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
