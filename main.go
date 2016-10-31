package main

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
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

func serializePhase1(fileInfo *FileInformation) string {
	offset := strconv.FormatInt(int64(fileInfo.physicalOffset), 10)
	path := base64.StdEncoding.EncodeToString([]byte(fileInfo.Path))
	return offset + " " + path + " " + strconv.FormatInt(fileInfo.Size, 10)
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

func dumpAsJson(prefix string, fileInfo *FileInformation, outfile *bufio.Writer) {
	bytes, err := json.Marshal(fileInfo)
	if err != nil {
		log.Printf("Error converting file information to Json")
		return
	}
	outfile.WriteString(prefix)
	outfile.WriteByte(' ')
	outfile.Write(bytes)
	outfile.WriteByte('\n')
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
		dumpAsJson(hex.EncodeToString(csum[:]), &file, outfile)
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
			serializePhase1(fileInformation)
			//dumpAsJson(strconv.FormatInt(int64(fileInformation.physicalOffset), 10), fileInformation, outfile)
		}
	}
}

// Submits the files for deduplication. Only if duplication seems to make sense the will actually be deduplicated
func submitForDedup(files []FileInformation, noact bool) {
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

func parse(line string) FileInformation {
	fileInfo := FileInformation{}
	json.Unmarshal([]byte(line), &fileInfo)
	return fileInfo
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
	lastOffset := ""
	files := make([]FileInformation, 0)
	for scanner.Scan() {
		line := scanner.Text()
		idx := strings.Index(line, " ")
		offset := line[:idx]
		fileInfo := parse(line[idx:])
		if offset != lastOffset {
			dumpChecksums(files, writer)
			files = files[0:0]
		}
		files = append(files, fileInfo)
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
	lastHash := ""
	files := make([]FileInformation, 0)
	for scanner.Scan() {
		line := scanner.Text()
		idx := strings.Index(line, " ")
		hash := line[:idx]
		fileInfo := parse(line[idx:])
		if hash != lastHash {
			submitForDedup(files, noact)
			files = files[0:0]
		}
		files = append(files, fileInfo)
	}
	submitForDedup(files, noact)
}

func main() {
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
