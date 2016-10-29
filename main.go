package main

import (
	"log"
	"os"
	"flag"
	"syscall"
	"github.com/bertbaron/btrdedup/btrfs"
	"github.com/pkg/errors"
	"crypto/md5"
	"math"
	"path/filepath"
	"io/ioutil"
	"encoding/json"
	"bufio"
	"strconv"
	"os/exec"
	"strings"
	"encoding/hex"
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
	csum := md5.Sum(buffer)
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
			dumpAsJson(strconv.FormatInt(int64(fileInformation.physicalOffset), 10), fileInformation, outfile)
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
		if (offset != lastOffset) {
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

func main() {
	//noact := flag.Bool("noact", false, "if provided or true, the tool will only scan and log results, but not actually deduplicate")
	flag.Parse()
	filenames := flag.Args()

	updateOpenFileLimit()

	filename := pass1(filenames)

	pass2(filename)

	//readChecksums()
	//
	//log.Println("Sorting by checksum")
	//sort.Sort(ByChecksum(files))
	//log.Println("Done sorting by checksum")
	//printFileInformation()
	//
	//for idxRange := range util.SortAndPartition(ByChecksum(files)) {
	//	submitForDedup(files[idxRange.Low:idxRange.High], *noact)
	//}
	log.Println("Done")
}
