package main

import (
	"log"
	"os"
	"path/filepath"
	"flag"
	"runtime"
	"syscall"
	"github.com/bertbaron/btrdedup/btrfs"
	"github.com/bertbaron/btrdedup/util"
	"github.com/pkg/errors"
	"crypto/md5"
	"sort"
	"fmt"
	"encoding/hex"
)

const (
	minSize int64 = 4 * 1024
)

type FilePath struct {
	parent *FilePath
	name   string
}

func (p FilePath) Path() string {
	if p.parent == nil {
		return p.name
	} else {
		return filepath.Join(p.parent.Path(), p.name)
	}
}

type FileInformation struct {
	path           FilePath
	size           int64
	physicalOffset uint64
	csum           *[16]byte
}

type BlockInformation struct {
	physicalOffset uint64
	fileInfo       *FileInformation // just one file having this as its first block
}

type BySize []FileInformation
type ByOffset []FileInformation
type ByChecksum []FileInformation

func (fis BySize) Len() int {
	return len(fis)
}
func (fis BySize) Swap(i, j int) {
	fis[i], fis[j] = fis[j], fis[i]
}
func (fis BySize) Less(i, j int) bool {
	return fis[i].size < fis[j].size
}

func (fis ByOffset) Len() int {
	return len(fis)
}
func (fis ByOffset) Swap(i, j int) {
	fis[i], fis[j] = fis[j], fis[i]
}
func (fis ByOffset) Less(i, j int) bool {
	return fis[i].physicalOffset < fis[j].physicalOffset
}

func (fis ByChecksum) Len() int {
	return len(fis)
}
func (fis ByChecksum) Swap(i, j int) {
	fis[i], fis[j] = fis[j], fis[i]
}
func (fis ByChecksum) Less(i, j int) bool {
	a := *fis[i].csum
	b := *fis[j].csum
	for i, v := range a {
		if v < b[i] {
			return true
		}
		if v > b[i] {
			return false
		}
	}
	return false
	//return *fis[i].csum < *fis[j].csum
}

var files = []FileInformation{}

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

func readFileMeta(filePath FilePath, size int64) (*FileInformation, error) {
	path := filePath.Path()
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
	return &FileInformation{filePath, size, physicalOffset, nil}, nil
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

func readChecksums() {
	for idxRange := range util.SortAndPartition(ByOffset(files)) {
		path := files[idxRange.Low].path.Path()
		csum, err := readChecksum(path)
		if err != nil {
			log.Printf("Error creating checksum for first block of file %s, %v", path, err)
			// FIXME SOMEHOW REMOVE THESE FROM FILES, WE DON'T WANT TO PANIC ON nil
		} else {
			subslice := files[idxRange.Low:idxRange.High]
			for idx, _ := range subslice {
				subslice[idx].csum = csum
			}
		}
	}
}

func collectFileInformation(filePath FilePath) {
	path := filePath.Path()
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
			collectFileInformation(FilePath{&filePath, e})
		}
	case mode.IsRegular():
		size := fi.Size()
		if size > minSize {
			fileInformation, err := readFileMeta(filePath, size)
			if err != nil {
				log.Printf("Error while trying to get the physical offset of file %s: %v", path, err)
				return
			}
			files = append(files, *fileInformation)
			if len(files) % 10000 == 0 {
				stats := runtime.MemStats{}
				runtime.ReadMemStats(&stats)
				log.Printf("%d files read, memory: %v", len(files), stats.Alloc)
			}
		}
	}
}

// Submits the files for deduplication. Only if duplication seems to make sense the will actually be deduplicated
func submitForDedup(files []FileInformation, noact bool) {
	if len(files) == 1 {
		log.Printf("Skipping file because there is no other file starting with the same checksum")
		return
	}

	// currently we assume that the files are equal up to the size of the smallest file
	size := int64(0)
	for _, file := range files {
		if file.size < size {
			size = file.size
		}
	}

	filenames := make([]string, len(files))
	sameOffset := true
	physicalOffset := files[0].physicalOffset
	for i, file := range files {
		if file.physicalOffset != physicalOffset {
			sameOffset = false
		}
		filenames[i] = file.path.Path()
	}
	if sameOffset {
		log.Printf("Skipping %s and %d other files, they all have the same physical offset", filenames[0], size, len(files) - 1)
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

func printFileInformation() {
	for _, file := range files {
		var csum [16]byte = *file.csum
		fmt.Printf("%s - %s\n", hex.EncodeToString(csum[:]), file.path.Path())
	}
}

func main() {
	noact := flag.Bool("noact", false, "if provided or true, the tool will only scan and log results, but not actually deduplicate")
	flag.Parse()
	filenames := flag.Args()

	updateOpenFileLimit()

	for _, filename := range filenames {
		collectFileInformation(FilePath{nil, filename})
	}

	readChecksums()

	log.Println("Sorting by checksum")
	sort.Sort(ByChecksum(files))
	log.Println("Done sorting by checksum")
	printFileInformation()

	for idxRange := range util.SortAndPartition(ByChecksum(files)) {
		submitForDedup(files[idxRange.Low:idxRange.High], *noact)
	}
	log.Println("Done")
}
