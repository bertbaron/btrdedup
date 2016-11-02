package main

import (
	"flag"
	"github.com/bertbaron/btrdedup/btrfs"
	"github.com/bertbaron/btrdedup/storage"
	"github.com/pkg/errors"
	"crypto/md5"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime/pprof"
	"syscall"
	"fmt"
	"github.com/bertbaron/btrdedup/filebased"
)

const (
	minSize int64 = 4 * 1024
)

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

func readFileMeta(path string) (*storage.FileInformation, error) {
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
	return &storage.FileInformation{path, physicalOffset, 0, nil}, nil
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

// PRE: all files start at the same offset and files is not empty
func dumpChecksums(files []*storage.FileInformation, state storage.DedupInterface) {
	path := files[0].Path
	csum, err := readChecksum(path)
	if err != nil {
		log.Printf("Error creating checksum for first block of file %s, %v", path, err)
		return
	}
	for _, file := range files {
		file.Csum = csum
	}
	state.ChecksumUpdated(files)
}

// todo: use filepath.Walk
func collectFileInformation(path string, state storage.DedupInterface) {
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
			collectFileInformation(filepath.Join(path, e), state)
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
			state.AddFile(*fileInformation)
			//prefix := strconv.FormatInt(int64(fileInformation.PhysicalOffset), 36)
			//writeFileInfo(prefix, *fileInformation, outfile)
		}
	}
}

// Submits the files for deduplication. Only if duplication seems to make sense the will actually be deduplicated
func submitForDedup(files []*storage.FileInformation, noact bool) {
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

func pass1(filenames []string, state storage.DedupInterface) {
	log.Printf("Pass 1, collecting fragmentation information")
	state.StartPass1()
	for _, filename := range filenames {
		collectFileInformation(filename, state)
	}
	state.EndPass1()
}

func pass2(state storage.DedupInterface) {
	log.Printf("Pass 2, calculating hashes for first block of files")
	state.StartPass2()
	state.PartitionOnOffset(func(files []*storage.FileInformation) {
		dumpChecksums(files, state)
	})
	state.EndPass2()
}

func pass3(state storage.DedupInterface, noact bool) {
	log.Printf("Pass 3, deduplucating files")
	state.StartPass3()
	state.PartitionOnHash(func(files []*storage.FileInformation) {
		submitForDedup(files, noact)
	})
	state.EndPass3()
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

	state := filebased.NewInterface()

	pass1(filenames, state)

	pass2(state)

	pass3(state, *noact)

	log.Println("Done")
}
