package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"github.com/bertbaron/btrdedup/storage"
	"github.com/bertbaron/btrdedup/sys"
	"github.com/pkg/errors"
	"log"
	"math"
	"os"
	"runtime/pprof"
	"syscall"
)

const (
	minSize int64 = 4 * 1024
)

// TODO this should preferably not be global state...
var pathstore = storage.NewPathStorage()

type statistics struct {
	add      int
	hash     int
	hashTot  int
	dedupPot int
	dedupAct int
	dedupTot int
}

var stats statistics

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

func readFileMeta(pathnr int32) (*storage.FileInformation, error) {
	f, err := os.Open(pathstore.Path(pathnr))
	if err != nil {
		return nil, errors.Wrap(err, "open file failed")
	}
	defer f.Close()

	fragments, err := sys.Fragments(f)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to read fragments for file")
	}
	physicalOffset := fragments[0].Start
	return &storage.FileInformation{pathnr, physicalOffset, 0, nil}, nil
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
		// We assume that the full block is read at once. If proven false we need to read in a loop
		return nil, errors.New("Less than 4k read, skipping block")
	}
	csum := makeChecksum(buffer)
	return &csum, nil
}

// Updates the file information with checksum. Returns true if successful, false otherwise
// PRE: all files start at the same offset and files is not empty
func createChecksums(files []*storage.FileInformation, state storage.DedupInterface) bool {
	pathnr := files[0].Path
	path := pathstore.Path(pathnr)
	csum, err := readChecksum(path)
	if err != nil {
		log.Printf("Error creating checksum for first block of file %s, %v", path, err)
		return false
	}
	stats.hash += 1
	for _, file := range files {
		stats.hashTot += 1
		file.Csum = csum
	}
	return true
}

func collectFileInformation(pathnr int32, state storage.DedupInterface) {
	path := pathstore.Path(pathnr)
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
			dirnr := pathstore.AddPath(pathnr, e)
			collectFileInformation(dirnr, state)
		}
	case mode.IsRegular():
		size := fi.Size()
		if size > minSize {
			fileInformation, err := readFileMeta(pathnr)
			if err != nil {
				log.Printf("Error while trying to get the physical offset of file %s: %v", path, err)
				return
			}
			fileInformation.Size = size
			stats.add += 1
			state.AddFile(*fileInformation)
		}
	}
}

// Submits the files for deduplication. Only if duplication seems to make sense the will actually be deduplicated
func submitForDedup(files []*storage.FileInformation, noact bool) {
	stats.dedupPot += 1
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
		filenames[i] = pathstore.Path(file.Path)
	}
	if sameOffset {
		log.Printf("Skipping %s and %d other files, they all have the same physical offset", filenames[0], len(files)-1)
		return
	}
	stats.dedupAct += 1
	stats.dedupTot += len(files)
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

func pass1(filenames []string, state storage.DedupInterface) {
	log.Printf("Pass 1, collecting fragmentation information")
	state.StartPass1()
	for _, filename := range filenames {
		collectFileInformation(pathstore.AddPath(-1, filename), state)
	}
	state.EndPass1()
}

func pass2(state storage.DedupInterface) {
	log.Printf("Pass 2, calculating hashes for first block of files")
	state.StartPass2()
	state.PartitionOnOffset(func(files []*storage.FileInformation) bool {
		return createChecksums(files, state)
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
	noact := flag.Bool("noact", false, "if provided, the tool will only scan and log results, but not actually deduplicate")
	memmode := flag.Bool("memmode", false, "if provided, the tool will run in memory mode. By default it uses temporary files which is somewhat slower but more scalable")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	memprofile := flag.String("memprofile", "", "write memory profile to this file")
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

	var state storage.DedupInterface = storage.NewFileBased()
	if *memmode {
		log.Printf("Running in memory mode")
		state = storage.NewMemoryBased()
	}

	pass1(filenames, state)

	pass2(state)

	pass3(state, *noact)

	fmt.Printf("Statistics: %+v\n", stats)

	log.Println("Done")

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	}
}
