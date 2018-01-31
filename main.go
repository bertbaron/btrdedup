package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"github.com/bertbaron/btrdedup/storage"
	"github.com/bertbaron/btrdedup/sys"
	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh/terminal"
	"io/ioutil"
	"log"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/pprof"
	"syscall"
)

const (
	minSize int64 = 4 * 1024
)

var (
	version   = "undefined"
	buildTime = "unknown"
)

type context struct {
	pathstore storage.PathStorage
	stats     *storage.Statistics
	state     storage.DedupInterface
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

func readFileMeta(pathnr int32, path string) (*storage.FileInformation, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "open file failed")
	}
	defer f.Close()

	fragments, err := sys.Fragments(f)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to read fragments for file")
	}
	return &storage.FileInformation{Path: pathnr, Fragments: fragments}, nil
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
func createChecksums(ctx context, files []*storage.FileInformation) bool {
	defer ctx.stats.HashesCalculated(len(files))
	pathnr := files[0].Path
	path := ctx.pathstore.FilePath(pathnr)
	csum, err := readChecksum(path)
	if err != nil {
		log.Printf("Error creating checksum for first block of file %s, %v", path, err)
		for _, file := range files {
			file.Error = true
		}
		return false
	}
	for _, file := range files {
		file.Csum = *csum
	}
	return true
}

func collectFiles(ctx context, parent int32, name string) {
	path := name
	if parent >= 0 {
		path = filepath.Join(ctx.pathstore.DirPath(parent), name)
	}

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
		pathnr := ctx.pathstore.AddDir(parent, name)
		for _, e := range elements {
			collectFiles(ctx, pathnr, e)
		}
	case mode.IsRegular():
		size := fi.Size()
		if size > minSize {
			ctx.pathstore.AddFile(parent, name)
		}
	}
}

func loadFileInformation(ctx context) {
	ctx.pathstore.ProcessFiles(func(filenr int32, path string) {
		defer ctx.stats.FileInfoRead()
		fileInformation, err := readFileMeta(filenr, path)
		if err != nil {
			log.Printf("Error while trying to get the fragments of file %s: %v", path, err)
			return
		}
		ctx.stats.FileAdded()
		ctx.state.AddFile(*fileInformation)

	})
}

// Submits the files for deduplication. Only if duplication seems to make sense they will actually be deduplicated
func submitForDedup(ctx context, files []*storage.FileInformation, noact bool) {
	defer ctx.stats.Deduplicating(len(files))

	defragged := false
	fragcount := len(files[0].Fragments)
	if fragcount > 100 {
		path := ctx.pathstore.FilePath(files[0].Path)
		log.Printf("File %s has %d fragments, we defragment it before deduplication", path, fragcount)
		command := exec.Command("btrfs", "filesystem", "defragment", "-f", path)
		stderr, err := command.StderrPipe()
		if err != nil {
			log.Fatal(err)
		}
		if err := command.Start(); err != nil {
			log.Printf("Defragmentation of %s failed to start: %v", path, err)
		} else {
			errorOutput, _ := ioutil.ReadAll(stderr)
			if err := command.Wait(); err != nil {
				log.Printf("Defragmentation of %s failed: %v", path, err)
				log.Printf("Defragmentation error output: %s", errorOutput)
			}
			defragged = true // even if defragmentation failed, the file might be (partly) defragmented
			if newFile, err := readFileMeta(files[0].Path, path); err != nil {
				log.Printf("Error while reading the fragmentation table again: %v", err)
			} else {
				files[0] = newFile
				log.Printf("Number of fragments was %d and is now %d for file %s", fragcount, len(newFile.Fragments), path)
			}
		}
	}

	if len(files) < 2 || files[0].Error {
		return
	}

	// currently we assume that the files are equal up to the size of the smallest file
	var size int64 = math.MaxInt64
	for _, file := range files {
		if file.Size() < size {
			size = file.Size()
		}
	}

	filenames := make([]string, len(files))
	sameOffset := true
	physicalOffset := files[0].PhysicalOffset()
	for i, file := range files {
		if file.PhysicalOffset() != physicalOffset {
			sameOffset = false
		}
		filenames[i] = ctx.pathstore.FilePath(file.Path)
	}
	if !defragged && sameOffset { // when a file is defragmented we always need to deduplicate because deduplication breaks extend sharing
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

func collectApplicableFiles(ctx context, filenames []string) {
	fmt.Printf("Searching for applicable files\n")
	for _, filename := range filenames {
		collectFiles(ctx, -1, filename)
	}
}

func pass1(ctx context, filenames []string) {
	fmt.Printf("Pass 1 of 3, collecting fragmentation information\n")
	ctx.state.StartPass1()
	ctx.stats.StartFileinfoProgress()
	loadFileInformation(ctx)
	ctx.stats.StopProgress()
	ctx.state.EndPass1()
}

func pass2(ctx context) {
	fmt.Printf("Pass 2 of 3, calculating hashes for first block of files\n")
	ctx.state.StartPass2()
	ctx.stats.StartHashProgress()
	ctx.state.PartitionOnOffset(func(files []*storage.FileInformation) bool {
		return createChecksums(ctx, files)
	})
	ctx.stats.StopProgress()
	ctx.state.EndPass2()
}

func pass3(ctx context, noact bool) {
	fmt.Printf("Pass 3 of 3, deduplucating files\n")
	ctx.state.StartPass3()
	ctx.stats.StartDedupProgress()
	ctx.state.PartitionOnHash(func(files []*storage.FileInformation) {
		submitForDedup(ctx, files, noact)
	})
	ctx.stats.StopProgress()
	ctx.state.EndPass3()
}

func writeHeapProfile(basename string, suffix string) {
	if basename != "" {
		f, err := os.Create(basename + suffix + ".mprof")
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	}
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [OPTION]... [FILE-OR-DIR]...\n", os.Args[0])
		flag.PrintDefaults()
	}
	showVersion := flag.Bool("version", false, "show version information and exits")
	noact := flag.Bool("noact", false, "if provided, the tool will only scan and log results, but not actually deduplicate")
	lowmem := flag.Bool("lowmem", false, "if provided, the tool will use much less memory by using temporary files and the external sort command")
	nopb := flag.Bool("nopb", false, "if provided, the tool will not show the progress bar")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	memprofile := flag.String("memprofile", "", "write memory profile to this file")
	flag.Parse()

	if *showVersion {
		fmt.Printf("btrdedup version '%s' built at '%s'\n", version, buildTime)
		return
	}

	if *cpuprofile != "" {
		f, err := os.Create((*cpuprofile) + ".prof")
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	var ctx context

	ctx.pathstore = storage.NewPathStorage()

	ctx.stats = storage.NewProgressLogStats()
	if !*nopb && terminal.IsTerminal(int(os.Stdout.Fd())) {
		ctx.stats = storage.NewProgressBarStats()
	}
	ctx.stats.Start()

	filenames := flag.Args()

	if len(filenames) < 1 {
		flag.Usage()
		return
	}

	updateOpenFileLimit()

	ctx.state = storage.NewMemoryBased()
	if *lowmem {
		log.Printf("Running in low memory mode")
		ctx.state = storage.NewFileBased()
	}

	collectApplicableFiles(ctx, filenames)
	ctx.stats.SetFileCount(ctx.pathstore.FileCount())

	pass1(ctx, filenames)

	writeHeapProfile(*memprofile, "_pass1")

	pass2(ctx)

	writeHeapProfile(*memprofile, "_pass1")

	pass3(ctx, *noact)

	writeHeapProfile(*memprofile, "_pass1")

	ctx.stats.Stop()
	fmt.Println("Done")
}
