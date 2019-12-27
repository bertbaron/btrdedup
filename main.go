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
	"strings"
	"syscall"
)

const (
	blockSize int64 = 4096
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

	var size int64
	if stat, err := f.Stat(); err == nil {
		size = stat.Size()
	}

	fragments, err := sys.Fragments(f)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to read fragments for file")
	}

	var tot uint64
	for _, frag := range fragments {
		tot += frag.Length
	}
	if uint64(size) > tot {
		log.Printf("Skipping sparse file %s", path)
		return nil, nil
	}

	return &storage.FileInformation{Path: pathnr, Size: size, Fragments: fragments}, nil
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

func collectFiles(ctx context, parent int32, name string, minSize int, exclude string) {
	path := name
	if parent >= 0 {
		path = filepath.Join(ctx.pathstore.DirPath(parent), name)
	}

	if exclude != "" && strings.HasPrefix(path, exclude) {
		log.Printf("Excluding %s", path)
		return
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
			collectFiles(ctx, pathnr, e, minSize, exclude)
		}
	case mode.IsRegular():
		size := fi.Size()
		if size/blockSize >= int64(minSize) {
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
		if fileInformation != nil {
			ctx.stats.FileAdded()
			ctx.state.AddFile(*fileInformation)
		}
	})
}

func allowedFragcount(file *storage.FileInformation, minBpf int) int {
	fragSize := int64(minBpf) * blockSize
	return int((file.Size - 1) / fragSize) +1
}
// Currently we always deduplicate towards the first file. Therefore we place the least-defragmented file in first
// position and, if the fragmentation is higher than the threshold, defragment it first.
//
// Note that when we will do the deduplication more clever (comparing all blocks of all files), we may also need to do
// the defragmentation in a more clever way.
func reorderAndDefragIfNeeded(ctx context, files []*storage.FileInformation, minBpf int, noact bool) (copy []*storage.FileInformation) {
	if len(files) == 0 {
		return files
	}
	copy = make([]*storage.FileInformation, len(files), len(files))
	for idx, file := range files {
		copy[idx] = file
	}

	// least-fragmented file first
	for idx, file := range copy {
		if len(file.Fragments) < len(copy[0].Fragments) {
			copy[0], copy[idx] = copy[idx], copy[0]
		}
	}

	if minBpf < 1 {
		return
	}
	fragcount := len(copy[0].Fragments)
	allowedFragcount := allowedFragcount(copy[0], minBpf)
	allowedFragcount = 1
	if fragcount <= allowedFragcount {
		return
	}

	// Non-writable files (i.e. from read-only snapshots) can not be defragmented, so find a writable file
	writableFound := false
	for idx, file := range copy {
		if file.Writable(ctx.pathstore) {
			copy[0], copy[idx] = copy[idx], copy[0]
			writableFound = true
			break;
		}
	}

	file := copy[0]
	path := ctx.pathstore.FilePath(file.Path)

	if !writableFound {
		log.Printf("File %s can not be defragmented, none of the duplicates are writable", path)
		return
	}

	fragcount = len(file.Fragments)

	if noact {
		log.Printf("File %s has %d fragments while we want max %d, but will not be defragmented because -noact option is specified", path, fragcount, allowedFragcount)
		return
	}

	log.Printf("File %s has %d fragments while we want max %d, starting defragmentation", path, fragcount, allowedFragcount)
	command := exec.Command("btrfs", "filesystem", "defragment", "-f", path)
	stderr, err := command.StderrPipe()
	if err != nil {
		log.Fatal(err)
	}
	if err := command.Start(); err != nil {
		log.Printf("Defragmentation of %s failed to start: %v", path, err)
		return
	}

	errorOutput, _ := ioutil.ReadAll(stderr)
	if err := command.Wait(); err != nil {
		log.Printf("Defragmentation of %s failed: %v", path, err)
		log.Printf("Defragmentation error output: %s", errorOutput)
		return
	}

	if newFile, err := readFileMeta(file.Path, path); err != nil {
		log.Printf("Error while reading the fragmentation table again: %v", err)
		return reorderAndDefragIfNeeded(ctx, copy[1:], minBpf, noact)
	} else if newFile == nil {
		log.Printf("File can not be deduplicated after defragmentation")
		return reorderAndDefragIfNeeded(ctx, copy[1:], minBpf, noact)
	} else {
		copy[0] = newFile
		log.Printf("Number of fragments was %d and is now %d for file %s", fragcount, len(newFile.Fragments), path)
	}
	return
}

// Returns the first offset that is not shared amongst the files, or size if the files are
// shared up to the specified size
func unsharedStart(files []*storage.FileInformation, size int64) int64 {
	for i := int64(0); i < size; i+=blockSize {
		offset := files[0].PhysicalOffsetAt(i)
		for _, file := range files[1:] {
			if file.PhysicalOffsetAt(i) != offset {
				return i
			}
		}
	}
	return size
}

// Submits the files for deduplication. Only if duplication seems to make sense they will actually be deduplicated
func submitForDedup(ctx context, files []*storage.FileInformation, minBpf int, noact bool) {
	defer ctx.stats.Deduplicating(len(files))

	files = reorderAndDefragIfNeeded(ctx, files, minBpf, noact)

	if len(files) < 2 || files[0].Error {
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
	for i, file := range files {
		filenames[i] = ctx.pathstore.FilePath(file.Path)
	}
	startUnshared := unsharedStart(files, size)
	if startUnshared == size {
		//log.Printf("Skipping %s and %d other files, they are already shared", filenames[0], len(files)-1)
		return
	}
	if !noact {
		log.Printf("Offering for deduplication: %s and %d other files from offset %d\n", filenames[0], len(files)-1, startUnshared)
		offset:=uint64(startUnshared)
		length:=uint64(size-startUnshared)
		Dedup(filenames, offset, length)
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
			log.Println("Error Setting Rlimit", err)
		}
		err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
		if err != nil {
			log.Println("Error Getting Rlimit", err)
		}
		log.Println("Open file limit increased to", rLimit.Cur)
	}
}

func collectApplicableFiles(ctx context, filenames []string, minSize int, exclude string) {
	fmt.Printf("Searching for applicable files\n")
	for _, filename := range filenames {
		collectFiles(ctx, -1, filename, minSize, exclude)
	}
}

func pass1(ctx context) {
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

func pass3(ctx context, minBpf int, noact bool) {
	fmt.Printf("Pass 3 of 3, deduplucating files\n")
	ctx.state.StartPass3()
	ctx.stats.StartDedupProgress()
	ctx.state.PartitionOnHash(func(files []*storage.FileInformation) {
		submitForDedup(ctx, files, minBpf, noact)
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
	nopb := flag.Bool("nopb", false, "if provided, the tool will not show the progress bar even if a terminal is detected")
	exclude := flag.String("exclude", "", "Path prefix to exclude (i.e. exclude=/var/lib/docker)")
	defrag := flag.Bool("defrag", false, "defragment files with less than the configured number of blocks per fragment")
	minBpf := flag.Int("bpf", 1024, "minimal average number of blocks per fragment before defragmentation, default=1024 (4MB)")
	minSize := flag.Int("minsize", 1, "skip files with size less than the given number of blocks, default is 1")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	memprofile := flag.String("memprofile", "", "write memory profile to this file")
	flag.Parse()

	if !*defrag {
		*minBpf = 0
	}

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

	collectApplicableFiles(ctx, filenames, *minSize, *exclude)
	ctx.stats.SetFileCount(ctx.pathstore.FileCount())

	pass1(ctx)

	writeHeapProfile(*memprofile, "_pass1")

	pass2(ctx)

	writeHeapProfile(*memprofile, "_pass2")

	pass3(ctx, *minBpf, *noact)

	writeHeapProfile(*memprofile, "_pass3")

	ctx.stats.Stop()
	fmt.Println("Done")
}
