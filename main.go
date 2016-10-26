package main

import (
	"log"
	"os"
	"path/filepath"
	"flag"
	"github.com/bertbaron/btrdedup/btrfs"
	"runtime"
	"syscall"
	"github.com/bertbaron/btrdedup/util"
)

const (
	minSize int64 = 1024 * 1024
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
}

type BySize []FileInformation

func (fis BySize) Len() int {
	return len(fis)
}
func (fis BySize) Swap(i, j int) {
	fis[i], fis[j] = fis[j], fis[i]
}
func (fis BySize) Less(i, j int) bool {
	return fis[i].size < fis[j].size
}

var files = []FileInformation{}

func isSymlink(path string) bool {
	fi, err := os.Lstat(path)
	if err != nil {
		log.Fatal("Error using os.Lstat on file %s: %v", path, err)
	}

	return (fi.Mode() & (os.ModeSymlink | os.ModeNamedPipe)) != 0
}

func collectFileInformation(filePath FilePath) {
	path := filePath.Path()
	if isSymlink(path) {
		return
	}
	f, err := os.Open(path)
	if err != nil {
		log.Printf("skipping %s because of error %v", path, err)
		return
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		log.Printf("Error using f.Stat on file %s: %v", path, err)
		return
	}
	switch mode := fi.Mode(); {
	case mode.IsDir():
		elements, err := f.Readdirnames(0)
		if err != nil {
			log.Fatal("Error while reading the contents of directory %s: %v", path, err)
			return
		}
		for _, e := range elements {
			collectFileInformation(FilePath{&filePath, e})
		}
	case mode.IsRegular():
		size := fi.Size()
		if size > minSize {
			physicalOffset, err := btrfs.PhysicalOffset(f)
			if err != nil {
				log.Printf("Error while trying to get the physical offset of file %s: %v", path, err)
				return
			}
			fileInformation := FileInformation{filePath, size, physicalOffset}
			files = append(files, fileInformation)
			if len(files) % 10000 == 0 {
				stats := runtime.MemStats{}
				runtime.ReadMemStats(&stats)
				log.Printf("%d files read, memory: %v", len(files), stats.Alloc)
			}
		}
	}
}

// Submits the files for deduplication. Only if duplication seems to make sense the will actually be deduplicated
func submitForDedup(files []FileInformation) {
	size := files[0].size
	if len(files) == 1 {
		log.Printf("Skipping size %d because there is only 1 file", size)
		return
	}
	for _, file := range files {
		if file.size != size {
			log.Fatal("Unequal sized files submitted!")
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
		log.Printf("Skipping size %d, all %d files have same physical offset", size, len(files))
		return
	}
	log.Printf("Offering for deduplication: %s of size %d and %d other files\n", filenames[0], size, len(files) - 1)
	Dedup(filenames, 0, uint64(size))
}

// Increase open file limit if possible, currently simply to the limit. We may want to make an option for this...
func checkOpenFileLimit() {
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

func main() {
	flag.Parse()
	filenames := flag.Args()

	checkOpenFileLimit()

	for _, filename := range filenames {
		collectFileInformation(FilePath{nil, filename})
	}

	for idxRange := range util.SortAndPartition(BySize(files)) {
		submitForDedup(files[idxRange.Low:idxRange.High])
	}
	log.Println("Done")
}
