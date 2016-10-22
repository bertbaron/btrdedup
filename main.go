package main

import (
	"log"
	"os"
	"path/filepath"
	"flag"
	"fmt"
	"github.com/bertbaron/btrdedup/btrfs"
	"sort"
	"runtime"
	"syscall"
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
		log.Fatal(err)
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
		log.Fatal(err)
	}
	switch mode := fi.Mode(); {
	case mode.IsDir():
		elements, err := f.Readdirnames(0)
		if err != nil {
			log.Fatal(err)
		}
		for _, e := range elements {
			collectFileInformation(FilePath{&filePath, e})
		}
	case mode.IsRegular():
		size := fi.Size()
		if size > minSize {
			physicalOffset, err := btrfs.PhysicalOffset(f)
			if err != nil {
				log.Fatal(err)
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

func sortFileInformation() {
	log.Println("Sorting the files by size")
	sort.Sort(BySize(files))
}

func printFileInformation() {
	for _, fi := range files {
		fmt.Printf("%d %s %d\n", fi.size, fi.path.Path(), fi.physicalOffset)
	}
}

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
	sortFileInformation()

	//printFileInformation()

	start := 0
	var size int64 = -1
	for i, file := range files {
		if file.size != size {
			if size != -1 {
				submitForDedup(files[start:i])
			}
			size = file.size
			start = i
		}
	}
	submitForDedup(files[start:len(files)])
	//Dedup(filenames)
	log.Println("Done")
}
