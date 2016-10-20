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
)

type FilePath struct {
	parent *FilePath
	name string
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
		physicalOffset := btrfs.PhysicalOffset(f)
		fileInformation := FileInformation{filePath, size, physicalOffset}
		files = append(files, fileInformation)
		if len(files) % 10000 == 0 {
			stats := runtime.MemStats{}
			runtime.ReadMemStats(&stats)
			log.Printf("%d files read, memory: %v", len(files), stats.Alloc)

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

func main() {
	flag.Parse()
	filenames := flag.Args()
	collectFileInformation(FilePath{nil, filenames[0]})
	sortFileInformation()
	//printFileInformation()
	//Dedup(filenames)
	log.Println("Done")
}
