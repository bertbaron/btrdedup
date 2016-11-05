package storage

import (
	"path/filepath"
	"gopkg.in/cheggaaa/pb.v1"
	"time"
	"github.com/bertbaron/btrdedup/sys"
)

type FileInformation struct {
	// Number from the PathStorage
	Path      int32
	Fragments []sys.Fragment
	Csum      *[16]byte
}

func (f *FileInformation) PhysicalOffset() uint64 {
	return f.Fragments[0].Start
}

func (f *FileInformation) Size() int64 {
	size := int64(0)
	for _, frag := range f.Fragments {
		size += int64(frag.Length)
	}
	return size
}

type DedupInterface interface {
	// phase 1, collect all file information grouped by their physical offset
	StartPass1()
	AddFile(file FileInformation)
	EndPass1()

	// phase 2, updates the file information with checksums of the first block
	StartPass2()
	PartitionOnOffset(receiver func(files []*FileInformation) bool)
	EndPass2()

	// phase 3, deduplicates files if possible
	StartPass3()
	PartitionOnHash(receiver func(files []*FileInformation))
	EndPass3()
}

// Stores pathnames in an efficient way. Directories and files are stored separately an can as such have the same
// number, because we are in the end only interested in files, not in directories
type PathStorage interface {
	// Adds the given path. Use parent -1 to add a root. Panics if the parent does not exist
	AddDir(parent int32, name string) int32

	// Adds the given path. Use parent -1 to add a root. Panics if the parent does not exist
	AddFile(parent int32, name string) int32

	// Returns the path of the file for the given number. Panics if it doesn't exist
	FilePath(number int32) string

	// Passes all the file names (not the dir names) to the consumer function
	ProcessFiles(consumer func(filenr int32, filename string))

	// Returns the number of files (not dirs)
	FileCount() int
}

type pathnode struct {
	// parent, -1 if there is no parent
	parent int32
	// name of this file or directory
	name   string
}

type pathstore struct {
	// in-trees
	dirs []pathnode
	files []pathnode
}

func NewPathStorage() PathStorage {
	return new(pathstore)
}

func (store *pathstore) AddDir(parent int32, name string) int32 {
	if parent != -1 {
		_ = store.dirs[parent] // issues panic if parent does not exist, we may want to do this more explicitly
	}
	store.dirs = append(store.dirs, pathnode{parent, name})
	return int32(len(store.dirs)) - 1
}

func (store *pathstore) AddFile(parent int32, name string) int32 {
	if parent != -1 {
		_ = store.dirs[parent] // issues panic if parent does not exist, we may want to do this more explicitly
	}
	store.files = append(store.files, pathnode{parent, name})
	return int32(len(store.files)) - 1
}

func (store *pathstore) dirPath(number int32) string {
	path := &store.dirs[number]
	if path.parent == -1 {
		return path.name
	}
	return filepath.Join(store.dirPath(path.parent), path.name)
}

func (store *pathstore) FilePath(number int32) string {
	path := &store.files[number]
	if path.parent == -1 {
		return path.name
	}
	return filepath.Join(store.dirPath(path.parent), path.name)
}

func (store *pathstore) ProcessFiles(consumer func(filenr int32, filename string)) {
	for filenr, _ := range store.files {
		consumer(int32(filenr), store.FilePath(int32(filenr)))
	}
}

func (store *pathstore) FileCount() int {
	return len(store.files)
}

type Statistics struct {
	fileCount  int
	filesFound int
	hash       int
	hashTot    int
	dedupPot   int
	dedupAct   int
	dedupTot   int

	showPb     bool
	bar        *pb.ProgressBar
}

func NewProgressBarStats() *Statistics {
	return &Statistics{showPb: true}
}

func NewProgressLogStats() *Statistics {
	return &Statistics{showPb: false}
}

func (s *Statistics) SetFileCount(count int) {
	s.fileCount = count
}

func (s *Statistics) StartFileinfoProgress() {
	if s.showPb {
		s.bar = pb.StartNew(s.fileCount)
		s.bar.SetRefreshRate(time.Second)
	}
}

func (s *Statistics) FileInfoRead() {
	if s.showPb {
		s.bar.Add(s.filesFound)
	}
}

func (s *Statistics) FileAdded() {
	s.filesFound += 1
}

func (s *Statistics) HashesCalculated(count int) {
	s.hash += 1
	s.hashTot += count
	if s.showPb {
		s.bar.Add(count)
	}
}

func (s *Statistics) Deduplicating(count int) {
	s.dedupPot += count
	s.bar.Add(count)
}

func (s *Statistics) StartHashProgress() {
	if s.showPb {
		s.bar = pb.StartNew(s.filesFound)
		s.bar.SetRefreshRate(time.Second)
	}
}

func (s *Statistics) StartDedupProgress() {
	if s.showPb {
		s.bar = pb.StartNew(s.filesFound)
		s.bar.SetRefreshRate(time.Second)
	}
}

func (s *Statistics) StopProgress() {
	if s.showPb {
		s.bar.Finish()
	}
}
