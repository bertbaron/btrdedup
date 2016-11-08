package storage

import (
	"github.com/bertbaron/btrdedup/sys"
	"gopkg.in/cheggaaa/pb.v1"
	"log"
	"path/filepath"
	"time"
)

const (
	HashSize = 16
)

type FileInformation struct {
	// Number from the PathStorage
	Path      int32
	Error     bool
	Fragments []sys.Fragment
	Csum      [HashSize]byte
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
	EndPass1() // sort here

	// phase 2, updates the file information with checksums of the first block
	StartPass2()
	PartitionOnOffset(receiver func(files []*FileInformation) bool)
	EndPass2() // sort here

	// phase 3, deduplicates files if possible
	StartPass3()
	PartitionOnHash(receiver func(files []*FileInformation))
	EndPass3()
}

// Stores pathnames in an efficient way. Directories and files are stored separately an can as such have the same
// number, because we are in the end only interested in files, not in directories
// Note that this interface is a bit tricky because the caller always need to know if he deals with a directory or a file.
// It shouldn't cost too much to improve this...
type PathStorage interface {
	// Adds the given path. Use parent -1 to add a root. Panics if the parent does not exist
	AddDir(parent int32, name string) int32

	// Adds the given path. Use parent -1 to add a root. Panics if the parent does not exist
	AddFile(parent int32, name string) int32

	// Returns the path of the file for the given number. Panics if it doesn't exist
	FilePath(number int32) string

	// Returns the path of the directory for the given number. Panics if it doesn't exist
	DirPath(number int32) string

	// Passes all the file names (not the dir names) to the consumer function
	ProcessFiles(consumer func(filenr int32, filename string))

	// Returns the number of files (not dirs)
	FileCount() int
}

type pathnode struct {
	// parent, -1 if there is no parent
	parent int32
	// name of this file or directory
	name string
}

type pathstore struct {
	// in-trees
	dirs  []pathnode
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

func (store *pathstore) DirPath(number int32) string {
	path := &store.dirs[number]
	if path.parent == -1 {
		return path.name
	}
	return filepath.Join(store.DirPath(path.parent), path.name)
}

func (store *pathstore) FilePath(number int32) string {
	path := &store.files[number]
	if path.parent == -1 {
		return path.name
	}
	return filepath.Join(store.DirPath(path.parent), path.name)
}

func (store *pathstore) ProcessFiles(consumer func(filenr int32, filename string)) {
	for filenr, _ := range store.files {
		consumer(int32(filenr), store.FilePath(int32(filenr)))
	}
}

func (store *pathstore) FileCount() int {
	return len(store.files)
}

type progressBar interface {
	Add(count int) int
	Finish()
}

func newConsoleProgressBar(count int) progressBar {
	bar := pb.StartNew(count)
	bar.SetRefreshRate(time.Second)
	return bar
}

type logProgressBar struct {
	total      int
	count      int
	lastLogged int
}

func (b *logProgressBar) Add(count int) int {
	b.count += count
	if b.total > 0 {
		percentage := b.count * 100 / b.total
		if percentage > b.lastLogged {
			log.Printf("Progress: %d (%d/%d)", percentage, b.count, b.total)
			b.lastLogged = percentage
		}
	}
	return b.count
}

func (b *logProgressBar) Finish() {
	// TODO
}

func newLogProgressBar(count int) progressBar {
	return &logProgressBar{total: count}
}

type Statistics struct {
	fileCount  int
	filesFound int
	hashTot    int

	showPb   bool
	progress progressBar
	passName string
	start    time.Time
}

func NewProgressBarStats() *Statistics {
	return &Statistics{showPb: true}
}

func NewProgressLogStats() *Statistics {
	return &Statistics{showPb: false}
}

func (s *Statistics) startProgress(name string, count int) {
	s.passName = name
	s.start = time.Now()
	s.progress = newLogProgressBar(count)
	if s.showPb {
		s.progress = newConsoleProgressBar(count)
	}
}

func (s *Statistics) updateProgress(count int) {
	s.progress.Add(count)
}

func (s *Statistics) StopProgress() {
	duration := time.Since(s.start)
	s.progress.Finish()
	log.Printf("Pass %s completed in %s", s.passName, duration)
}

func (s *Statistics) SetFileCount(count int) {
	s.fileCount = count
}

func (s *Statistics) StartFileinfoProgress() {
	s.startProgress("Collecting file information", s.fileCount)
}

func (s *Statistics) FileInfoRead() {
	s.updateProgress(1)
}

func (s *Statistics) FileAdded() {
	s.filesFound += 1
}

func (s *Statistics) HashesCalculated(count int) {
	s.hashTot += count
	s.updateProgress(count)
}

func (s *Statistics) Deduplicating(count int) {
	s.updateProgress(count)
}

func (s *Statistics) StartHashProgress() {
	s.startProgress("Calculating hashes for first block of each file", s.filesFound)
}

func (s *Statistics) StartDedupProgress() {
	s.startProgress("Deduplication", s.hashTot)
}
