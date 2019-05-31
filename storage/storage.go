package storage

import (
	"github.com/bertbaron/btrdedup/sys"
	"path/filepath"
	"sync"
	"golang.org/x/sys/unix"
	"log"
)

const (
	HashSize = 16
)

type FileInformation struct {
	// Number from the PathStorage
	Path      int32
	Error     bool
	Size	  int64
	Fragments []sys.Fragment
	Csum      [HashSize]byte
}

func (f *FileInformation) PhysicalOffset() uint64 {
	return f.Fragments[0].Start
}

// Pre: 0 <= i < file size
func (information *FileInformation) PhysicalOffsetAt(i int64) uint64 {
	remaining := uint64(i)
	for _, frag := range information.Fragments {
		if remaining < frag.Length {
			return frag.Start + remaining
		}
		remaining = remaining - frag.Length
	}
	log.Printf("WARNING: Offset %d is beyond file fragments:")
	log.Printf("DEBUG: size: %d, fragments: ", information.Size)
	for idx, frag := range information.Fragments {
		log.Printf("DEBUG:     %d, %d, %d", idx, frag.Start, frag.Length)
	}
	return 0 // TODO Return an error?
}

//func (f *FileInformation) Size() int64 {
//	size := int64(0)
//	for _, frag := range f.Fragments {
//		size += int64(frag.Length)
//	}
//	return size
//}

func (f *FileInformation) Writable(pathstore PathStorage) bool {
	return unix.Access(pathstore.FilePath(f.Path), unix.W_OK) == nil
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
// Access is thread-safe as long as it is not modified during iteration by ProcessFiles
type PathStorage interface {
	// Adds the given path. Use parent -1 to add a root. Panics if the parent does not exist
	AddDir(parent int32, name string) int32

	// Adds the given path. Use parent -1 to add a root. Panics if the parent does not exist
	AddFile(parent int32, name string) int32

	// Returns the path of the file for the given number. Panics if it doesn't exist
	FilePath(number int32) string

	// Returns the path of the directory for the given number. Panics if it doesn't exist
	DirPath(number int32) string

	// Returns the number of files (not dirs)
	FileCount() int

	// Passes all the file names (not the dir names) to the consumer function.
	// NOTE: During iteration no files or directories should be added
	ProcessFiles(consumer func(filenr int32, filename string))
}

type pathnode struct {
	// parent, -1 if there is no parent
	parent int32
	// name of this file or directory
	name   string
}

type pathstore struct {
	lock   sync.RWMutex
	// in-trees
	dirs  []pathnode
	files []pathnode
}

func NewPathStorage() PathStorage {
	return new(pathstore)
}

func (store *pathstore) AddDir(parent int32, name string) int32 {
	store.lock.Lock()
	defer store.lock.Unlock()

	if parent != -1 {
		_ = store.dirs[parent] // issues panic if parent does not exist, we may want to do this more explicitly
	}
	store.dirs = append(store.dirs, pathnode{parent, name})
	return int32(len(store.dirs)) - 1
}

func (store *pathstore) AddFile(parent int32, name string) int32 {
	store.lock.Lock()
	defer store.lock.Unlock()

	if parent != -1 {
		_ = store.dirs[parent] // issues panic if parent does not exist, we may want to do this more explicitly
	}
	store.files = append(store.files, pathnode{parent, name})
	return int32(len(store.files)) - 1
}

func (store *pathstore) DirPath(number int32) string {
	store.lock.RLock()
	defer store.lock.RUnlock()

	path := &store.dirs[number]
	if path.parent == -1 {
		return path.name
	}
	return filepath.Join(store.DirPath(path.parent), path.name)
}

func (store *pathstore) FilePath(number int32) string {
	store.lock.RLock()
	defer store.lock.RUnlock()

	path := &store.files[number]
	if path.parent == -1 {
		return path.name
	}
	return filepath.Join(store.DirPath(path.parent), path.name)
}

func (store *pathstore) ProcessFiles(consumer func(filenr int32, filename string)) {
	// Is the following necessary and sufficient to ensure memory visibility?
	store.lock.RLock()
	s := store
	store.lock.RUnlock()

	for filenr, _ := range s.files {
		consumer(int32(filenr), s.FilePath(int32(filenr)))
	}
}

func (store *pathstore) FileCount() int {
	return len(store.files)
}
