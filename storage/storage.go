package storage

import (
	"path/filepath"
	"gopkg.in/cheggaaa/pb.v1"
	"time"
)

type FileInformation struct {
	// Number from the PathStorage
	Path           int32
	PhysicalOffset uint64
	Size           int64
	Csum           *[16]byte
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

type PathStorage interface {
	// Adds the given path. Use parent -1 to add a root. Panics if the parent does not exist
	AddPath(parent int32, name string) int32

	// Returns the path for the given number. Panics if it doesn't exist
	Path(number int32) string
}

type pathnode struct {
	// parent, -1 if there is no parent
	parent int32
	// name of this file or directory
	name   string
}

type pathstore struct {
	// in-trees
	paths []pathnode
}

func NewPathStorage() PathStorage {
	return new(pathstore)
}

func (store *pathstore) AddPath(parent int32, name string) int32 {
	if parent != -1 {
		_ = store.paths[parent] // issues panic if parent does not exist, we may want to do this more explicitly
	}
	store.paths = append(store.paths, pathnode{parent, name})
	return int32(len(store.paths)) - 1
}

func (store *pathstore) Path(number int32) string {
	path := &store.paths[number]
	if path.parent == -1 {
		return path.name
	}
	return filepath.Join(store.Path(path.parent), path.name)
}

type Statistics struct {
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
