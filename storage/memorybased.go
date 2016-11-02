package storage

import (
	"sort"
)

type MemoryBased struct {
	files []*FileInformation
}

type ByOffset []*FileInformation
type ByChecksum []*FileInformation

func (fis ByOffset) Len() int {
	return len(fis)
}
func (fis ByOffset) Swap(i, j int) {
	fis[i], fis[j] = fis[j], fis[i]
}
func (fis ByOffset) Less(i, j int) bool {
	return fis[i].PhysicalOffset < fis[j].PhysicalOffset
}

func (fis ByChecksum) Len() int {
	return len(fis)
}
func (fis ByChecksum) Swap(i, j int) {
	fis[i], fis[j] = fis[j], fis[i]
}
func (fis ByChecksum) Less(i, j int) bool {
	a := fis[i].Csum
	b := fis[j].Csum
	if a == nil {
		return true
	}
	if b == nil {
		return false
	}
	for i, v := range a {
		if v < b[i] {
			return true
		}
		if v > b[i] {
			return false
		}
	}
	return false
}

// Creates a file based storage instance
func NewMemoryBased() *MemoryBased {
	return new(MemoryBased)
}

// ** PASS 1 **
func (state *MemoryBased) StartPass1() {}

func (state *MemoryBased) AddFile(file FileInformation) {
	state.files = append(state.files, &file)
}

func (state *MemoryBased) EndPass1() {}

// ** PASS 2 **

func (state *MemoryBased) StartPass2() {}

func (state *MemoryBased) PartitionOnOffset(receiver func(files []*FileInformation) bool) {
	sort.Sort(ByOffset(state.files))

	lastOffset := uint64(0)
	var partition []*FileInformation
	for _, file := range state.files {
		if file.PhysicalOffset != lastOffset {
			if len(partition) != 0 {
				receiver(partition)
			}
			partition = partition[0:0]
			lastOffset = file.PhysicalOffset
		}
		partition = append(partition, file)
	}
	if len(partition) != 0 {
		receiver(partition)
	}
}

func (state *MemoryBased) EndPass2() {}

// ** PASS 3 **

func (state *MemoryBased) StartPass3() {}

func (state *MemoryBased) PartitionOnHash(receiver func(files []*FileInformation)) {
	sort.Sort(ByOffset(state.files))

	var lastHash [16]byte
	var partition []*FileInformation
	for _, file := range state.files {
		if file.Csum != nil {
			if *file.Csum != lastHash {
				if len(partition) != 0 {
					receiver(partition)
				}
				partition = partition[0:0]
				lastHash = *file.Csum
			}
			partition = append(partition, file)
		}
	}
	if len(partition) != 0 {
		receiver(partition)
	}
}

func (state *MemoryBased) EndPass3() {}

// ** private functions **
