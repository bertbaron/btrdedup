package storage

type FileInformation struct {
	Path           string
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