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

	StartPass2()
	PartitionOnOffset(receiver func(files []*FileInformation))
	ChecksumUpdated(files []*FileInformation)
	EndPass2()

	StartPass3()
	PartitionOnHash(receiver func(files []*FileInformation))
	EndPass3()
}