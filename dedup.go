package main

import (
	"os"
	"log"
	"github.com/bertbaron/btrdedup/btrfs"
)

const (
	maxSize uint64 = 64 * 1024 * 1024
)

// returns true if the data was the successfull, false otherwise
func dedup(filenames []string, offset, length uint64) bool {
	same := make([]btrfs.BtrfsSameExtendInfo, 0)
	for _, filename := range filenames {
		file, err := os.OpenFile(filename, os.O_RDONLY, 0)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()
		same = append(same, btrfs.BtrfsSameExtendInfo{file, offset})
	}

	result, err := btrfs.BtrfsExtendSame(same, length)
	if err != nil {
		//log.Printf("Error while deduplicating %s and %d other files: %v", filenames[0], len(filenames) - 1, err)
		log.Fatalf("Error while deduplicating %s and %d other files: %v", filenames[0], len(filenames) - 1, err) // for now we want to feel to identify issues
		return false
	}
	var bytesDeduped uint64 = 0
	dataDiffers := false
	for _, r := range result {
		dataDiffers = dataDiffers || r.DataDiffers
		if r.BytesDeduped > bytesDeduped {
			bytesDeduped = r.BytesDeduped
		}
	}
	log.Printf("Result for length %d: same=%v, deduped=%d\n", length, !dataDiffers, bytesDeduped)
	return !dataDiffers && bytesDeduped > 0
}

func Dedup(filenames []string, offset, length uint64) bool {
	size := offset + length

	max := maxSize / uint64(len(filenames))
	same := true
	for same && offset < size {
		len := size - offset
		if len > max {
			len = max &^ 0xF000
		}
		same = same && dedup(filenames, offset, len)
		offset = offset + len
	}
	return same
}