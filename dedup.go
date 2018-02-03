package main

import (
	"github.com/bertbaron/btrdedup/sys"
	"log"
	"os"
)

const (
	maxSize uint64 = 64 * 1024 * 1024
)

// returns true if deduplication was successfull, false otherwise
func dedup(filenames []string, offset, length uint64) bool {
	same := make([]sys.BtrfsSameExtendInfo, 0)
	for _, filename := range filenames {
		file, err := os.OpenFile(filename, os.O_RDONLY, 0)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()
		same = append(same, sys.BtrfsSameExtendInfo{file, offset})
	}

	result, err := sys.BtrfsExtendSame(same, length)
	if err != nil {
		log.Printf("Error while deduplicating %s and %d other files: %v", filenames[0], len(filenames)-1, err)
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
	return !dataDiffers 
}

// Returns true if deduplication was successfull
func Dedup(filenames []string, offset, length uint64) {
	size := offset + length

	max := maxSize / uint64(len(filenames))
	same := true
	// continue until the data is different
	for same && offset < size {
		len := size - offset
		if len > max {
			len = max &^ 0x0FFF // multiple of 4k
		}
		same = same && dedup(filenames, offset, len)
		offset = offset + len
	}
}
