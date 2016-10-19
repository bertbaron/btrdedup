package main

import (
	"math"
	"os"
	"log"
	"github.com/bertbaron/btrdedup/btrfs"
)

func minFileSize(filenames []string) uint64 {
	var minSize int64 = math.MaxInt64
	for _, filename := range filenames {
		stat, err := os.Stat(filename)
		if (err != nil) {
			log.Fatal(err)
		}
		log.Printf("Stats for %s: %v", filename, stat)
		if stat.Size() < minSize { minSize = stat.Size() }
	}
	return uint64(minSize)
}

func dedup(filenames []string, offset, len uint64) {
	same := make([]btrfs.BtrfsSameExtendInfo, 0)
	for _, filename := range filenames {
		file, err := os.OpenFile(filename, os.O_RDWR, 0)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()
		same = append(same, btrfs.BtrfsSameExtendInfo{file, offset})
		//btrfs.PhysicalOffset(file)
	}

	result, err := btrfs.BtrfsExtendSame(same, len)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Result: %v", result)
}

func Dedup(filenames []string) {
	size := minFileSize(filenames)
	dedup(filenames, 0, size)
}