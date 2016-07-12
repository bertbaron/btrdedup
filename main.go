package main

import (
	"log"
	"golang.org/x/sys/unix"
	"os"
	"github.com/bertbaron/btrdedup/btrfs"
	"path/filepath"
)

func c2s(ca [65]int8) string {
	s := make([]byte, len(ca))
	var lens int
	for ; lens < len(ca); lens++ {
		if ca[lens] == 0 {
			break
		}
		s[lens] = uint8(ca[lens])
	}
	return string(s[0:lens])
}

func currentWorkdingDir() string {
	finfo, err := os.Stat(".")
	if err != nil {
		log.Fatal("Error looking up current directory: %v", err)
	}
	path, err := filepath.Abs(finfo.Name())
	if err != nil {
		log.Fatal("Error looking up absolute path: %v", err)
	}
	return path
}

func printSystemInfo() {
	buf := unix.Utsname{}
	if err := unix.Uname(&buf); err != nil {
		log.Printf("Error: %v", err)
	} else {
		log.Printf("Sysname: %v", c2s(buf.Sysname))
		log.Printf("Release: %v", c2s(buf.Release))
	}
}

func dedup(filename1, filename2 string, offset, len uint64) {
	file1, err := os.OpenFile(filename1, os.O_RDWR, 0) // For read access.
	if err != nil {
		log.Fatal(err)
	}
	defer file1.Close()
	file2, err := os.OpenFile(filename2, os.O_RDWR, 0) // For read access.
	if err != nil {
		log.Fatal(err)
	}
	defer file2.Close()
	log.Printf("Files: %v and %v", file1, file2)

	same := make([]btrfs.BtrfsSameExtendInfo, 0)
	same = append(same, btrfs.BtrfsSameExtendInfo{file1, offset})
	same = append(same, btrfs.BtrfsSameExtendInfo{file2, offset})
	result, err := btrfs.BtrfsExtendSame(same, len)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Result: %v", result)
}

func main() {
	log.Printf("Current working directory: %v", currentWorkdingDir())
	printSystemInfo()
	var M uint64 = 1024 * 1024
	dedup("local/mnt/a1", "local/mnt/a2", 0 * M, 2 * M)
	//dedup("local/mnt/a1", "local/mnt/a2", 10 * M, 10 * M)
	//dedup("local/mnt/a1", "local/mnt/a2", 20 * M, 10 * M)
}
