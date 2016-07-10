package main

import (
	//	"crypto/md5"
	//	"flag"
	//"fmt"
	//	"io"
	//	"io/ioutil"
	"log"
	"golang.org/x/sys/unix"
	//"strings"
	"os"
	//"path/filepath"
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

func dedup(filename1, filename2 string, len uint64) {
	file1, err := os.Open(filename1) // For read access.
	if err != nil {
		log.Fatal(err)
	}
	defer file1.Close()
	file2, err := os.Open(filename2) // For read access.
	if err != nil {
		log.Fatal(err)
	}
	defer file2.Close()
	log.Printf("Files: %v and %v", file1, file2)
	//xtInfo := btrfs.BtrfsSameExtendInfo{}
	//xtInfo.File = file2
	//xtInfo.LogicalOffset = 0
	same := make([]btrfs.BtrfsSameExtendInfo,0)
	same = append(same, btrfs.BtrfsSameExtendInfo{File: file1, LogicalOffset: 0})
	same = append(same, btrfs.BtrfsSameExtendInfo{File: file2, LogicalOffset: 0})
	btrfs.BtrfsExtendSame(same, len)
	//log.Printf("xtInfo: %v", xtInfo)
}

func main() {
	log.Printf("Current working directory: %v", currentWorkdingDir())
	printSystemInfo()
	dedup("local/mnt/a1", "local/mnt/a2", 2097152)
}
