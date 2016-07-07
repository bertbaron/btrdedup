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
	"github.com/bertbaron/dedup/btrfs"
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
	same_xt_info := btrfs.Btrfs_ioctl_same_extent_info{}
	log.Printf("Struct: %v", same_xt_info)
	return string(s[0:lens])
}

func dedup(filename1, filename2 string, len int64) {
	file1, err := os.Open(filename1) // For read access.
	if err != nil {
		log.Fatal(err)
	}
	defer file1.Close()
	file2, err := os.Open(filename2) // For read access.
	if err != nil {
		log.Fatal(err)
	}
	defer file1.Close()
	log.Printf("Files: %v and %v", file1, file2)
}

func main() {
	log.Printf("Hello world\n")
	buf := unix.Utsname{}
	if err := unix.Uname(&buf); err != nil {
		log.Printf("Error: %v", err)
	} else {
		log.Printf("Sysname: %v", c2s(buf.Sysname))
		log.Printf("Release: %v", c2s(buf.Release))
	}
	dedup("/home/bert/gocode/src/github.com/bertbaron/dedup/local/mnt/dir1/5m.mts", "/home/bert/gocode/src/github.com/bertbaron/dedup/local/mnt/dir2/5m.mts", 4096)
}
