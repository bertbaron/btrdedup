package main

import (
	"log"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"flag"
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



func main() {
	flag.Parse()
	filenames := flag.Args()
	Dedup(filenames)
}
