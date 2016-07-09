package btrfs

/*
#include <stdio.h>
#include <stdlib.h>

void myprint(char* s) {
    printf("%s\n", s);
}
*/
import "C"

import (
	//"golang.org/x/sys/unix"
	"os"
	//"github.com/bertbaron/btrdedup/ioctl"
	//"unsafe"
	"log"
	"unsafe"
)

const (
	btrfs_ioctl_magic = 0x94
	btrfs_same_data_differs = 1
)

// size 32
type btrfs_ioctl_same_extent_info struct {
	fd             int64  /* in - destination file */
	logical_offset uint64 /* in - start of extent in destination */
	bytes_deduped  uint64 /* out - total # of bytes we were able to dedupe from this file */
			      /* status of this dedupe operation:
			       * 0 if dedup succeeds
			       * < 0 for error
			       * == BTRFS_SAME_DATA_DIFFERS if data differs
			       */
	status         int32  /* out - see above description */
	reserved       uint32
}

// size 24
type btrfs_ioctl_same_args struct {
	logical_offset uint64 /* in - start of extent in source */
	length         uint64 /* in - length of extent */
	dest_count     uint16 /* in - total elements in info array */
	reserved1      uint16
	reserved2      uint32
			      //info      btrfs_ioctl_same_extent_info[]
}

func Example() {
	cs := C.CString("Hello from stdio\n")
	C.myprint(cs)
	C.free(unsafe.Pointer(cs))
}

//func Btrfs_extent_same(file *os.File, same btrfs_ioctl_same_args) int {
//	size := unsafe.Sizeof(same)
//	op := ioctl.IOWR(btrfs_ioctl_magic, 54, size)
//	ioctl.IOCTL(file.Fd(), op, same)
//	return 0
//	//return ioctl(fd, BTRFS_IOC_FILE_EXTENT_SAME, same)
//}

type BtrfsSameExtendInfo struct {
	File          *os.File
	LogicalOffset uint64
}

func BtrfsExtendSame(same []BtrfsSameExtendInfo, length uint64) {
	var size uintptr = uintptr(24 + 32 * (len(same) - 1))
	//var l uint = uint(len(same) - 1)
	log.Printf("Len: %v", len(same))
	//size := uint(unsafe.Sizeof(btrfs_ioctl_same_extent_info{})) // * l
	//size := unsafe.Sizeof(btrfs_ioctl_same_args{} + (len(same) - 1) * unsafe.Sizeof(btrfs_ioctl_same_extent_info{}))
	log.Printf("Bericht grootte: %v", size)
	Example()
}