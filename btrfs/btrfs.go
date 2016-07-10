package btrfs

/*
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/vfs.h>

#include <errno.h>

void print_mem(void const *vp, size_t n)
{
    unsigned char const *p = vp;
    for (size_t i=0; i<n; i++) {
        printf("%02x", p[i]);
        if (i % 8 == 3) { printf(" "); }
        if (i % 8 == 7) { printf("\n"); }
    }
    putchar('\n');
};
*/
import "C"

import (
	"os"
	"log"
	"unsafe"
	"github.com/bertbaron/btrdedup/ioctl"
)

const (
	btrfs_ioctl_magic = 0x94
	btrfs_same_data_differs = 1
)

// size 32
type sameExtendInfo struct {
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
type sameArgs struct {
	logical_offset uint64 /* in - start of extent in source */
	length         uint64 /* in - length of extent */
	dest_count     uint16 /* in - total elements in info array */
	reserved1      uint16
	reserved2      uint32
			      //info      btrfs_ioctl_same_extent_info[]
}

func messageSize(fileCount int) int {
	return 32 * fileCount - 8
}

// Allocates memory in C. Note that the C struct contains a dynamic array at the end, which is not possible in go,
// therefore we return a go slice which is backed by that dynamic array in addition to the pointer to the args struct
func allocate(fileCount int) (*sameArgs, []sameExtendInfo) {
	size := C.size_t(messageSize(fileCount))
	ptr := C.malloc(size) // allocate memory for sameArgs + n-1*sameExtendInfo
	log.Printf("Allocated memory at: %v", ptr)
	args := (*sameArgs)(ptr)

	// Create a slice backed by the dynamic array at the end of the struct
	infoPtr := unsafe.Pointer(uintptr(ptr) + 24)
	extendInfo := (*[1 << 30]sameExtendInfo)(infoPtr)[:fileCount - 1]
	return args, extendInfo
}

func free(args *sameArgs) {
	ptr := (unsafe.Pointer)(args)
	log.Printf("Freeing memory at: %v", ptr)
	C.free(ptr)
}

func fillArgumentStructure(same []BtrfsSameExtendInfo, length uint64, args *sameArgs, info []sameExtendInfo) {
	args.logical_offset = same[0].LogicalOffset
	args.length = length
	args.dest_count = uint16(len(same) - 1)
	args.reserved1 = 0
	args.reserved2 = 0

	for index, element := range same[1:] {
		info[index].fd = int64(element.File.Fd())
		info[index].logical_offset = element.LogicalOffset
		info[index].bytes_deduped = 0
		info[index].status = 0
		info[index].reserved = 0
	}
}

type BtrfsSameExtendInfo struct {
	File          *os.File
	LogicalOffset uint64
}

func BtrfsExtendSame(same []BtrfsSameExtendInfo, length uint64) {
	if len(same) < 2 {
		log.Fatalf("Assertion error, there should be at least two files two deduplicate, found: %v", same)
	}
	args, info := allocate(len(same))
	defer free(args)

	fillArgumentStructure(same, length, args, info)

	log.Printf("IN:  args: %v, info: %v", args, info)
	log.Printf("MEMORY DUMP:\n")
	C.print_mem((unsafe.Pointer)(args), 56)

	//op := ioctl.IOWR(btrfs_ioctl_magic, 54, uintptr(messageSize(len(same))))
	op := ioctl.IOWR(btrfs_ioctl_magic, 54, 24)
	log.Printf("Operation: %X", op)
	err := ioctl.IOCTL(same[0].File.Fd(), op, (uintptr)((unsafe.Pointer)(args)))
	if (err != nil) {
		log.Fatalf("Error while deduplicating: %v", err)
	}
	log.Printf("OUT: args: %v, info: %v", args, info)

	for _, element := range info {
		if element.status < 0 {
			log.Printf("Error: %v", C.GoString(C.strerror(C.int(-element.status))))
		}
	}
}