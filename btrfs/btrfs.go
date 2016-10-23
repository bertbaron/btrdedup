package btrfs

/*
#include <string.h>
*/
import "C"

import (
	"os"
	"log"
	"unsafe"
	"fmt"
	"github.com/bertbaron/btrdedup/ioctl"
)

const (
	sameExtendOp = 0xc0189436 // IOWR(0x94, 54, 24)
)

// size 32
type sameExtendInfo struct {
	fd             int64  /* in - destination file */
	logical_offset uint64 /* in - start of extent in destination */
	bytes_deduped  uint64 /* out - total # of bytes we were able to dedupe from this file */
	status         int32  /* out, 0 if ok, < 0 for error, 1 if data differs */
	reserved       uint32
}

// size 24
type sameArgs struct {
	logical_offset uint64 /* in - start of extent in source */
	length         uint64 /* in - length of extent */
	dest_count     uint16 /* in - total elements in info array */
	reserved1      uint16
	reserved2      uint32
}

func sameMessageSize(fileCount int) int {
	return 32 * fileCount - 8
}

// Allocates memory in C. Note that the C struct contains a dynamic array at the end, which is not possible in go,
// therefore we return a go slice which is backed by that dynamic array in addition to the pointer to the args struct
func allocate(fileCount int) (*sameArgs, []sameExtendInfo) {
	size := C.size_t(sameMessageSize(fileCount))
	ptr := C.malloc(size) // allocate memory for sameArgs + (n-1)*sameExtendInfo
	args := (*sameArgs)(ptr)

	// Create a slice backed by the dynamic array at the end of the struct
	infoPtr := unsafe.Pointer(uintptr(ptr) + 24)
	extendInfo := (*[1 << 30]sameExtendInfo)(infoPtr)[:fileCount - 1]
	return args, extendInfo
}

func fillSameArgumentStructure(same []BtrfsSameExtendInfo, length uint64, args *sameArgs, info []sameExtendInfo) {
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

type BtrfsSameResult struct {
	Error        *string
	DataDiffers  bool
	BytesDeduped uint64
}

func (result BtrfsSameResult) String() string {
	s := fmt.Sprintf("Ok, %d bytes deduplicated", result.BytesDeduped)
	if (result.Error != nil) {
		s = fmt.Sprintf("Error: %s", result.Error)
	} else if (result.DataDiffers) {
		s = "Data was different"
	}
	return s
}

func makeSameResult(info []sameExtendInfo) []BtrfsSameResult {
	results := make([]BtrfsSameResult, len(info))
	for i, element := range info {
		var result BtrfsSameResult
		if (element.status < 0) {
			errMsg := C.GoString(C.strerror(C.int(-element.status))) // TODO Do we need to free this?
			result = BtrfsSameResult{&errMsg, false, 0}
		} else if (element.status == 1) {
			result = BtrfsSameResult{nil, true, 0}
		} else {
			result = BtrfsSameResult{nil, false, element.bytes_deduped}
		}
		results[i] = result
	}
	return results
}

func BtrfsExtendSame(same []BtrfsSameExtendInfo, length uint64) ([]BtrfsSameResult, error) {
	if len(same) < 2 {
		log.Fatalf("Assertion error, there should be at least two files to deduplicate, found: %v", same)
	}
	args, info := allocate(len(same))
	defer free((unsafe.Pointer)(args))

	fillSameArgumentStructure(same, length, args, info)

	//log.Printf("IN:  args: %v, info: %v", args, info)
	if err := ioctl.IOCTL(same[0].File.Fd(), sameExtendOp, (uintptr)((unsafe.Pointer)(args))); err != nil {
		return nil, err
	}
	//log.Printf("OUT: args: %v, info: %v", args, info)

	return makeSameResult(info), nil
}