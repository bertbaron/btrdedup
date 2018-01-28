package sys

/*
#include <string.h>
*/
import "C"

import (
	"fmt"
	"log"
	"os"
	"unsafe"
)

const (
	sameExtendOp = 0xc0189436 // IOWR(0x94, 54, 24)
	maxFileCount = 1024
)

type sameExtendInfo struct {
	fd             int64  /* in - destination file */
	logical_offset uint64 /* in - start of extent in destination */
	bytes_deduped  uint64 /* out - total # of bytes we were able to dedupe from this file */
	status         int32  /* out, 0 if ok, < 0 for error, 1 if data differs */
	reserved       uint32
}

type sameArgs struct {
	logical_offset uint64 /* in - start of extent in source */
	length         uint64 /* in - length of extent */
	dest_count     uint16 /* in - total elements in info array */
	reserved1      uint16
	reserved2      uint32
	extend_info    [maxFileCount]sameExtendInfo // go doesn't support dynamic array, but since memory allocation is much much faster than deduplication we simply reserve a big pile of it
}

func makeSameRequest(same []BtrfsSameExtendInfo, length uint64) *sameArgs {
	var args sameArgs
	args.logical_offset = same[0].LogicalOffset
	args.length = length
	args.dest_count = uint16(len(same) - 1)
	args.reserved1 = 0
	args.reserved2 = 0

	for index, element := range same[1:] {
		args.extend_info[index].fd = int64(element.File.Fd())
		args.extend_info[index].logical_offset = element.LogicalOffset
		args.extend_info[index].bytes_deduped = 0
		args.extend_info[index].status = 0
		args.extend_info[index].reserved = 0
	}
	return &args
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
	if result.Error != nil {
		s = fmt.Sprintf("Error: %s", result.Error)
	} else if result.DataDiffers {
		s = "Data was different"
	}
	return s
}

func makeSameResult(args *sameArgs) []BtrfsSameResult {
	results := make([]BtrfsSameResult, args.dest_count)
	for i, element := range args.extend_info[:args.dest_count] {
		var result BtrfsSameResult
		if element.status < 0 {
			errMsg := C.GoString(C.strerror(C.int(-element.status))) // TODO Do we need to free this?
			result = BtrfsSameResult{&errMsg, false, 0}
		} else if element.status == 1 {
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
	if len(same) > maxFileCount {
		for i, file := range same {
			log.Printf("DEBUG: poptential dup %d: %s", i, file.File.Name())
		}
		return nil, fmt.Errorf("Deduplication is currently supported for at most %d files, but was %d", maxFileCount, len(same))
	}

	args := makeSameRequest(same, length)

	if err := IOCTL(same[0].File.Fd(), sameExtendOp, uintptr(unsafe.Pointer(args))); err != nil {
		return nil, err
	}

	return makeSameResult(args), nil
}
