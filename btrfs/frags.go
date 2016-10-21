package btrfs

import "C"

import (
	"os"
	"unsafe"
	"github.com/bertbaron/btrdedup/ioctl"
)

const (
	fiemapOp = 0xc020660b
)

// size 56
type fiemap_extent struct {
	fe_logical    uint64  /* logical offset in bytes for the start of the extent from the beginning of the file */
	fe_physical   uint64  /* physical offset in bytes for the start of the extent from the beginning of the disk */
	fe_length     uint64; /* length in bytes for this extent */
	fe_reserved64 [2]uint64;
	fe_flags      uint32  /* FIEMAP_EXTENT_* flags for this extent */
	fe_reserved   [3]uint32;
}

// size 32
type fiemap struct {
	fm_start          uint64 /* logical offset (inclusive) at which to start mapping (in) */
	fm_length         uint64 /* logical length of mapping which userspace wants (in) */
	fm_flags          uint32 /* FIEMAP_FLAG_* flags for request (in/out) */
	fm_mapped_extents uint32 /* number of extents that were mapped (out) */
	fm_extent_count   uint32 /* size of fm_extents array (in) */
	fm_reserved       uint32
				 //struct fiemap_extent fm_extents[0]; /* array of mapped extents (out) */
}

func fiemapSize(extendCount int) int {
	return 32 + 56 * extendCount
}

// Allocates memory in C. Note that the C struct contains a dynamic array at the end, which is not possible in go,
// therefore we return a go slice which is backed by that dynamic array in addition to the pointer to the args struct
func allocateFiemap(extendCount int) (*fiemap, []fiemap_extent) {
	size := C.size_t(fiemapSize(extendCount))
	ptr := C.malloc(size)
	args := (*fiemap)(ptr)

	// Create a slice backed by the dynamic array at the end of the struct
	infoPtr := unsafe.Pointer(uintptr(ptr) + 32)
	extendInfo := (*[1 << 30]fiemap_extent)(infoPtr)[:extendCount]
	return args, extendInfo
}

func PhysicalOffset(file *os.File) (uint64, error) {
	data, extends := allocateFiemap(10)
	defer free((unsafe.Pointer)(data))
	data.fm_start = 0
	data.fm_length = 1024 * 1024 * 1024
	data.fm_extent_count = 10

	if err := ioctl.IOCTL(file.Fd(), fiemapOp, (uintptr)((unsafe.Pointer)(data))); err != nil {
		return 0, err
	}
	if data.fm_mapped_extents == 0 {
		return 0, nil
	}
	return extends[0].fe_physical, nil
}

