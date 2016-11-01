package btrfs

import (
	"os"
	"unsafe"
	"github.com/bertbaron/btrdedup/ioctl"
)

const (
	fiemapOp = 0xc020660b
	extendBufferCount = 10
)

type fiemap_extent struct {
	fe_logical    uint64  /* logical offset in bytes for the start of the extent from the beginning of the file */
	fe_physical   uint64  /* physical offset in bytes for the start of the extent from the beginning of the disk */
	fe_length     uint64; /* length in bytes for this extent */
	fe_reserved64 [2]uint64;
	fe_flags      uint32  /* FIEMAP_EXTENT_* flags for this extent */
	fe_reserved   [3]uint32;
}

type fiemap struct {
	fm_start          uint64 /* logical offset (inclusive) at which to start mapping (in) */
	fm_length         uint64 /* logical length of mapping which userspace wants (in) */
	fm_flags          uint32 /* FIEMAP_FLAG_* flags for request (in/out) */
	fm_mapped_extents uint32 /* number of extents that were mapped (out) */
	fm_extent_count   uint32 /* size of fm_extents array (in) */
	fm_reserved       uint32
	fm_extends        [extendBufferCount]fiemap_extent // go doesn't support flexible array, so the easiest way is to fix the size with a constant
}

func PhysicalOffset(file *os.File) (uint64, error) {
	var data fiemap
	data.fm_start = 0
	data.fm_length = 4 * 1024
	data.fm_extent_count = extendBufferCount

	if err := ioctl.IOCTL(file.Fd(), fiemapOp, uintptr(unsafe.Pointer(&data))); err != nil {
		return 0, err
	}
	if data.fm_mapped_extents == 0 {
		return 0, nil
	}
	return data.fm_extends[0].fe_physical, nil
}
