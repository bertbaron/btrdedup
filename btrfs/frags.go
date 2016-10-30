package btrfs

import (
	"os"
	"unsafe"
	"github.com/bertbaron/btrdedup/ioctl"
)

const (
	fiemapOp = 0xc020660b
	extendBufferCount = 10
	fiemapSize = 32
	fiemapExtendSize = 56
	datasize = fiemapSize + fiemapExtendSize * extendBufferCount
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

func PhysicalOffset(file *os.File) (uint64, error) {
	var buffer [datasize]byte // FIXME Are we sure or how do we make sure that the buffer is not gc'ed too soon
	argsPtr := (unsafe.Pointer(&buffer[0]))
	xtPtr := (unsafe.Pointer(&buffer[fiemapSize]))
	var data *fiemap = (*fiemap)(argsPtr)
	var extends []fiemap_extent = (*[1 << 30]fiemap_extent)(xtPtr)[:extendBufferCount]
	data.fm_start = 0
	data.fm_length = 4 * 1024
	data.fm_extent_count = extendBufferCount

	if err := ioctl.IOCTL(file.Fd(), fiemapOp, (uintptr)((unsafe.Pointer)(data))); err != nil {
		return 0, err
	}
	if data.fm_mapped_extents == 0 {
		return 0, nil
	}
	return extends[0].fe_physical, nil
}

