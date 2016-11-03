package sys

import (
	"github.com/pkg/errors"
	"os"
	"unsafe"
)

const (
	fiemapOp          = 0xc020660b
	extendBufferCount = 20

	FIEMAP_EXTENT_LAST = 0x00000001 /* Last extent in file. */
)

type fiemap_extent struct {
	fe_logical    uint64 /* logical offset in bytes for the start of the extent from the beginning of the file */
	fe_physical   uint64 /* physical offset in bytes for the start of the extent from the beginning of the disk */
	fe_length     uint64 /* length in bytes for this extent */
	fe_reserved64 [2]uint64
	fe_flags      uint32 /* FIEMAP_EXTENT_* flags for this extent */
	fe_reserved   [3]uint32
}

type fiemap struct {
	fm_start          uint64 /* logical offset (inclusive) at which to start mapping (in) */
	fm_length         uint64 /* logical length of mapping which userspace wants (in) */
	fm_flags          uint32 /* FIEMAP_FLAG_* flags for request (in/out) */
	fm_mapped_extents uint32 /* number of extents that were mapped (out) */
	fm_extent_count   uint32 /* size of fm_extents array (in) */
	fm_reserved       uint32
	fm_extents        [extendBufferCount]fiemap_extent // go doesn't support flexible array, so the easiest way is to fix the size with a constant
}

type Fragment struct {
	Start  uint64
	Length uint64
}

// Returns all the fragments of the file in logical order. Sparse files are not supported with this function,
// Therefore the logical offset is not needed.
func Fragments(file *os.File) ([]Fragment, error) {
	var result []Fragment

	start := uint64(0)
	last := false
	for !last {
		var data fiemap
		data.fm_start = start
		data.fm_length = 1024 * 1024 * 1024
		data.fm_extent_count = extendBufferCount

		if err := IOCTL(file.Fd(), fiemapOp, uintptr(unsafe.Pointer(&data))); err != nil {
			return nil, err
		}
		if data.fm_mapped_extents == 0 {
			return nil, errors.New("No (more) extends found")
		}
		for _, extend := range data.fm_extents[0:data.fm_mapped_extents] {
			last = last || extend.fe_flags&FIEMAP_EXTENT_LAST != 0
			if extend.fe_logical != start {
				return nil, errors.New("Sparse files are not supported")
			}
			result = append(result, Fragment{extend.fe_physical, extend.fe_length})
			start += extend.fe_length
		}
	}
	return result, nil
}
