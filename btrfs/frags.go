package btrfs

import (
	"os"
	"unsafe"
	"github.com/bertbaron/btrdedup/ioctl"
	"github.com/pkg/errors"
	"fmt"
)

const (
	fiemapOp = 0xc020660b
	extendBufferCount = 20

	FIEMAP_EXTENT_LAST = 0x00000001 /* Last extent in file. */
	FIEMAP_EXTENT_UNKNOWN = 0x00000002 /* Data location unknown. */
	FIEMAP_EXTENT_DELALLOC = 0x00000004 /* Location still pending. Sets EXTENT_UNKNOWN. */
	FIEMAP_EXTENT_ENCODED = 0x00000008 /* Data can not be read while fs is unmounted */
	FIEMAP_EXTENT_DATA_ENCRYPTED = 0x00000080 /* Data is encrypted by fs. Sets EXTENT_NO_BYPASS. */
	FIEMAP_EXTENT_NOT_ALIGNED = 0x00000100 /* Extent offsets may not be block aligned. */
	FIEMAP_EXTENT_DATA_INLINE = 0x00000200 /* Data mixed with metadata. Sets EXTENT_NOT_ALIGNED.*/
	FIEMAP_EXTENT_DATA_TAIL = 0x00000400 /* Multiple files in block. Sets EXTENT_NOT_ALIGNED.*/
	FIEMAP_EXTENT_UNWRITTEN = 0x00000800 /* Space allocated, but no data (i.e. zero). */
	FIEMAP_EXTENT_MERGED = 0x00001000 /* File does not natively support extents. Result merged for efficiency. */
	FIEMAP_EXTENT_SHARED = 0x00002000 /* Space shared with other files. */
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
	fm_start          uint64                           /* logical offset (inclusive) at which to start mapping (in) */
	fm_length         uint64                           /* logical length of mapping which userspace wants (in) */
	fm_flags          uint32                           /* FIEMAP_FLAG_* flags for request (in/out) */
	fm_mapped_extents uint32                           /* number of extents that were mapped (out) */
	fm_extent_count   uint32                           /* size of fm_extents array (in) */
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

		if err := ioctl.IOCTL(file.Fd(), fiemapOp, uintptr(unsafe.Pointer(&data))); err != nil {
			return nil, err
		}
		if data.fm_mapped_extents == 0 {
			return nil, errors.New("No (more) extends found")
		}
		for _, extend := range data.fm_extents[0:data.fm_mapped_extents] {
			fmt.Printf("  %d, %+v\n", start, extend)
			last = last || extend.fe_flags & FIEMAP_EXTENT_LAST != 0
			if extend.fe_logical != start {
				return nil, errors.New("Sparse files are not supported")
			}
			result = append(result, Fragment{extend.fe_physical, extend.fe_length})
			start += extend.fe_length
		}
	}
	return result, nil
}