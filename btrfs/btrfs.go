package btrfs

import (
	//"golang.org/x/sys/unix"
	"os"
	"github.com/bertbaron/btrdedup/ioctl"
	"unsafe"
)

const (
	btrfs_ioctl_magic = 0x94
	btrfs_same_data_differs = 1
)

/* For extent-same ioctl */
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

type btrfs_ioctl_same_args struct {
	logical_offset uint64 /* in - start of extent in source */
	length         uint64 /* in - length of extent */
	dest_count     uint16 /* in - total elements in info array */
	reserved1      uint16
	reserved2      uint32
	info           *btrfs_ioctl_same_extent_info
}

func Btrfs_extent_same(file *os.File, same btrfs_ioctl_same_args) int {
	size := unsafe.Sizeof(same)
	op := ioctl.IOWR(btrfs_ioctl_magic, 54, size)
	ioctl.IOCTL(file.Fd(), op, same)
	return 0
	//return ioctl(fd, BTRFS_IOC_FILE_EXTENT_SAME, same)
}

type BtrfsSameExtendInfo struct {
	file *os.File
	logicalOffset uint64
}

func BtrfsExtendSame(file *os.File, logicalOffset, length uint64, destCount uint16, same []BtrfsSameExtendInfo) {

}