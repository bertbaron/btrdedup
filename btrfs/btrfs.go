package btrfs

import (
	//"golang.org/x/sys/unix"
	"os"
	"unsafe"
	"golang.org/x/sys/unix"

	"syscall"
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

func ioctl(fd, op, arg uintptr) error {
	_, _, ep := unix.Syscall(unix.SYS_IOCTL, fd, op, arg)
	if ep != 0 {
		return syscall.Errno(ep)
	}
	return nil
}
func Btrfs_extent_same(file *os.File, same *btrfs_ioctl_same_args) int {
	ioctl(file.Fd(), BTRFS_IOC_FILE_EXTENT_SAME, same)
	return 0
	//return ioctl(fd, BTRFS_IOC_FILE_EXTENT_SAME, same)
}