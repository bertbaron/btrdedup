/* Thanks to the goPi project on github... */
package sys

import (
	"golang.org/x/sys/unix"
)

func IOCTL(fd, op, arg uintptr) error {
	if _, _, err := unix.Syscall(unix.SYS_IOCTL, fd, op, arg); err != 0 {
		return err
	}
	return nil
}
