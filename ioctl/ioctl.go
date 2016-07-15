/* Thanks to the goPi project on github... */
package ioctl

import (
	"log"
	"golang.org/x/sys/unix"
)

const (
	IOC_NRBITS   = 8
	IOC_TYPEBITS = 8

	IOC_SIZEBITS = 14

	IOC_NRSHIFT   = 0
	IOC_TYPESHIFT = IOC_NRSHIFT + IOC_NRBITS
	IOC_SIZESHIFT = IOC_TYPESHIFT + IOC_TYPEBITS
	IOC_DIRSHIFT  = IOC_SIZESHIFT + IOC_SIZEBITS

	// Direction bits
	IOC_NONE  = 0
	IOC_WRITE = 1
	IOC_READ  = 2
)

func IOC(dir, t, nr, size uintptr) uintptr {
	return (dir << IOC_DIRSHIFT) | (t << IOC_TYPESHIFT) | (nr << IOC_NRSHIFT) | (size << IOC_SIZESHIFT)
}

// used to create ioctl numbers

func IO(t, nr uintptr) uintptr {
	return IOC(IOC_NONE, t, nr, 0)
}

func IOR(t, nr, size uintptr) uintptr {
	return IOC(IOC_READ, t, nr, size)
}

func IOW(t, nr, size uintptr) uintptr {
	return IOC(IOC_WRITE, t, nr, size)
}

func IOWR(t, nr, size uintptr) uintptr {
	log.Printf("IOWR(%x, %x, %x)", t, nr, size)
	return IOC(IOC_READ|IOC_WRITE, t, nr, size)
}

//func IOCTL(fd, op, arg uintptr) error {
//	log.Printf("IOCTL(%x, %x, %x)", fd, op, arg)
//	r1, r2, ep := syscall.Syscall(syscall.SYS_IOCTL, fd, op, arg)
//	if ep != 0 {
//		return syscall.Errno(ep)
//	}
//	log.Printf("Syscall result: %v, %v", r1, r2)
//	return nil
//}
func IOCTL(fd, op, arg uintptr) error {
	if _, _, err := unix.Syscall(unix.SYS_IOCTL, fd, op, arg); err != 0 {
		return err
	}
	return nil
}
