package btrfs

/*
#include <stdlib.h>
#include <string.h>
*/
import "C"
import "unsafe"

func free(ptr unsafe.Pointer) {
	C.free(ptr)
}
