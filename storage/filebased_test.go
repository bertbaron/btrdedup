package storage

import (
	"fmt"
	"testing"
	"github.com/bertbaron/btrdedup/sys"
)

func TestSerialization(*testing.T) {
	var in FileInformation
	in.Path = 123
	in.Error = true
	in.Fragments = []sys.Fragment{sys.Fragment{12345, 123}}
	in.Csum = [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	data := serialize(in)
	fmt.Printf("data size: %d (%v)\n", len(data), data)
}
