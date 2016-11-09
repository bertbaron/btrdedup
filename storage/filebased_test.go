package storage

import (
	"fmt"
	"testing"
	"github.com/bertbaron/btrdedup/sys"
)

func equalsInfo(a, b FileInformation) bool {
	equal := a.Path == b.Path && a.Error == b.Error && len(a.Fragments) == len(b.Fragments) && a.Csum == b.Csum
	if equal {
		for i, frag := range a.Fragments {
			equal = equal && frag == b.Fragments[i]
		}
	}
	return equal
}

func TestSerialization(t *testing.T) {
	var in FileInformation
	in.Path = 123
	in.Error = true
	in.Fragments = []sys.Fragment{sys.Fragment{12345, 123}}
	in.Csum = [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	data := serialize(in)
	fmt.Printf("data size: %d (%v)\n", len(data), data)

	out, err := deserialize(data)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if out == nil {
		t.Error("Deserialization should return an error or a result")
	}
	out.Csum = in.Csum
	if !equalsInfo(in, *out) {
		t.Errorf("Exepected: %+v, but was: %+v", in, out)
	}
}
