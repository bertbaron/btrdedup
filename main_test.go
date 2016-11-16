package main

import (
	"testing"
)

type MockFileSystem struct {
	next uint64
}

type MockFragment struct {
	data   string
	start  uint64
	length uint64
}

type MockFile struct {
	fragments []MockFragment
}

// creates a new fragment on the virtual disk with the characters of the given string
// representing the contents of the block. The fragment is not adjacent to any other
// fragment.
func (fs *MockFileSystem) fragment(data string) MockFragment {
	fragment := MockFragment{data: data, start: fs.next, length: uint64(len(data))}
	fs.next = fragment.start + fragment.length + 1
	return fragment
}

func (fs *MockFileSystem) file(frags ...MockFragment) MockFile {
	return MockFile{fragments: frags}
}

func assertDefragResult(defraggedSize uint64, files ...MockFile) {
	// TODO implement assertion
}

func TestSubmitForDefrag(t *testing.T) {
	var fs MockFileSystem

	a1 := fs.fragment("abc")
	a2 := fs.fragment("abc")

	f1 := fs.file(a1)
	f2 := fs.file(a2)

	assertDefragResult(3, f1, f2)
}
