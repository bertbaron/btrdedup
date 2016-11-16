package main

import (
	"testing"
)


struct MockFileSystem {
}

struct MockFragment { }

// creates a new fragment on the virtual disk with the characters of the given string
// representing the contents of the block. The fragment is not adjacent to any other
// fragment.
func (fs *FileSystem) fragment(data string) MockFragment {
	return MockFragment
}

TestSubmitForDefrag(t *testing.T) {
	var fs FileSystem

	a1 := fs.fragment("abc")
	b1 := fs.fragment("def")
	c1 := fs.fragment("ghi")
	d1 := fs.fragment("jkl")
	e1 := fs.fragment("mno")
	f1 := fs.fragment("pqr")
	g1 := fs.fragment("stu")
	h1 := fs.fragment("vwx")

	a2 := fs.fragment("abc")
	
	f1 := "ABC.DEF.GHI.JKL"
	l1 := "000011112222333"
	f2 := "ABC.DEF.MNO"
	l2 := "44441111555"
	f3 := "ABC.PQR.STU"
	l3 := "00006666777"
	f4 := "ABC.PQR.VWX"
	l4 := "44447777888"
}
