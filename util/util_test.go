package util

import (
	"testing"
	"fmt"
)

type simpleData struct {
	value int
	name string

}

func data(value int, name string) simpleData {
	return simpleData{value, name}
}

type ByValue []simpleData
func (d ByValue) Size() int {
	return len(d)
}
func (d ByValue) Same(i, j int) bool {
	return d[i].value == d[j].value
}

func assertFormatted(t *testing.T, actual interface{}, expected string) {
	s := fmt.Sprintf("%v", actual)
	if s != expected {
		t.Error("Expected", expected, "but was", actual)
	}
}
func TestPartitioning1(t *testing.T) {
	in := []simpleData{data(3, "x"), data(2,"a"), data(2, "x"), data(1, "a")}
	parts := Partition(ByValue(in))
	assertFormatted(t, parts, "[{0, 1}, {1, 3}, {3, 4}]")
}
func TestPartitioningOfEmptySlice(t *testing.T) {
	in := []simpleData{}
	parts := Partition(ByValue(in))
	assertFormatted(t, parts, "[]")
}