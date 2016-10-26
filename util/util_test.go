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

type PartitionByValue []simpleData
func (d PartitionByValue) Len() int {
	return len(d)
}
func (d PartitionByValue) Same(i, j int) bool {
	return d[i].value == d[j].value
}

type SortByValue []simpleData
func (d SortByValue) Len() int {
	return len(d)
}
func (d SortByValue) Less(i, j int) bool {
	return d[i].value < d[j].value
}
func (d SortByValue) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

func assertFormatted(t *testing.T, actual chan IdxRange, expected string) {
	actualSlice := make([]IdxRange, 0)
	for idxRange := range actual {
		actualSlice = append(actualSlice, idxRange)
	}
	s := fmt.Sprintf("%v", actualSlice)
	if s != expected {
		t.Error("Expected", expected, "but was", actualSlice)
	}
}

func TestPartitioning1(t *testing.T) {
	in := []simpleData{data(3, "x"), data(2,"a"), data(2, "x"), data(1, "a"), data(3, "b")}
	parts := Partition(PartitionByValue(in))
	assertFormatted(t, parts, "[{0 1} {1 3} {3 4} {4 5}]")
}

func TestPartitioningOfEmptySlice(t *testing.T) {
	in := []simpleData{}
	parts := Partition(PartitionByValue(in))
	assertFormatted(t, parts, "[]")
}

func TestPartitioningOfSliceWithSingleElement(t *testing.T) {
	in := []simpleData{data(1, "z")}
	parts := Partition(PartitionByValue(in))
	assertFormatted(t, parts, "[{0 1}]")
}

func TestSortAndPartition(t *testing.T) {
	in := []simpleData{data(3, "x"), data(2,"a"), data(2, "x"), data(1, "a"), data(3, "b")}
	parts := SortAndPartition(SortByValue(in))
	assertFormatted(t, parts, "[{0 1} {1 3} {3 5}]")
}
