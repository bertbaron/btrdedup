// A bit similar to github.com/meirf/gopart, but we partition into slices of subsequent elements that are equal according
// to some sorting function.
package util

import "sort"

// IdxRange specifies a single range. Low and High
// are the indexes in the larger collection at which this
// range begins and ends, respectively. Note that High
// is exclusive, whereas Low is inclusive.
type IdxRange struct {
	Low, High int
}

type Partitioner interface {
	Len() int
	Same(i, j int) bool
}

// Adapts sort.Interface to a Partitioner. This may be somewhat slower because two calls to Less are needed instead of
// one to Same, but it can be quite convenient and if Less is fast it shouldn't differ that much
type SortAdapter struct {
	Sort sort.Interface
}

func (s SortAdapter) Len() int {
	return s.Sort.Len()
}

func (s SortAdapter) Same(i, j int) bool {
	return !s.Sort.Less(i, j) && !s.Sort.Less(j, i)
}

// Partitions
func Partition(data Partitioner) chan IdxRange {
	c := make(chan IdxRange)
	size := data.Len()
	if size <= 0 {
		close(c)
		return c
	}

	go func() {
		start := 0
		idx := 1
		for idx < size {
			if !data.Same(idx - 1, idx) {
				c <- IdxRange{start, idx}
				start = idx
			}
			idx ++
		}
		c <- IdxRange{start, idx}
		close(c)
	}()
	return c
}

// First sorts the data IN PLACE and then partitions it using the sort SortAdapter.
func SortAndPartition(data sort.Interface) chan IdxRange {
	sort.Sort(data)
	return Partition(SortAdapter{data})
}