// A bit similar to github.com/meirf/gopart, but we partition into slices of subsequent elements that are equal according
// to some sorting function.
package util

// IdxRange specifies a single range. Low and High
// are the indexes in the larger collection at which this
// range begins and ends, respectively. Note that High
// is exclusive, whereas Low is inclusive.
type IdxRange struct {
	Low, High int
}

type Partitioner interface {
	Size() int
	Same(i, j int) bool
}

// Partitions
func Partition(data Partitioner) []IdxRange {

	return []IdxRange{IdxRange{0, data.Size()}}
}
