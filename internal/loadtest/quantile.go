package loadtest

import (
	"math"
	"sort"
)

// Bucket is one cumulative histogram bucket: Count observations were at most Le.
type Bucket struct {
	Le    float64
	Count uint64
}

// Quantile estimates the q-th quantile (0..1) from cumulative buckets by linear
// interpolation inside the bucket the rank falls in, as Prometheus does. It is an estimate
// whose error is up to one bucket's width, so it shows the shape of a distribution and is
// never the figure a claim rests on. NaN means there were no observations, and +Inf that the rank falls above the last finite bucket.
func Quantile(q float64, buckets []Bucket) float64 {
	if len(buckets) == 0 {
		return math.NaN()
	}
	b := append([]Bucket(nil), buckets...)
	sort.Slice(b, func(i, j int) bool { return b[i].Le < b[j].Le })
	total := b[len(b)-1].Count
	if total == 0 {
		return math.NaN()
	}
	rank := q * float64(total)
	lowCount, lowLe := uint64(0), 0.0
	for _, bk := range b {
		if float64(bk.Count) >= rank {
			if math.IsInf(bk.Le, 1) {
				return math.Inf(1) // beyond the last finite bucket: the estimate is unbounded
			}
			if bk.Count == lowCount {
				return bk.Le
			}
			return lowLe + (bk.Le-lowLe)*(rank-float64(lowCount))/float64(bk.Count-lowCount)
		}
		lowCount, lowLe = bk.Count, bk.Le
	}
	return lowLe
}
