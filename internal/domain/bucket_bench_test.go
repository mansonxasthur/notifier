package domain

import (
	"testing"
	"time"
)

func BenchmarkBucketTake(b *testing.B) {
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	bucket := NewBucket(BucketPolicy{Rate: 1e6, Burst: 1e6}, now)
	b.ReportAllocs()
	for range b.N {
		now = now.Add(time.Microsecond)
		if ok, _ := bucket.Take(1, now); !ok {
			b.Fatal("bucket ran dry")
		}
	}
}

// BenchmarkBucketTakeDenied is the path taken under overload: an empty bucket and the wait
// it has to work out.
func BenchmarkBucketTakeDenied(b *testing.B) {
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	bucket := NewBucket(BucketPolicy{Rate: 1, Burst: 1}, now)
	bucket.Take(1, now)
	b.ReportAllocs()
	for range b.N {
		if ok, _ := bucket.Take(1, now); ok {
			b.Fatal("empty bucket allowed a take")
		}
	}
}
