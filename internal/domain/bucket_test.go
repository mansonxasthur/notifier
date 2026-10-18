package domain

import (
	"math"
	"testing"
	"time"
)

var epoch = time.Unix(1_000_000, 0)

func TestBucketPolicyValidate(t *testing.T) {
	tests := []struct {
		name    string
		policy  BucketPolicy
		wantErr bool
	}{
		{"valid", BucketPolicy{Rate: 10, Burst: 20}, false},
		{"fractional rate", BucketPolicy{Rate: 0.5, Burst: 1}, false},
		{"zero rate", BucketPolicy{Rate: 0, Burst: 20}, true},
		{"negative rate", BucketPolicy{Rate: -1, Burst: 20}, true},
		{"zero burst", BucketPolicy{Rate: 10, Burst: 0}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.policy.Validate(); (err != nil) != tc.wantErr {
				t.Fatalf("Validate() = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestBucketStartsFullAndCapsAtBurst(t *testing.T) {
	b := NewBucket(BucketPolicy{Rate: 10, Burst: 5}, epoch)
	if ok, _ := b.Take(5, epoch); !ok {
		t.Fatal("a new bucket refused its whole burst")
	}
	// A long idle period refills to the burst and no further.
	later := epoch.Add(time.Hour)
	if ok, _ := b.Take(6, later); ok {
		t.Fatal("bucket held more than its burst")
	}
	if ok, _ := b.Take(5, later); !ok {
		t.Fatal("bucket did not refill to its burst")
	}
}

func TestBucketTakeIsAllOrNothing(t *testing.T) {
	b := NewBucket(BucketPolicy{Rate: 10, Burst: 10}, epoch)
	b.Take(8, epoch)
	ok, wait := b.Take(5, epoch)
	if ok {
		t.Fatal("took 5 with 2 left")
	}
	// 3 tokens missing at 10 per second.
	if want := 300 * time.Millisecond; wait < want || wait > want+time.Microsecond {
		t.Fatalf("wait = %v, want %v plus rounding", wait, want)
	}
	// The refused charge took nothing, so the 2 tokens are still there.
	if ok, _ := b.Take(2, epoch); !ok {
		t.Fatal("a refused charge consumed tokens")
	}
}

func TestBucketWaitIsEnough(t *testing.T) {
	b := NewBucket(BucketPolicy{Rate: 3, Burst: 10}, epoch)
	b.Take(10, epoch)
	ok, wait := b.Take(7, epoch)
	if ok {
		t.Fatal("took from an empty bucket")
	}
	if ok, _ := b.Take(7, epoch.Add(wait)); !ok {
		t.Fatalf("charge still refused after the returned wait of %v", wait)
	}
}

func TestBucketRefillsGradually(t *testing.T) {
	b := NewBucket(BucketPolicy{Rate: 2, Burst: 4}, epoch)
	b.Take(4, epoch)
	if ok, _ := b.Take(1, epoch.Add(400*time.Millisecond)); ok {
		t.Fatal("0.8 tokens covered a charge of 1")
	}
	if ok, _ := b.Take(1, epoch.Add(500*time.Millisecond)); !ok {
		t.Fatal("1 token earned after 500ms at 2/s was refused")
	}
}

func TestBucketIgnoresClockGoingBackwards(t *testing.T) {
	b := NewBucket(BucketPolicy{Rate: 10, Burst: 10}, epoch)
	b.Take(10, epoch)
	if ok, _ := b.Take(1, epoch.Add(-time.Hour)); ok {
		t.Fatal("a backwards clock refilled the bucket")
	}
	if ok, _ := b.Take(1, epoch.Add(100*time.Millisecond)); !ok {
		t.Fatal("bucket stopped refilling after the clock went backwards")
	}
}

func TestBucketPolicyFits(t *testing.T) {
	p := BucketPolicy{Rate: 1, Burst: 9}
	if !p.Fits(9) || p.Fits(10) {
		t.Fatal("Fits does not follow the burst")
	}
}

func TestBucketFreeAndOversizedCharges(t *testing.T) {
	b := NewBucket(BucketPolicy{Rate: 10, Burst: 5}, epoch)
	for _, n := range []int{0, -1000} {
		if ok, wait := b.Take(n, epoch); !ok || wait != 0 {
			t.Fatalf("Take(%d) = %v, %v; want a free charge", n, ok, wait)
		}
	}
	// A negative charge must not have stored extra tokens.
	if ok, _ := b.Take(6, epoch); ok {
		t.Fatal("a negative charge inflated the bucket")
	}
	if ok, wait := b.Take(6, epoch); ok || wait != MaxWait {
		t.Fatalf("charge above the burst = %v, %v; want refused with MaxWait", ok, wait)
	}
}

func TestBucketWaitIsCapped(t *testing.T) {
	b := NewBucket(BucketPolicy{Rate: 1e-12, Burst: 2}, epoch)
	b.Take(2, epoch)
	if ok, wait := b.Take(1, epoch); ok || wait != MaxWait {
		t.Fatalf("Take = %v, %v; want refused with MaxWait, not an overflowed wait", ok, wait)
	}
}

func TestBucketWaitIsEnoughAcrossRates(t *testing.T) {
	for _, rate := range []float64{0.3, 3, 7, 13.7, 1000, 123456.789} {
		b := NewBucket(BucketPolicy{Rate: rate, Burst: 1000}, epoch)
		b.Take(1000, epoch)
		ok, wait := b.Take(333, epoch)
		if ok {
			t.Fatalf("rate %v: took from an empty bucket", rate)
		}
		if ok, _ := b.Take(333, epoch.Add(wait)); !ok {
			t.Fatalf("rate %v: refused after the returned wait of %v", rate, wait)
		}
	}
}

func TestBucketPolicyValidateRejectsNonFinite(t *testing.T) {
	for _, rate := range []float64{math.NaN(), math.Inf(1)} {
		if (BucketPolicy{Rate: rate, Burst: 1}).Validate() == nil {
			t.Fatalf("rate %v accepted", rate)
		}
	}
}
