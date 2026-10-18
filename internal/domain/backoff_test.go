package domain

import (
	"testing"
	"time"
)

func TestBackoffDelay(t *testing.T) {
	b := Backoff{Initial: time.Second, Max: time.Minute, Multiplier: 2, Jitter: 0.2}
	tests := []struct {
		name     string
		attempts int
		unit     float64
		want     time.Duration
	}{
		{"first attempt, middle of band", 1, 0.5, time.Second},
		{"second attempt doubles", 2, 0.5, 2 * time.Second},
		{"zero attempts counts as one", 0, 0.5, time.Second},
		{"low edge of jitter", 1, 0, 800 * time.Millisecond},
		{"high edge of jitter", 1, 1, 1200 * time.Millisecond},
		{"capped at max", 20, 0.5, time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := b.Delay(tt.attempts, tt.unit); got != tt.want {
				t.Errorf("Delay(%d, %v) = %s, want %s", tt.attempts, tt.unit, got, tt.want)
			}
		})
	}
}
