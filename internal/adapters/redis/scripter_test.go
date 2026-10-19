package redis

import (
	"context"
	"testing"

	"github.com/mansonxasthur/notifier/internal/domain"
)

func TestScripterBeforeStartFailsInsteadOfPanicking(t *testing.T) {
	c := New("redis://127.0.0.1:1/0") // never started, so the client is nil
	l := NewLimiter(c.Scripter(), "rl:unstarted", domain.BucketPolicy{Rate: 1, Burst: 1})
	if d, err := l.Allow(context.Background(), 1); err == nil {
		t.Fatalf("Allow = %+v, nil; want an error the fallback can answer", d)
	}
}
