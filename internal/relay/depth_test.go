package relay_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
	"github.com/mansonxasthur/notifier/internal/relay"
)

type depthRecorder struct {
	ports.NopRecorder
	mu  sync.Mutex
	got map[domain.Channel]int
}

func (r *depthRecorder) QueueDepth(ch domain.Channel, n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got[ch] = n
}

func TestDepthSamplerReportsEveryChannelAndSurvivesAFailedReading(t *testing.T) {
	rec := &depthRecorder{got: map[domain.Channel]int{}}
	depth := func(_ context.Context, ch domain.Channel) (int, error) {
		if ch == domain.ChannelEmail {
			return 0, errors.New("broker down")
		}
		return 7, nil
	}
	s := relay.NewDepthSampler(depth, rec, 5*time.Millisecond, quiet)
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "a reading of sms and whatsapp", func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return rec.got[domain.ChannelSMS] == 7 && rec.got[domain.ChannelWhatsApp] == 7
	})
	if err := s.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if _, ok := rec.got[domain.ChannelEmail]; ok {
		t.Error("a failed reading must not be reported as a depth")
	}
}
