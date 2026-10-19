package sender

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

// scriptedClaims is an inbox whose ClaimMany answers from a script and, like the real one,
// reports an id it already handed out as a duplicate.
type scriptedClaims struct {
	ports.InboxStore
	mu      sync.Mutex
	missing map[int64]bool
	failing map[int64]bool
	claimed map[int64]bool
}

func (s *scriptedClaims) ClaimMany(_ context.Context, ids []int64, provider string, _ domain.ClaimLimits) ([]domain.InboxClaim, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.InboxClaim, len(ids))
	for i, id := range ids {
		switch {
		case s.failing[id]:
			return nil, errors.New("db down")
		case s.missing[id]:
			out[i] = domain.InboxClaim{Result: domain.InboxClaimMissing}
		case s.claimed[id]:
			out[i] = domain.InboxClaim{Result: domain.InboxClaimDuplicate}
		default:
			s.claimed[id] = true
			out[i] = domain.InboxClaim{Result: domain.InboxClaimWon, Provider: provider, ClaimCount: 1}
		}
	}
	return out, nil
}

func newTestIntake(t *testing.T, inbox ports.InboxStore, sent *[]int64, mu *sync.Mutex) ports.Handler {
	t.Helper()
	send := func(_ context.Context, c Claimed) error {
		mu.Lock()
		defer mu.Unlock()
		*sent = append(*sent, c.DeliveryID)
		return nil
	}
	in := NewIntake(inbox, map[domain.Channel]string{domain.ChannelSMS: "sms-a"}, domain.Claims(5), send, slog.New(slog.DiscardHandler)).
		WithBatching(1, time.Millisecond)
	h, err := in.Handler(domain.ChannelSMS)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(in.Close)
	return h
}

// A delivery id that does not exist is dropped and does not hold up the rest of its message.
func TestMessageWithPoisonIDCompletes(t *testing.T) {
	inbox := &scriptedClaims{missing: map[int64]bool{2: true}, claimed: map[int64]bool{}}
	var sent []int64
	var mu sync.Mutex
	h := newTestIntake(t, inbox, &sent, &mu)

	if err := h(context.Background(), []int64{1, 2, 3}); err != nil {
		t.Fatalf("handler = %v, want nil so the message is acknowledged", err)
	}
	slices.Sort(sent)
	if !slices.Equal(sent, []int64{1, 3}) {
		t.Errorf("sent %v, want [1 3]", sent)
	}
}

// A claim that fails names only its own id, and the message delivered again sends only what
// was not handled the first time.
func TestPartialFailureNamesFailedIDsAndRedeliveryIsIdempotent(t *testing.T) {
	inbox := &scriptedClaims{failing: map[int64]bool{4: true}, claimed: map[int64]bool{}}
	var sent []int64
	var mu sync.Mutex
	h := newTestIntake(t, inbox, &sent, &mu)

	err := h(context.Background(), []int64{3, 4, 5})
	var be *ports.BatchError
	if !errors.As(err, &be) || !slices.Equal(be.Failed, []int64{4}) {
		t.Fatalf("handler = %v, want a BatchError for [4]", err)
	}

	// The whole message comes back, as it does when the republish is not confirmed.
	inbox.failing = nil
	if err := h(context.Background(), []int64{3, 4, 5}); err != nil {
		t.Fatal(err)
	}
	slices.Sort(sent)
	if !slices.Equal(sent, []int64{3, 4, 5}) {
		t.Errorf("sent %v, want each of 3, 4 and 5 once", sent)
	}
}
