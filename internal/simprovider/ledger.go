package simprovider

import (
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/mansonxasthur/notifier/internal/domain"
)

// Entry is what the simulator decided for one delivery at one provider.
type Entry struct {
	DeliveryID int64           `json:"delivery_id"`
	Provider   string          `json:"provider"`
	Scenario   domain.Scenario `json:"scenario"`
	// Accepted says whether the provider holds the message: a status lookup finds it.
	Accepted bool `json:"accepted"`
	// Sends counts the calls received for this delivery, replays included.
	Sends      int       `json:"sends"`
	ReceivedAt time.Time `json:"received_at"`
}

// holdsMessage reports whether the scenario ends with the provider having the message.
// A call that times out still got there: the provider was only too slow to answer.
func holdsMessage(s domain.Scenario) bool {
	switch s {
	case domain.ScenarioSucceed, domain.ScenarioSlow, domain.ScenarioTimeout, domain.ScenarioLateFailure:
		return true
	}
	return false
}

type ledgerKey struct {
	provider string
	id       int64
}

// Ledger registers the scenario chosen for each delivery, so a repeat send replays it and
// a test or load run can compare the system's final states with what was decided.
// It keeps everything in memory, which suits a simulator.
type Ledger struct {
	mu      sync.Mutex
	entries map[ledgerKey]*Entry
}

func NewLedger() *Ledger { return &Ledger{entries: map[ledgerKey]*Entry{}} }

// Record registers a call for the delivery. The first call picks its scenario; later calls
// get the same one. first says whether this call was the first.
func (l *Ledger) Record(provider string, id int64, pick func() domain.Scenario, now time.Time) (e Entry, first bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	k := ledgerKey{provider, id}
	cur, ok := l.entries[k]
	if !ok {
		s := pick()
		cur = &Entry{DeliveryID: id, Provider: provider, Scenario: s, Accepted: holdsMessage(s), ReceivedAt: now}
		l.entries[k] = cur
	}
	cur.Sends++
	return *cur, !ok
}

// Lookup returns the entry for the delivery at the provider.
func (l *Ledger) Lookup(provider string, id int64) (Entry, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.entries[ledgerKey{provider, id}]
	if !ok {
		return Entry{}, false
	}
	return *e, true
}

// ForDelivery returns the entries for the delivery at every provider, by provider name.
func (l *Ledger) ForDelivery(id int64) []Entry {
	return l.filter(func(e *Entry) bool { return e.DeliveryID == id })
}

// All returns every entry, by delivery then provider.
func (l *Ledger) All() []Entry { return l.filter(func(*Entry) bool { return true }) }

func (l *Ledger) filter(keep func(*Entry) bool) []Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := []Entry{}
	for _, e := range l.entries {
		if keep(e) {
			out = append(out, *e)
		}
	}
	slices.SortFunc(out, func(a, b Entry) int {
		if a.DeliveryID != b.DeliveryID {
			if a.DeliveryID < b.DeliveryID {
				return -1
			}
			return 1
		}
		return strings.Compare(a.Provider, b.Provider)
	})
	return out
}
