package lifecycle

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

type fake struct {
	name     string
	events   *[]string
	mu       *sync.Mutex
	startErr error
	stopErr  error
	stopWait bool // block in Stop until ctx is done
}

func (f *fake) Name() string { return f.name }

func (f *fake) record(e string) {
	f.mu.Lock()
	*f.events = append(*f.events, e)
	f.mu.Unlock()
}

func (f *fake) Start(context.Context) error {
	f.record("start " + f.name)
	return f.startErr
}

func (f *fake) Stop(ctx context.Context) error {
	f.record("stop " + f.name)
	if f.stopWait {
		<-ctx.Done()
		return ctx.Err()
	}
	return f.stopErr
}

func setup(names ...string) ([]*fake, *[]string) {
	events := &[]string{}
	mu := &sync.Mutex{}
	fakes := make([]*fake, len(names))
	for i, n := range names {
		fakes[i] = &fake{name: n, events: events, mu: mu}
	}
	return fakes, events
}

func comps(fs []*fake) []Component {
	out := make([]Component, len(fs))
	for i, f := range fs {
		out[i] = f
	}
	return out
}

var quiet = slog.New(slog.DiscardHandler)

func cancelled() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func TestStartsInOrderStopsInReverse(t *testing.T) {
	fs, events := setup("db", "mq", "http")
	if err := Run(cancelled(), quiet, time.Second, comps(fs)...); err != nil {
		t.Fatal(err)
	}
	got := strings.Join(*events, ",")
	want := "start db,start mq,start http,stop http,stop mq,stop db"
	if got != want {
		t.Errorf("events = %s, want %s", got, want)
	}
}

func TestStartFailureStopsOnlyStarted(t *testing.T) {
	fs, events := setup("db", "mq", "http")
	boom := errors.New("boom")
	fs[1].startErr = boom

	err := Run(context.Background(), quiet, time.Second, comps(fs)...)
	if !errors.Is(err, boom) {
		t.Fatalf("want start error, got %v", err)
	}
	got := strings.Join(*events, ",")
	// mq failed to start, so only db is stopped; http never starts.
	if want := "start db,start mq,stop db"; got != want {
		t.Errorf("events = %s, want %s", got, want)
	}
}

func TestStopErrorsAreJoinedAndStopContinues(t *testing.T) {
	fs, events := setup("db", "mq")
	errMQ, errDB := errors.New("mq stuck"), errors.New("db stuck")
	fs[0].stopErr, fs[1].stopErr = errDB, errMQ

	err := Run(cancelled(), quiet, time.Second, comps(fs)...)
	if !errors.Is(err, errMQ) || !errors.Is(err, errDB) {
		t.Fatalf("want both stop errors, got %v", err)
	}
	if got, want := strings.Join(*events, ","), "start db,start mq,stop mq,stop db"; got != want {
		t.Errorf("events = %s, want %s", got, want)
	}
}

func TestStopTimeout(t *testing.T) {
	fs, _ := setup("slow")
	fs[0].stopWait = true

	begin := time.Now()
	err := Run(cancelled(), quiet, 50*time.Millisecond, comps(fs)...)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline exceeded, got %v", err)
	}
	if time.Since(begin) > 2*time.Second {
		t.Error("shutdown did not honour the timeout")
	}
}

func TestRetry(t *testing.T) {
	calls := 0
	err := Retry(context.Background(), 3, time.Millisecond, func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("not yet")
		}
		return nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("err=%v calls=%d, want success on third call", err, calls)
	}

	boom := errors.New("down")
	err = Retry(context.Background(), 2, time.Millisecond, func(context.Context) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("want wrapped last error, got %v", err)
	}

	err = Retry(cancelled(), 5, time.Hour, func(context.Context) error { return boom })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context cancellation, got %v", err)
	}
}

type failing struct {
	fake
	ch chan error
}

func (f *failing) Failed() <-chan error { return f.ch }

func TestRuntimeFailureEndsRun(t *testing.T) {
	fs, events := setup("db")
	f := &failing{fake: *fs[0], ch: make(chan error, 1)}
	f.name = "http"
	boom := errors.New("listener died")
	f.ch <- boom

	err := Run(context.Background(), quiet, time.Second, f)
	if !errors.Is(err, boom) {
		t.Fatalf("want the runtime failure, got %v", err)
	}
	if got, want := strings.Join(*events, ","), "start http,stop http"; got != want {
		t.Errorf("events = %s, want %s", got, want)
	}
}

func TestRetryRunsAtLeastOnce(t *testing.T) {
	calls := 0
	err := Retry(context.Background(), 0, time.Millisecond, func(context.Context) error { calls++; return nil })
	if err != nil || calls != 1 {
		t.Fatalf("err=%v calls=%d, want one successful call", err, calls)
	}
}
