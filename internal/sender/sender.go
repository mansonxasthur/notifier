package sender

import (
	"context"
	"errors"
	"sync"

	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

// Sender is the lifecycle component that consumes every channel queue and scans for due retries.
type Sender struct {
	consumer ports.Consumer
	intake   *Intake
	scan     *Scan

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func New(consumer ports.Consumer, intake *Intake, scan *Scan) *Sender {
	return &Sender{consumer: consumer, intake: intake, scan: scan}
}

func (s *Sender) Name() string { return "sender" }

// Start begins consuming, one goroutine per channel queue, and the retry scan.
func (s *Sender) Start(ctx context.Context) error {
	ctx, s.cancel = context.WithCancel(context.WithoutCancel(ctx))
	for _, ch := range domain.Channels() {
		h, err := s.intake.Handler(ch)
		if err != nil {
			s.cancel()
			return err
		}
		s.wg.Go(func() { _ = s.consumer.Consume(ctx, ch, h) })
	}
	s.wg.Go(func() { s.scan.Run(ctx) })
	return nil
}

// Stop stops consuming and scanning and waits for in-flight messages, up to ctx.
func (s *Sender) Stop(ctx context.Context) error {
	if s.cancel == nil {
		return nil
	}
	s.cancel()
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return errors.New("stop sender: " + ctx.Err().Error())
	}
}
