package rabbitmq

import (
	"context"
	"fmt"

	"github.com/mansonxasthur/notifier/internal/domain"
)

// QueueDepth returns the number of messages waiting in a channel's queue, not counting
// those delivered and not yet acknowledged. It uses a channel of its own, because a passive
// declare of a missing queue closes the channel that asked.
func (c *Conn) QueueDepth(ctx context.Context, ch domain.Channel) (int, error) {
	amqpCh, err := c.Channel(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = amqpCh.Close() }()
	q, err := amqpCh.QueueDeclarePassive(QueueName(ch), true, false, false, false, nil)
	if err != nil {
		return 0, fmt.Errorf("inspect queue %s: %w", ch, err)
	}
	return q.Messages, nil
}
