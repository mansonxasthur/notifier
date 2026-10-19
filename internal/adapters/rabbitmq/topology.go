package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/mansonxasthur/notifier/internal/domain"
)

// EnqueuedHeader is the message header holding the Unix time, in nanoseconds, at which the
// publisher sent the message. The AMQP timestamp property only has a resolution of a second.
const EnqueuedHeader = "enqueued_at_ns"

// QueueName is the queue for a channel: the default exchange routes by this name.
func QueueName(ch domain.Channel) string { return string(ch) }

// declareQueues declares one durable queue per channel. Declaring is idempotent, so both
// the publisher and the consumer do it whenever they open a channel.
func declareQueues(ch *amqp.Channel) error {
	for _, c := range domain.Channels() {
		if _, err := ch.QueueDeclare(QueueName(c), true, false, false, false, nil); err != nil {
			return fmt.Errorf("declare queue %s: %w", c, err)
		}
	}
	return nil
}
