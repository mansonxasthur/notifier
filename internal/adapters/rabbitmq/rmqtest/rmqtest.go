// Package rmqtest starts a throwaway RabbitMQ broker for integration tests.
package rmqtest

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Broker is a running RabbitMQ container.
type Broker struct {
	// URL is the AMQP connection string. It stays valid across Restart.
	URL string
	ctr testcontainers.Container
}

// Start runs a broker on a fixed host port, so a Restart keeps the same URL. It skips the
// test under -short and removes the container when the test ends.
func Start(t *testing.T) *Broker {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()

	port := freePort(t)
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "rabbitmq:4-alpine",
			ExposedPorts: []string{"5672/tcp"},
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.PortBindings = network.PortMap{
					network.MustParsePort("5672/tcp"): {{HostPort: strconv.Itoa(port)}},
				}
			},
			Env: map[string]string{
				"RABBITMQ_DEFAULT_USER": "notifier",
				"RABBITMQ_DEFAULT_PASS": "notifier",
			},
			WaitingFor: wait.ForLog("Server startup complete").WithStartupTimeout(90 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start rabbitmq: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(ctr) })

	return &Broker{URL: fmt.Sprintf("amqp://notifier:notifier@127.0.0.1:%d/", port), ctr: ctr}
}

// Restart stops the broker and starts it again with its data, as an operator would.
func (b *Broker) Restart(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	timeout := 30 * time.Second
	if err := b.ctr.Stop(ctx, &timeout); err != nil {
		t.Fatalf("stop rabbitmq: %v", err)
	}
	if err := b.ctr.Start(ctx); err != nil {
		t.Fatalf("restart rabbitmq: %v", err)
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	var lc net.ListenConfig
	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}
