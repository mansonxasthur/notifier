package provider

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/mansonxasthur/notifier/internal/config"
	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

var _ ports.ProviderChecker = (*Registry)(nil)

// Registry maps each channel to its providers, in configured try order, and answers
// status lookups by provider name.
type Registry struct {
	byChannel map[domain.Channel][]ports.Provider
	byName    map[string]*HTTP
}

// NewRegistry builds one HTTP provider per configured provider. When cfg.Simulation is
// set, every provider is served by the simulated provider service under its own name;
// otherwise it is called at its own URL. A nil client uses one sized for many concurrent
// calls to a few hosts that does not follow redirects, so a misconfigured URL fails loudly.
func NewRegistry(cfg *config.Config, client *http.Client) *Registry {
	if client == nil {
		client = defaultClient()
	}
	r := &Registry{
		byChannel: make(map[domain.Channel][]ports.Provider, len(cfg.Channels)),
		byName:    map[string]*HTTP{},
	}
	for ch, cc := range cfg.Channels {
		for _, p := range cc.Providers {
			h := &HTTP{
				name:         p.Name,
				base:         baseURL(cfg, p),
				timeout:      p.Timeout,
				statusLookup: p.StatusLookup,
				client:       client,
			}
			r.byChannel[ch] = append(r.byChannel[ch], h)
			r.byName[p.Name] = h
		}
	}
	return r
}

// idleConnsPerHost keeps connections to a provider warm across concurrent sends.
const idleConnsPerHost = 256

func defaultClient() *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = idleConnsPerHost
	return &http.Client{
		Transport:     tr,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

func baseURL(cfg *config.Config, p config.Provider) string {
	if cfg.Simulation {
		return strings.TrimRight(cfg.SimulationURL, "/") + "/providers/" + p.Name
	}
	return strings.TrimRight(p.URL, "/")
}

// Providers returns the channel's providers in try order: the primary first.
func (r *Registry) Providers(ch domain.Channel) []ports.Provider { return r.byChannel[ch] }

// Provider returns the provider with the given name.
func (r *Registry) Provider(name string) (ports.Provider, bool) {
	h, ok := r.byName[name]
	if !ok {
		return nil, false
	}
	return h, true
}

// Check implements ports.ProviderChecker.
func (r *Registry) Check(ctx context.Context, provider string, deliveryID int64) (ports.CheckResult, error) {
	h, ok := r.byName[provider]
	if !ok {
		return ports.CheckUnknown, fmt.Errorf("unknown provider %q", provider)
	}
	return h.Lookup(ctx, deliveryID)
}
