package provider_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mansonxasthur/notifier/internal/adapters/provider"
	"github.com/mansonxasthur/notifier/internal/config"
	"github.com/mansonxasthur/notifier/internal/domain"
	"github.com/mansonxasthur/notifier/internal/ports"
)

// registryFor serves every provider from handler, in simulation mode like the real stack.
func registryFor(t *testing.T, handler http.Handler, timeout time.Duration, lookup bool) *provider.Registry {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	p := func(name string, primary bool) config.Provider {
		return config.Provider{Name: name, Primary: primary, Timeout: timeout, StatusLookup: lookup}
	}
	cfg := &config.Config{
		Simulation:    true,
		SimulationURL: srv.URL + "/",
		Channels: map[domain.Channel]config.Channel{
			domain.ChannelSMS: {Providers: []config.Provider{p("sms-a", true), p("sms-b", false)}},
		},
	}
	return provider.NewRegistry(cfg, nil)
}

func smsProvider(t *testing.T, r *provider.Registry) ports.Provider {
	t.Helper()
	p, ok := r.Provider("sms-a")
	if !ok {
		t.Fatal("sms-a not registered")
	}
	return p
}

func respond(code int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) })
}

func TestRegistryOrdersProvidersAndRoutesByName(t *testing.T) {
	var path string
	r := registryFor(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		path = req.URL.Path
		w.WriteHeader(http.StatusAccepted)
	}), time.Second, true)

	got := r.Providers(domain.ChannelSMS)
	if len(got) != 2 || got[0].Name() != "sms-a" || got[1].Name() != "sms-b" {
		t.Fatalf("providers = %v, want sms-a then sms-b", got)
	}
	if err := got[1].Send(context.Background(), ports.SendRequest{DeliveryID: 1, Channel: domain.ChannelSMS}); err != nil {
		t.Fatal(err)
	}
	if path != "/providers/sms-b/send" {
		t.Errorf("path = %q, want /providers/sms-b/send", path)
	}
	if _, ok := r.Provider("nope"); ok {
		t.Error("unknown provider should not be found")
	}
}

func TestSendClassifiesFailures(t *testing.T) {
	tests := []struct {
		name string
		code int
		want domain.FailureClass // empty: success
	}{
		{"accepted", http.StatusAccepted, ""},
		{"ok", http.StatusOK, ""},
		{"bad request is message level", http.StatusBadRequest, domain.FailureMessage},
		{"unprocessable is message level", http.StatusUnprocessableEntity, domain.FailureMessage},
		{"unauthorized is provider side", http.StatusUnauthorized, domain.FailureProvider},
		{"forbidden is provider side", http.StatusForbidden, domain.FailureProvider},
		{"not found is provider side", http.StatusNotFound, domain.FailureProvider},
		{"throttled is provider side", http.StatusTooManyRequests, domain.FailureProvider},
		{"request timeout is provider side", http.StatusRequestTimeout, domain.FailureProvider},
		{"server error is provider side", http.StatusServiceUnavailable, domain.FailureProvider},
		{"redirect is provider side", http.StatusFound, domain.FailureProvider},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := smsProvider(t, registryFor(t, respond(tt.code), time.Second, false)).
				Send(context.Background(), ports.SendRequest{DeliveryID: 1, Channel: domain.ChannelSMS})
			class, classified := ports.ClassOf(err)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("err = %v, want nil", err)
				}
				return
			}
			if !classified || class != tt.want {
				t.Errorf("class = %q (classified %v), err %v; want %q", class, classified, err, tt.want)
			}
		})
	}
}

func TestSendTimeoutAndRefusal(t *testing.T) {
	slow := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		select {
		case <-req.Context().Done():
		case <-time.After(2 * time.Second):
		}
	})
	err := smsProvider(t, registryFor(t, slow, 50*time.Millisecond, false)).
		Send(context.Background(), ports.SendRequest{DeliveryID: 1, Channel: domain.ChannelSMS})
	if class, _ := ports.ClassOf(err); class != domain.FailureTimeout {
		t.Errorf("slow provider: class = %q, err %v; want timeout", class, err)
	}

	// A closed server refuses the connection: the provider is down, not slow.
	srv := httptest.NewServer(respond(http.StatusOK))
	srv.Close()
	cfg := &config.Config{Simulation: true, SimulationURL: srv.URL, Channels: map[domain.Channel]config.Channel{
		domain.ChannelSMS: {Providers: []config.Provider{{Name: "sms-a", Primary: true, Timeout: time.Second}}},
	}}
	err = smsProvider(t, provider.NewRegistry(cfg, nil)).
		Send(context.Background(), ports.SendRequest{DeliveryID: 1, Channel: domain.ChannelSMS})
	if class, _ := ports.ClassOf(err); class != domain.FailureProvider {
		t.Errorf("refused connection: class = %q, err %v; want provider", class, err)
	}
}

func TestSendCallerCancellationIsNotAProviderFailure(t *testing.T) {
	release := make(chan struct{})
	r := registryFor(t, http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		_, _ = io.Copy(io.Discard, req.Body) // lets the server notice the client hanging up
		select {
		case <-req.Context().Done():
		case <-release:
		}
	}), time.Second, false)
	t.Cleanup(func() { close(release) }) // runs before the server closes

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	err := smsProvider(t, r).Send(ctx, ports.SendRequest{DeliveryID: 1, Channel: domain.ChannelSMS})
	if err == nil {
		t.Fatal("want an error")
	}
	if _, classified := ports.ClassOf(err); classified {
		t.Errorf("a cancelled caller must not be classified as a provider failure: %v", err)
	}
}

func TestSendCallerDeadlineIsATimeout(t *testing.T) {
	r := registryFor(t, http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		_, _ = io.Copy(io.Discard, req.Body)
		<-req.Context().Done()
	}), 5*time.Second, false)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := smsProvider(t, r).Send(ctx, ports.SendRequest{DeliveryID: 1, Channel: domain.ChannelSMS})
	if class, _ := ports.ClassOf(err); class != domain.FailureTimeout {
		t.Errorf("class = %q, err %v; want timeout: the outcome is unknown", class, err)
	}
}

func TestSendDoesNotFollowRedirects(t *testing.T) {
	var hits int
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(target.Close)
	r := registryFor(t, http.RedirectHandler(target.URL, http.StatusTemporaryRedirect), time.Second, false)

	err := smsProvider(t, r).Send(context.Background(), ports.SendRequest{DeliveryID: 1, Channel: domain.ChannelSMS})
	if class, _ := ports.ClassOf(err); class != domain.FailureProvider || hits != 0 {
		t.Errorf("redirect: class = %q, target hits = %d, err %v; want a provider failure and no follow", class, hits, err)
	}
}

func TestSendBodyCarriesReferenceKeyAndUTF8(t *testing.T) {
	var got provider.SendPayload
	var contentType string
	r := registryFor(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		contentType = req.Header.Get("Content-Type")
		_ = json.NewDecoder(req.Body).Decode(&got)
		w.WriteHeader(http.StatusAccepted)
	}), time.Second, false)

	req := ports.SendRequest{
		DeliveryID: 42, Channel: domain.ChannelWhatsApp, Contact: "+201000000000",
		Content: domain.Content{TemplateID: "welcome", Variables: map[string]string{"name": "مرحبا"}},
	}
	if err := smsProvider(t, r).Send(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if got.DeliveryID != 42 || got.Channel != domain.ChannelWhatsApp || got.Contact != "+201000000000" {
		t.Errorf("payload = %+v", got)
	}
	if got.Content.TemplateID != "welcome" || got.Content.Variables["name"] != "مرحبا" {
		t.Errorf("template content not forwarded intact: %+v", got.Content)
	}
	if !strings.Contains(contentType, "utf-8") {
		t.Errorf("content type = %q, want utf-8", contentType)
	}
}

func TestCheck(t *testing.T) {
	tests := []struct {
		name    string
		code    int
		lookup  bool
		want    ports.CheckResult
		wantErr bool
	}{
		{"has it", http.StatusOK, true, ports.CheckAccepted, false},
		{"not found", http.StatusNotFound, true, ports.CheckNotFound, false},
		{"lookup disabled on the provider", http.StatusNotImplemented, true, ports.CheckUnknown, false},
		{"server error", http.StatusInternalServerError, true, ports.CheckUnknown, true},
		{"lookup not configured", http.StatusOK, false, ports.CheckUnknown, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var path string
			r := registryFor(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				path = req.URL.Path
				w.WriteHeader(tt.code)
			}), time.Second, tt.lookup)

			got, err := r.Check(context.Background(), "sms-a", 7)
			if got != tt.want || (err != nil) != tt.wantErr {
				t.Errorf("Check = %v, %v; want %v, error %v", got, err, tt.want, tt.wantErr)
			}
			if tt.lookup && path != "/providers/sms-a/status/7" {
				t.Errorf("path = %q", path)
			}
		})
	}
}

func TestCheckUnknownProvider(t *testing.T) {
	r := registryFor(t, respond(http.StatusOK), time.Second, true)
	if res, err := r.Check(context.Background(), "nope", 1); err == nil || res != ports.CheckUnknown {
		t.Errorf("Check = %v, %v; want unknown with an error", res, err)
	}
}

func TestRealModeUsesProviderURL(t *testing.T) {
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		path = req.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	cfg := &config.Config{Channels: map[domain.Channel]config.Channel{
		domain.ChannelSMS: {Providers: []config.Provider{{Name: "sms-a", Primary: true, URL: srv.URL + "/", Timeout: time.Second}}},
	}}
	if err := smsProvider(t, provider.NewRegistry(cfg, nil)).Send(context.Background(), ports.SendRequest{DeliveryID: 1}); err != nil {
		t.Fatal(err)
	}
	if path != "/send" {
		t.Errorf("path = %q, want /send", path)
	}
}
