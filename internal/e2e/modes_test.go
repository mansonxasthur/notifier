package e2e

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/mansonxasthur/notifier/internal/config"
	"github.com/mansonxasthur/notifier/internal/domain"
)

// modeRecipients keeps each pinned run small: it starts a whole stack per scenario.
const modeRecipients = 5

// Each scenario, pinned at both providers, takes every delivery to the one end the design
// gives it. TestRequestToFinalStateThroughTheSimulator mixes the scenarios and checks them
// against an oracle; here each one stands alone, so a failure names the mode.
func TestEachScenarioAlone(t *testing.T) {
	want := map[domain.Scenario]row{
		domain.ScenarioSucceed: {state: "DELIVERED", inbox: "DONE"},
		domain.ScenarioSlow:    {state: "DELIVERED", inbox: "DONE"},
		// A late failure webhook turns an accepted delivery into a bounce.
		domain.ScenarioLateFailure: {state: "FAILED", reason: "BOUNCED", inbox: "DONE"},
		// A permanent rejection is the message's fault: no retry, no second provider.
		domain.ScenarioFailPermanent: {state: "FAILED", reason: "PROVIDER_REJECTED", inbox: "DEAD", class: "message"},
		// Both providers fail transiently: the last one ends the delivery.
		domain.ScenarioFailTransient: {state: "FAILED", reason: "PROVIDER_FAILED", inbox: "DEAD", class: "provider"},
	}
	for _, sc := range domain.Scenarios() {
		r, ok := want[sc]
		if sc == domain.ScenarioTimeout {
			continue // TestTimeoutAlone
		}
		if !ok {
			t.Fatalf("scenario %s has no expected end in this test", sc)
		}
		t.Run(string(sc), func(t *testing.T) {
			s := newStack(t, func(c *config.Config) { c.SimProvider.Mode = string(sc) })
			n := s.submitN(t, modeRecipients)
			s.waitRows(t, n, func(got row) bool { return got == r })
			for id, got := range s.rows(t) {
				if got != r {
					t.Errorf("delivery %d ended %+v, want %+v", id, got, r)
				}
			}
		})
	}
}

// A timeout leaves the call ambiguous. Where the provider answers status lookups it settles
// as delivered; where it does not, the delivery goes on to the second provider, whose own
// timeout leaves it UNRESOLVED until the late webhook delivers it.
func TestTimeoutAlone(t *testing.T) {
	s := newStack(t, func(c *config.Config) { c.SimProvider.Mode = string(domain.ScenarioTimeout) })
	n := s.submitN(t, modeRecipients)
	// Every primary provider answers status lookups, so it confirms it holds the message:
	// the delivery is settled there, without trying the second provider.
	want := row{state: "DELIVERED", inbox: "DONE"}
	s.waitRows(t, n, func(got row) bool { return got == want })
	for id, got := range s.rows(t) {
		if got != want {
			t.Errorf("delivery %d ended %+v, want %+v", id, got, want)
		}
	}
	for _, e := range s.entries(t) {
		if !strings.HasSuffix(e.Provider, "-a") {
			t.Errorf("delivery %d reached %s: a confirmed timeout must not fail over", e.DeliveryID, e.Provider)
		}
	}
}

// The text goes from the request to the provider as the same UTF-8 bytes.
func TestArabicReachesTheProviderUnchanged(t *testing.T) {
	s := newStack(t, func(c *config.Config) { c.SimProvider.Mode = string(domain.ScenarioSucceed) })
	n := s.submitN(t, modeRecipients)
	s.waitRows(t, n, func(got row) bool { return got.state == "DELIVERED" })

	const text, team = "مباراة الهلال والنصر", "الهلال"
	sms, email := 0, 0
	for _, b := range s.sent.all() {
		var p struct {
			Channel domain.Channel `json:"channel"`
			Content struct {
				Text      string            `json:"text"`
				Variables map[string]string `json:"variables"`
			} `json:"content"`
		}
		if err := json.Unmarshal([]byte(b), &p); err != nil {
			t.Fatalf("provider body %q: %v", b, err)
		}
		switch p.Channel {
		case domain.ChannelSMS:
			sms++
			if p.Content.Text != text {
				t.Errorf("sms text = %q, want %q", p.Content.Text, text)
			}
		case domain.ChannelEmail:
			email++
			if !strings.Contains(b, team) {
				t.Errorf("body %q does not carry the variable as UTF-8", b)
			}
			if p.Content.Variables["team"] != team {
				t.Errorf("email variables = %v, want team %q", p.Content.Variables, team)
			}
		}
		// Raw, not escaped: a \uXXXX form would still decode, but the bytes must be UTF-8.
		if p.Channel == domain.ChannelSMS && !strings.Contains(b, text) {
			t.Errorf("body %q does not carry the text as UTF-8", b)
		}
	}
	// At least one call each; a retry would add more, and this mode never fails.
	if sms < modeRecipients || email < modeRecipients {
		t.Errorf("provider received %d sms and %d email calls, want at least %d of each", sms, email, modeRecipients)
	}
}

// submitN is submit with n recipients.
func (s *stack) submitN(t *testing.T, n int) int {
	t.Helper()
	status, _, resp := s.postN(t, defaultKey, n)
	if status != http.StatusAccepted {
		t.Fatalf("submit: %d %s", status, resp)
	}
	var out struct {
		Accepted int `json:"accepted"`
	}
	if err := json.Unmarshal(resp, &out); err != nil {
		t.Fatal(err)
	}
	if out.Accepted != n*2 {
		t.Fatalf("accepted = %d, want %d", out.Accepted, n*2)
	}
	return out.Accepted
}

// waitRows waits until n deliveries exist and ok holds for all of them.
func (s *stack) waitRows(t *testing.T, n int, ok func(row) bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		rows := s.rows(t)
		done := len(rows) == n
		for _, r := range rows {
			done = done && ok(r)
		}
		if done {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("not settled after 60s: %+v", s.rows(t))
}
