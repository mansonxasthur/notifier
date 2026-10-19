package loadtest

import (
	"os"
	"testing"
)

func TestCountDuplicates(t *testing.T) {
	before := []ledgerEntry{
		{DeliveryID: 1, Provider: "sms-a", Accepted: true, Sends: 1},
		{DeliveryID: 2, Provider: "sms-a", Accepted: false, Sends: 3}, // retried, never held: no duplicate
		{DeliveryID: 3, Provider: "sms-a", Accepted: true, Sends: 1},  // held, then the sender failed over
	}
	after := []ledgerEntry{
		{DeliveryID: 1, Provider: "sms-a", Accepted: true, Sends: 1}, // the same delivery again after a simulator restart
		{DeliveryID: 3, Provider: "sms-b", Accepted: true, Sends: 1},
		{DeliveryID: 4, Provider: "sms-a", Accepted: true, Sends: 1},
	}
	got := CountDuplicates([][]ledgerEntry{before, after})
	want := Duplicates{Deliveries: 4, HeldTwice: 1, DuplicateSends: 1}
	if got != want {
		t.Errorf("duplicates = %+v, want %+v", got, want)
	}
}

func TestRecovery(t *testing.T) {
	trace := []point{
		{ms: 1000, depth: 0},
		{ms: 2000, depth: 90, max: 2, open: 1},
		{ms: 3000, depth: 40, max: 2, open: 1}, // still backed up with a breaker open
		{ms: 4000, depth: 4, max: 1},           // a half-open breaker counts as recovered
		{ms: 5000, depth: 0},
	}
	if got := Recovery(trace, 2000, 5); got != 4000 {
		t.Errorf("recovered at %d, want 4000", got)
	}
	if got := Recovery(trace, 2000, 0); got != 5000 {
		t.Errorf("with a limit of 0, recovered at %d, want 5000", got)
	}
	if got := Recovery(trace[:3], 2000, 5); got != -1 {
		t.Errorf("recovered at %d, want -1", got)
	}
}

func TestReadTraceCountsAProviderOpenIfAnySampleInTheSecondSaysSo(t *testing.T) {
	path := t.TempDir() + "/samples.csv"
	csv := "unix_ms,target,series,value\n" +
		"1000,sender,notifier_breaker_state{provider=sms-a},1\n" +
		"1500,sender,notifier_breaker_state{provider=sms-a},2\n" +
		"junk,sender,notifier_breaker_state{provider=sms-b},2\n"
	if err := os.WriteFile(path, []byte(csv), 0o600); err != nil {
		t.Fatal(err)
	}
	trace, err := readTrace(path)
	if err != nil || len(trace) != 1 || trace[0].open != 1 || len(trace[0].which) != 1 {
		t.Fatalf("trace = %+v, %v; want one second with one open breaker", trace, err)
	}
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readTrace(path); err == nil {
		t.Error("an empty file was read without error")
	}
}
