package logging

import (
	"bytes"
	"strings"
	"testing"
)

func TestNewTagsService(t *testing.T) {
	var buf bytes.Buffer
	log, err := New(&buf, "relay", "info", "json")
	if err != nil {
		t.Fatal(err)
	}
	log.Info("hello")
	if got := buf.String(); !strings.Contains(got, `"service":"relay"`) {
		t.Errorf("missing service attribute: %s", got)
	}
}

func TestNewLevelFilters(t *testing.T) {
	var buf bytes.Buffer
	log, err := New(&buf, "api", "warn", "text")
	if err != nil {
		t.Fatal(err)
	}
	log.Info("quiet")
	if buf.Len() != 0 {
		t.Errorf("info should be filtered at warn level: %s", buf.String())
	}
}

func TestNewRejectsBadInput(t *testing.T) {
	if _, err := New(&bytes.Buffer{}, "api", "loud", "json"); err == nil {
		t.Error("bad level should fail")
	}
	if _, err := New(&bytes.Buffer{}, "api", "info", "xml"); err == nil {
		t.Error("bad format should fail")
	}
}
