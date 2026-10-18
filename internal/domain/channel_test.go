package domain

import "testing"

func TestChannelValid(t *testing.T) {
	for _, c := range Channels() {
		if !c.Valid() {
			t.Errorf("%q should be valid", c)
		}
	}
	if Channel("push").Valid() {
		t.Error("unknown channel should be invalid")
	}
}
