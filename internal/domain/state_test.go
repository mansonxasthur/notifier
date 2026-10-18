package domain

import "testing"

func TestDeliveryMoves(t *testing.T) {
	all := []DeliveryState{DeliveryPending, DeliveryAccepted, DeliveryDelivered, DeliveryFailed, DeliveryUnresolved}
	allowed := map[[2]DeliveryState]bool{
		{DeliveryPending, DeliveryAccepted}:    true,
		{DeliveryPending, DeliveryDelivered}:   true, // an early webhook
		{DeliveryPending, DeliveryFailed}:      true,
		{DeliveryPending, DeliveryUnresolved}:  true, // the last provider timed out
		{DeliveryAccepted, DeliveryDelivered}:  true,
		{DeliveryAccepted, DeliveryFailed}:     true, // a late failure webhook
		{DeliveryAccepted, DeliveryUnresolved}: true,
	}
	for _, from := range all {
		for _, to := range all {
			if got := from.CanMoveTo(to); got != allowed[[2]DeliveryState{from, to}] {
				t.Errorf("%s -> %s = %v, want %v", from, to, got, !got)
			}
		}
	}
}

func TestTerminalStatesStayTerminal(t *testing.T) {
	for _, s := range []DeliveryState{DeliveryDelivered, DeliveryFailed, DeliveryUnresolved} {
		if !s.Terminal() {
			t.Errorf("%s should be terminal", s)
		}
	}
	for _, s := range []DeliveryState{DeliveryPending, DeliveryAccepted} {
		if s.Terminal() {
			t.Errorf("%s should not be terminal", s)
		}
	}
}

func TestMovesInto(t *testing.T) {
	tests := []struct {
		to   DeliveryState
		want []DeliveryState
	}{
		{DeliveryAccepted, []DeliveryState{DeliveryPending}},
		{DeliveryDelivered, []DeliveryState{DeliveryPending, DeliveryAccepted}},
		{DeliveryFailed, []DeliveryState{DeliveryPending, DeliveryAccepted}},
		{DeliveryPending, nil},
	}
	for _, tt := range tests {
		got := MovesInto(tt.to)
		if len(got) != len(tt.want) {
			t.Errorf("MovesInto(%s) = %v, want %v", tt.to, got, tt.want)
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("MovesInto(%s) = %v, want %v", tt.to, got, tt.want)
			}
		}
	}
}
