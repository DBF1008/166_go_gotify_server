package model

import (
	"testing"
	"time"
)

func TestClientIsElevatedAt(t *testing.T) {
	base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	future := base.Add(time.Hour)
	past := base.Add(-time.Hour)

	cases := []struct {
		name          string
		elevatedUntil *time.Time
		now           time.Time
		want          bool
	}{
		{"nil is never elevated", nil, base, false},
		{"before expiry is elevated", &future, base, true},
		{"after expiry is not elevated", &past, base, false},
		{"exactly at expiry is not elevated", &base, base, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &Client{ElevatedUntil: tc.elevatedUntil}
			if got := c.IsElevatedAt(tc.now); got != tc.want {
				t.Errorf("IsElevatedAt(%v) with ElevatedUntil=%v = %v, want %v", tc.now, tc.elevatedUntil, got, tc.want)
			}
		})
	}
}
