package bites

import (
	"context"
	"testing"
	"time"
)

// forgetful is an IdempotencyStore that claims every key and keeps nothing, so a request sent
// again on one connection runs the whole claim, run and keep path each time.
type forgetful struct{}

func (forgetful) Claim(context.Context, string, string, time.Time, time.Duration) (IdempotencyEntry, error) {
	return IdempotencyEntry{State: IdempotencyClaimed}, nil
}
func (forgetful) Complete(context.Context, string, string, IdempotencyAnswer, time.Time, time.Duration) error {
	return nil
}
func (forgetful) Release(context.Context, string) error { return nil }

func TestATypedJSONPOSTBehindIdempotencyCostsWhatTheTableOfADR0089Says(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector changes allocation counts")
	}
	const key = "Idempotency-Key: 9f1c2a3b-4d5e-4f60-8a7b-0c1d2e3f4a5b\r\n"
	mw := func(o IdempotencyOptions) Middleware { return NewIdempotency(o).Middleware() }
	for _, tc := range []struct {
		name, extra string
		mw          Middleware
		want        float64
	}{
		{"no middleware, the route of ADR 0003", "", nil, 2},
		{"Idempotency, a request with no key", "", mw(IdempotencyOptions{}), 2},
		{"Idempotency, a key whose answer is kept: the request sent again, which is a replay", key, mw(IdempotencyOptions{}), 22},
		{"Idempotency, a key claimed and kept every time (the first request of each key)", key, mw(IdempotencyOptions{Store: forgetful{}}), 24},
	} {
		c := memRigWith(t, nil, func(a *App) {
			if tc.mw == nil {
				a.Post("/users/{id}", budgetHandler)
				return
			}
			a.Post("/users/{id}", budgetHandler, Use(tc.mw))
		})
		got := keepAlive(t, c, postWith("/users/42?expand=true&page=3", tc.extra, typedBody), "HTTP/1.1 200 OK")
		t.Logf("%s: %v allocations per request", tc.name, got)
		if got > tc.want || (tc.want == 2 && got != 2) {
			t.Errorf("%s: allocs/request = %v, want 2 for a request with no key and at most %v otherwise", tc.name, got, tc.want)
		}
	}
}
