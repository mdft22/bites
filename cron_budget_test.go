package bites

import (
	"context"
	"testing"
	"time"
)

func TestARouteBesideACronKeepsTheRequestAllocationsAtTwo(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector changes allocation counts")
	}
	c := memRig(t, func(a *App) {
		a.Service(Cron("nightly", "30 2 * * *", time.UTC, func(context.Context) error { return nil }, CronOptions{Lock: NewMemoryJobLock()}))
		a.Post("/users/{id}", budgetHandler)
	})
	got := keepAlive(t, c, post("/users/42?expand=true&page=3", typedBody), "HTTP/1.1 200 OK")
	t.Logf("allocations per request, typed JSON POST beside a Cron: %v", got)
	if got != 2 {
		t.Fatalf("allocs/request = %v, want exactly 2 (ADR 0092: a Cron adds nothing to the request path)", got)
	}
}
