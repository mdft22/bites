package bitetest

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nevindra/bites"
)

// TestJobLock checks a bites.JobLock against the contract of its doc comment, one subtest a
// promise: a key is claimed by exactly one caller, a refusal is (false, nil), a claim holds for
// the whole ttl and ends after it, keys never share a claim, any text is a key, a done ctx claims
// nothing, and Claim is atomic for a key under concurrent callers. newLock is called once for each
// subtest and must return a lock with no state of any earlier one (a new table, a new key
// prefix); it may call t.Cleanup. The suite judges ttl on the app's clock, as bites.Now(ctx)
// reads it: each subtest hands Claim a context that carries a bitetest.Clock and moves it with
// Advance, so it never sleeps, and a store that reads its own clock fails at once. A nil newLock
// fails the test.
func TestJobLock(t *testing.T, newLock func(t *testing.T) bites.JobLock) {
	t.Helper()
	if newLock == nil {
		t.Fatal("bitetest.TestJobLock: newLock is nil: give a function that returns a new bites.JobLock for each subtest")
	}
	const ttl = time.Minute
	claim := func(t *testing.T, ctx context.Context, l bites.JobLock, key string) bool {
		t.Helper()
		ok, err := l.Claim(ctx, key, ttl)
		if err != nil {
			t.Fatalf("Claim(%q) returned the error %v", key, err)
		}
		return ok
	}
	run := func(name string, f func(t *testing.T, ctx context.Context, clk *Clock, l bites.JobLock)) {
		t.Run(name, func(t *testing.T) {
			ctx, clk := clockContext(t)
			f(t, ctx, clk, newLock(t))
		})
	}
	run("a key nobody holds is claimed", func(t *testing.T, ctx context.Context, _ *Clock, l bites.JobLock) {
		if !claim(t, ctx, l, "nightly@1") {
			t.Fatal("Claim of a key nobody holds answered false; want true")
		}
	})
	run("a key that is held is refused with false and no error", func(t *testing.T, ctx context.Context, _ *Clock, l bites.JobLock) {
		claim(t, ctx, l, "nightly@1")
		ok, err := l.Claim(ctx, "nightly@1", ttl)
		if err != nil || ok {
			t.Fatalf("second Claim of a held key = (%v, %v); want (false, nil): another instance holding it is not an error", ok, err)
		}
	})
	run("a claim holds until its ttl has passed and no longer", func(t *testing.T, ctx context.Context, clk *Clock, l bites.JobLock) {
		claim(t, ctx, l, "nightly@1")
		clk.Advance(ttl - time.Millisecond)
		if claim(t, ctx, l, "nightly@1") {
			t.Fatal("Claim answered true a millisecond before the ttl ended; the claim must hold for the whole ttl, judged on bites.Now(ctx)")
		}
		clk.Advance(time.Millisecond)
		if !claim(t, ctx, l, "nightly@1") {
			t.Fatal("Claim answered false when the ttl had passed; the key must be free again, judged on bites.Now(ctx)")
		}
	})
	run("a claim is kept for the ttl and is not released by anything", func(t *testing.T, ctx context.Context, clk *Clock, l bites.JobLock) {
		claim(t, ctx, l, "nightly@1")
		clk.Advance(ttl / 2)
		if claim(t, ctx, l, "nightly@1") {
			t.Fatal("Claim answered true half way through the ttl; a claim is not a lease that ends with the run, so it holds for the ttl")
		}
	})
	run("each claim has the ttl it was made with", func(t *testing.T, ctx context.Context, clk *Clock, l bites.JobLock) {
		if ok, err := l.Claim(ctx, "short", time.Second); err != nil || !ok {
			t.Fatalf("Claim(short) = (%v, %v); want (true, nil)", ok, err)
		}
		if ok, err := l.Claim(ctx, "long", time.Hour); err != nil || !ok {
			t.Fatalf("Claim(long) = (%v, %v); want (true, nil)", ok, err)
		}
		clk.Advance(2 * time.Second)
		if !claim(t, ctx, l, "short") {
			t.Fatal("a key claimed for a second was still held after two")
		}
		if claim(t, ctx, l, "long") {
			t.Fatal("a key claimed for an hour was free after two seconds")
		}
	})
	run("different keys never share a claim", func(t *testing.T, ctx context.Context, _ *Clock, l bites.JobLock) {
		for _, k := range []string{"nightly@1", "nightly@2", "weekly@1", "Nightly@1", "nightly@1 "} {
			if !claim(t, ctx, l, k) {
				t.Fatalf("Claim(%q) answered false though no other claim has that key; keys are compared whole, byte for byte", k)
			}
		}
	})
	run("any text is a key", func(t *testing.T, ctx context.Context, _ *Clock, l bites.JobLock) {
		for _, k := range []string{"café@1", "a:b/c%d'e\"f@1", "日本語@1", strings.Repeat("k", 200) + "@1"} {
			if !claim(t, ctx, l, k) {
				t.Fatalf("Claim(%q) answered false for a key nobody holds", k)
			}
			if claim(t, ctx, l, k) {
				t.Fatalf("second Claim(%q) answered true while it is held", k)
			}
		}
	})
	run("a done context claims nothing and returns its error", func(t *testing.T, ctx context.Context, _ *Clock, l bites.JobLock) {
		done, cancel := context.WithCancel(ctx)
		cancel()
		ok, err := l.Claim(done, "nightly@1", ttl)
		if ok || err == nil {
			t.Fatalf("Claim with a done ctx = (%v, %v); want (false, error)", ok, err)
		}
		if !claim(t, ctx, l, "nightly@1") {
			t.Fatal("a Claim with a done ctx held the key: it must claim nothing")
		}
	})
	run("two callers never both claim one key", func(t *testing.T, ctx context.Context, _ *Clock, l bites.JobLock) {
		const rounds, callers = 60, 8
		for r := range rounds {
			key := fmt.Sprintf("race@%d", r)
			var (
				wg    sync.WaitGroup
				mu    sync.Mutex
				wins  int
				start = make(chan struct{})
			)
			for range callers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					ok, err := l.Claim(ctx, key, ttl)
					if err != nil {
						t.Errorf("Claim(%q) returned the error %v", key, err)
						return
					}
					if ok {
						mu.Lock()
						wins++
						mu.Unlock()
					}
				}()
			}
			close(start)
			wg.Wait()
			if wins != 1 {
				t.Fatalf("round %d: %d of %d callers claimed %q at once; want exactly 1", r, wins, callers, key)
			}
		}
	})
}
