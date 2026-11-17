package bitetest

import (
	"context"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nevindra/bites"
)

// TestIdempotencyStore checks a bites.IdempotencyStore against the contract of its doc comment, one
// subtest a promise: Claim of a free key is Claimed, and of a key another request holds is Running
// with that request's fingerprint and claims nothing; a claim lapses after inFlight; Complete keeps
// an answer that a later Claim returns as Done, equal field for field and with the first
// request's fingerprint, until its ttl passes; Complete keeps the answer of a request whose claim
// lapsed or was released; Release frees a claim and nothing else; keys never share an entry; and
// Claim is atomic for a key under concurrent callers. newStore is called once for each subtest and
// must return a store with no state of any earlier one (a new table, a new key prefix); it may call
// t.Cleanup. The suite gives every call its own times, a fixed base moved forward by the test, and
// never sleeps, so a store must decide with the now it is given and not with a clock of its own. A
// nil newStore fails the test.
func TestIdempotencyStore(t *testing.T, newStore func(t *testing.T) bites.IdempotencyStore) {
	t.Helper()
	if newStore == nil {
		t.Fatal("bitetest.TestIdempotencyStore: newStore is nil: give a function that returns a new bites.IdempotencyStore for each subtest")
	}
	const sec = time.Second
	base := time.Unix(1_700_000_000, 0)
	ctx := context.Background()
	claim := func(t *testing.T, s bites.IdempotencyStore, key, fp string, now time.Time, inFlight time.Duration) bites.IdempotencyEntry {
		t.Helper()
		e, err := s.Claim(ctx, key, fp, now, inFlight)
		if err != nil {
			t.Fatalf("Claim(%q) returned the error %v", key, err)
		}
		return e
	}
	complete := func(t *testing.T, s bites.IdempotencyStore, key, fp string, a bites.IdempotencyAnswer, now time.Time, ttl time.Duration) {
		t.Helper()
		if err := s.Complete(ctx, key, fp, a, now, ttl); err != nil {
			t.Fatalf("Complete(%q) returned the error %v", key, err)
		}
	}
	release := func(t *testing.T, s bites.IdempotencyStore, key string) {
		t.Helper()
		if err := s.Release(ctx, key); err != nil {
			t.Fatalf("Release(%q) returned the error %v", key, err)
		}
	}
	want := func(t *testing.T, e bites.IdempotencyEntry, state bites.IdempotencyState, fp string) {
		t.Helper()
		if e.State != state || e.Fingerprint != fp {
			t.Fatalf("got State %d and Fingerprint %q, want State %d and Fingerprint %q", e.State, e.Fingerprint, state, fp)
		}
	}
	answer := bites.IdempotencyAnswer{
		Status:      201,
		ContentType: "application/json",
		Headers:     []bites.IdempotencyHeader{{Name: "Location", Value: "/payments/1"}, {Name: "Set-Cookie", Value: "a=1"}, {Name: "Set-Cookie", Value: "b=2"}, {Name: "X-Empty", Value: ""}},
		Body:        []byte(`{"id":1}`),
	}
	run := func(name string, f func(t *testing.T, s bites.IdempotencyStore)) {
		t.Run(name, func(t *testing.T) { f(t, newStore(t)) })
	}

	run("a key nobody holds is claimed and its fingerprint is empty", func(t *testing.T, s bites.IdempotencyStore) {
		want(t, claim(t, s, "k", "fp1", base, 10*sec), bites.IdempotencyClaimed, "")
	})
	run("a key another request holds is running with that request's fingerprint, whatever the caller's", func(t *testing.T, s bites.IdempotencyStore) {
		claim(t, s, "k", "fp1", base, 10*sec)
		want(t, claim(t, s, "k", "fp2", base.Add(sec), 10*sec), bites.IdempotencyRunning, "fp1")
		want(t, claim(t, s, "k", "fp1", base.Add(2*sec), 10*sec), bites.IdempotencyRunning, "fp1")
	})
	run("a claim that was refused takes nothing, so the first claim still lapses on its own time", func(t *testing.T, s bites.IdempotencyStore) {
		claim(t, s, "k", "fp1", base, 10*sec)
		claim(t, s, "k", "fp2", base.Add(9*sec), time.Hour)
		want(t, claim(t, s, "k", "fp3", base.Add(11*sec), 10*sec), bites.IdempotencyClaimed, "")
	})
	run("a claim lapses after inFlight and the key is claimed again", func(t *testing.T, s bites.IdempotencyStore) {
		claim(t, s, "k", "fp1", base, 10*sec)
		want(t, claim(t, s, "k", "fp2", base.Add(10*sec-time.Millisecond), 10*sec), bites.IdempotencyRunning, "fp1")
		want(t, claim(t, s, "k", "fp2", base.Add(10*sec+time.Millisecond), 10*sec), bites.IdempotencyClaimed, "")
	})
	run("a completed answer is returned as done with the first request's fingerprint, equal field for field", func(t *testing.T, s bites.IdempotencyStore) {
		claim(t, s, "k", "fp1", base, 10*sec)
		complete(t, s, "k", "fp1", answer, base.Add(sec), time.Hour)
		e := claim(t, s, "k", "fp2", base.Add(2*sec), 10*sec)
		want(t, e, bites.IdempotencyDone, "fp1")
		if !reflect.DeepEqual(e.Answer, answer) {
			t.Fatalf("got the answer %+v, want %+v", e.Answer, answer)
		}
	})
	run("an answer is returned again by every later claim until it expires", func(t *testing.T, s bites.IdempotencyStore) {
		claim(t, s, "k", "fp1", base, 10*sec)
		complete(t, s, "k", "fp1", answer, base, time.Hour)
		for i := range 3 {
			want(t, claim(t, s, "k", "fp1", base.Add(time.Duration(i+1)*sec), 10*sec), bites.IdempotencyDone, "fp1")
		}
	})
	run("an answer expires ttl after Complete and the key is claimed again", func(t *testing.T, s bites.IdempotencyStore) {
		claim(t, s, "k", "fp1", base, 10*sec)
		complete(t, s, "k", "fp1", answer, base.Add(5*sec), time.Hour)
		want(t, claim(t, s, "k", "fp1", base.Add(5*sec+time.Hour-time.Millisecond), 10*sec), bites.IdempotencyDone, "fp1")
		want(t, claim(t, s, "k", "fp2", base.Add(5*sec+time.Hour+time.Millisecond), 10*sec), bites.IdempotencyClaimed, "")
	})
	run("an answer that was too large to keep is kept as that fact and nothing more", func(t *testing.T, s bites.IdempotencyStore) {
		claim(t, s, "k", "fp1", base, 10*sec)
		complete(t, s, "k", "fp1", bites.IdempotencyAnswer{TooLarge: true}, base, time.Hour)
		e := claim(t, s, "k", "fp1", base.Add(sec), 10*sec)
		want(t, e, bites.IdempotencyDone, "fp1")
		if !e.Answer.TooLarge || e.Answer.Status != 0 || len(e.Answer.Body) != 0 {
			t.Fatalf("got the answer %+v, want one with TooLarge set and nothing else", e.Answer)
		}
	})
	run("Complete keeps an answer whose claim lapsed, so the work that was done is not lost", func(t *testing.T, s bites.IdempotencyStore) {
		claim(t, s, "k", "fp1", base, 10*sec)
		complete(t, s, "k", "fp1", answer, base.Add(time.Minute), time.Hour)
		want(t, claim(t, s, "k", "fp1", base.Add(time.Minute+sec), 10*sec), bites.IdempotencyDone, "fp1")
	})
	run("Complete keeps an answer for a key that was never claimed", func(t *testing.T, s bites.IdempotencyStore) {
		complete(t, s, "k", "fp1", answer, base, time.Hour)
		want(t, claim(t, s, "k", "fp1", base.Add(sec), 10*sec), bites.IdempotencyDone, "fp1")
	})
	run("Release frees a claim so the next claim of the key is claimed", func(t *testing.T, s bites.IdempotencyStore) {
		claim(t, s, "k", "fp1", base, time.Hour)
		release(t, s, "k")
		want(t, claim(t, s, "k", "fp2", base.Add(sec), 10*sec), bites.IdempotencyClaimed, "")
	})
	run("Release leaves a kept answer alone", func(t *testing.T, s bites.IdempotencyStore) {
		claim(t, s, "k", "fp1", base, 10*sec)
		complete(t, s, "k", "fp1", answer, base, time.Hour)
		release(t, s, "k")
		want(t, claim(t, s, "k", "fp1", base.Add(sec), 10*sec), bites.IdempotencyDone, "fp1")
	})
	run("Release of a key nobody holds is not an error", func(t *testing.T, s bites.IdempotencyStore) {
		release(t, s, "never")
	})
	run("keys never share a claim or an answer", func(t *testing.T, s bites.IdempotencyStore) {
		claim(t, s, "a", "fa", base, 10*sec)
		want(t, claim(t, s, "b", "fb", base, 10*sec), bites.IdempotencyClaimed, "")
		complete(t, s, "a", "fa", answer, base, time.Hour)
		want(t, claim(t, s, "b", "fb", base.Add(sec), 10*sec), bites.IdempotencyRunning, "fb")
		release(t, s, "b")
		want(t, claim(t, s, "a", "fa", base.Add(2*sec), 10*sec), bites.IdempotencyDone, "fa")
	})
	run("of several callers claiming one key at once exactly one is claimed, in every round", func(t *testing.T, s bites.IdempotencyStore) {
		const rounds, callers = 60, 8
		for r := range rounds {
			key := "round-" + strconv.Itoa(r)
			var mu sync.Mutex
			claimed, running := 0, 0
			start := make(chan struct{})
			var wg sync.WaitGroup
			for c := range callers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					e, err := s.Claim(ctx, key, "fp"+strconv.Itoa(c), base, time.Hour)
					if err != nil {
						t.Errorf("Claim returned the error %v", err)
						return
					}
					mu.Lock()
					defer mu.Unlock()
					switch e.State {
					case bites.IdempotencyClaimed:
						claimed++
					case bites.IdempotencyRunning:
						running++
					}
				}()
			}
			close(start)
			wg.Wait()
			if claimed != 1 || running != callers-1 {
				t.Fatalf("round %d: %d callers were claimed and %d running, want 1 and %d", r, claimed, running, callers-1)
			}
		}
	})
}
