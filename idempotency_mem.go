package bites

import (
	"container/list"
	"context"
	"sync"
	"time"
)

// memoryIdempotency is the in-memory IdempotencyStore of an Idempotency (ADR 0090): one map from
// key to entry under one lock, bounded in keys and in bytes. A claim is held only while a request
// runs, so what grows is the answers, and they are dropped oldest first, a claim never: dropping a
// claim would let a duplicate run beside the request it duplicates. The lock is held for a map
// operation and a list move, a claim of tens of nanoseconds, and only requests that carry a key
// reach it.
type memoryIdempotency struct {
	mu       sync.Mutex
	m        map[string]*idemEntry
	done     list.List // the entries that hold an answer, oldest completion at the front
	bytes    int
	maxKeys  int
	maxBytes int
	evicted  int64 // answers dropped before their expiry, since takeEvicted last read it
}

type idemEntry struct {
	key     string
	fp      string
	expires time.Time
	claim   bool
	ans     IdempotencyAnswer
	size    int
	el      *list.Element // in done, nil for a claim
}

// idemOverhead is what an entry costs besides its strings and its answer: the struct, the map slot
// and the list element, rounded up.
const idemOverhead = 192

func newMemoryIdempotency(maxKeys, maxBytes int) *memoryIdempotency {
	return &memoryIdempotency{m: make(map[string]*idemEntry), maxKeys: maxKeys, maxBytes: maxBytes}
}

func answerSize(a IdempotencyAnswer) int {
	n := len(a.ContentType) + len(a.Body)
	for _, h := range a.Headers {
		n += len(h.Name) + len(h.Value) + 32
	}
	return n
}

// forget drops e from the map, the list and the byte count.
func (s *memoryIdempotency) forget(e *idemEntry) {
	delete(s.m, e.key)
	if e.el != nil {
		s.done.Remove(e.el)
		e.el = nil
	}
	s.bytes -= e.size
}

// makeRoom drops answers, oldest first, until one more key of need bytes fits, and reports whether
// it does. A claim is never dropped, except one past its inFlight, which no request holds any more.
func (s *memoryIdempotency) makeRoom(need int, now time.Time) bool {
	for len(s.m)+1 > s.maxKeys || s.bytes+need > s.maxBytes {
		front := s.done.Front()
		if front == nil {
			if !s.sweepClaims(now) {
				return false
			}
			continue
		}
		e := front.Value.(*idemEntry)
		if now.Before(e.expires) {
			s.evicted++
		}
		s.forget(e)
	}
	return true
}

// sweepClaims drops the claims whose time has passed, and reports whether it dropped any.
func (s *memoryIdempotency) sweepClaims(now time.Time) bool {
	dropped := false
	for _, e := range s.m {
		if e.claim && !now.Before(e.expires) {
			s.forget(e)
			dropped = true
		}
	}
	return dropped
}

func (s *memoryIdempotency) Claim(_ context.Context, key, fp string, now time.Time, inFlight time.Duration) (IdempotencyEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.m[key]; e != nil {
		if now.Before(e.expires) {
			if e.claim {
				return IdempotencyEntry{State: IdempotencyRunning, Fingerprint: e.fp}, nil
			}
			return IdempotencyEntry{State: IdempotencyDone, Fingerprint: e.fp, Answer: e.ans}, nil
		}
		s.forget(e)
	}
	need := idemOverhead + len(key) + len(fp)
	if !s.makeRoom(need, now) {
		return IdempotencyEntry{}, ErrIdempotencyStoreFull
	}
	s.m[key] = &idemEntry{key: key, fp: fp, expires: now.Add(inFlight), claim: true, size: need}
	s.bytes += need
	return IdempotencyEntry{State: IdempotencyClaimed}, nil
}

func (s *memoryIdempotency) Complete(_ context.Context, key, fp string, ans IdempotencyAnswer, now time.Time, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.m[key]; e != nil {
		s.forget(e)
	}
	need := idemOverhead + len(key) + len(fp) + answerSize(ans)
	if need > s.maxBytes {
		// Larger than the whole store: keep only that the work was done.
		ans = IdempotencyAnswer{TooLarge: true}
		need = idemOverhead + len(key) + len(fp)
	}
	if !s.makeRoom(need, now) {
		return ErrIdempotencyStoreFull
	}
	e := &idemEntry{key: key, fp: fp, expires: now.Add(ttl), ans: ans, size: need}
	e.el = s.done.PushBack(e)
	s.m[key] = e
	s.bytes += need
	return nil
}

func (s *memoryIdempotency) Release(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.m[key]; e != nil && e.claim {
		s.forget(e)
	}
	return nil
}

// takeEvicted returns how many answers were dropped before their expiry since it was last called.
func (s *memoryIdempotency) takeEvicted() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.evicted
	s.evicted = 0
	return n
}
