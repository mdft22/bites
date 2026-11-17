package bites

import (
	"context"
	"errors"
	"sync"
	"time"
)

// JobLock lets one instance of several run each occurrence of a Cron job (ADR 0092). Before it
// runs an occurrence the job calls Claim with a key that names that occurrence (the job's name,
// "@" and the due time in Unix seconds), and runs it only when the answer is true. It is the
// interface a Redis or Postgres module implements, so the instances of an application share one
// store; NewMemoryJobLock is the one of a single process. bitetest.TestJobLock holds an
// implementation to the promises below, and an implementation must pass it.
//
// What Claim promises:
//
//   - It answers true to exactly one caller of a key, across every instance that shares the
//     store, and false to every other until ttl has passed. Two callers at the same moment never
//     both get true.
//   - A key that was claimed stays claimed for the whole ttl: nothing releases it, not even the
//     end of the job's run. That is what stops an instance whose clock is a few milliseconds
//     behind from running the occurrence again after the first has finished.
//   - After ttl the key can be claimed again. ttl is judged on Now(ctx), the app's clock and not
//     the store's own, so every instance agrees with the Cron job on the time and a test moves it
//     (bitetest.Clock); a store records the end of a claim as Now(ctx) plus ttl.
//   - A key is any text, in any case, of the length the store can hold; two keys differing in one
//     byte are two keys.
//   - A refusal because another instance holds the key is (false, nil), not an error. An error
//     means the store could not decide (it is down, ctx is done): the answer is then
//     "not claimed" and the job is skipped, never run without the lock.
//   - When ctx is done, Claim returns ctx's error and claims nothing.
//
// A claim is not a lease: it says who starts an occurrence, not who is still running it. An
// instance that claims and then crashes loses the occurrence, and a job that outlasts the time to
// its next occurrence can overlap itself across instances; the job is written to be safe to run
// twice when that matters (ADR 0092).
type JobLock interface {
	// Claim reports whether the caller is the one to run key. ttl is how long the claim is
	// kept, always positive.
	Claim(ctx context.Context, key string, ttl time.Duration) (bool, error)
}

// MemoryJobLock is the JobLock of one process: the instances are the Apps that share it, which
// is what a test of two instances racing needs, and the one app of an application that runs
// alone and wants Cron's other rules. It judges ttl on Now(ctx), so a test moves it with the
// app's clock. Safe for concurrent use. The zero value is not usable; use NewMemoryJobLock.
type MemoryJobLock struct {
	mu      sync.Mutex
	until   map[string]time.Time // key to the end of its claim
	sweepAt time.Time            // the earliest end in until, when the next sweep can free something
}

// NewMemoryJobLock returns an empty MemoryJobLock. It keeps one entry for each claim that has
// not expired and frees the expired ones as it goes, so its size is the jobs' claims inside one
// ttl.
func NewMemoryJobLock() *MemoryJobLock {
	return &MemoryJobLock{until: make(map[string]time.Time)}
}

// Claim implements JobLock.
func (l *MemoryJobLock) Claim(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if ttl <= 0 {
		return false, errors.New("bites: MemoryJobLock.Claim: ttl is not positive: give how long the claim is kept")
	}
	now := Now(ctx)
	l.mu.Lock()
	defer l.mu.Unlock()
	if !now.Before(l.sweepAt) {
		l.sweepAt = time.Time{}
		for k, end := range l.until {
			if !now.Before(end) {
				delete(l.until, k)
			} else if l.sweepAt.IsZero() || end.Before(l.sweepAt) {
				l.sweepAt = end
			}
		}
	}
	if end, ok := l.until[key]; ok && now.Before(end) {
		return false, nil
	}
	end := now.Add(ttl)
	l.until[key] = end
	if l.sweepAt.IsZero() || end.Before(l.sweepAt) {
		l.sweepAt = end
	}
	return true, nil
}
