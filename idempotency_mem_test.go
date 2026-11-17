package bites

import (
	"context"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The in-memory Idempotency store's bound under a client that sends a new key every request
// (ADR 0090).

func TestAClientFloodingUniqueKeysNeverGrowsTheMemoryIdempotencyStorePastItsBounds(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	s := newMemoryIdempotency(1000, 1<<20)
	ans := IdempotencyAnswer{Status: 201, ContentType: "application/json", Body: []byte(strings.Repeat("x", 4096))}
	for i := range 100_000 {
		key := "key-" + strconv.Itoa(i)
		if e, err := s.Claim(ctx, key, "fp", now, time.Minute); err != nil || e.State != IdempotencyClaimed {
			t.Fatalf("claim %d: %v %v", i, e.State, err)
		}
		if err := s.Complete(ctx, key, "fp", ans, now, time.Hour); err != nil {
			t.Fatal(err)
		}
		if len(s.m) > 1000 || s.bytes > 1<<20 {
			t.Fatalf("after %d keys the store holds %d keys and %d bytes", i+1, len(s.m), s.bytes)
		}
	}
	if len(s.m) < 200 {
		t.Fatalf("the store holds only %d keys: it drops more than it needs to", len(s.m))
	}
	if e, _ := s.Claim(ctx, "key-99999", "fp", now, time.Minute); e.State != IdempotencyDone {
		t.Fatal("the newest answer was dropped")
	}
	if e, _ := s.Claim(ctx, "key-0", "fp", now, time.Minute); e.State != IdempotencyClaimed {
		t.Fatal("the oldest answer was kept past the bound")
	}
	if n := s.takeEvicted(); n < 99_000 {
		t.Fatalf("%d answers counted as dropped before their expiry", n)
	}
}

func TestTheMemoryIdempotencyStoreNeverDropsAClaimToMakeRoomForAnother(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	s := newMemoryIdempotency(3, 1<<20)
	for _, k := range []string{"a", "b", "c"} {
		if e, err := s.Claim(ctx, k, "fp", now, time.Minute); err != nil || e.State != IdempotencyClaimed {
			t.Fatal(k, e.State, err)
		}
	}
	if _, err := s.Claim(ctx, "d", "fp", now, time.Minute); err != ErrIdempotencyStoreFull {
		t.Fatalf("got %v, want ErrIdempotencyStoreFull", err)
	}
	// a claim past its time is no one's, and makes room
	if e, err := s.Claim(ctx, "d", "fp", now.Add(2*time.Minute), time.Minute); err != nil || e.State != IdempotencyClaimed {
		t.Fatalf("%v %v", e.State, err)
	}
}

func TestTheMemoryIdempotencyStoreHoldsAnAnswerOfAFewHundredBytesInUnderAKilobyte(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	const n = 10_000
	s := newMemoryIdempotency(n, 64<<20)
	ans := IdempotencyAnswer{Status: 201, ContentType: "application/json", Body: []byte(strings.Repeat("x", 200)),
		Headers: []IdempotencyHeader{{"Location", "/payments/1"}, {"X-Receipt", "r1"}}}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := range n {
		key := strconv.Itoa(i) + strings.Repeat("0", 32-len(strconv.Itoa(i)))
		s.Claim(ctx, key, strings.Repeat("f", 64), now, time.Minute)
		a := ans
		a.Body = append([]byte(nil), ans.Body...) // the middleware hands the store a body of its own
		a.Headers = append([]IdempotencyHeader(nil), ans.Headers...)
		s.Complete(ctx, key, strings.Repeat("f", 64), a, now, time.Hour)
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	per := (int64(after.HeapAlloc) - int64(before.HeapAlloc)) / n
	t.Logf("%d B a key for a 200 B body and two headers (the store counts %d B)", per, s.bytes/n)
	if per > 1024 {
		t.Fatalf("%d B a key, want under 1 KiB", per)
	}
	runtime.KeepAlive(s)
}
