package bites

import (
	"context"
	"strconv"
	"testing"
	"time"
)

func TestTheMemoryJobLockFreesTheClaimsThatExpiredSoItDoesNotGrow(t *testing.T) {
	l := NewMemoryJobLock()
	for i := range 1000 {
		if ok, err := l.Claim(context.Background(), "k"+strconv.Itoa(i), time.Nanosecond); err != nil || !ok {
			t.Fatalf("Claim %d = (%v, %v)", i, ok, err)
		}
	}
	time.Sleep(time.Millisecond)
	if ok, _ := l.Claim(context.Background(), "after", time.Hour); !ok {
		t.Fatal("Claim after the others expired answered false")
	}
	l.mu.Lock()
	n := len(l.until)
	l.mu.Unlock()
	if n != 1 {
		t.Fatalf("the lock holds %d entries after the others expired; want 1", n)
	}
}
