package bites_test

import (
	"context"
	"testing"

	"github.com/nevindra/bites"
	"github.com/nevindra/bites/bitetest"
)

func TestTheMemoryJobLockKeepsTheContractOfAJobLock(t *testing.T) {
	bitetest.TestJobLock(t, func(*testing.T) bites.JobLock { return bites.NewMemoryJobLock() })
}

func TestTheMemoryJobLockRefusesATtlThatIsNotPositive(t *testing.T) {
	ok, err := bites.NewMemoryJobLock().Claim(context.Background(), "k", 0)
	if ok || err == nil {
		t.Fatalf("Claim with a ttl of 0 = (%v, %v); want (false, error)", ok, err)
	}
}
