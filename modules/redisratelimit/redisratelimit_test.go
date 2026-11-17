package redisratelimit_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nevindra/bites"
	"github.com/nevindra/bites/bitetest"
	"github.com/nevindra/bites/modules/redisratelimit"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

// redisURL is the Redis or Valkey the integration tests use. They are skipped without it, with a
// message naming the variable, so a run with no server says what it left out.
func redisURL(t *testing.T) string {
	t.Helper()
	u := os.Getenv("BITES_REDIS_URL")
	if u == "" {
		t.Skip("BITES_REDIS_URL names no Redis or Valkey, so this test that needs one is skipped; for example BITES_REDIS_URL=redis://127.0.0.1:56379/0")
	}
	return u
}

// newClient returns a client on the server of BITES_REDIS_URL, closed at the end of the test.
func newClient(t *testing.T) *redis.Client {
	t.Helper()
	o, err := redis.ParseURL(redisURL(t))
	if err != nil {
		t.Fatal(err)
	}
	c := redis.NewClient(o)
	t.Cleanup(func() { c.Close() })
	return c
}

var seq atomic.Int32

// newStore returns a Store on a prefix of its own to the test, whose keys are deleted at the end, so
// tests that share a server run in parallel without hearing each other.
func newStore(t *testing.T, o redisratelimit.Options) (*redisratelimit.Store, *redis.Client, string) {
	t.Helper()
	c := newClient(t)
	o.Client = c
	o.Prefix = fmt.Sprintf("t_rl_%d_%d", os.Getpid(), seq.Add(1))
	t.Cleanup(func() { deleteAll(c, o.Prefix) })
	return redisratelimit.New(o), c, o.Prefix
}

func deleteAll(c redis.UniversalClient, prefix string) {
	keys, _ := c.Keys(context.Background(), prefix+":*").Result()
	if len(keys) > 0 {
		c.Del(context.Background(), keys...)
	}
}

func TestTheStoreKeepsTheContractOfEveryRateLimitStore(t *testing.T) {
	bitetest.TestRateLimitStore(t, func(t *testing.T) bites.RateLimitStore {
		st, _, _ := newStore(t, redisratelimit.Options{})
		return st
	})
}

// A cluster is optional: BITES_REDIS_CLUSTER_ADDR names a node of one (a single node that owns every
// slot is enough, since a script that names keys of two slots is refused there as on any cluster).
func TestTheStoreKeepsTheContractOnACluster(t *testing.T) {
	addr := os.Getenv("BITES_REDIS_CLUSTER_ADDR")
	if addr == "" {
		t.Skip("BITES_REDIS_CLUSTER_ADDR names no Redis Cluster node, so this test is skipped; for example BITES_REDIS_CLUSTER_ADDR=127.0.0.1:56381")
	}
	c := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{addr}})
	t.Cleanup(func() { c.Close() })
	bitetest.TestRateLimitStore(t, func(t *testing.T) bites.RateLimitStore {
		return redisratelimit.New(redisratelimit.Options{Client: c, Prefix: fmt.Sprintf("t_rl_c_%d_%d", os.Getpid(), seq.Add(1))})
	})
}

// commands counts the commands a client sends, one for a script that is sent by its hash.
type commands struct{ n atomic.Int32 }

func (c *commands) DialHook(next redis.DialHook) redis.DialHook { return next }
func (c *commands) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error { c.n.Add(1); return next(ctx, cmd) }
}
func (c *commands) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error { c.n.Add(int32(len(cmds))); return next(ctx, cmds) }
}

func TestATakeIsOneRoundTripWhetherItIsAllowedOrRefused(t *testing.T) {
	t.Parallel()
	st, c, _ := newStore(t, redisratelimit.Options{})
	cmds := &commands{}
	c.AddHook(cmds)
	now := time.Unix(1_700_000_000, 0)
	st.Take(ctx, "warm", now, 1, time.Hour) // the first use may send the script whole
	for i, want := range []bool{true, true, false, false} {
		before := cmds.n.Load()
		d, err := st.Take(ctx, "k", now, 2, time.Hour)
		if err != nil || d.Allowed != want {
			t.Fatalf("take %d: %+v, %v", i, d, err)
		}
		if n := cmds.n.Load() - before; n != 1 {
			t.Fatalf("take %d (allowed %v) sent %d commands, want 1", i, d.Allowed, n)
		}
	}
}

// Not parallel: SCRIPT FLUSH is the server's, so a parallel test would find its script gone.
func TestTheScriptIsSentAgainWhenRedisForgotIt(t *testing.T) {
	st, c, _ := newStore(t, redisratelimit.Options{})
	now := time.Unix(1_700_000_000, 0)
	if _, err := st.Take(ctx, "k", now, 5, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := c.ScriptFlush(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if d, err := st.Take(ctx, "k", now, 5, time.Hour); err != nil || !d.Allowed || d.Remaining != 3 {
		t.Fatalf("a Take after SCRIPT FLUSH = %+v, %v, want Allowed with Remaining 3", d, err)
	}
}

func TestTwoStoresOnOneRedisShareTheirBucketsAndNeverBothGetTheLastRequest(t *testing.T) {
	t.Parallel()
	a, _, prefix := newStore(t, redisratelimit.Options{Name: "login"})
	b := redisratelimit.New(redisratelimit.Options{Client: newClient(t), Prefix: prefix, Name: "login"}) // an instance has a client of its own
	now := time.Unix(1_700_000_000, 0)
	for round := range 40 {
		key := fmt.Sprint("shared-", round)
		var allowed atomic.Int32
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := range 8 {
			st := []*redisratelimit.Store{a, b}[i%2]
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if d, err := st.Take(ctx, key, now, 3, time.Hour); err != nil {
					t.Error(err)
				} else if d.Allowed {
					allowed.Add(1)
				}
			}()
		}
		close(start)
		wg.Wait()
		if allowed.Load() != 3 {
			t.Fatalf("round %d: two instances were allowed %d requests of a burst of 3", round, allowed.Load())
		}
	}
}

func TestARateLimiterOnTwoInstancesLimitsOnceNotTwice(t *testing.T) {
	t.Parallel()
	a, _, prefix := newStore(t, redisratelimit.Options{Name: "login"})
	b := redisratelimit.New(redisratelimit.Options{Client: newClient(t), Prefix: prefix, Name: "login"})
	opts := func(s bites.RateLimitStore) bites.RateLimitOptions {
		return bites.RateLimitOptions{Limit: 3, Period: time.Minute, Store: s, Name: "login"}
	}
	one, two := bites.NewRateLimiter(opts(a)), bites.NewRateLimiter(opts(b))
	for i := range 3 {
		l := []*bites.RateLimiter{one, two}[i%2]
		if err := l.Allow(ctx, "ana@example.com"); err != nil {
			t.Fatalf("request %d: %v", i+1, err)
		}
	}
	var se *bites.StatusError
	for _, l := range []*bites.RateLimiter{one, two} {
		if err := l.Allow(ctx, "ana@example.com"); !errors.As(err, &se) || se.Status != 429 {
			t.Fatalf("the fourth request, on either instance, was %v, want a 429", err)
		}
	}
	if err := two.Allow(ctx, "budi@example.com"); err != nil {
		t.Fatalf("another key was limited: %v", err)
	}
}

func TestLimitersWithDifferentNamesOnOnePrefixDoNotShareABucket(t *testing.T) {
	t.Parallel()
	login, c, prefix := newStore(t, redisratelimit.Options{Name: "login"})
	invite := redisratelimit.New(redisratelimit.Options{Client: c, Prefix: prefix, Name: "invite"})
	now := time.Unix(1_700_000_000, 0)
	if d, _ := login.Take(ctx, "ana@example.com", now, 1, time.Hour); !d.Allowed {
		t.Fatal("the first request was refused")
	}
	if d, _ := login.Take(ctx, "ana@example.com", now, 1, time.Hour); d.Allowed {
		t.Fatal("the second request of one name was allowed")
	}
	if d, err := invite.Take(ctx, "ana@example.com", now, 1, time.Hour); err != nil || !d.Allowed {
		t.Fatalf("the same key under another Name shared the bucket: %+v, %v", d, err)
	}
	// A Name that ends where a key begins is not the same bucket as the Name and key joined.
	ab := redisratelimit.New(redisratelimit.Options{Client: c, Prefix: prefix, Name: "a"})
	ab2 := redisratelimit.New(redisratelimit.Options{Client: c, Prefix: prefix, Name: "a\x00b"})
	ab.Take(ctx, "b\x00c", now, 1, time.Hour)
	if d, _ := ab2.Take(ctx, "c", now, 1, time.Hour); !d.Allowed {
		t.Fatal("a Name and a key were shifted into one another")
	}
}

func TestStoresWithDifferentPrefixesDoNotShareABucket(t *testing.T) {
	t.Parallel()
	a, c, prefix := newStore(t, redisratelimit.Options{})
	b := redisratelimit.New(redisratelimit.Options{Client: c, Prefix: prefix + "_other"})
	t.Cleanup(func() { deleteAll(c, prefix+"_other") })
	now := time.Unix(1_700_000_000, 0)
	a.Take(ctx, "k", now, 1, time.Hour)
	if d, err := b.Take(ctx, "k", now, 1, time.Hour); err != nil || !d.Allowed {
		t.Fatalf("a Store on another prefix shared the bucket: %+v, %v", d, err)
	}
}

func TestTheKeyIsNotStoredInTheClear(t *testing.T) {
	t.Parallel()
	st, c, prefix := newStore(t, redisratelimit.Options{Name: "login"})
	st.Take(ctx, "ana@example.com", time.Now(), 3, time.Minute)
	keys, err := c.Keys(ctx, prefix+":*").Result()
	if err != nil || len(keys) != 1 {
		t.Fatalf("keys = %v, %v, want one", keys, err)
	}
	if strings.Contains(keys[0], "ana") || strings.Contains(keys[0], "login") || len(keys[0]) != len(prefix)+1+43 {
		t.Fatalf("the key is %q, not the prefix and a base64url SHA-256", keys[0])
	}
	if v, _ := c.Get(ctx, keys[0]).Result(); strings.Contains(v, "ana") {
		t.Fatalf("the value is %q", v)
	}
}

func TestARefusedTakeWritesNothing(t *testing.T) {
	t.Parallel()
	st, c, prefix := newStore(t, redisratelimit.Options{})
	now := time.Unix(1_700_000_000, 0)
	st.Take(ctx, "k", now, 1, time.Hour)
	keys, _ := c.Keys(ctx, prefix+":*").Result()
	before, _ := c.Get(ctx, keys[0]).Result()
	ttl, _ := c.PTTL(ctx, keys[0]).Result()
	for range 20 {
		if d, _ := st.Take(ctx, "k", now, 1, time.Hour); d.Allowed {
			t.Fatal("a request over the burst was allowed")
		}
	}
	after, _ := c.Get(ctx, keys[0]).Result()
	ttl2, _ := c.PTTL(ctx, keys[0]).Result()
	if before != after || ttl2 > ttl {
		t.Fatalf("refused requests wrote the bucket: %s became %s, TTL %v became %v", before, after, ttl, ttl2)
	}
}

func TestABucketKeyExpiresOneGraceAfterTheBucketIsFull(t *testing.T) {
	t.Parallel()
	st, c, prefix := newStore(t, redisratelimit.Options{})
	// A bucket of 3 at one every 10 s, one request spent: full again in 10 s; the key stays 60 s more.
	if _, err := st.Take(ctx, "k", time.Now(), 3, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	keys, _ := c.Keys(ctx, prefix+":*").Result()
	ttl, err := c.PTTL(ctx, keys[0]).Result()
	if err != nil || ttl < 69*time.Second || ttl > 70*time.Second {
		t.Fatalf("TTL = %v, %v, want about 70s (10s of bucket and a minute of grace)", ttl, err)
	}
	// An hour's bucket is kept an hour and a minute, so a bucket is never deleted while it is not full.
	if _, err := st.Take(ctx, "h", time.Now(), 1, time.Hour); err != nil {
		t.Fatal(err)
	}
	all, _ := c.Keys(ctx, prefix+":*").Result()
	for _, k := range all {
		if ttl, _ := c.PTTL(ctx, k).Result(); ttl > 61*time.Minute+time.Second {
			t.Fatalf("TTL of %s = %v, want at most 61m", k, ttl)
		}
	}
}

func TestATakeThatCannotWorkIsAnErrorSayingWhatToChange(t *testing.T) {
	t.Parallel()
	st, _, _ := newStore(t, redisratelimit.Options{})
	now := time.Now()
	for name, c := range map[string]struct {
		burst    int
		interval time.Duration
		now      time.Time
		want     string
	}{
		"no burst":            {0, time.Second, now, "burst is 0: a bucket holds at least one request"},
		"no interval":         {1, 0, now, "interval is 0s"},
		"under a microsecond": {1, 500 * time.Nanosecond, now, "give an interval of at least 1µs"},
		"a huge burst":        {1 << 40, 24 * time.Hour, now, "give a shorter interval or a smaller burst"},
		"a time too far on":   {1, time.Second, time.Unix(1<<40, 0), "outside the years 1970 to 2112"},
		"a time before 1970":  {1, time.Second, time.Unix(-1, 0), "outside the years 1970 to 2112"},
	} {
		if _, err := st.Take(ctx, "k", c.now, c.burst, c.interval); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: Take = %v, want it to say %q", name, err, c.want)
		}
	}
}

func TestAnIntervalThatIsNotWholeMicrosecondsStillLimitsToTheBurst(t *testing.T) {
	t.Parallel()
	st, _, _ := newStore(t, redisratelimit.Options{})
	now := time.Unix(1_700_000_000, 0)
	// 7 requests a minute: an interval of 8571428571 ns, which is kept as 8571428 µs.
	allowed := 0
	for range 20 {
		if d, err := st.Take(ctx, "k", now, 7, time.Minute/7); err != nil {
			t.Fatal(err)
		} else if d.Allowed {
			allowed++
		}
	}
	if allowed != 7 {
		t.Fatalf("%d requests allowed of a burst of 7", allowed)
	}
}

func TestAStoreErrorIsWrappedWithTheOperation(t *testing.T) {
	t.Parallel()
	l, err := net.Listen("tcp", "127.0.0.1:0") // a port nothing listens on once it is closed
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	c := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: -1, DialTimeout: time.Second})
	defer c.Close()
	st := redisratelimit.New(redisratelimit.Options{Client: c})
	_, err = st.Take(ctx, "x", time.Now(), 1, time.Second)
	var op *net.OpError
	if err == nil || !strings.HasPrefix(err.Error(), "redisratelimit: take: ") || !errors.As(err, &op) {
		t.Fatalf("Take with no Redis = %v, want redisratelimit: take: wrapping the net error", err)
	}
	var se *bites.StatusError
	if errors.As(err, &se) {
		t.Fatal("a store error is a StatusError: the limiter would answer it as a client error and not a 500")
	}
}

func TestNewRefusesABadOptionAndSaysWhatToChange(t *testing.T) {
	t.Parallel()
	func() {
		defer func() {
			msg, _ := recover().(string)
			if !strings.Contains(msg, "Client is nil: give the application's client, such as redis.NewClient(") || !strings.Contains(msg, "registered at redisratelimit_test.go:") {
				t.Errorf("panic = %q; want it to say what to give and the caller's line", msg)
			}
		}()
		redisratelimit.New(redisratelimit.Options{})
		t.Error("New did not panic")
	}()
}

// BenchmarkTake is the cost of one Take against a Redis on this machine, over distinct keys and over
// one key shared by every goroutine.
func BenchmarkTake(b *testing.B) {
	u := os.Getenv("BITES_REDIS_URL")
	if u == "" {
		b.Skip("BITES_REDIS_URL names no Redis or Valkey")
	}
	o, err := redis.ParseURL(u)
	if err != nil {
		b.Fatal(err)
	}
	c := redis.NewClient(o)
	defer c.Close()
	prefix := fmt.Sprintf("t_rl_bench_%d", os.Getpid())
	defer deleteAll(c, prefix)
	st := redisratelimit.New(redisratelimit.Options{Client: c, Prefix: prefix})
	now := time.Now()
	b.Run("a new key each time", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			if _, err := st.Take(ctx, "k"+strconv.Itoa(i), now, 5, time.Minute); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("a refused request", func(b *testing.B) {
		st.Take(ctx, "refused", now, 1, time.Hour)
		for b.Loop() {
			if _, err := st.Take(ctx, "refused", now, 1, time.Hour); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("one key from 16 goroutines", func(b *testing.B) {
		b.SetParallelism(16)
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := st.Take(ctx, "hot", now, 5, time.Minute); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
}
