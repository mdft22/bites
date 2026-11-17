// Package redisratelimit is a bites.RateLimitStore over Redis or Valkey: an application that runs
// several instances behind one balancer keeps the buckets of its RateLimiters in the Redis it
// already has, so a limit holds for the application and not for each instance (ADR 0093, ADR 0055).
//
// A bucket is one key: the SHA-256 of the limiter's name and the limit's key (keys hold emails and
// IP addresses, which a dump of the keyspace then does not hand out) holding one number, the time
// the bucket is next full (GCRA, the algorithm of the in-memory store). Take is one Lua script and
// so one round trip: Redis runs a script without running anything else, so two instances taking at
// once cannot both get the last request, with no lock and no retry. The key carries a TTL that Redis
// enforces, so a bucket that has been full for a while is deleted by Redis and nothing sweeps: there
// is no Service to register. The time is the one Take is given, the application's clock, never
// Redis's, so instances whose clocks differ by a skew differ in their limit by at most that skew.
// The package depends on github.com/redis/go-redis/v9 and works with Valkey as it does with Redis.
//
//	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	store := redisratelimit.New(redisratelimit.Options{Client: client, Name: "login"})
//	perEmail := bites.NewRateLimiter(bites.RateLimitOptions{Limit: 5, Period: time.Minute, Store: store, Name: "login"})
package redisratelimit

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/nevindra/bites"
	"github.com/redis/go-redis/v9"
)

// DefaultPrefix is the key prefix used when Options.Prefix is empty. A bucket's key is the prefix,
// a colon and the base64url of a SHA-256.
const DefaultPrefix = "bites:ratelimit"

// grace is how long a bucket's key outlives the moment it is full, so that deleting it by TTL loses
// nothing when the clock of the instance that wrote it is behind the clock of the one that reads it
// by less than this (a full bucket is what a new key gets, ADR 0055).
const grace = time.Minute

// maxExact is the largest integer a Lua number (a double) holds exactly, 2^53. Times and lengths
// are microseconds so that a Unix time (about 1.7e15) is exact in it; a nanosecond one (1.7e18) is
// not.
const maxExact = 1 << 53

var (
	_ bites.RateLimitStore = (*Store)(nil)
)

// Options are the settings of New.
type Options struct {
	// Client is the application's client. Required. It is any redis.UniversalClient: a *redis.Client
	// for one server (Redis or Valkey), a Sentinel client from redis.NewFailoverClient, a
	// *redis.ClusterClient or a *redis.Ring. A bucket is one key, so a cluster spreads buckets over
	// its shards. The Store never closes it.
	Client redis.UniversalClient
	// Prefix is what every key of this Store starts with, before a colon. Empty means
	// "bites:ratelimit". Applications that share a Redis and want separate buckets use different
	// prefixes.
	Prefix string
	// Name names the limiter this Store serves. It is mixed into the hash of every key, so two
	// limiters that share a Redis and a prefix (and a key, such as one email limited for sign-in and
	// for invitations) do not share a bucket: give each RateLimiter its own Store with its own Name,
	// as its Name. Empty is a name like any other, so at most one limiter per prefix may leave it
	// empty. Instances of one limiter must give the same Name, or they do not share its buckets.
	Name string
}

// Store implements bites.RateLimitStore over Redis or Valkey. Make one with New; the zero value is
// not usable. It is safe for concurrent use, and any number of instances may share a Redis.
type Store struct {
	opt Options
}

// take spends one request. KEYS[1] is the bucket; ARGV is now, the interval and the bucket's length
// in microseconds, and the grace in microseconds. It returns {1, tat} when the request was spent and
// {0, tat} when it was refused, tat being the bucket's new (or, refused, unchanged) full time. A
// refused request writes nothing, so a client that keeps trying does not push its own retry time
// away. The numbers are written with %.0f because Lua prints a double with 14 digits otherwise, and
// a Unix time in microseconds has 16.
var take = redis.NewScript(`
local now, iv, limit, grace = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
local tat = tonumber(redis.call('GET', KEYS[1])) or 0
local base = math.max(tat, now)
local new = base + iv
if new - now <= limit then
	redis.call('SET', KEYS[1], string.format('%.0f', new), 'PX', math.ceil((new - now + grace) / 1000))
	return {1, string.format('%.0f', new)}
end
return {0, string.format('%.0f', base)}
`)

// New returns a Store; it connects nothing and creates nothing. It panics, naming the line that
// called it, when Client is nil.
func New(o Options) *Store {
	_, file, line, _ := runtime.Caller(1)
	where := fmt.Sprintf("%s:%d", filepath.Base(file), line)
	fail := func(format string, args ...any) {
		panic(fmt.Sprintf("redisratelimit.New: %s\n\tregistered at %s", fmt.Sprintf(format, args...), where))
	}
	if o.Client == nil {
		fail("Client is nil: give the application's client, such as redis.NewClient(&redis.Options{Addr: \"localhost:6379\"})")
	}
	if o.Prefix == "" {
		o.Prefix = DefaultPrefix
	}
	return &Store{opt: o}
}

// key is the Redis key a bucket is stored under: the limit's key is never written to Redis. The Name
// is hashed in front of it with its length, so a Name and a key cannot be shifted into one another.
func (s *Store) key(key string) string {
	h := sha256.New()
	h.Write(binary.BigEndian.AppendUint64(nil, uint64(len(s.opt.Name))))
	h.Write([]byte(s.opt.Name))
	h.Write([]byte(key))
	return s.opt.Prefix + ":" + base64.RawURLEncoding.EncodeToString(h.Sum(nil))
}

// Take spends one request of key's bucket at now, for a bucket of burst requests that earns one
// back every interval, with one script and so one round trip (two the first time after Redis
// started or flushed its scripts, when the script is sent whole), and says what is left (see
// bites.RateLimitStore). It is atomic for a key across every instance that shares the Redis. A
// request that is refused changes nothing. now is the time Take decides with (the application's
// clock), never Redis's: see ADR 0093 for what a skew between instances does. Time is kept in whole
// microseconds, so an interval under one microsecond is an error, as are a burst under one, an
// interval that is not positive and a bucket whose length does not fit (about 71 years of
// microseconds); so is any failure of Redis, which a RateLimiter answers as a 500.
func (s *Store) Take(ctx context.Context, key string, now time.Time, burst int, interval time.Duration) (bites.RateLimitDecision, error) {
	iv := interval.Microseconds()
	switch {
	case burst < 1:
		return bites.RateLimitDecision{}, fmt.Errorf("redisratelimit: burst is %d: a bucket holds at least one request", burst)
	case interval <= 0:
		return bites.RateLimitDecision{}, fmt.Errorf("redisratelimit: interval is %v: a request must take a positive time to earn back", interval)
	case iv < 1:
		return bites.RateLimitDecision{}, fmt.Errorf("redisratelimit: interval is %v: time is kept in microseconds, so give an interval of at least 1µs", interval)
	case int64(burst) > maxExact/4/iv:
		return bites.RateLimitDecision{}, fmt.Errorf("redisratelimit: a burst of %d at one request every %v does not fit in the 2^53 microseconds a Lua number holds exactly: give a shorter interval or a smaller burst", burst, interval)
	}
	t, limit := now.UnixMicro(), int64(burst)*iv
	if t < 0 || t > maxExact/2 {
		return bites.RateLimitDecision{}, fmt.Errorf("redisratelimit: now is %v, which is outside the years 1970 to 2112 that a Lua number holds exactly in microseconds", now)
	}
	res, err := take.Run(ctx, s.opt.Client, []string{s.key(key)}, t, iv, limit, grace.Microseconds()).Slice()
	if err != nil {
		return bites.RateLimitDecision{}, fmt.Errorf("redisratelimit: take: %w", err)
	}
	if len(res) != 2 {
		return bites.RateLimitDecision{}, fmt.Errorf("redisratelimit: take: the script answered %d values, want 2: is this Redis or Valkey running the script?", len(res))
	}
	allowed, _ := res[0].(int64)
	text, _ := res[1].(string)
	tat, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return bites.RateLimitDecision{}, fmt.Errorf("redisratelimit: take: the script answered %v for the bucket's time, want a number", res[1])
	}
	us := time.Microsecond
	if allowed == 1 {
		return bites.RateLimitDecision{
			Allowed:   true,
			Remaining: int((limit - (tat - t)) / iv),
			Reset:     time.Duration(tat-t) * us,
		}, nil
	}
	return bites.RateLimitDecision{
		RetryAfter: time.Duration(max(tat+iv-t-limit, 1)) * us,
		Reset:      time.Duration(tat-t) * us,
	}, nil
}
