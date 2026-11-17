# redisratelimit

**The module that keeps the buckets of a `RateLimiter` in Redis or Valkey as a `bites.RateLimitStore`, so a limit holds across every instance of an application with one Lua script and one round trip for each limited request, the key stored only as its SHA-256 and idle buckets deleted by Redis.**

**Guide:** [Security](../guide/security.md) · **Design:** [Security](../design/security.md)

`github.com/nevindra/bites/modules/redisratelimit` is a module of its own ([ADR 0070](../adr/0070-optional-features-are-modules-under-modules-each-with-its-own-go-mod.md)), so the core carries none of it. It depends on `github.com/redis/go-redis/v9` at v9.23.0 and works with Redis 4.0 or later and with Valkey. Its names are not checked by the documentation tests, which read the root, `bitetest` and `stdhttp`; its own tests are run with `cd modules/redisratelimit && go test ./...`, and the tests that need a server run when `BITES_REDIS_URL` names one (`redis://127.0.0.1:56379/0`) and are skipped, with a message naming it, otherwise; `BITES_REDIS_CLUSTER_ADDR` names a Redis Cluster node for the test that runs the suite against a cluster. The decision is [ADR 0093](../adr/0093-redis-and-valkey-stores-are-two-modules-and-every-change-is-one-lua-script.md); the interface it implements is [`RateLimitStore`](./security.md#ratelimitstore) ([ADR 0055](../adr/0055-rate-limiting-is-a-token-bucket-per-key-with-a-bounded-store.md)), and it passes [`bitetest.TestRateLimitStore`](./bitetest.md#bitetesttestratelimitstore).

```go
client := redis.NewClient(&redis.Options{Addr: "localhost:6379"}) // Valkey too
store := redisratelimit.New(redisratelimit.Options{Client: client, Name: "login"})
perEmail := bites.NewRateLimiter(bites.RateLimitOptions{Limit: 5, Period: time.Minute, Store: store, Name: "login"})
```

There is no table to create and no Service to register: a bucket is one key, and Redis deletes it when its TTL ends.

What to know before using it:

- **One round trip for each limited request** (two the first time after Redis restarted or flushed its scripts). Measured against Redis 7 over loopback, about 21 to 31 µs a `Take` for a new key; a network adds its round trip.
- **The time is the application's clock**, the `now` a `Take` is given, not Redis's. Instances whose clocks differ by `s` differ in their limits by at most `s` of bucket (`s/interval` requests): keep them within a second with NTP.
- **Time is kept in microseconds**, because a Lua number holds an integer exactly up to 2^53. An interval under 1µs, a bucket longer than about 71 years and a `now` after 2112 are errors that say so; an interval that is not a whole number of microseconds is rounded to one.
- **A key lives until its bucket is full again plus one minute.** A bucket that has been full for a minute is what a new key gets, so Redis deleting it loses nothing.
- **Redis must not evict the buckets.** A server with `maxmemory-policy allkeys-lru` (or any eviction under memory pressure) deletes buckets early and resets limits; a restart without persistence, or a failover to a replica that was behind, resets them once. A limit whose loss is not acceptable (a quota that is billed) is not a rate limit.
- **The key is stored as a hash** of the `Name` and the key, so a dump of the keyspace shows no email or address. A key from a small space (an email) can still be tried against a list.
- **Give each `RateLimiter` its own store with its own `Name`.** A store is not told which limiter calls it, so two limiters on one prefix and one `Name` share the bucket of a key (an email limited for sign-in and for invitations).
- **A cluster spreads the buckets**, one key each. A client that reads from replicas is no problem for this store: it writes every take.

### `redisratelimit.New`

```go
func New(o Options) *Store
```

New returns a Store and connects nothing. It panics, `redisratelimit.New: <reason>` and `registered at file.go:N` on the next line, for each row of this table:

| Mistake | Message begins |
|---|---|
| `Client` is nil | `Client is nil: give the application's client, such as redis.NewClient(&redis.Options{Addr: "localhost:6379"})` |

Not refused, because nothing can see it before a request: two limiters that share a prefix and a `Name`, instances of one limiter given different `Name`s, which do not share their buckets, and a Redis that evicts keys.

### `redisratelimit.Options`

```go
type Options struct {
	Client redis.UniversalClient
	Prefix string
	Name   string
}
```

`Client` is the application's client; required. It is any `redis.UniversalClient`: a `*redis.Client` for one server (Redis or Valkey), a Sentinel client from `redis.NewFailoverClient`, a `*redis.ClusterClient` or a `*redis.Ring`. The Store never closes it. `Prefix` is what every key starts with, before a colon, `bites:ratelimit` when empty; applications that share a Redis and want separate buckets use different prefixes. `Name` names the limiter this Store serves and is mixed into the hash of every key, so limiters that share a Redis and a prefix do not share a bucket; empty is a name like any other, so at most one limiter per prefix leaves it empty, and the instances of one limiter must give the same `Name`.

### `redisratelimit.DefaultPrefix`

```go
const DefaultPrefix = "bites:ratelimit"
```

DefaultPrefix is the key prefix used when `Options.Prefix` is empty. A bucket's key is the prefix, a colon and the base64url of a SHA-256.

### `redisratelimit.Store`

```go
type Store struct { /* unexported */ }
```

Store implements `bites.RateLimitStore` over Redis or Valkey. Make one with `New`; the zero value is not usable. It is safe for concurrent use, and any number of instances may share a Redis. A Redis error is returned wrapped, `redisratelimit: <operation>: <error>`, with `errors.Is` and `errors.As` working through it; a limiter answers a store error with a 500 and so fails closed.

### `redisratelimit.Store.Take`

```go
func (s *Store) Take(ctx context.Context, key string, now time.Time, burst int, interval time.Duration) (bites.RateLimitDecision, error)
```

Take spends one request of `key`'s bucket at `now`, for a bucket of `burst` requests that earns one back every `interval`, and says what is left, as [`RateLimitStore`](./security.md#ratelimitstore) defines. It is one Lua script, atomic for a key across every instance that shares the Redis, so two callers never both get the last request. A refused request writes nothing, so a client that keeps trying does not push its own `RetryAfter` away. A `burst` under one, an `interval` that is not positive or is under one microsecond, a bucket whose length does not fit in 2^53 microseconds, a `now` outside the years 1970 to 2112, and a failure of Redis are each an error that says what to change. You do not call it: the `RateLimiter` does.
