# redissession

**The module that keeps sessions in Redis or Valkey as a `bites.OwnerSessionStore`, so a session can be ended at once and every session of one User with `RevokeOwner`, with the session id and the owner stored only as SHA-256 hashes and expired sessions deleted by Redis.**

**Guide:** [Security](../guide/security.md) · **Design:** [Security](../design/security.md)

`github.com/nevindra/bites/modules/redissession` is a module of its own ([ADR 0070](../adr/0070-optional-features-are-modules-under-modules-each-with-its-own-go-mod.md)), so the core carries none of it. It depends on `github.com/redis/go-redis/v9` at v9.23.0 and works with Redis 4.0 or later and with Valkey. Its names are not checked by the documentation tests, which read the root, `bitetest` and `stdhttp`; its own tests are run with `cd modules/redissession && go test ./...`, and the tests that need a server run when `BITES_REDIS_URL` names one (`redis://127.0.0.1:56379/0`) and are skipped, with a message naming it, otherwise; `BITES_REDIS_CLUSTER_ADDR` names a Redis Cluster node for the test that runs the suites against a cluster. The decision is [ADR 0093](../adr/0093-redis-and-valkey-stores-are-two-modules-and-every-change-is-one-lua-script.md); the interfaces it implements are [`SessionStore`](./security.md#sessionstore) and `OwnerSessionStore` ([ADR 0056](../adr/0056-a-session-is-sealed-in-a-cookie-and-changed-by-returning-its-cookie.md)), and it passes [`bitetest.TestSessionStore`](./bitetest.md#bitetesttestsessionstore) and [`bitetest.TestOwnerSessionStore`](./bitetest.md#bitetesttestownersessionstore).

```go
client := redis.NewClient(&redis.Options{Addr: "localhost:6379"}) // Valkey too
store := redissession.New(redissession.Options{Client: client})
logins := bites.NewSessions[Login](bites.SessionOptions{Keys: keys, Store: store})
```

There is no table to create and no Service to register: Redis deletes a session when its TTL ends. What a Store keeps, for the default prefix:

| Key | Type | Holds |
|---|---|---|
| `{bites:session}:id:<base64url SHA-256 of the id>` | hash | `e` the expiry in Unix microseconds, `o` the base64url SHA-256 of the owner (empty for none), `d` the data; the key has a TTL of the time to the expiry |
| `{bites:session}:owner:<base64url SHA-256 of the owner>` | sorted set | the hashes of the owner's sessions, each scored by its expiry in microseconds; the key has a TTL of its longest session |

What to know before using it:

- **One round trip** for `Load`, `Save`, `SaveOwned`, `Delete` and `DeleteOwner` of up to 500 sessions; **two for a `Touch`**, which happens only when a session slides ([`RefreshAfter`](./security.md#sessionoptions)). A request that does not read the Session does not touch Redis.
- **Expiry is judged on the application's clock**, `bites.Now(ctx)`, against the expiry kept in the session, never on Redis's. The TTL of the key only frees the space, so an application on a test clock and a server whose clock drifts do not disagree with the sealed cookie.
- **The id and the owner are stored only as hashes**, so a dump of the keyspace holds no id a cookie could be forged from and no owner. The data is kept in the clear: a session that holds a secret should not hold it in the data.
- **Every key starts with `{prefix}`, a Redis Cluster hash tag**, so a Store lives on one shard and its scripts run on a cluster. Sessions of one Store do not spread over a cluster's shards; give Stores different prefixes to spread them.
- **Redis must not evict the sessions.** A server with `maxmemory-policy allkeys-lru` (or any eviction under memory pressure) signs users out early; a restart without persistence, or a failover to a replica that was behind, loses the sessions written since. Turn on persistence (AOF) if that is not acceptable.
- **Do not read sessions from replicas.** A client configured to (`ReadOnly`, `RouteByLatency` on a cluster, `ReplicaOnly`) would load a session a revoke has already ended.
- **Two `Sessions` types over one prefix share owners** ([ADR 0056](../adr/0056-a-session-is-sealed-in-a-cookie-and-changed-by-returning-its-cookie.md)): give each its own `Prefix`.

### `redissession.New`

```go
func New(o Options) *Store
```

New returns a Store and connects nothing. It panics, `redissession.New: <reason>` and `registered at file.go:N` on the next line, for each row of this table:

| Mistake | Message begins |
|---|---|
| `Client` is nil | `Client is nil: give the application's client, such as redis.NewClient(&redis.Options{Addr: "localhost:6379"})` |
| `Prefix` contains `{` or `}` | `Prefix "<p>" contains a brace: the Store puts the prefix inside braces, a Redis Cluster hash tag, so give a prefix such as "<p without braces>" without '{' or '}', or leave it empty for "bites:session"` |

### `redissession.Options`

```go
type Options struct {
	Client redis.UniversalClient
	Prefix string
}
```

`Client` is the application's client; required. It is any `redis.UniversalClient`: a `*redis.Client` for one server (Redis or Valkey), a Sentinel client from `redis.NewFailoverClient`, a `*redis.ClusterClient` or a `*redis.Ring`. The Store never closes it. `Prefix` is what every key starts with, inside braces, `bites:session` when empty; it must not contain `{` or `}`. Applications that share a Redis and want separate sessions use different prefixes.

### `redissession.DefaultPrefix`

```go
const DefaultPrefix = "bites:session"
```

DefaultPrefix is the prefix used when `Options.Prefix` is empty. A session's key is `{bites:session}:id:` and the base64url of a SHA-256.

### `redissession.Store`

```go
type Store struct { /* unexported */ }
```

Store implements `bites.OwnerSessionStore` over Redis or Valkey. Make one with `New`; the zero value is not usable. It is safe for concurrent use, and any number of instances may share a Redis. A Redis error is returned wrapped, `redissession: <operation>: <error>`, with `errors.Is` and `errors.As` working through it; the framework answers a store error with a 500.

### `redissession.Store.Load`

```go
func (s *Store) Load(ctx context.Context, id string) ([]byte, bool, error)
```

Load returns the data saved for `id`, and false when there is none or it has expired. A session has expired when its expiry is not after `bites.Now(ctx)`, the application's clock (`WithClock`, `bitetest.Clock`): the clock that computed the expiry and that checks the one sealed in the cookie ([ADR 0056](../adr/0056-a-session-is-sealed-in-a-cookie-and-changed-by-returning-its-cookie.md)) decides, never Redis's. A ctx that carries no app reads `time.Now()`.

### `redissession.Store.Save`

```go
func (s *Store) Save(ctx context.Context, id string, data []byte, expires time.Time) error
```

Save keeps `data` for `id` until `expires`, replacing what `id` had, its owner included, which becomes none. The framework calls `SaveOwned` instead for a session type with a `SessionOwner` method.

### `redissession.Store.SaveOwned`

```go
func (s *Store) SaveOwned(ctx context.Context, id, owner string, data []byte, expires time.Time) error
```

SaveOwned is `Save` that also records `owner` for `id`, replacing the owner `id` had. It is one script that writes the session and adds it to its owner's set, and the session's key gets a TTL of `expires` minus `bites.Now(ctx)`, rounded up to a millisecond. An `expires` that is not after now keeps nothing and deletes what `id` had. An empty owner is kept as none: the session is kept and `DeleteOwner` never finds it.

### `redissession.Store.Touch`

```go
func (s *Store) Touch(ctx context.Context, id string, expires time.Time) (bool, error)
```

Touch moves the expiry of `id` to `expires`, and the TTL of its key with it, and changes nothing else, owner and data included. It writes only to a session that is live on `bites.Now(ctx)` and never creates one, so a session that was deleted, ended by `RevokeOwner` or has expired stays gone and Touch returns false: the framework calls it to slide a session ([`RefreshAfter`](./security.md#sessionoptions)), so a slide that races `Sessions.RevokeOwner` cannot bring the session back. It is two round trips, one to read the session's owner and one script that moves the expiry only if the owner is still that one (it reads again, three times at most, when another call changed it in between).

### `redissession.Store.Delete`

```go
func (s *Store) Delete(ctx context.Context, id string) error
```

Delete forgets `id`. Deleting an id that has nothing is not an error. The session stays in its owner's set until the set is trimmed or `DeleteOwner` passes it, and is never counted there.

### `redissession.Store.DeleteOwner`

```go
func (s *Store) DeleteOwner(ctx context.Context, owner, keep string) (int, error)
```

DeleteOwner forgets every session of `owner` but the one whose id is `keep` and returns how many it deleted, sessions that expired and that Redis had not yet deleted included. An empty `keep` keeps none (`Sessions.RevokeOwner`); a `keep` that is a session of `owner` is the one `Session.RevokeOthers` leaves. It counts a session only if its own owner is `owner`, so one that changed owner or was deleted, and is still a member of the set, is removed from the set and not counted. An owner with no session is `(0, nil)`. The empty owner names no one: it is `(0, nil)` without a call to Redis, and `Sessions.RevokeOwner` refuses it before it gets here. It reads the owner's set and ends the sessions in scripts of 500, so a session saved for the owner after the read is not seen by this call.
