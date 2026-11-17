# Idempotency

**`NewIdempotency` and the `Idempotency` middleware that answers a write sent again with the same `Idempotency-Key` with the first answer, and the `IdempotencyStore` that keeps the claims and the answers.**

**Guide:** [idempotency](../guide/idempotency.md) · **Design:** [middleware and Providers](../design/middleware-and-providers.md)

A `POST` or `PATCH` that carries the key is claimed in a store before its handler runs, the handler runs once, and the answer is kept when the request did its work (a 2xx or a 3xx). The same request sent again gets that answer and `Idempotent-Replayed: true` and the handler does not run. A key sent again with another method, path, query or body is a 422, one sent while the first request still runs is a 409 with `Retry-After: 1`. A 4xx, a 5xx, a panic and a stream are never kept, and the claim is released so the retry runs ([ADR 0090](../adr/0090-a-write-sent-again-with-the-same-idempotency-key-is-answered-with-the-first-answer.md)).

The constructor refuses a configuration that cannot work with a `RegistrationError` naming the constructor, the field and the line ([ADR 0032](../adr/0032-four-first-party-middleware.md)). Registration refuses `Idempotency` given to `App.Use`, and on a route that returns a `File`, a `Stream`, `SSE` or `Events` or whose body is an `Upload`, a multipart form or a `BodyReader`.

Allocations: a route without the middleware is unchanged, and a request with no key through it is 2, the same as without; a request that carries a key is 19 to 21 for a replay and 22 for the first request of a key ([ADR 0003](../adr/0003-the-trade-budget.md)).

### `NewIdempotency`

```go
func NewIdempotency(o IdempotencyOptions) *Idempotency
```

NewIdempotency returns an `Idempotency`. It panics when `Header` is not an HTTP token, `MaxKeyLength` is negative or over 1024, `TTL` or `InFlight` is under a second, `InFlight` is over `TTL`, `MaxAnswer`, `MaxKeys` or `MaxBytes` is negative, `MaxBytes` is under twice `MaxAnswer`, or `MaxKeys` or `MaxBytes` is given with a `Store`.

### `Idempotency`

```go
type Idempotency struct {
	// unexported fields
}
```

Idempotency keeps the answers of the writes that carry a key. One `Idempotency` is one store, shared by every route it is given to; a key belongs to the route it was sent to, so the same key on two routes is two keys.

### `Idempotency.Middleware`

```go
func (d *Idempotency) Middleware() Middleware
```

Middleware is given to a route, a group or `UseEach` with `Use`; `App.Use` refuses it, because the answer is kept for a route and the body is read with the route's `MaxBody` and `BodyTimeout`. Any method but `POST` and `PATCH` passes through untouched. A `POST` or `PATCH` with no key passes through unprotected unless `Required` is set, which answers a 400 problem+json whose error is located `header.Idempotency-Key`. Put it inside `RequestID`, `CORS` and the rate limiter, so a replay still carries the request's id and counts against the limit. It reads the whole request body to fingerprint it, before the Providers run, so the route's `MaxBody` bounds what a client without credentials can make it read.

### `IdempotencyOptions`

```go
type IdempotencyOptions struct {
	Header       string
	Required     bool
	MaxKeyLength int
	Scope        func(Request) string
	TTL          time.Duration
	InFlight     time.Duration
	MaxAnswer    int
	Store        IdempotencyStore
	MaxKeys      int
	MaxBytes     int
}
```

IdempotencyOptions configures `NewIdempotency`; the zero value is usable.

- `Header` is the request header that carries the key, `Idempotency-Key` when empty. A key is 1 to `MaxKeyLength` bytes of printable ASCII with no space, quote or comma; one pair of double quotes around it is removed, so `"k"` and `k` are one key. An empty, long or malformed key, and a key sent on two header lines, is a 400 problem+json located `header.Idempotency-Key`.
- `Required` refuses a `POST` or `PATCH` with no key with a 400. False, such a request goes on unprotected.
- `MaxKeyLength` is 255 when zero, at most 1024.
- `Scope` says who a key belongs to. Nil, it is the request's `Authorization` header and `Cookie` headers, so two clients with different credentials never share a key, and a renewed token is a new scope (the request runs again; another client's answer is never shown). A stable value, such as an API key's prefix or a tenant header, replaces it; "" gives every request one scope, for a route whose callers are not authenticated and whose keys are unguessable.
- `TTL` is how long an answer is kept, 24 hours when zero, at least a second.
- `InFlight` is how long a claim holds while the handler runs, a minute when zero, from a second to `TTL`. A handler that runs longer loses its claim and a duplicate may run it again.
- `MaxAnswer` is the largest answer body kept, 64 KiB when zero. Over it the request is kept only as handled: a duplicate is a 409 saying so, the work is not run twice, and a line at Warn names the route and the size.
- `Store` keeps the claims and the answers across instances; nil is the in-memory store of this process, which is per instance.
- `MaxKeys` (10,000 when zero) and `MaxBytes` (16 MiB when zero, at least twice `MaxAnswer`) bound the in-memory store, and are refused together with `Store`. A full store drops answers oldest first and never a claim, and says so at Warn at most every ten seconds; one that holds only running requests answers a new key 503 with `Retry-After`.

### `IdempotencyStore`

```go
type IdempotencyStore interface {
	Claim(ctx context.Context, key, fingerprint string, now time.Time, inFlight time.Duration) (IdempotencyEntry, error)
	Complete(ctx context.Context, key, fingerprint string, answer IdempotencyAnswer, now time.Time, ttl time.Duration) error
	Release(ctx context.Context, key string) error
}
```

IdempotencyStore keeps what an `Idempotency` remembers, so instances share it (Redis, Postgres). `Claim` records that a request with `fingerprint` is about to run for `key` if none has: it returns `IdempotencyClaimed` when the key was free, and then holds the claim until `now+inFlight`; `IdempotencyRunning` with the claim's own fingerprint while another claim is live; and `IdempotencyDone` with the first request's fingerprint and its answer until the `ttl` of `Complete` passes. It is atomic for a key. `Complete` keeps the answer until `now+ttl` whether or not the caller still holds the claim, because the work was done. `Release` frees a claim and nothing else. Every method decides with the `now` it is given, the app's clock, and is safe for concurrent use. A store that cannot take a new key returns an error matching `ErrIdempotencyStoreFull`; any other error is a 500 and the handler does not run. [`bitetest.TestIdempotencyStore`](./bitetest.md#bitetesttestidempotencystore) holds a store to these promises.

### `IdempotencyState`

```go
type IdempotencyState uint8
```

IdempotencyState is what a store found for a key: `IdempotencyClaimed`, `IdempotencyRunning` or `IdempotencyDone`. The zero value is none of them, and a store that returns it is logged and answered 500.

### `IdempotencyClaimed`

```go
const IdempotencyClaimed IdempotencyState = iota + 1
```

IdempotencyClaimed means the key was free and now belongs to the caller, who runs the request.

### `IdempotencyRunning`

```go
const IdempotencyRunning
```

IdempotencyRunning means another request holds the key and has not finished. The duplicate is answered 409 with `Retry-After: 1`, or 422 when it is not the same request.

### `IdempotencyDone`

```go
const IdempotencyDone
```

IdempotencyDone means a request finished and its answer is kept. The same request is answered with it; another is a 422.

### `IdempotencyEntry`

```go
type IdempotencyEntry struct {
	State       IdempotencyState
	Fingerprint string
	Answer      IdempotencyAnswer
}
```

IdempotencyEntry is what `Claim` found for a key. `Fingerprint` is that of the request that holds or held the key, empty when `State` is `IdempotencyClaimed`; `Answer` is set when `State` is `IdempotencyDone`, and neither the middleware nor a store changes the slices it holds afterwards.

### `IdempotencyAnswer`

```go
type IdempotencyAnswer struct {
	Status      int
	ContentType string
	Headers     []IdempotencyHeader
	Body        []byte
	TooLarge    bool
}
```

IdempotencyAnswer is an answer kept to be sent again: its status (200 to 399), Content-Type, the other headers in the order they were written (a `Location`, a `Set-Cookie`; not the `Date`, `Content-Length` and `Connection` the engine adds), and the body as the handler produced it, before compression. `TooLarge` is true when the request did its work but its answer was over `MaxAnswer`: the other fields are then empty. It is plain data, so a module stores it as JSON.

### `IdempotencyHeader`

```go
type IdempotencyHeader struct{ Name, Value string }
```

IdempotencyHeader is one header line of a kept answer.

### `ErrIdempotencyStoreFull`

```go
var ErrIdempotencyStoreFull error
```

ErrIdempotencyStoreFull is what a store's `Claim` returns, wrapped or not, when it cannot take a new key because every key it holds belongs to a request still running. The middleware answers 503 with `Retry-After: 1` and never runs the handler; the in-memory store returns it.
