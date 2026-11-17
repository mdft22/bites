# Idempotency keys

**Answer a write that a client sends again, after a timeout or a redelivery, with the answer of the first one, so a payment is made once.**

**Reference:** [Idempotency](../reference/idempotency.md) · **Design:** [Middleware and Providers](../design/middleware-and-providers.md)

A client that times out on a `POST` does not know whether the server did the work. It sends the request again with the same `Idempotency-Key`, a value it made up (a UUID) and keeps for every attempt at that one request. The middleware claims the key before the handler runs, keeps the answer of a request that did its work, and answers the retry with it ([ADR 0090](../adr/0090-a-write-sent-again-with-the-same-idempotency-key-is-answered-with-the-first-answer.md)).

The examples use one handler.

<!-- compiles: decls -->
```go
type Charge struct {
	Path struct{ Account int }
	Body struct{ Cents int }
}

type Receipt struct {
	ID    string `json:"id"`
	Cents int    `json:"cents"`
}

func charge(ctx context.Context, in Charge) (bites.Created[Receipt], error) {
	id := "ch_" + strconv.Itoa(in.Path.Account)
	return bites.Created[Receipt]{Body: Receipt{ID: id, Cents: in.Body.Cents}, Location: "/charges/" + id}, nil
}
```

## Protecting a route

**Make one `Idempotency` and give its `Middleware()` to the routes that take payments with `Use`.** A route that must never run without a key sets `Required`, which answers a request with no key 400 problem+json located `header.Idempotency-Key`. Without it, a request with no key runs unprotected, which is what an optional key means.

<!-- compiles: body -->
```go
idem := bites.NewIdempotency(bites.IdempotencyOptions{Required: true})
app.Use(bites.RequestID())
app.Post("/accounts/{account}/charges", charge, bites.Use(idem.Middleware()))
```

Give it to the route or its group, never `app.Use`: Registration refuses that, and refuses it on a route that returns a `File` or a stream or reads an `Upload`, a multipart form or a `BodyReader`, whose answer or body cannot be kept or compared whole. Only `POST` and `PATCH` are claimed; a `PUT` or a `DELETE` is idempotent by definition and passes through. Put it inside `RequestID`, `CORS` and the rate limiter, so a replay still carries the request's id and counts against the limit.

## What the client sees

**The same request sent again gets the first answer, whole, with `Idempotent-Replayed: true`.** Its status, `Location`, `Set-Cookie` and body are the first one's, and the handler does not run. The access log of the retry has `idempotency=replayed`.

| The retry | The answer |
|---|---|
| the same method, path, query and body, the first one finished with a 2xx or 3xx | the first answer, `Idempotent-Replayed: true` |
| the first one is still running | 409 problem+json with `Retry-After: 1`; send it again after |
| another method, path, query or body | 422 problem+json: a key stands for one request, so send a new key for a different one |
| the first one answered a 4xx, a 5xx or panicked | it runs again, the key was released |
| the first one's answer was over `MaxAnswer` | 409 saying the request was handled and its answer was too large to keep: read the resource it changed |
| the key is empty, too long, has a space, quote or comma, or is sent twice | 400 problem+json located `header.Idempotency-Key` |

A kept 4xx would turn away the corrected request that has the same key, so only a request that did its work is kept. A handler that does part of its work and then answers a 4xx or a 5xx runs again on the retry, and making that safe is the handler's own job.

## Whose key it is

**A key belongs to the credentials that sent it and to one route**, so one client cannot read another's answer by guessing a key. By default that is the request's `Authorization` and `Cookie` headers. A middleware runs before the Providers, so it cannot know the user's id; if your tokens are renewed between a request and its retry, the retry is a new key and runs again, so give `Scope` a value that stays the same for a client:

<!-- compiles: body -->
```go
idem := bites.NewIdempotency(bites.IdempotencyOptions{
	Scope: func(req bites.Request) string { return req.Header("X-Tenant") },
})
app.Post("/accounts/{account}/charges", charge, bites.Use(idem.Middleware()))
```

A webhook has no credentials of its own, signed or not. Give its route `Scope` returning `""`, `Required`, and the provider's event id as the key, and a second delivery gets the first answer. Such a key must be unguessable, since it is all a client needs to read the answer.

## How long, and how much

`TTL` is how long an answer is kept (24 hours), `InFlight` how long a claim holds while the handler runs (a minute), and `MaxAnswer` the largest body kept (64 KiB). Set `InFlight` above the longest the route can take, and put a `Timeout` on the route: a handler that runs past it loses its claim and a duplicate may run it again.

The in-memory store holds `MaxKeys` keys (10,000) and `MaxBytes` bytes (16 MiB) and never more, at about 560 B for a 200 B answer. When it is full it drops the oldest answers, never a request that is running, and says so in the log at Warn; a retry of a dropped key runs again. A client that sends a new key on every request is also a handler that runs on every request, so put a [rate limiter](./security.md#limiting-requests) in front. A store that holds only running requests answers a new key 503 with `Retry-After`.

## Two instances

**The in-memory store is per instance.** Behind a balancer, a retry that lands on another instance runs the handler again. Give `Store` an `IdempotencyStore` that all instances share: a few methods that `Claim` a key atomically, `Complete` it with the answer and `Release` it, held to its promises by [`bitetest.TestIdempotencyStore`](../reference/bitetest.md#bitetesttestidempotencystore) ([testing a store you wrote](./testing.md#testing-a-store-you-wrote)). A Redis or Postgres store is a module when an application asks.

## Testing a handler behind it

Send the same request twice with the same key and read `Idempotent-Replayed` on the second, on a clock the test moves to pass the `TTL`.

<!-- compiles: decls -->
```go
func TestAChargeSentAgainIsAnsweredOnce(t *testing.T) {
	clk := bitetest.NewClock()
	app := bites.New(bites.WithClock(clk))
	app.Post("/accounts/{account}/charges", charge, bites.Use(bites.NewIdempotency(bites.IdempotencyOptions{}).Middleware()))
	srv := bitetest.Start(t, app)
	send := func() bitetest.Response {
		return srv.Do(bitetest.Request{
			Method: "POST", Target: "/accounts/7/charges", Body: []byte(`{"cents":500}`),
			Header: map[string][]string{"Content-Type": {"application/json"}, "Idempotency-Key": {"order-42"}},
		})
	}
	first, again := send(), send()
	if first.Get("idempotent-replayed") != "" || again.Get("idempotent-replayed") != "true" {
		t.Fatalf("the retry was not answered with the first answer: %v", again.Header)
	}
	clk.Advance(25 * time.Hour) // after the TTL the key is a new request
	if send().Get("idempotent-replayed") != "" {
		t.Fatal("an answer past its TTL was sent again")
	}
}
```
