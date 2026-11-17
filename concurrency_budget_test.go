package bites

import (
	"context"
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/nevindra/bites/internal/clock"
	"github.com/nevindra/bites/internal/memconn"
)

// The axes a ConcurrencyLimiter spends (ADR 0091, ADR 0003): none for a request that finds a free
// slot, and a counted number for one that waits and for one that is refused.

func TestATypedJSONPOSTBehindAConcurrencyLimiterWithAFreeSlotStillAllocatesTwice(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector changes allocation counts")
	}
	lim := NewConcurrencyLimiter(ConcurrencyOptions{Name: "users", Limit: 8})
	c := memRig(t, func(a *App) { a.Post("/users/{id}", budgetHandler, Use(lim.Middleware())) })
	got := keepAlive(t, c, post("/users/42?expand=true&page=3", typedBody), "HTTP/1.1 200 OK")
	t.Logf("a typed JSON POST behind a ConcurrencyLimiter, a slot free: %v allocations per request", got)
	if got != 2 {
		t.Fatalf("allocs/request = %v, want exactly 2: the limiter takes a free slot with no allocation", got)
	}
	// The route with a middleware that does nothing costs the same, so the limiter adds nothing.
	pass := func(ctx context.Context, _ Request, next Next) (Response, error) { return next.Call(ctx), nil }
	c2 := memRig(t, func(a *App) { a.Post("/users/{id}", budgetHandler, Use(pass)) })
	if base := keepAlive(t, c2, post("/users/42?expand=true&page=3", typedBody), "HTTP/1.1 200 OK"); base != got {
		t.Fatalf("the limiter costs %v allocations and an empty middleware %v", got, base)
	}
}

// limiterRigPair serves /hold, which blocks until gate has a token, and /users, both behind lim,
// and returns two connections.
func limiterRigPair(t *testing.T, lim *ConcurrencyLimiter) (hold, other net.Conn, gate chan struct{}) {
	t.Helper()
	gate = make(chan struct{})
	app := New(WithClock(clock.NewFake(time.Now())), WithLogger(quiet()))
	app.Get("/hold", func(context.Context, GetPlainNoPath) (budgetOut, error) { <-gate; return budgetOut{}, nil }, Use(lim.Middleware()))
	app.Post("/users/{id}", budgetHandler, Use(lim.Middleware()))
	ln := memconn.Listen(app.Clock())
	go app.Serve(ln)
	t.Cleanup(func() { app.Close() })
	dial := func() net.Conn {
		c, err := ln.Dial(memconn.Addr{Name: "203.0.113.9:4000"})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	return dial(), dial(), gate
}

type GetPlainNoPath struct{}

func spin(t *testing.T, cond func() bool) {
	t.Helper()
	for i := 0; !cond(); i++ {
		if i > 5_000_000 {
			t.Fatal("not reached")
		}
		runtime.Gosched()
	}
}

// cycle holds one slot with a request on hold, sends a typed POST on other and lets both finish,
// and returns the process's mallocs it took.
func limiterCycle(t *testing.T, lim *ConcurrencyLimiter, hold, other net.Conn, gate chan struct{}, wantOther int, buf []byte) uint64 {
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	hold.Write([]byte("GET /hold HTTP/1.1\r\nHost: x\r\n\r\n"))
	spin(t, func() bool { return lim.Stats().Running == 1 })
	other.Write(post("/users/42?expand=true&page=3", typedBody))
	if wantOther == 2 { // a second slot is free: the POST runs beside the hold and ends first
		readOne(other, buf)
		gate <- struct{}{}
		readOne(hold, buf)
	} else { // it waits for the hold's slot
		spin(t, func() bool { return lim.Stats().Waiting == 1 })
		gate <- struct{}{}
		readOne(hold, buf)
		readOne(other, buf)
	}
	runtime.ReadMemStats(&m1)
	return m1.Mallocs - m0.Mallocs
}

// readOne reads one whole response: head, then the Content-Length bytes of body.
func readOne(c net.Conn, buf []byte) {
	n, _ := c.Read(buf)
	for {
		head := string(buf[:n])
		i := indexOf(head, "\r\n\r\n")
		if i >= 0 {
			cl := 0
			if j := indexOf(head, "Content-Length: "); j >= 0 {
				for k := j + len("Content-Length: "); k < len(head) && head[k] >= '0' && head[k] <= '9'; k++ {
					cl = cl*10 + int(head[k]-'0')
				}
			}
			if n >= i+4+cl {
				return
			}
		}
		m, err := c.Read(buf[n:])
		if err != nil && err != io.EOF {
			return
		}
		n += m
	}
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func TestARequestThatWaitsInAConcurrencyLimitersBacklogCostsALogAttributeAndAChannelMore(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector changes allocation counts")
	}
	const rounds = 300
	run := func(limit int) float64 {
		lim := NewConcurrencyLimiter(ConcurrencyOptions{Name: "users", Limit: limit, Backlog: 2, MaxWait: time.Minute})
		hold, other, gate := limiterRigPair(t, lim)
		buf := make([]byte, 16<<10)
		want := 1
		if limit == 2 {
			want = 2
		}
		for range 50 { // warm the pools
			limiterCycle(t, lim, hold, other, gate, want, buf)
		}
		var total uint64
		for range rounds {
			total += limiterCycle(t, lim, hold, other, gate, want, buf)
		}
		return float64(total) / rounds
	}
	free, queued := run(2), run(1)
	t.Logf("a hold and a typed POST: %.1f mallocs a cycle when the POST finds a free slot, %.1f when it waits for the hold's: %.1f more", free, queued, queued-free)
	if d := queued - free; d > 6 {
		t.Fatalf("waiting costs %.1f allocations more than not waiting, over the 6 the ADR states", d)
	}
}

func TestARequestRefusedByAConcurrencyLimiterCostsWhatAnErrorAnswerCosts(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector changes allocation counts")
	}
	lim := NewConcurrencyLimiter(ConcurrencyOptions{Name: "users", Limit: 1, NoBacklog: true})
	hold, other, gate := limiterRigPair(t, lim)
	hold.Write([]byte("GET /hold HTTP/1.1\r\nHost: x\r\n\r\n"))
	spin(t, func() bool { return lim.Stats().Running == 1 })
	got := keepAlive(t, other, post("/users/42?expand=true&page=3", typedBody), "HTTP/1.1 503 Service Unavailable")
	// The same answer from a middleware that returns a 503 error it made beforehand: what any
	// refusal from a middleware costs, which the limiter must not exceed.
	refusal := ServiceUnavailable("busy").WithHeader("Retry-After", "1")
	c2 := memRig(t, func(a *App) {
		a.Post("/users/{id}", budgetHandler, Use(func(context.Context, Request, Next) (Response, error) { return Response{}, refusal }))
	})
	base := keepAlive(t, c2, post("/users/42?expand=true&page=3", typedBody), "HTTP/1.1 503 Service Unavailable")
	t.Logf("a typed JSON POST refused by a ConcurrencyLimiter: %v allocations per request; refused by a middleware returning a ready 503: %v", got, base)
	if got != base {
		t.Fatalf("allocs/request = %v, but a middleware that returns a ready 503 costs %v: a refusal must allocate nothing of its own", got, base)
	}
	gate <- struct{}{}
	readOne(hold, make([]byte, 4096))
}
