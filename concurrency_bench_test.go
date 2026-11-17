package bites

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/nevindra/bites/internal/clock"
	"github.com/nevindra/bites/internal/memconn"
)

// BenchmarkAConcurrencyLimiterWithAFreeSlot is the cost of the limiter's fast path under
// parallel load, against a middleware that does nothing, on in-memory connections (ADR 0091).
func BenchmarkAConcurrencyLimiterWithAFreeSlot(b *testing.B) {
	pass := func(ctx context.Context, _ Request, next Next) (Response, error) { return next.Call(ctx), nil }
	for _, tc := range []struct {
		name string
		mw   Middleware
	}{
		{"empty middleware", pass},
		{"limiter", NewConcurrencyLimiter(ConcurrencyOptions{Name: "bench", Limit: 1 << 20}).Middleware()},
	} {
		b.Run(tc.name, func(b *testing.B) {
			app := New(WithClock(clock.NewFake(time.Now())), WithLogger(quiet()))
			app.Get("/users/{id}", func(_ context.Context, in GetPlain) (budgetOut, error) { return budgetOut{ID: in.Path.ID}, nil }, Use(tc.mw))
			ln := memconn.Listen(app.Clock())
			go app.Serve(ln)
			defer app.Close()
			request := []byte("GET /users/42 HTTP/1.1\r\nHost: x\r\n\r\n")
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				c, err := ln.Dial(memconn.Addr{Name: "203.0.113.9:4000"})
				if err != nil {
					b.Error(err)
					return
				}
				defer c.Close()
				buf := make([]byte, 4096)
				want := 0
				for pb.Next() {
					c.Write(request)
					if want == 0 {
						want, _ = c.Read(buf)
						continue
					}
					io.ReadFull(c, buf[:want])
				}
			})
		})
	}
}
