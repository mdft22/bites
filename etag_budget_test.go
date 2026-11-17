package bites

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/nevindra/bites/internal/clock"
	"github.com/nevindra/bites/internal/memconn"
)

// The allocation axis of ADR 0003 for the validators of ADR 0089: a Tagged answer, its 304, an
// answer whose tag is made from the body, and a write that checks If-Match each cost what a typed
// route costs, and a route that uses none of them is the row it was.

type etagBudgetDoc struct {
	ID    int64  `json:"id"`
	Title string `json:"title"`
}

type etagBudgetPut struct {
	Path   struct{ ID int64 }
	Header struct{ IfMatch IfMatch }
	Body   struct{ Title string }
}

func TestATaggedGetItsMatching304AndATagMadeFromTheBodyAllocateTwice(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector changes allocation counts")
	}
	c := memRig(t, func(a *App) {
		a.Get("/docs/{id}", func(_ context.Context, in GetPlain) (Tagged[etagBudgetDoc], error) {
			return Tagged[etagBudgetDoc]{Body: etagBudgetDoc{ID: in.Path.ID, Title: "Ada"}, ETag: ETag{Version: "v1"}}, nil
		})
		a.Get("/hashed/{id}", func(_ context.Context, in GetPlain) (Tagged[etagBudgetDoc], error) {
			return Tagged[etagBudgetDoc]{Body: etagBudgetDoc{ID: in.Path.ID, Title: "Ada"}, ETag: ETagOfBody()}, nil
		})
	})
	for _, row := range []struct{ name, req, status string }{
		{"a Tagged with a version", "GET /docs/42 HTTP/1.1\r\nHost: x\r\n\r\n", "HTTP/1.1 200 OK"},
		{"its 304", "GET /docs/42 HTTP/1.1\r\nHost: x\r\nIf-None-Match: \"v1\"\r\n\r\n", "HTTP/1.1 304 Not Modified"},
		{"a Tagged made from the body", "GET /hashed/42 HTTP/1.1\r\nHost: x\r\n\r\n", "HTTP/1.1 200 OK"},
	} {
		got := keepAlive(t, c, []byte(row.req), row.status)
		t.Logf("allocations per request, %s: %v", row.name, got)
		if got != 2 {
			t.Errorf("%s: allocs/request = %v, want exactly 2 (ADR 0003: the slab and the context)", row.name, got)
		}
	}
}

func TestAPutThatChecksIfMatchAndAnswersATaggedAllocatesTwice(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector changes allocation counts")
	}
	c := memRig(t, func(a *App) {
		a.Put("/docs/{id}", func(_ context.Context, in etagBudgetPut) (Tagged[etagBudgetDoc], error) {
			if err := in.Header.IfMatch.Check("v1"); err != nil {
				return Tagged[etagBudgetDoc]{}, err
			}
			return Tagged[etagBudgetDoc]{Body: etagBudgetDoc{ID: in.Path.ID, Title: in.Body.Title}, ETag: ETag{Version: "v2"}}, nil
		})
	})
	body := `{"title":"Ada"}`
	put := fmt.Sprintf("PUT /docs/42 HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nIf-Match: \"v1\"\r\nContent-Length: %d\r\n\r\n%s", len(body), body)
	got := keepAlive(t, c, []byte(put), "HTTP/1.1 200 OK")
	t.Logf("allocations per request, a PUT that checks If-Match and answers a Tagged: %v", got)
	if got != 2 {
		t.Fatalf("allocs/request = %v, want exactly 2", got)
	}
}

type etagBenchList struct {
	Items []etagBudgetDoc `json:"items"`
}

// BenchmarkTaggedAnswers is the time axis of ADR 0089, not a guard: the same list answered plain,
// as a Tagged with a version, as a Tagged made from the body, and as the 304 of the version, over
// the in-memory connection, for a body of about 0.8 KB (20 items) and about 8 KB (200 items).
func BenchmarkTaggedAnswers(b *testing.B) {
	for _, n := range []int{20, 200} {
		items := make([]etagBudgetDoc, n)
		for i := range items {
			items[i] = etagBudgetDoc{ID: int64(i), Title: "title number " + strconv.Itoa(i*7919)}
		}
		app := New(WithClock(clock.NewFake(time.Now())), WithLogger(quiet()))
		app.Get("/plain", func(context.Context, struct{}) (etagBenchList, error) { return etagBenchList{items}, nil })
		app.Get("/version", func(context.Context, struct{}) (Tagged[etagBenchList], error) {
			return Tagged[etagBenchList]{Body: etagBenchList{items}, ETag: ETag{Version: "v1"}}, nil
		})
		app.Get("/body", func(context.Context, struct{}) (Tagged[etagBenchList], error) {
			return Tagged[etagBenchList]{Body: etagBenchList{items}, ETag: ETagOfBody()}, nil
		})
		ln := memconn.Listen(app.Clock())
		go app.Serve(ln)
		c, err := ln.Dial(memconn.Addr{Name: "203.0.113.9:4000"})
		if err != nil {
			b.Fatal(err)
		}
		for _, row := range []struct{ name, req string }{
			{"plain", "GET /plain HTTP/1.1\r\nHost: x\r\n\r\n"},
			{"version", "GET /version HTTP/1.1\r\nHost: x\r\n\r\n"},
			{"body", "GET /body HTTP/1.1\r\nHost: x\r\n\r\n"},
			{"version304", "GET /version HTTP/1.1\r\nHost: x\r\nIf-None-Match: \"v1\"\r\n\r\n"},
		} {
			b.Run(fmt.Sprintf("%s/%d", row.name, n), func(b *testing.B) {
				buf := make([]byte, 64<<10)
				c.Write([]byte(row.req))
				want, _ := c.Read(buf)
				for want < 1 {
					want, _ = c.Read(buf)
				}
				// The first read may be a part of the answer: read what is left of it.
				do := func() {
					c.Write([]byte(row.req))
					io.ReadFull(c, buf[:want])
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					do()
				}
			})
		}
		c.Close()
		app.Close()
	}
}
