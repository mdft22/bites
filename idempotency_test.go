package bites_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nevindra/bites"
	"github.com/nevindra/bites/bitetest"
)

type payIn struct {
	Path struct{ ID int }
	Body struct{ Amount int }
}

type payOut struct {
	N      int64
	Amount int
}

// payments serves POST /accounts/{id}/payments behind idem, counting how many times the handler ran.
type payments struct {
	calls atomic.Int64
	fail  atomic.Int64 // the handler returns a plain error this many more times
	hold  chan struct{}
	enter chan struct{}
	big   int // when set, the answer carries this many bytes of padding
}

func (p *payments) handler(_ context.Context, in payIn) (bites.Created[payOut], error) {
	n := p.calls.Add(1)
	if p.enter != nil {
		p.enter <- struct{}{}
	}
	if p.hold != nil {
		<-p.hold
	}
	if p.fail.Add(-1) >= 0 {
		return bites.Created[payOut]{}, errors.New("the bank is down")
	}
	return bites.Created[payOut]{
		Body:     payOut{N: n, Amount: in.Body.Amount},
		Location: fmt.Sprintf("/payments/%d", n),
		Header:   bites.Header{"X-Receipt": {fmt.Sprintf("r%d", n)}},
	}, nil
}

func startPayments(t *testing.T, o bites.IdempotencyOptions, p *payments, opts ...bites.Option) (*bitetest.Server, *bitetest.Clock, *logBuf) {
	t.Helper()
	clk := bitetest.NewClock()
	srv, logs := startLogged(t, func(app *bites.App) {
		app.Post("/accounts/{id}/payments", p.handler, bites.Use(bites.NewIdempotency(o).Middleware()))
	}, append([]bites.Option{bites.WithClock(clk)}, opts...)...)
	return srv, clk, logs
}

func pay(key, body string, header ...string) bitetest.Request {
	r := jsonReq("POST", "/accounts/7/payments", body)
	if key != "" {
		r.Header["Idempotency-Key"] = []string{key}
	}
	for i := 0; i+1 < len(header); i += 2 {
		r.Header[header[i]] = append(r.Header[header[i]], header[i+1])
	}
	return r
}

func TestAWriteSentAgainWithTheSameKeyIsAnsweredWithTheFirstAnswerAndTheHandlerRunsOnce(t *testing.T) {
	p := &payments{}
	srv, _, _ := startPayments(t, bites.IdempotencyOptions{}, p)
	first := srv.Do(pay("order-1", `{"amount":50}`))
	again := srv.Do(pay("order-1", `{"amount":50}`))
	if first.Status != 201 || first.Text() != `{"n":1,"amount":50}` || first.Get("location") != "/payments/1" || first.Get("x-receipt") != "r1" {
		t.Fatalf("first: %d %v %s", first.Status, first.Header, first.Text())
	}
	if again.Status != 201 || again.Text() != first.Text() || again.Get("location") != "/payments/1" || again.Get("x-receipt") != "r1" || again.Get("content-type") != "application/json" {
		t.Fatalf("again: %d %v %s", again.Status, again.Header, again.Text())
	}
	if first.Get("idempotent-replayed") != "" || again.Get("idempotent-replayed") != "true" {
		t.Fatalf("replayed header: first %q, again %q", first.Get("idempotent-replayed"), again.Get("idempotent-replayed"))
	}
	if n := p.calls.Load(); n != 1 {
		t.Fatalf("the handler ran %d times", n)
	}
}

func TestAKeySentInQuotesIsTheSameKeyAsOneSentBare(t *testing.T) {
	p := &payments{}
	srv, _, _ := startPayments(t, bites.IdempotencyOptions{}, p)
	srv.Do(pay(`"k-1"`, `{"amount":1}`))
	if r := srv.Do(pay("k-1", `{"amount":1}`)); r.Get("idempotent-replayed") != "true" || p.calls.Load() != 1 {
		t.Fatalf("%v, ran %d times", r.Header, p.calls.Load())
	}
}

func TestARequestWithoutAKeyIsNotProtectedUnlessTheKeyIsRequired(t *testing.T) {
	p := &payments{}
	srv, _, _ := startPayments(t, bites.IdempotencyOptions{}, p)
	srv.Do(pay("", `{"amount":1}`))
	srv.Do(pay("", `{"amount":1}`))
	if n := p.calls.Load(); n != 2 {
		t.Fatalf("the handler ran %d times for two requests with no key", n)
	}
}

func TestARequiredKeyThatIsMissingIsA400NamingTheHeader(t *testing.T) {
	p := &payments{}
	srv, _, _ := startPayments(t, bites.IdempotencyOptions{Required: true}, p)
	r := srv.Do(pay("", `{"amount":1}`))
	want := `{"title":"Bad Request","status":400,"detail":"the Idempotency-Key header is required: send a key that is the same for every retry of this request and new for a different one","errors":[{"location":"header.Idempotency-Key","message":"the Idempotency-Key header is required: send a key that is the same for every retry of this request and new for a different one"}]}`
	wantProblem(t, r, 400, want)
	if p.calls.Load() != 0 {
		t.Fatal("the handler ran")
	}
}

func TestAKeyThatIsEmptyTooLongOrMadeOfForbiddenCharactersIsA400NamingTheHeader(t *testing.T) {
	p := &payments{}
	srv, _, _ := startPayments(t, bites.IdempotencyOptions{MaxKeyLength: 16}, p)
	for name, key := range map[string]string{
		"empty": " ", "too long": strings.Repeat("k", 17), "a space": "a b", "a comma": "a,b", "a quote": `a"b`, "a non-ASCII character": "café",
	} {
		r := srv.Do(pay(key, `{"amount":1}`))
		if r.Status != 400 || !strings.Contains(r.Text(), `"location":"header.Idempotency-Key"`) || !strings.Contains(r.Text(), "1 to 16 characters") {
			t.Errorf("%s: %d %s", name, r.Status, r.Text())
		}
	}
	if p.calls.Load() != 0 {
		t.Fatal("the handler ran")
	}
}

func TestAKeySentTwiceIsA400BecauseNoOneCanSayWhichIsMeant(t *testing.T) {
	srv, _, _ := startPayments(t, bites.IdempotencyOptions{}, &payments{})
	r := srv.Do(pay("a", `{"amount":1}`, "Idempotency-Key", "b"))
	if r.Status != 400 || !strings.Contains(r.Text(), "is sent 2 times: send it once") {
		t.Fatalf("%d %s", r.Status, r.Text())
	}
}

func TestTheSameKeyWithAnotherBodyPathOrMethodIsA422AndTheHandlerDoesNotRun(t *testing.T) {
	p := &payments{}
	srv, _, _ := startPayments(t, bites.IdempotencyOptions{}, p)
	srv.Do(pay("k", `{"amount":50}`))
	for name, r := range map[string]bitetest.Request{
		"another body": pay("k", `{"amount":51}`),
		"another path": func() bitetest.Request { r := pay("k", `{"amount":50}`); r.Target = "/accounts/8/payments"; return r }(),
		"another query": func() bitetest.Request {
			r := pay("k", `{"amount":50}`)
			r.Target += "?dry=1"
			return r
		}(),
	} {
		got := srv.Do(r)
		if got.Status != 422 || !strings.Contains(got.Text(), "was already used with a different request") {
			t.Errorf("%s: %d %s", name, got.Status, got.Text())
		}
	}
	if n := p.calls.Load(); n != 1 {
		t.Fatalf("the handler ran %d times", n)
	}
}

func TestADuplicateWhileTheFirstRunsIsA409WithRetryAfterAndOnceItFinishedItIsReplayed(t *testing.T) {
	p := &payments{hold: make(chan struct{}), enter: make(chan struct{}, 4)}
	srv, _, _ := startPayments(t, bites.IdempotencyOptions{}, p)
	first := doAsync(srv, pay("k", `{"amount":50}`))
	<-p.enter
	r := srv.Do(pay("k", `{"amount":50}`))
	if r.Status != 409 || r.Get("retry-after") != "1" || !strings.Contains(r.Text(), "is still running") {
		t.Fatalf("%d %v %s", r.Status, r.Header, r.Text())
	}
	close(p.hold)
	if f := wait(t, first); f.Status != 201 {
		t.Fatalf("first: %d", f.Status)
	}
	if r := srv.Do(pay("k", `{"amount":50}`)); r.Status != 201 || r.Get("idempotent-replayed") != "true" {
		t.Fatalf("after it finished: %d %v", r.Status, r.Header)
	}
	if n := p.calls.Load(); n != 1 {
		t.Fatalf("the handler ran %d times", n)
	}
}

func TestAnotherCredentialCannotReplayAKeyAndTheSameOneCan(t *testing.T) {
	p := &payments{}
	srv, _, _ := startPayments(t, bites.IdempotencyOptions{}, p)
	a := srv.Do(pay("k", `{"amount":50}`, "Authorization", "Bearer alice"))
	b := srv.Do(pay("k", `{"amount":50}`, "Authorization", "Bearer bob"))
	a2 := srv.Do(pay("k", `{"amount":50}`, "Authorization", "Bearer alice"))
	if b.Get("idempotent-replayed") != "" || b.Get("x-receipt") != "r2" {
		t.Fatalf("bob was answered with alice's answer: %v %s", b.Header, b.Text())
	}
	if a2.Get("idempotent-replayed") != "true" || a2.Get("x-receipt") != a.Get("x-receipt") {
		t.Fatalf("alice's retry: %v", a2.Header)
	}
	if n := p.calls.Load(); n != 2 {
		t.Fatalf("the handler ran %d times", n)
	}
}

func TestAScopeFunctionDecidesWhoOwnsAKey(t *testing.T) {
	p := &payments{}
	scope := func(r bites.Request) string { return r.Header("X-Tenant") }
	srv, _, _ := startPayments(t, bites.IdempotencyOptions{Scope: scope}, p)
	srv.Do(pay("k", `{"amount":1}`, "X-Tenant", "t1", "Authorization", "Bearer one"))
	same := srv.Do(pay("k", `{"amount":1}`, "X-Tenant", "t1", "Authorization", "Bearer two"))
	other := srv.Do(pay("k", `{"amount":1}`, "X-Tenant", "t2"))
	if same.Get("idempotent-replayed") != "true" || other.Get("idempotent-replayed") != "" || p.calls.Load() != 2 {
		t.Fatalf("same tenant %v, other tenant %v, ran %d times", same.Header, other.Header, p.calls.Load())
	}
}

func TestTheSameKeyOnTwoRoutesIsTwoKeys(t *testing.T) {
	var a, b atomic.Int64
	idem := bites.NewIdempotency(bites.IdempotencyOptions{})
	srv := start(t, func(app *bites.App) {
		g := app.Group("/v", bites.Use(idem.Middleware()))
		g.Post("/a", func(context.Context, struct{}) (string, error) { return fmt.Sprint("a", a.Add(1)), nil })
		g.Post("/b", func(context.Context, struct{}) (string, error) { return fmt.Sprint("b", b.Add(1)), nil })
	})
	send := func(path string) bitetest.Response {
		r := req("POST", path)
		r.Header = map[string][]string{"Idempotency-Key": {"k"}}
		return srv.Do(r)
	}
	send("/v/a")
	send("/v/b")
	if r := send("/v/a"); r.Get("idempotent-replayed") != "true" || a.Load() != 1 || b.Load() != 1 {
		t.Fatalf("%v, a ran %d, b ran %d", r.Header, a.Load(), b.Load())
	}
}

func TestAnAnswerThatIsNot2xxOr3xxIsNotKeptSoTheRetryRunsAgain(t *testing.T) {
	p := &payments{}
	p.fail.Store(1)
	srv, _, _ := startPayments(t, bites.IdempotencyOptions{}, p)
	if r := srv.Do(pay("k", `{"amount":1}`)); r.Status != 500 {
		t.Fatalf("first: %d", r.Status)
	}
	r := srv.Do(pay("k", `{"amount":1}`))
	if r.Status != 201 || r.Get("idempotent-replayed") != "" || p.calls.Load() != 2 {
		t.Fatalf("retry: %d %v, ran %d times", r.Status, r.Header, p.calls.Load())
	}
	// a 400 for the body is the framework's: the key is free for the corrected request
	if r := srv.Do(pay("k2", `{"amount":"x"}`)); r.Status != 400 {
		t.Fatalf("%d", r.Status)
	}
	if r := srv.Do(pay("k2", `{"amount":2}`)); r.Status != 201 {
		t.Fatalf("after the fix: %d %s", r.Status, r.Text())
	}
}

func TestAPanickingHandlerReleasesTheClaimSoTheRetryIsNotAConflict(t *testing.T) {
	var calls atomic.Int64
	idem := bites.NewIdempotency(bites.IdempotencyOptions{})
	srv := start(t, func(app *bites.App) {
		app.Post("/p", func(context.Context, struct{}) (string, error) {
			if calls.Add(1) == 1 {
				panic("boom")
			}
			return "ok", nil
		}, bites.Use(idem.Middleware()))
	})
	r := req("POST", "/p")
	r.Header = map[string][]string{"Idempotency-Key": {"k"}}
	if got := srv.Do(r); got.Status != 500 {
		t.Fatalf("first: %d", got.Status)
	}
	if got := srv.Do(r); got.Status != 200 {
		t.Fatalf("retry after the panic: %d %s", got.Status, got.Text())
	}
}

func TestAnAnswerIsKeptForTheTTLAndThenTheKeyIsNew(t *testing.T) {
	p := &payments{}
	srv, clk, _ := startPayments(t, bites.IdempotencyOptions{TTL: time.Hour}, p)
	srv.Do(pay("k", `{"amount":1}`))
	clk.Advance(59 * time.Minute)
	if r := srv.Do(pay("k", `{"amount":1}`)); r.Get("idempotent-replayed") != "true" {
		t.Fatalf("before the TTL: %v", r.Header)
	}
	clk.Advance(2 * time.Minute)
	if r := srv.Do(pay("k", `{"amount":1}`)); r.Get("idempotent-replayed") != "" || p.calls.Load() != 2 {
		t.Fatalf("after the TTL: %v, ran %d times", r.Header, p.calls.Load())
	}
}

func TestAnAnswerOverMaxAnswerIsNotKeptAndADuplicateIsAConflictNotASecondRun(t *testing.T) {
	var calls atomic.Int64
	idem := bites.NewIdempotency(bites.IdempotencyOptions{MaxAnswer: 100})
	srv, logs := startLogged(t, func(app *bites.App) {
		app.Post("/big", func(context.Context, struct{}) (string, error) {
			calls.Add(1)
			return strings.Repeat("x", 500), nil
		}, bites.Use(idem.Middleware()))
	})
	r := req("POST", "/big")
	r.Header = map[string][]string{"Idempotency-Key": {"k"}}
	if got := srv.Do(r); got.Status != 200 || len(got.Body) != 502 {
		t.Fatalf("first: %d, %d bytes", got.Status, len(got.Body))
	}
	got := srv.Do(r)
	if got.Status != 409 || !strings.Contains(got.Text(), "too large to keep") || calls.Load() != 1 {
		t.Fatalf("duplicate: %d %s, ran %d times", got.Status, got.Text(), calls.Load())
	}
	if l := logs.String(); !strings.Contains(l, "level=WARN") || !strings.Contains(l, "the answer body is 502 bytes, over MaxAnswer") || !strings.Contains(l, "limit=100") {
		t.Fatalf("log: %s", l)
	}
}

func TestOnlyPostAndPatchAreClaimed(t *testing.T) {
	var puts atomic.Int64
	idem := bites.NewIdempotency(bites.IdempotencyOptions{Required: true})
	srv := start(t, func(app *bites.App) {
		app.Put("/x", func(context.Context, struct{}) (string, error) { return fmt.Sprint(puts.Add(1)), nil }, bites.Use(idem.Middleware()))
		app.Patch("/x", func(context.Context, struct{}) (string, error) { return "patched", nil }, bites.Use(idem.Middleware()))
	})
	put := req("PUT", "/x")
	put.Header = map[string][]string{"Idempotency-Key": {"k"}}
	srv.Do(put)
	srv.Do(put)
	if puts.Load() != 2 {
		t.Fatalf("a PUT, idempotent by definition, ran %d times for two requests", puts.Load())
	}
	if r := srv.Do(req("PUT", "/x")); r.Status != 200 {
		t.Fatalf("a PUT needs no key: %d", r.Status)
	}
	if r := srv.Do(req("PATCH", "/x")); r.Status != 400 {
		t.Fatalf("a PATCH with Required and no key: %d", r.Status)
	}
}

func TestAnAnswerIsKeptWhenTheClientHasGoneAwayBeforeItWasReady(t *testing.T) {
	p := &payments{hold: make(chan struct{}), enter: make(chan struct{}, 4)}
	srv, _, _ := startPayments(t, bites.IdempotencyOptions{}, p)
	c := srv.Dial("")
	r := pay("k", `{"amount":50}`)
	fmt.Fprintf(c, "POST %s HTTP/1.1\r\nHost: test\r\nContent-Type: application/json\r\nIdempotency-Key: k\r\nContent-Length: %d\r\n\r\n%s", r.Target, len(r.Body), r.Body)
	<-p.enter
	c.Close() // the client times out and goes away while the handler is still working
	time.Sleep(50 * time.Millisecond)
	close(p.hold)
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := srv.Do(pay("k", `{"amount":50}`))
		if got.Status == 201 {
			if got.Get("idempotent-replayed") != "true" || p.calls.Load() != 1 {
				t.Fatalf("%v, ran %d times", got.Header, p.calls.Load())
			}
			return
		}
		if got.Status != 409 || time.Now().After(deadline) {
			t.Fatalf("the retry got %d %s", got.Status, got.Text())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestAFloodOfUniqueKeysNeverHoldsMoreThanMaxKeysAndSaysItDroppedAnswers(t *testing.T) {
	p := &payments{}
	srv, _, logs := startPayments(t, bites.IdempotencyOptions{MaxKeys: 20}, p)
	for i := range 200 {
		if r := srv.Do(pay(fmt.Sprintf("flood-%d", i), `{"amount":1}`)); r.Status != 201 {
			t.Fatalf("%d: %d", i, r.Status)
		}
	}
	if r := srv.Do(pay("flood-199", `{"amount":1}`)); r.Get("idempotent-replayed") != "true" {
		t.Fatalf("the newest key was dropped: %v", r.Header)
	}
	if r := srv.Do(pay("flood-0", `{"amount":1}`)); r.Get("idempotent-replayed") != "" {
		t.Fatalf("the oldest key was kept past MaxKeys: %v", r.Header)
	}
	if l := logs.String(); !strings.Contains(l, "level=WARN") || !strings.Contains(l, "dropped answers before their TTL") {
		t.Fatalf("the drop was not logged: %s", l)
	}
}

func TestAStoreHoldingOnlyRunningRequestsAnswers503WithRetryAfterAndRunsNothing(t *testing.T) {
	p := &payments{hold: make(chan struct{}), enter: make(chan struct{}, 4)}
	srv, _, _ := startPayments(t, bites.IdempotencyOptions{MaxKeys: 2}, p)
	a := doAsync(srv, pay("a", `{"amount":1}`))
	b := doAsync(srv, pay("b", `{"amount":1}`))
	<-p.enter
	<-p.enter
	r := srv.Do(pay("c", `{"amount":1}`))
	if r.Status != 503 || r.Get("retry-after") != "1" {
		t.Fatalf("%d %v %s", r.Status, r.Header, r.Text())
	}
	close(p.hold)
	wait(t, a)
	wait(t, b)
	if p.calls.Load() != 2 {
		t.Fatalf("the handler ran %d times", p.calls.Load())
	}
}

type failingIdempotencyStore struct{ bites.IdempotencyStore }

func (failingIdempotencyStore) Claim(context.Context, string, string, time.Time, time.Duration) (bites.IdempotencyEntry, error) {
	return bites.IdempotencyEntry{}, errors.New("redis is down")
}

func TestAStoreThatFailsIsA500AndTheHandlerDoesNotRun(t *testing.T) {
	p := &payments{}
	srv, _, _ := startPayments(t, bites.IdempotencyOptions{Store: failingIdempotencyStore{}}, p)
	if r := srv.Do(pay("k", `{"amount":1}`)); r.Status != 500 || p.calls.Load() != 0 {
		t.Fatalf("%d, ran %d times", r.Status, p.calls.Load())
	}
}

func TestARetryIsLoggedAsReplayedInTheAccessLog(t *testing.T) {
	p := &payments{}
	srv, logs := startLogged(t, func(app *bites.App) {
		app.Post("/accounts/{id}/payments", p.handler, bites.Use(bites.NewIdempotency(bites.IdempotencyOptions{}).Middleware()))
	}, bites.WithObserver(bites.AccessLog()))
	srv.Do(pay("k", `{"amount":1}`))
	srv.Do(pay("k", `{"amount":1}`))
	srv.WaitEnded()
	if l := logs.String(); strings.Count(l, "idempotency=replayed") != 1 {
		t.Fatalf("%s", l)
	}
}

func TestIdempotencyOnARouteThatReturnsAStreamOrReadsAnUploadIsRefusedAtRegistration(t *testing.T) {
	idem := bites.NewIdempotency(bites.IdempotencyOptions{}).Middleware()
	for name, f := range map[string]func(app *bites.App){
		"a stream": func(app *bites.App) {
			app.Post("/s", func(context.Context, struct{}) (bites.Stream, error) { return bites.Stream{}, nil }, bites.Use(idem))
		},
		"an upload": func(app *bites.App) {
			app.Post("/u", func(context.Context, struct{ Body bites.Upload }) (string, error) { return "", nil }, bites.MaxBody(1<<20), bites.Use(idem))
		},
		"the app": func(app *bites.App) { app.Use(idem) },
	} {
		func() {
			defer func() {
				e, _ := recover().(*bites.RegistrationError)
				if e == nil || !strings.Contains(e.Error(), "Idempotency") {
					t.Errorf("%s: %v", name, e)
				}
			}()
			f(bites.New())
		}()
	}
}

func TestTheMemoryIdempotencyStoreKeepsTheContractOfEveryIdempotencyStore(t *testing.T) {
	bitetest.TestIdempotencyStore(t, func(*testing.T) bites.IdempotencyStore { return bites.MemoryIdempotencyStore() })
}
