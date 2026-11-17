package bites_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nevindra/bites"
	"github.com/nevindra/bites/bitetest"
)

// limiterRig is a route whose handler holds each request until the test lets it go, counting how
// many run at once.
type limiterRig struct {
	entered chan string
	release chan struct{}
	running atomic.Int64
	peak    atomic.Int64
	started atomic.Int64
}

func newLimiterRig() *limiterRig {
	return &limiterRig{entered: make(chan string, 1024), release: make(chan struct{})}
}

type limiterIn struct {
	Query struct{ ID bites.Opt[string] }
}

func (g *limiterRig) handler(ctx context.Context, in limiterIn) (Empty, error) {
	n := g.running.Add(1)
	defer g.running.Add(-1)
	for p := g.peak.Load(); n > p && !g.peak.CompareAndSwap(p, n); p = g.peak.Load() {
	}
	g.started.Add(1)
	g.entered <- in.Query.ID.Or("")
	<-g.release
	return Empty{}, nil
}

// limiterUntil waits for cond, polling in real time, and fails the test after five seconds.
func limiterUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("not reached in 5 s: %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// shedRecorder is an Observer that keeps the Outcome of every request.
type shedRecorder struct {
	mu   sync.Mutex
	outs []bites.Outcome
}

func (r *shedRecorder) Start(ctx context.Context, _ bites.Request) context.Context { return ctx }
func (r *shedRecorder) End(_ context.Context, _ bites.Request, out bites.Outcome) {
	r.mu.Lock()
	r.outs = append(r.outs, out)
	r.mu.Unlock()
}

func (r *shedRecorder) withStatus(s int) (n int, shed int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, o := range r.outs {
		if o.Status == s {
			n++
			if errors.Is(o.Err, bites.ErrShed) {
				shed++
			}
		}
	}
	return n, shed
}

func TestARequestUnderTheLimitRunsAtOnceAndNeverWaits(t *testing.T) {
	rig := newLimiterRig()
	close(rig.release)
	lim := bites.NewConcurrencyLimiter(bites.ConcurrencyOptions{Name: "reports", Limit: 2})
	srv := startWith(t, func(app *bites.App) { app.Get("/r", rig.handler, bites.Use(lim.Middleware())) })
	for range 5 {
		if r := srv.Do(req("GET", "/r")); r.Status != 200 {
			t.Fatalf("%d %s", r.Status, r.Text())
		}
	}
	s := lim.Stats()
	if s.Queued != 0 || s.Running != 0 || s.Waiting != 0 || s.ShedFull+s.ShedWait+s.ShedShutdown != 0 {
		t.Fatalf("%+v", s)
	}
}

func TestRequestsOverTheLimitWaitAndRunInTheOrderTheyArrived(t *testing.T) {
	rig := newLimiterRig()
	lim := bites.NewConcurrencyLimiter(bites.ConcurrencyOptions{Name: "reports", Limit: 1, Backlog: 4})
	srv := startWith(t, func(app *bites.App) { app.Get("/r", rig.handler, bites.Use(lim.Middleware())) })
	chs := make([]<-chan bitetest.Response, 5)
	for i := range chs {
		chs[i] = doAsync(srv, req("GET", "/r?id="+strconv.Itoa(i)))
		want := i
		limiterUntil(t, "request "+strconv.Itoa(i)+" to run or wait", func() bool {
			s := lim.Stats()
			return s.Running+s.Waiting == want+1
		})
	}
	if s := lim.Stats(); s.Running != 1 || s.Waiting != 4 {
		t.Fatalf("%+v", s)
	}
	var order []string
	for range 5 {
		order = append(order, <-rig.entered)
		rig.release <- struct{}{}
	}
	if got := strings.Join(order, ","); got != "0,1,2,3,4" {
		t.Fatalf("they ran in the order %s", got)
	}
	for i, ch := range chs {
		if r := wait(t, ch); r.Status != 200 {
			t.Fatalf("request %d: %d %s", i, r.Status, r.Text())
		}
	}
	if s := lim.Stats(); s.Waited != 4 || s.Queued != 4 || s.Running != 0 || s.Waiting != 0 {
		t.Fatalf("%+v", s)
	}
}

func TestARequestPastTheBacklogIsA503ProblemNamingTheLimiterWithRetryAfter(t *testing.T) {
	rig := newLimiterRig()
	lim := bites.NewConcurrencyLimiter(bites.ConcurrencyOptions{Name: "reports", Limit: 1, Backlog: 1, MaxWait: 3 * time.Second})
	rec := &shedRecorder{}
	srv := startWith(t, func(app *bites.App) {
		app.Use(bites.RequestID())
		app.Get("/r", rig.handler, bites.Use(lim.Middleware()))
	}, bites.WithObserver(rec))
	running := doAsync(srv, req("GET", "/r"))
	<-rig.entered
	waiting := doAsync(srv, req("GET", "/r"))
	limiterUntil(t, "one request waiting", func() bool { return lim.Stats().Waiting == 1 })
	r := srv.Do(req("GET", "/r"))
	if r.Status != 503 || r.Get("Retry-After") != "3" || !strings.HasPrefix(r.Get("Content-Type"), "application/problem+json") {
		t.Fatalf("%d %v %s", r.Status, r.Header, r.Text())
	}
	for _, want := range []string{`\"reports\" limit of 1 running requests`, "backlog of 1 is full", `"request_id"`} {
		if !strings.Contains(r.Text(), want) {
			t.Fatalf("the answer does not say %q: %s", want, r.Text())
		}
	}
	limiterUntil(t, "the shed request ended", func() bool { n, shed := rec.withStatus(503); return n == 1 && shed == 1 })
	if s := lim.Stats(); s.ShedFull != 1 || s.Running != 1 || s.Waiting != 1 {
		t.Fatalf("%+v", s)
	}
	close(rig.release)
	for _, ch := range []<-chan bitetest.Response{running, waiting} {
		if r := wait(t, ch); r.Status != 200 {
			t.Fatalf("%d", r.Status)
		}
	}
}

func TestARequestThatWaitsMaxWaitIsA503AndLeavesTheBacklog(t *testing.T) {
	clk := bitetest.NewClock()
	rig := newLimiterRig()
	lim := bites.NewConcurrencyLimiter(bites.ConcurrencyOptions{Name: "reports", Limit: 1, Backlog: 2, MaxWait: 2 * time.Second})
	srv := startWith(t, func(app *bites.App) { app.Get("/r", rig.handler, bites.Use(lim.Middleware())) }, bites.WithClock(clk))
	running := doAsync(srv, req("GET", "/r"))
	<-rig.entered
	waiting := doAsync(srv, req("GET", "/r"))
	limiterUntil(t, "one request waiting", func() bool { return lim.Stats().Waiting == 1 })
	clk.Advance(time.Second)
	if lim.Stats().Waiting != 1 {
		t.Fatal("it left before MaxWait")
	}
	clk.Advance(time.Second)
	r := wait(t, waiting)
	if r.Status != 503 || r.Get("Retry-After") != "2" || !strings.Contains(r.Text(), "no slot freed within 2s") {
		t.Fatalf("%d %v %s", r.Status, r.Header, r.Text())
	}
	if s := lim.Stats(); s.ShedWait != 1 || s.Waiting != 0 || s.Running != 1 {
		t.Fatalf("%+v", s)
	}
	// Its place is free again, and the slot it never took still belongs to the running request.
	close(rig.release)
	if r := wait(t, running); r.Status != 200 {
		t.Fatalf("%d", r.Status)
	}
	if r := srv.Do(req("GET", "/r")); r.Status != 200 {
		t.Fatalf("%d %s", r.Status, r.Text())
	}
	if s := lim.Stats(); s.Running != 0 || s.Waiting != 0 {
		t.Fatalf("%+v", s)
	}
}

func TestAClientThatDisconnectsWhileQueuedLeavesTheBacklogAndFreesItsPlace(t *testing.T) {
	rig := newLimiterRig()
	lim := bites.NewConcurrencyLimiter(bites.ConcurrencyOptions{Name: "reports", Limit: 1, Backlog: 1})
	rec := &shedRecorder{}
	srv := startWith(t, func(app *bites.App) { app.Get("/r", rig.handler, bites.Use(lim.Middleware())) }, bites.WithObserver(rec))
	running := doAsync(srv, req("GET", "/r"))
	<-rig.entered
	c := srv.Dial("203.0.113.9:1")
	fmt.Fprint(c, "GET /r HTTP/1.1\r\nHost: x\r\n\r\n")
	limiterUntil(t, "the client waiting", func() bool { return lim.Stats().Waiting == 1 })
	c.Close()
	limiterUntil(t, "the client to leave the backlog", func() bool { s := lim.Stats(); return s.Waiting == 0 && s.Left == 1 })
	// The place it gave back is taken by the next request, which is not shed.
	next := doAsync(srv, req("GET", "/r"))
	limiterUntil(t, "the next request waiting", func() bool { return lim.Stats().Waiting == 1 })
	close(rig.release)
	for _, ch := range []<-chan bitetest.Response{running, next} {
		if r := wait(t, ch); r.Status != 200 {
			t.Fatalf("%d", r.Status)
		}
	}
	if s := lim.Stats(); s.ShedFull != 0 || s.Running != 0 || s.Waiting != 0 {
		t.Fatalf("%+v", s)
	}
}

func TestATimeoutOutsideTheLimiterTakesAWaitingRequestOutOfTheBacklog(t *testing.T) {
	clk := bitetest.NewClock()
	rig := newLimiterRig()
	lim := bites.NewConcurrencyLimiter(bites.ConcurrencyOptions{Name: "reports", Limit: 1, Backlog: 1, MaxWait: 30 * time.Second})
	srv := startWith(t, func(app *bites.App) {
		app.Get("/r", rig.handler, bites.Use(bites.Timeout(time.Second), lim.Middleware()))
	}, bites.WithClock(clk))
	running := doAsync(srv, req("GET", "/r"))
	<-rig.entered
	waiting := doAsync(srv, req("GET", "/r"))
	limiterUntil(t, "one request waiting", func() bool { return lim.Stats().Waiting == 1 })
	clk.Advance(2 * time.Second)
	if r := wait(t, waiting); r.Status != 503 {
		t.Fatalf("%d %s", r.Status, r.Text())
	}
	if s := lim.Stats(); s.Waiting != 0 || s.Left < 1 {
		t.Fatalf("%+v", s)
	}
	close(rig.release)
	wait(t, running)
}

func TestAPanicInAHandlerReleasesItsSlot(t *testing.T) {
	lim := bites.NewConcurrencyLimiter(bites.ConcurrencyOptions{Name: "reports", Limit: 1, NoBacklog: true})
	srv := startWith(t, func(app *bites.App) {
		app.Get("/boom", func(context.Context, Empty) (Empty, error) { panic("boom") }, bites.Use(lim.Middleware()))
		app.Get("/ok", func(context.Context, Empty) (Empty, error) { return Empty{}, nil }, bites.Use(lim.Middleware()))
	})
	for range 3 {
		if r := srv.Do(req("GET", "/boom")); r.Status != 500 {
			t.Fatalf("%d %s", r.Status, r.Text())
		}
	}
	if r := srv.Do(req("GET", "/ok")); r.Status != 200 {
		t.Fatalf("the slot of a panicked request was kept: %d %s", r.Status, r.Text())
	}
	if s := lim.Stats(); s.Running != 0 || s.ShedFull != 0 {
		t.Fatalf("%+v", s)
	}
}

func TestAHandlerThatReturnsAnErrorReleasesItsSlot(t *testing.T) {
	lim := bites.NewConcurrencyLimiter(bites.ConcurrencyOptions{Name: "reports", Limit: 1, NoBacklog: true})
	srv := startWith(t, func(app *bites.App) {
		app.Get("/e", func(context.Context, Empty) (Empty, error) { return Empty{}, bites.NewError(409, "no") }, bites.Use(lim.Middleware()))
	})
	for range 3 {
		if r := srv.Do(req("GET", "/e")); r.Status != 409 {
			t.Fatalf("%d %s", r.Status, r.Text())
		}
	}
}

func TestABurstOfTenTimesTheLimitRunsLimitAtOnceQueuesBacklogAndRefusesTheRest(t *testing.T) {
	const limit = 4
	rig := newLimiterRig()
	lim := bites.NewConcurrencyLimiter(bites.ConcurrencyOptions{Name: "reports", Limit: limit})
	rec := &shedRecorder{}
	srv := startWith(t, func(app *bites.App) { app.Get("/r", rig.handler, bites.Use(lim.Middleware())) }, bites.WithObserver(rec))
	const burst = 10 * limit
	chs := make([]<-chan bitetest.Response, burst)
	for i := range chs {
		chs[i] = doAsync(srv, req("GET", "/r"))
	}
	limiterUntil(t, "the burst to be sorted", func() bool {
		s := lim.Stats()
		return s.Running == limit && s.Waiting == limit && s.ShedFull == burst-2*limit
	})
	close(rig.release)
	ok, shed := 0, 0
	for _, ch := range chs {
		switch r := wait(t, ch); r.Status {
		case 200:
			ok++
		case 503:
			shed++
			if r.Get("Retry-After") == "" {
				t.Fatal("a shed answer without Retry-After")
			}
		default:
			t.Fatalf("%d %s", r.Status, r.Text())
		}
	}
	if ok != 2*limit || shed != burst-2*limit {
		t.Fatalf("%d answered, %d refused", ok, shed)
	}
	if p := rig.peak.Load(); p > limit {
		t.Fatalf("%d handlers ran at once, over the limit %d", p, limit)
	}
	limiterUntil(t, "every request observed", func() bool { n, shed := rec.withStatus(503); return n == burst-2*limit && shed == n })
	if s := lim.Stats(); s.Running != 0 || s.Waiting != 0 {
		t.Fatalf("%+v", s)
	}
}

func TestNoBacklogRefusesAtOnceWhenLimitAreRunning(t *testing.T) {
	rig := newLimiterRig()
	lim := bites.NewConcurrencyLimiter(bites.ConcurrencyOptions{Name: "reports", Limit: 1, NoBacklog: true})
	srv := startWith(t, func(app *bites.App) { app.Get("/r", rig.handler, bites.Use(lim.Middleware())) })
	running := doAsync(srv, req("GET", "/r"))
	<-rig.entered
	r := srv.Do(req("GET", "/r"))
	if r.Status != 503 || r.Get("Retry-After") != "1" || !strings.Contains(r.Text(), "it has no backlog") {
		t.Fatalf("%d %v %s", r.Status, r.Header, r.Text())
	}
	close(rig.release)
	wait(t, running)
}

func TestShutdownSendsTheWaitingRequestsAwayAndLetsTheRunningOnesFinish(t *testing.T) {
	rig := newLimiterRig()
	lim := bites.NewConcurrencyLimiter(bites.ConcurrencyOptions{Name: "reports", Limit: 1, Backlog: 3, MaxWait: time.Minute})
	var app *bites.App
	srv := startWith(t, func(a *bites.App) {
		app = a
		a.Get("/r", rig.handler, bites.Use(lim.Middleware()))
	})
	running := doAsync(srv, req("GET", "/r"))
	<-rig.entered
	var waiting []<-chan bitetest.Response
	for range 3 {
		waiting = append(waiting, doAsync(srv, req("GET", "/r")))
	}
	limiterUntil(t, "three waiting", func() bool { return lim.Stats().Waiting == 3 })
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		done <- app.Shutdown(ctx)
	}()
	for i, ch := range waiting {
		r := wait(t, ch)
		if r.Status != 503 || r.Get("Retry-After") != "60" || !strings.Contains(r.Text(), "shutting down") {
			t.Fatalf("waiting request %d: %d %v %s", i, r.Status, r.Header, r.Text())
		}
	}
	if s := lim.Stats(); s.ShedShutdown != 3 || s.Waiting != 0 || s.Running != 1 {
		t.Fatalf("%+v", s)
	}
	select {
	case err := <-done:
		t.Fatalf("Shutdown returned while a request ran: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(rig.release)
	if r := wait(t, running); r.Status != 200 {
		t.Fatalf("the running request was cut: %d", r.Status)
	}
	if err := <-done; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestOneLimiterOnAGroupIsOneLimitForItsRoutesAndOneForEachRouteIsALimitEach(t *testing.T) {
	rig := newLimiterRig()
	shared := bites.NewConcurrencyLimiter(bites.ConcurrencyOptions{Name: "admin", Limit: 1, NoBacklog: true})
	srv := startWith(t, func(app *bites.App) {
		g := app.Group("/admin", bites.Use(shared.Middleware()))
		g.Get("/a", rig.handler)
		g.Get("/b", rig.handler)
		app.Get("/own1", rig.handler, bites.Use(bites.NewConcurrencyLimiter(bites.ConcurrencyOptions{Name: "own1", Limit: 1, NoBacklog: true}).Middleware()))
		app.Get("/own2", rig.handler, bites.Use(bites.NewConcurrencyLimiter(bites.ConcurrencyOptions{Name: "own2", Limit: 1, NoBacklog: true}).Middleware()))
	})
	a := doAsync(srv, req("GET", "/admin/a"))
	<-rig.entered
	if r := srv.Do(req("GET", "/admin/b")); r.Status != 503 {
		t.Fatalf("a second route of the group ran beside the first: %d", r.Status)
	}
	o1 := doAsync(srv, req("GET", "/own1"))
	<-rig.entered
	o2 := doAsync(srv, req("GET", "/own2"))
	<-rig.entered
	close(rig.release)
	for _, ch := range []<-chan bitetest.Response{a, o1, o2} {
		if r := wait(t, ch); r.Status != 200 {
			t.Fatalf("%d", r.Status)
		}
	}
}

func TestTheLogSaysOnceASecondThatARouteIsShedding(t *testing.T) {
	clk := bitetest.NewClock()
	rig := newLimiterRig()
	lim := bites.NewConcurrencyLimiter(bites.ConcurrencyOptions{Name: "reports", Limit: 1, NoBacklog: true})
	srv, logs := startLogged(t, func(app *bites.App) { app.Get("/r", rig.handler, bites.Use(lim.Middleware())) }, bites.WithClock(clk))
	running := doAsync(srv, req("GET", "/r"))
	<-rig.entered
	for range 20 {
		srv.Do(req("GET", "/r"))
	}
	count := func() int { return strings.Count(logs.String(), "bites: load shed") }
	if n := count(); n != 1 {
		t.Fatalf("%d lines for a burst of 20: %s", n, logs)
	}
	for _, want := range []string{"limiter=reports", `reason="backlog full"`, "level=WARN", "limit=1", "path=/r"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("the line does not say %q: %s", want, logs)
		}
	}
	clk.Advance(1100 * time.Millisecond)
	srv.Do(req("GET", "/r"))
	if n := count(); n != 2 || !strings.Contains(logs.String(), "shed=20") {
		t.Fatalf("%d lines, the second must count the 20 refused since the first: %s", n, logs)
	}
	close(rig.release)
	wait(t, running)
}

func TestARequestThatWaitedSaysSoInTheAccessLog(t *testing.T) {
	rig := newLimiterRig()
	lim := bites.NewConcurrencyLimiter(bites.ConcurrencyOptions{Name: "reports", Limit: 1})
	lb := &logBuf{}
	app := bites.New(bites.WithClock(bitetest.NewClock()), bites.WithObserver(bites.AccessLog(bites.AccessLogOptions{Logger: slog.New(slog.NewTextHandler(lb, nil))})))
	app.Get("/r", rig.handler, bites.Use(lim.Middleware()))
	srv := bitetest.Start(t, app)
	running := doAsync(srv, req("GET", "/r"))
	<-rig.entered
	waiting := doAsync(srv, req("GET", "/r"))
	limiterUntil(t, "one waiting", func() bool { return lim.Stats().Waiting == 1 })
	close(rig.release)
	wait(t, running)
	wait(t, waiting)
	limiterUntil(t, "both records written", func() bool { return strings.Count(lb.String(), "path=/r") == 2 })
	if n := strings.Count(lb.String(), "limiter=reports"); n != 1 || !strings.Contains(lb.String(), "limiter_wait=") {
		t.Fatalf("only the request that waited names the limiter and its wait: %s", lb)
	}
}

func TestAManyClientsThatLeaveWhileQueuedNeverLeakASlotOrAPlace(t *testing.T) {
	rig := newLimiterRig()
	lim := bites.NewConcurrencyLimiter(bites.ConcurrencyOptions{Name: "reports", Limit: 2, Backlog: 8, MaxWait: 50 * time.Millisecond})
	srv := startWith(t, func(app *bites.App) { app.Get("/r", rig.handler, bites.Use(lim.Middleware())) })
	var wg sync.WaitGroup
	for i := range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := srv.Dial("203.0.113." + strconv.Itoa(1+i%200) + ":1")
			fmt.Fprint(c, "GET /r HTTP/1.1\r\nHost: x\r\n\r\n")
			time.Sleep(time.Duration(i%7) * 10 * time.Millisecond) // some leave before MaxWait, some after, some as a slot frees
			c.Close()
		}()
		if i%25 == 0 {
			go func() { rig.release <- struct{}{} }()
		}
	}
	wg.Wait()
	close(rig.release)
	limiterUntil(t, "everything to settle", func() bool { s := lim.Stats(); return s.Running == 0 && s.Waiting == 0 })
	s := lim.Stats()
	// Every request that joined the backlog ended in exactly one way, and every other arrival
	// either ran at once or was refused for want of room.
	ended := s.Waited + s.ShedWait + s.ShedShutdown + s.Left
	if ended != s.Queued {
		t.Fatalf("%d joined the backlog and %d are accounted for: %+v", s.Queued, ended, s)
	}
	if p := rig.peak.Load(); p > 2 {
		t.Fatalf("%d handlers ran at once, over the limit 2", p)
	}
}

func TestAConcurrencyLimiterGivenToTheAppBoundsEveryRequestIncluding404(t *testing.T) {
	rig := newLimiterRig()
	lim := bites.NewConcurrencyLimiter(bites.ConcurrencyOptions{Name: "app", Limit: 1, NoBacklog: true})
	srv := startWith(t, func(app *bites.App) {
		app.Use(lim.Middleware())
		app.Get("/r", rig.handler)
	})
	running := doAsync(srv, req("GET", "/r"))
	<-rig.entered
	if r := srv.Do(req("GET", "/nothing")); r.Status != 503 {
		t.Fatalf("%d", r.Status)
	}
	close(rig.release)
	wait(t, running)
	if r := srv.Do(req("GET", "/nothing")); r.Status != 404 {
		t.Fatalf("%d", r.Status)
	}
}

func TestAnErrorFromTheLimiterIsFoundAsErrShedInTheOutcomeAndMetricsCountsIt(t *testing.T) {
	rig := newLimiterRig()
	lim := bites.NewConcurrencyLimiter(bites.ConcurrencyOptions{Name: "reports", Limit: 1, NoBacklog: true})
	m := bites.NewMetrics()
	srv := startWith(t, func(app *bites.App) {
		app.Get("/r", rig.handler, bites.Use(lim.Middleware()))
		app.ServeMetrics("/metrics", m)
	}, bites.WithObserver(m))
	running := doAsync(srv, req("GET", "/r"))
	<-rig.entered
	for range 3 {
		srv.Do(req("GET", "/r"))
	}
	limiterUntil(t, "the three requests observed", func() bool {
		return strings.Contains(srv.Do(req("GET", "/metrics")).Text(), `bites_requests_shed_total{method="GET",route="/r"} 3`)
	})
	close(rig.release)
	wait(t, running)
}

func TestSustainedParallelTrafficNeverRunsMoreThanLimitAndEndsWithNothingHeld(t *testing.T) {
	var running, peak atomic.Int64
	lim := bites.NewConcurrencyLimiter(bites.ConcurrencyOptions{Name: "reports", Limit: 3, Backlog: 3, MaxWait: 5 * time.Millisecond})
	srv := startWith(t, func(app *bites.App) {
		app.Get("/r", func(context.Context, Empty) (Empty, error) {
			n := running.Add(1)
			for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
			}
			time.Sleep(time.Millisecond)
			running.Add(-1)
			return Empty{}, nil
		}, bites.Use(lim.Middleware()))
	})
	var wg sync.WaitGroup
	var ok, refused atomic.Int64
	for range 24 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 25 {
				switch r := srv.Do(req("GET", "/r")); r.Status {
				case 200:
					ok.Add(1)
				case 503:
					refused.Add(1)
				default:
					t.Errorf("%d %s", r.Status, r.Text())
				}
			}
		}()
	}
	wg.Wait()
	if p := peak.Load(); p > 3 {
		t.Fatalf("%d handlers ran at once, over the limit 3", p)
	}
	s := lim.Stats()
	if s.Running != 0 || s.Waiting != 0 {
		t.Fatalf("slots or places are still held: %+v", s)
	}
	if got := ok.Load() + refused.Load(); got != 24*25 {
		t.Fatalf("%d answers for %d requests", got, 24*25)
	}
	if want := uint64(refused.Load()); s.ShedFull+s.ShedWait != want {
		t.Fatalf("the limiter counts %d refusals and the clients saw %d: %+v", s.ShedFull+s.ShedWait, want, s)
	}
	t.Logf("%d ran, %d refused (%d backlog full, %d waited too long), %d waited and ran, %+v", ok.Load(), refused.Load(), s.ShedFull, s.ShedWait, s.Waited, s)
}

func TestTheAPIDocumentStatesTheA503WithRetryAfterOnARouteBehindAConcurrencyLimiterAndOnNoOther(t *testing.T) {
	lim := bites.NewConcurrencyLimiter(bites.ConcurrencyOptions{Name: "reports", Limit: 2})
	doc := docOf(t, func(app *bites.App) {
		app.OpenAPI("/openapi.json", bites.Info{Title: "t", Version: "1"})
		app.Get("/plain", func(context.Context, Empty) (bites.OK[docUser], error) { return bites.OK[docUser]{}, nil })
		g := app.Group("/reports", bites.Use(lim.Middleware()))
		g.Get("/year", func(context.Context, Empty) (bites.OK[docUser], error) { return bites.OK[docUser]{}, nil })
	})
	if got := dig(doc, "paths", "/reports/year", "get", "responses", "503", "$ref"); got != "#/components/responses/ServiceUnavailable" {
		t.Errorf("503 on the limited route: %v", got)
	}
	if got := dig(doc, "paths", "/plain", "get", "responses", "503"); got != nil {
		t.Errorf("503 on a route with no limiter: %v", got)
	}
	if dig(doc, "components", "responses", "ServiceUnavailable", "headers", "Retry-After") == nil {
		t.Error("the answer is in the components, with Retry-After")
	}
}

func TestAConcurrencyLimiterOnTheAppGivesEveryOperationTheA503(t *testing.T) {
	lim := bites.NewConcurrencyLimiter(bites.ConcurrencyOptions{Name: "app", Limit: 2})
	doc := docOf(t, func(app *bites.App) {
		app.OpenAPI("/openapi.json", bites.Info{Title: "t", Version: "1"})
		app.Get("/a", func(context.Context, Empty) (bites.OK[docUser], error) { return bites.OK[docUser]{}, nil })
		app.Use(lim.Middleware()) // after the routes, which app middleware may
	})
	if got := dig(doc, "paths", "/a", "get", "responses", "503", "$ref"); got != "#/components/responses/ServiceUnavailable" {
		t.Errorf("503: %v", got)
	}
}
