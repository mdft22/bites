package bites

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nevindra/bites/internal/clock"
)

// A concurrency limit with a backlog, and load shedding (ADR 0091): at most Limit requests run
// at once behind the limiter, the next Backlog wait their turn in arrival order for at most
// MaxWait, and every other request is refused at once with a 503 and Retry-After.

// ErrShed is what errors.Is finds in the error of a request a ConcurrencyLimiter refused, in the
// Err of its Outcome (ADR 0051), so an Observer can tell a request the framework shed from a 503
// the handler chose. The answer itself is a 503 problem+json with Retry-After.
var ErrShed = errors.New("bites: the request was shed by a concurrency limit")

const (
	defaultMaxWait  = time.Second
	maxLimiterName  = 32
	shedLogInterval = time.Second
	maxLimit        = 1 << 30 // the running and the waiting counts share one 64-bit word, 32 bits each
)

// The reasons a request is refused, in the order shedReasons and shedErrs name them.
const (
	shedReasonFull = iota
	shedReasonWait
	shedReasonDrain
	shedReasonLeft
	shedReasonsCount
)

// ConcurrencyOptions configures a ConcurrencyLimiter.
type ConcurrencyOptions struct {
	// Name is what the limiter is called in the 503 it answers, in the log line that says it is
	// shedding and in Stats. Required: at most 32 letters, digits, '-' or '_', such as "reports".
	Name string

	// Limit is the most requests that run at once behind the limiter. Required, at least 1.
	Limit int

	// Backlog is how many requests may wait, in arrival order, when Limit are running; a request
	// that finds Backlog waiting is refused at once. Zero is Limit, so an app that states nothing
	// else holds as many waiting as it runs. A queue is never unbounded. Given with NoBacklog it
	// is refused.
	Backlog int

	// MaxWait is how long a request may wait for a slot before it is refused. Zero is one second.
	// It is also what Retry-After says, rounded up to whole seconds. Given with NoBacklog it is
	// refused.
	MaxWait time.Duration

	// NoBacklog refuses a request at once when Limit are running, with no waiting at all. It is
	// the way to say "no queue", because a zero Backlog is the default and not none. Given with
	// Backlog or MaxWait it is refused.
	NoBacklog bool
}

// ConcurrencyStats is a ConcurrencyLimiter's counts at one moment, from Stats.
type ConcurrencyStats struct {
	// Running is the number of requests holding a slot now; Waiting, the number in the backlog.
	Running, Waiting int
	// Queued counts the requests that entered the backlog, whatever came of them: some are in
	// Waited, some in ShedWait, some in Left and some in ShedShutdown, and those still waiting
	// are in Waiting.
	Queued uint64
	// Waited counts the requests that ran after waiting in the backlog, and WaitTotal is the time
	// they spent there, so WaitTotal divided by Waited is the mean wait of a request that got in.
	Waited    uint64
	WaitTotal time.Duration
	// ShedFull counts the requests refused because Limit were running and Backlog waiting;
	// ShedWait, those that waited MaxWait without a slot; ShedShutdown, those refused because
	// Shutdown had started, waiting or arriving.
	ShedFull, ShedWait, ShedShutdown uint64
	// Left counts the requests that left the backlog because their context was done: the client
	// disconnected, or a Timeout outside the limiter passed. They are not shed, nobody is waiting
	// for an answer, and they gave their place back.
	Left uint64
}

// ConcurrencyLimiter bounds how many requests run at once behind its Middleware, queues a bounded
// number for a bounded time and answers the rest 503 (ADR 0091). Make one with
// NewConcurrencyLimiter and give its Middleware to the app, a group or a route with Use: one
// limiter given to several scopes is one limit shared by all of them, and one limiter for each
// route is a limit for each.
//
// A request that finds a free slot takes it and runs; no waiting structure is touched and nothing
// is allocated. When Limit are running a request joins the backlog if it has room, behind those
// already there, and is handed the next slot that frees, in arrival order. A request leaves the
// backlog with a 503 when it has waited MaxWait, or when Shutdown starts, and silently, giving its
// place back, when its context is done. The 503 carries Retry-After and names the limiter and the
// reason; errors.Is(err, ErrShed) finds it in an Outcome. A slot is released when the chain
// inside the limiter returns, a panic included.
//
// A Stream or SSE that a handler returns runs after the chain has returned (ADR 0030), so it is
// not counted: the limit bounds the time to produce the stream.
//
// Give a limiter to one app: its waiting requests are told of Shutdown through the app they were
// first seen by.
type ConcurrencyLimiter struct {
	name     string
	limit    int
	backlog  int
	maxWait  time.Duration
	retry    string // Retry-After, in seconds
	shedErrs [shedReasonsCount]error

	bound atomic.Pointer[App]
	pool  sync.Pool // *waiter

	// state packs the two counts the fast path reads: the requests holding a slot in the low 32
	// bits and the requests in the backlog in the high 32. A request that finds a free slot and an
	// empty backlog takes the slot with one compare-and-swap, and one that frees a slot with an
	// empty backlog gives it back with one, so neither takes mu. The waiting half changes only
	// under mu, with the list; the running half changes anywhere.
	state      atomic.Uint64
	mu         sync.Mutex
	waiting    int // the length of the list, equal to the high half of state while mu is held
	head, tail *waiter
	draining   bool
	others     []*App // apps beyond the first that gave it a request; guarded by mu
	st         ConcurrencyStats
	lastShed   int64  // the app's clock in nanoseconds when a refusal was last logged; 0 for never
	shedSince  uint64 // refusals counted since the last log line
}

// NewConcurrencyLimiter returns a ConcurrencyLimiter. It panics, naming the field and the line it
// was called from, when Name is empty or not a short token, Limit is below 1, Backlog is negative
// or is given without a Limit, MaxWait is negative or is given without a Limit, or NoBacklog is
// given with a Backlog or a MaxWait.
func NewConcurrencyLimiter(o ConcurrencyOptions) *ConcurrencyLimiter {
	const fn = "NewConcurrencyLimiter"
	if o.Limit < 1 && (o.Backlog != 0 || o.MaxWait != 0) {
		refuseMiddleware(fn, "Backlog or MaxWait is given with no Limit: they only bound the requests waiting for a slot, so give Limit, the most requests that run at once, at least 1")
	}
	if o.Limit < 1 {
		refuseMiddleware(fn, "Limit is %d: give the most requests that may run at once, at least 1", o.Limit)
	}
	if o.Limit > maxLimit || o.Backlog > maxLimit {
		refuseMiddleware(fn, "Limit is %d and Backlog is %d: the limiter counts requests in 32 bits, so give each at most %d", o.Limit, o.Backlog, maxLimit)
	}
	if o.Backlog < 0 {
		refuseMiddleware(fn, "Backlog is %d, which is not a bound: give zero for Limit, or the most requests that may wait, or NoBacklog to refuse at once", o.Backlog)
	}
	if o.MaxWait < 0 {
		refuseMiddleware(fn, "MaxWait is %v, which is not a wait: give zero for one second, or how long a request may wait for a slot", o.MaxWait)
	}
	if o.NoBacklog && (o.Backlog != 0 || o.MaxWait != 0) {
		refuseMiddleware(fn, "NoBacklog is given with Backlog or MaxWait: NoBacklog refuses a request at once when Limit are running, so nothing waits, and drop one of the two")
	}
	if o.Name == "" || len(o.Name) > maxLimiterName || !isPolicyName(o.Name) {
		refuseMiddleware(fn, "Name %q is not a name: give the limiter a short name such as \"reports\", at most %d letters, digits, '-' or '_', for the 503, the log and Stats to name it by", o.Name, maxLimiterName)
	}
	l := &ConcurrencyLimiter{name: o.Name, limit: o.Limit, backlog: o.Backlog, maxWait: o.MaxWait}
	switch {
	case o.NoBacklog:
		l.backlog = 0
	case l.backlog == 0:
		l.backlog = l.limit
	}
	if l.maxWait == 0 {
		l.maxWait = defaultMaxWait
	}
	secs := int64(1)
	if l.backlog > 0 {
		secs = max(1, ceilSeconds(l.maxWait))
	}
	l.retry = strconv.FormatInt(secs, 10)
	q := "its backlog of " + strconv.Itoa(l.backlog) + " is full"
	if l.backlog == 0 {
		q = "it has no backlog"
	}
	base := `the "` + l.name + `" limit of ` + strconv.Itoa(l.limit) + " running requests is reached"
	l.shedErrs[shedReasonFull] = l.shedError(base + " and " + q + "; retry in " + l.retry + " s")
	l.shedErrs[shedReasonWait] = l.shedError(base + " and no slot freed within " + l.maxWait.String() + "; retry in " + l.retry + " s")
	l.shedErrs[shedReasonDrain] = l.shedError("the server is shutting down and the \"" + l.name + "\" limit is not taking more requests; retry in " + l.retry + " s")
	l.shedErrs[shedReasonLeft] = l.shedError("the request's context ended while it waited for a slot of the \"" + l.name + "\" limit")
	return l
}

// shedError is the error of a shed request: a 503 with Retry-After that errors.Is finds ErrShed
// in. It is made once for each reason, so shedding allocates nothing.
type shedError struct {
	se   *StatusError
	errs []error // se and ErrShed, kept so that Unwrap allocates nothing
}

func (e *shedError) Error() string   { return e.se.Error() }
func (e *shedError) Unwrap() []error { return e.errs }

func (l *ConcurrencyLimiter) shedError(detail string) error {
	se := ServiceUnavailable(detail).WithHeader("Retry-After", l.retry)
	return &shedError{se, []error{se, ErrShed}}
}

// Middleware limits the requests that reach it. Give it to app.Use to bound every request the app
// serves, 404s included, or with Use to a group or a route to bound those. Put a RateLimiter
// outside it: the limit protects the handler's capacity and shares it first come first served, so
// one client's burst fills the backlog unless something per client is outside it.
func (l *ConcurrencyLimiter) Middleware() Middleware { return l.serve }

// Stats returns the limiter's counts now. It takes the limiter's lock briefly and is for a gauge
// scraped every few seconds, not for every request.
func (l *ConcurrencyLimiter) Stats() ConcurrencyStats {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.st
	st := l.state.Load()
	s.Running, s.Waiting = int(uint32(st)), int(st>>32)
	return s
}

const (
	wWaiting uint8 = iota
	wGranted
	wTimedOut
	wDrained
	wGone
)

// waiter is one request waiting for a slot. It sits in the limiter's list, in arrival order, and
// is woken by a token on ch after its state was set under the limiter's lock. It is pooled with
// its channel and its timer, so a request that waits allocates nothing once the pool is warm.
type waiter struct {
	l          *ConcurrencyLimiter
	prev, next *waiter
	ch         chan struct{} // capacity 1: at most one token, sent while the lock is held
	state      uint8
	at         int64 // the app's clock in nanoseconds when it joined the backlog
	clk        clock.Clock
	timer      clock.Timer
	resettable bool
	expireFn   func()
}

func (l *ConcurrencyLimiter) getWaiter() *waiter {
	if w, _ := l.pool.Get().(*waiter); w != nil {
		return w
	}
	w := &waiter{l: l, ch: make(chan struct{}, 1)}
	w.expireFn = w.expire
	return w
}

// arm starts the waiter's MaxWait on clk, re-arming its timer in place when it can.
func (w *waiter) arm(clk clock.Clock, d time.Duration) {
	if w.timer != nil && w.resettable && w.clk == clk {
		w.timer.(clock.Resetter).Reset(d)
		return
	}
	w.clk = clk
	w.timer = clk.AfterFunc(d, w.expireFn)
	_, w.resettable = w.timer.(clock.Resetter)
}

// expire is the timer's callback: the waiter has waited MaxWait. It does nothing when a slot, the
// context or Shutdown got to the waiter first.
func (w *waiter) expire() {
	l := w.l
	l.mu.Lock()
	if w.state == wWaiting {
		l.unlink(w)
		w.state = wTimedOut
		l.st.ShedWait++
		w.ch <- struct{}{}
	}
	l.mu.Unlock()
}

// push adds w at the tail of the list. l.mu is held.
func (l *ConcurrencyLimiter) push(w *waiter) {
	w.prev, w.next = l.tail, nil
	if l.tail != nil {
		l.tail.next = w
	} else {
		l.head = w
	}
	l.tail = w
	l.waiting++
	l.state.Add(1 << 32)
}

// unlink removes w from the list. l.mu is held and w is in it.
func (l *ConcurrencyLimiter) unlink(w *waiter) {
	if w.prev != nil {
		w.prev.next = w.next
	} else {
		l.head = w.next
	}
	if w.next != nil {
		w.next.prev = w.prev
	} else {
		l.tail = w.prev
	}
	w.prev, w.next = nil, nil
	l.waiting--
	l.state.Add(^uint64(1<<32 - 1)) // minus one in the high half
}

// bind remembers the app a request came from, so Shutdown can tell its waiting requests to go.
func (l *ConcurrencyLimiter) bind(a *App) {
	l.mu.Lock()
	known := l.bound.Load() == a
	for _, o := range l.others {
		known = known || o == a
	}
	if !known {
		if l.bound.Load() == nil {
			l.bound.Store(a)
		} else {
			l.others = append(l.others, a)
		}
	}
	l.mu.Unlock()
	if known {
		return
	}
	if a.limiters.add(l) {
		l.drain() // the app was already sending its waiting requests away
	}
	a.log.Info("bites: concurrency limiter in use", "limiter", l.name, "limit", l.limit, "backlog", l.backlog, "max_wait", l.maxWait)
}

func (l *ConcurrencyLimiter) serve(ctx context.Context, req Request, next Next) (Response, error) {
	a := req.run.a
	if l.bound.Load() != a {
		l.bind(a)
	}
	if !l.tryAcquire() {
		l.mu.Lock()
		if err := l.wait(ctx, req, a); err != nil {
			return Response{}, err
		}
	}
	defer l.release()
	return next.Call(ctx), nil
}

// tryAcquire takes a slot if one is free and nobody is waiting for it, which is what keeps a
// request that arrives late from passing those that arrived first.
func (l *ConcurrencyLimiter) tryAcquire() bool {
	for {
		s := l.state.Load()
		if s>>32 != 0 || int(uint32(s)) >= l.limit {
			return false
		}
		if l.state.CompareAndSwap(s, s+1) {
			return true
		}
	}
}

// wait is the slow path: no slot is free. It is called with l.mu held and returns without it. It
// returns nil when the caller holds a slot, and the error to answer with when it does not.
func (l *ConcurrencyLimiter) wait(ctx context.Context, req Request, a *App) error {
	var w *waiter
	for {
		st := l.state.Load()
		if st>>32 == 0 && int(uint32(st)) < l.limit { // a slot freed while the caller took the lock
			if l.state.CompareAndSwap(st, st+1) {
				l.mu.Unlock()
				return nil
			}
			continue
		}
		if l.draining || l.waiting >= l.backlog {
			reason := shedReasonFull
			if l.draining {
				reason = shedReasonDrain
				l.st.ShedShutdown++
			} else {
				l.st.ShedFull++
			}
			line, err := l.shedLocked(a, reason)
			l.mu.Unlock()
			l.logShed(a, req, line)
			return err
		}
		// Join the backlog. The count in state moves with the list, and only if the running half
		// is as it was read: a request that gave its slot back in between would otherwise leave a
		// waiter nobody hands a slot to.
		w = l.getWaiter()
		w.state = wWaiting
		w.at = a.clk.Now().UnixNano()
		l.push(w) // adds one to the waiting half
		if st2 := l.state.Load(); uint32(st2) < uint32(l.limit) {
			// A slot was released between the read and the push: it has not been handed to the
			// waiter, so take it back out and look again.
			l.unlink(w)
			l.pool.Put(w)
			continue
		}
		break
	}
	l.st.Queued++
	l.mu.Unlock()

	w.arm(a.clk, l.maxWait)
	select {
	case <-w.ch:
	case <-ctx.Done():
	}

	var (
		err  error
		line shedLine
		lag  time.Duration
	)
	l.mu.Lock()
	state := w.state
	if state == wGranted && ctx.Err() != nil {
		// A slot came as the request ended: hand it on rather than run a request nobody waits for.
		l.handOnLocked()
		state = wGone
		l.st.Left++
	}
	switch state {
	case wWaiting: // the context ended first
		l.unlink(w)
		w.state = wGone
		l.st.Left++
		err = l.shedErrs[shedReasonLeft]
	case wGranted:
		lag = time.Duration(a.clk.Now().UnixNano() - w.at)
		l.st.Waited++
		l.st.WaitTotal += lag
	case wTimedOut:
		line, err = l.shedLocked(a, shedReasonWait)
	case wDrained:
		line, err = l.shedLocked(a, shedReasonDrain)
	default:
		err = l.shedErrs[shedReasonLeft]
	}
	l.mu.Unlock()
	l.finish(w)
	if err != nil {
		l.logShed(a, req, line)
		return err
	}
	req.AddLogAttrs(slog.String("limiter", l.name), slog.Duration("limiter_wait", lag))
	return nil
}

// finish gives a waiter back to the pool when no callback of its timer can still reach it. A
// timer that was stopped in time has none; one that fired for this wait has finished its work,
// since the state it set is the one that was read. A timer that fired just as a slot was granted
// may still be about to run, and would act on whatever request the waiter served next, so that
// waiter is left to the garbage collector.
func (l *ConcurrencyLimiter) finish(w *waiter) {
	if w.timer.Stop() || w.state == wTimedOut {
		select {
		case <-w.ch:
		default:
		}
		l.pool.Put(w)
	}
}

// release gives the request's slot back: to the head of the backlog if there is one, as it frees,
// or to nobody. With an empty backlog it is one compare-and-swap.
func (l *ConcurrencyLimiter) release() {
	for {
		s := l.state.Load()
		if s>>32 != 0 {
			break
		}
		if l.state.CompareAndSwap(s, s-1) {
			return
		}
	}
	l.mu.Lock()
	l.handOnLocked()
	l.mu.Unlock()
}

// handOnLocked passes one slot, which the caller holds, to the first waiter, or frees it. The
// list and the waiting half of state only change under mu, so a waiter seen there stays.
func (l *ConcurrencyLimiter) handOnLocked() {
	if w := l.head; w != nil {
		l.unlink(w) // the running half stays: the slot is the waiter's now
		w.state = wGranted
		w.ch <- struct{}{}
		return
	}
	l.state.Add(^uint64(0)) // minus one running
}

// drain tells every waiting request to go, as Shutdown starts, and refuses a request that would
// have to wait from now on: the instance is leaving and a client may retry on another.
func (l *ConcurrencyLimiter) drain() {
	l.mu.Lock()
	l.draining = true
	for w := l.head; w != nil; w = l.head {
		l.unlink(w)
		w.state = wDrained
		l.st.ShedShutdown++
		w.ch <- struct{}{}
	}
	l.mu.Unlock()
}

// shedLine is what the log line of a refusal says, and whether one is due.
type shedLine struct {
	due              bool
	reason           int
	n                uint64
	since            time.Duration
	running, waiting int
}

// shedLocked counts one refusal and returns its answer, and the line to write if one is due: the
// first refusal says so, and after it at most one line a second says how many there were since,
// so an overload is told once a second and not once a request. l.mu is held; the caller writes
// the line after releasing it.
func (l *ConcurrencyLimiter) shedLocked(a *App, reason int) (shedLine, error) {
	l.shedSince++
	now := a.clk.Now().UnixNano()
	if l.lastShed != 0 && now-l.lastShed < int64(shedLogInterval) {
		return shedLine{}, l.shedErrs[reason]
	}
	line := shedLine{due: true, reason: reason, n: l.shedSince, running: int(uint32(l.state.Load())), waiting: l.waiting}
	if l.lastShed != 0 {
		line.since = time.Duration(now - l.lastShed)
	}
	l.lastShed, l.shedSince = now, 0
	return line, l.shedErrs[reason]
}

var shedReasons = [shedReasonsCount]string{"backlog full", "waited the whole MaxWait", "shutting down", "context ended"}

// logShed writes the line that says the limiter is refusing requests.
func (l *ConcurrencyLimiter) logShed(a *App, req Request, line shedLine) {
	if !line.due {
		return
	}
	a.log.Warn("bites: load shed: a concurrency limit is refusing requests with 503",
		"limiter", l.name, "reason", shedReasons[line.reason], "shed", line.n, "since_last_line", line.since,
		"running", line.running, "waiting", line.waiting, "limit", l.limit, "backlog", l.backlog, "max_wait", l.maxWait,
		"method", req.Method(), "path", req.Path(), "request_id", req.RequestID())
}

// limiterSet is the limiters an app has seen, so Shutdown can tell their waiting requests to go.
type limiterSet struct {
	mu      sync.Mutex
	list    []*ConcurrencyLimiter
	drained bool
}

// add records l and reports whether the app has already drained its limiters.
func (s *limiterSet) add(l *ConcurrencyLimiter) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.list = append(s.list, l)
	return s.drained
}

// drain refuses the requests waiting behind every limiter the app has seen.
func (s *limiterSet) drain() {
	s.mu.Lock()
	s.drained = true
	list := append([]*ConcurrencyLimiter(nil), s.list...)
	s.mu.Unlock()
	for _, l := range list {
		l.drain()
	}
}

// hasConcurrencyLimiter reports whether a ConcurrencyLimiter's middleware is among mw, so the API
// document states the 503 it answers (ADR 0069, ADR 0091).
func hasConcurrencyLimiter(mw []Middleware) bool {
	ref := (*ConcurrencyLimiter)(nil).serve
	for _, m := range mw {
		if sameCode(m, ref) {
			return true
		}
	}
	return false
}
