package bites

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/nevindra/bites/internal/clock"
)

const (
	// cronGrace is how late after its due time a run may still start. Later than this the clock
	// jumped forward or the process was paused (a suspended laptop, a stopped container), and the
	// run is skipped and logged: a report due at 9:00 that starts at 9:40 is not the report.
	cronGrace = time.Minute
	// cronClaimTTL is how long a claim of an occurrence is kept in a JobLock: far longer than
	// the lateness and clock skew between instances the claim has to outlast, and short enough
	// that a store holds an hour of keys and no more.
	cronClaimTTL = time.Hour
	// cronClaimTimeout bounds one Claim, so a store that hangs skips the occurrence instead of
	// holding the schedule.
	cronClaimTimeout = 10 * time.Second
	// cronCountSkipped bounds the occurrences counted for the log of a pause: "at least" this many.
	cronCountSkipped = 10_000
)

// CronOptions are the settings of a Cron. The zero value is valid: the job runs on every
// instance of the app.
type CronOptions struct {
	// Lock, when not nil, makes the job run on one instance of several: before each occurrence
	// the job claims it in Lock, and runs it only when the claim is its own (ADR 0092). It is
	// the same JobLock, and the same store, on every instance, and the name of the job is its
	// identity there. A store that fails or hangs skips the occurrence and logs it; the job is
	// never run unlocked in its place. nil (the zero value) is no lock: every instance runs
	// every occurrence, and the start-up line of the job says "lock none".
	Lock JobLock
}

// scheduledKey is the context key of the occurrence a Cron run is for.
type scheduledKey struct{}

// ScheduledAt is the time a Cron run was due, the occurrence the run belongs to, or the zero
// time when ctx is not the ctx of a Cron job. It is the schedule's time and not the clock's: a
// run that starts a few milliseconds late, or a minute late within the grace, has the same
// ScheduledAt, so it is the value to key an idempotent write on ("the report for 2026-03-08
// 02:30") and so make a job safe to run twice (ADR 0092). It is in UTC; call In for the job's
// zone.
func ScheduledAt(ctx context.Context) time.Time {
	t, _ := ctx.Value(scheduledKey{}).(time.Time)
	return t
}

// Cron returns a Service that calls fn at the times a cron expression names, in the time zone
// loc, for the life of the app, on the app's clock (WithClock, bitetest.Clock), so a test moves
// time instead of waiting (ADR 0092). Register the result with App.Service.
//
// spec is the five fields of cron, minute hour day-of-month month day-of-week, or one of the
// shortcuts @yearly, @annually, @monthly, @weekly, @daily, @midnight and @hourly. A field is *, a
// number, a range a-b, a list a,b,c of those, or a range or * with a step, */15 and 10-40/10;
// months and days of the week may be three-letter names (jan, mon), and 0 and 7 are both Sunday.
// When both the day of month and the day of week name days (neither is *), a day matches either,
// as in Vixie cron. There are no seconds, no ?, L, W or #, and no @every (an interval is
// bites.Every). The time zone is required, because a schedule read in the machine's zone is a
// different schedule on another machine: give time.UTC or the result of time.LoadLocation, never
// nil and not time.Local.
//
// Daylight saving: a spec that names hours is a time of day, run once. A time skipped when the
// clocks go forward runs once, at the first instant after the gap (02:30 on the day 02:00 becomes
// 03:00 runs at 03:00); a time repeated when they go back runs once, at its first occurrence. A
// spec that runs every hour (@hourly, */15 * * * *) follows the clock instead: a time that does
// not exist is not run, and one that occurs twice runs twice.
//
// There is no first run at start: the first occurrence is the first one after the app started
// serving, and the job is logged at Info with it when the service starts. Runs are serial: fn
// never runs twice at once, and an occurrence that comes due while the last run is still going is
// skipped and logged at Warn, never queued. An occurrence missed because the clock jumped forward
// or the process was paused for more than a minute is skipped and logged too, and the runs after
// a pause are never made up in a burst. A clock that goes back never repeats an occurrence that
// was run: the next is the first one after the last that ran.
//
// Options.Lock makes one instance of several run each occurrence. An error from fn is logged at
// Warn, a panic is recovered and logged at Error with its stack, and the schedule goes on.
// fn's ctx is the Service's life, with ScheduledAt; Stop cancels it and waits for a run in
// progress, bounded as every Service function is.
//
// Cron panics at the call (a RegistrationError naming the line) for a name that is empty or has a
// space, a quote or a control character, for a nil fn, for a nil loc or time.Local, for a spec
// that is not a schedule (the message names the field and what to write) or never matches a
// date, and when more than one CronOptions is given.
func Cron(name, spec string, loc *time.Location, fn func(ctx context.Context) error, opts ...CronOptions) Service {
	where := callerWhere()
	if badServiceName(name) {
		refuseService("bites.Cron", "", where, "the name is empty or has a space, a quote or a control character: give the word the log and the errors call this job by, such as \"nightly-report\"")
	}
	if fn == nil {
		refuseService("bites.Cron", name, where, "fn is nil: give the function called at each time of the schedule")
	}
	switch loc {
	case nil:
		refuseService("bites.Cron", name, where, "the time zone is nil: give the place the times are read in, such as time.UTC or the result of time.LoadLocation(\"Asia/Jakarta\"); a zone is never assumed")
	case time.Local:
		refuseService("bites.Cron", name, where, "the time zone is time.Local, the machine's own: two machines of one app would run the job at different times; name the place, such as time.LoadLocation(\"Asia/Jakarta\"), or give time.UTC")
	}
	sched, err := parseCron(spec)
	if err != nil {
		refuseService("bites.Cron", name, where, "spec %q is not a schedule: %v", spec, err)
	}
	var o CronOptions
	switch len(opts) {
	case 0:
	case 1:
		o = opts[0]
	default:
		refuseService("bites.Cron", name, where, "%d options values were given; give one CronOptions", len(opts))
	}
	c := &cronJob{name: name, spec: spec, sched: sched, loc: loc, fn: fn, lock: o.Lock}
	return Service{
		Name: name,
		Start: func(ctx context.Context) error {
			c.begin(ctx)
			return nil
		},
		Stop: func(context.Context) error {
			c.end()
			return nil
		},
	}
}

// cronJob is the state of one Cron: when it is next due, the timer for it, and whether a run is
// going. Its timer function runs on the clock's goroutine and never blocks: a run is a goroutine.
type cronJob struct {
	name  string
	spec  string
	sched *cronSpec
	loc   *time.Location
	fn    func(context.Context) error
	lock  JobLock

	ctx context.Context // the Service's life
	clk clock.Clock

	mu      sync.Mutex
	due     time.Time // the occurrence the armed timer is for; zero when none is armed
	gen     int64     // the generation of the armed timer, so a stopped timer's late call is ignored
	timer   clock.Timer
	running bool
	stopped bool
	wg      sync.WaitGroup // the runs in progress
}

// begin arms the first timer before Start returns, so a test that advances the clock right
// after bitetest.Start cannot miss it.
func (c *cronJob) begin(ctx context.Context) {
	c.ctx = ctx
	c.clk = clock.Real()
	if a := serviceApp(ctx); a != nil && a.Clock != nil {
		c.clk = a.Clock
	}
	now := c.clk.Now()
	first, ok := c.sched.next(now, c.loc)
	if !ok {
		Logger(ctx).Error("bites: job has no time to run at and is not scheduled", "job", c.name, "spec", c.spec, "zone", c.loc.String())
		return
	}
	lock := "none"
	if c.lock != nil {
		lock = "set"
	}
	Logger(ctx).Info("bites: job scheduled", "job", c.name, "spec", c.spec, "zone", c.loc.String(), "next", first.In(c.loc), "lock", lock)
	c.mu.Lock()
	c.armLocked(first, now)
	c.mu.Unlock()
}

// armLocked sets the timer for due, d from now. The caller holds mu.
func (c *cronJob) armLocked(due, now time.Time) {
	c.gen++
	g := c.gen
	c.due = due
	if c.timer != nil {
		c.timer.Stop()
	}
	c.timer = c.clk.AfterFunc(due.Sub(now), func() { c.fire(g) })
}

// fire is the timer: it decides what the occurrence that is due becomes (a run, or a skip that
// says why) and arms the next one before any run starts, so the schedule never waits for a job.
func (c *cronJob) fire(g int64) {
	log := Logger(c.ctx)
	c.mu.Lock()
	if c.stopped || g != c.gen || c.ctx.Err() != nil {
		c.mu.Unlock()
		return
	}
	now := c.clk.Now()
	if now.Before(c.due) { // the clock was set back since the timer was armed
		due := c.due
		c.armLocked(due, now)
		c.mu.Unlock()
		log.Warn("bites: the clock went back before a job's time, it waits for the clock to reach it", "job", c.name, "due", due.In(c.loc), "now", now.In(c.loc))
		return
	}
	due := c.due
	// The next occurrence is the first at or after now that follows due: a run that overran or
	// a pause drops what it passed and never queues it.
	from := now.Add(-time.Nanosecond)
	if from.Before(due) {
		from = due
	}
	next, ok := c.sched.next(from, c.loc)
	skipped := 0
	if ok {
		for x, more := c.sched.next(due, c.loc); more && x.Before(next) && skipped < cronCountSkipped; x, more = c.sched.next(x, c.loc) {
			skipped++
		}
		c.armLocked(next, now)
	} else {
		c.due = time.Time{}
	}
	late := now.Sub(due)
	busy := c.running
	run := !busy && late <= cronGrace
	if run {
		c.running = true
		c.wg.Add(1)
	}
	c.mu.Unlock()

	if !ok {
		log.Error("bites: job has no further time to run at and stops", "job", c.name, "spec", c.spec)
	}
	switch {
	case busy:
		log.Warn("bites: job skipped: its last run is still going", "job", c.name, "due", due.In(c.loc), "dropped", skipped+1)
	case !run:
		log.Warn("bites: job skipped: its time passed while the clock jumped or the process was paused", "job", c.name, "due", due.In(c.loc), "late", late.Round(time.Second), "grace", cronGrace, "dropped", skipped+1)
	default:
		go c.run(due)
	}
}

// run claims the occurrence when there is a lock, calls fn and logs how it ended.
func (c *cronJob) run(due time.Time) {
	defer c.wg.Done()
	defer func() {
		c.mu.Lock()
		c.running = false
		c.mu.Unlock()
	}()
	ctx := context.WithValue(c.ctx, scheduledKey{}, due.UTC())
	if c.lock != nil && !c.claim(ctx, due) {
		return
	}
	if err := guard(ctx, c.name, "job", c.fn); err != nil && c.ctx.Err() == nil {
		Logger(c.ctx).Warn("bites: job failed, trying again at its next time", "job", c.name, "due", due.In(c.loc), "err", err)
	}
}

// claim asks the lock for the occurrence and says whether this instance runs it. A store that
// fails, panics or hangs is a skip, logged at Error; never a run without the lock.
func (c *cronJob) claim(ctx context.Context, due time.Time) (claimed bool) {
	log := Logger(c.ctx)
	key := c.name + "@" + strconv.FormatInt(due.Unix(), 10)
	cctx, cancel := context.WithTimeout(ctx, cronClaimTimeout)
	defer cancel()
	ok, err := func() (ok bool, err error) {
		defer func() {
			if v := recover(); v != nil {
				ok, err = false, fmt.Errorf("panic: %v", v)
			}
		}()
		return c.lock.Claim(cctx, key, cronClaimTTL)
	}()
	switch {
	case err != nil && c.ctx.Err() != nil:
		log.Warn("bites: job skipped: the app is stopping", "job", c.name, "due", due.In(c.loc))
	case err != nil:
		log.Error("bites: job skipped: the lock store failed, so this occurrence is not run here", "job", c.name, "due", due.In(c.loc), "key", key, "err", err)
	case !ok:
		log.Debug("bites: job not run here: another instance claimed this occurrence", "job", c.name, "due", due.In(c.loc), "key", key)
	}
	return err == nil && ok
}

// end stops the timer and waits for a run in progress; the app has cancelled its ctx.
func (c *cronJob) end() {
	c.mu.Lock()
	c.stopped = true
	if c.timer != nil {
		c.timer.Stop()
	}
	c.mu.Unlock()
	c.wg.Wait()
}
