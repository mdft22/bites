package redissession_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nevindra/bites"
	"github.com/nevindra/bites/bitetest"
	"github.com/nevindra/bites/modules/redissession"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

// redisURL is the Redis or Valkey the integration tests use. They are skipped without it, with a
// message naming the variable, so a run with no server says what it left out.
func redisURL(t *testing.T) string {
	t.Helper()
	u := os.Getenv("BITES_REDIS_URL")
	if u == "" {
		t.Skip("BITES_REDIS_URL names no Redis or Valkey, so this test that needs one is skipped; for example BITES_REDIS_URL=redis://127.0.0.1:56379/0")
	}
	return u
}

func newClient(t *testing.T) *redis.Client {
	t.Helper()
	o, err := redis.ParseURL(redisURL(t))
	if err != nil {
		t.Fatal(err)
	}
	c := redis.NewClient(o)
	t.Cleanup(func() { c.Close() })
	return c
}

var seq atomic.Int32

// newStore returns a Store on a prefix of its own to the test, whose keys are deleted at the end, so
// tests that share a server run in parallel without hearing each other.
func newStore(t *testing.T) (*redissession.Store, *redis.Client, string) {
	t.Helper()
	c := newClient(t)
	prefix := fmt.Sprintf("t_sess_%d_%d", os.Getpid(), seq.Add(1))
	t.Cleanup(func() { deleteAll(c, prefix) })
	return redissession.New(redissession.Options{Client: c, Prefix: prefix}), c, prefix
}

func deleteAll(c redis.UniversalClient, prefix string) {
	keys, _ := c.Keys(context.Background(), "{"+prefix+"}:*").Result()
	if len(keys) > 0 {
		c.Del(context.Background(), keys...)
	}
}

func keysOf(t *testing.T, c redis.UniversalClient, prefix string) []string {
	t.Helper()
	keys, err := c.Keys(ctx, "{"+prefix+"}:*").Result()
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

func TestTheStoreKeepsTheContractOfEverySessionStoreAndOwnerSessionStore(t *testing.T) {
	newOwned := func(t *testing.T) bites.OwnerSessionStore {
		st, _, _ := newStore(t)
		return st
	}
	bitetest.TestSessionStore(t, func(t *testing.T) bites.SessionStore { return newOwned(t) })
	bitetest.TestOwnerSessionStore(t, newOwned)
}

// A cluster is optional: BITES_REDIS_CLUSTER_ADDR names a node of one (a single node that owns every
// slot is enough, since a script that names keys of two slots is refused there as on any cluster).
func TestTheStoreKeepsTheContractOnACluster(t *testing.T) {
	addr := os.Getenv("BITES_REDIS_CLUSTER_ADDR")
	if addr == "" {
		t.Skip("BITES_REDIS_CLUSTER_ADDR names no Redis Cluster node, so this test is skipped; for example BITES_REDIS_CLUSTER_ADDR=127.0.0.1:56381")
	}
	c := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{addr}})
	t.Cleanup(func() { c.Close() })
	newOwned := func(t *testing.T) bites.OwnerSessionStore {
		return redissession.New(redissession.Options{Client: c, Prefix: fmt.Sprintf("t_sess_c_%d_%d", os.Getpid(), seq.Add(1))})
	}
	bitetest.TestSessionStore(t, func(t *testing.T) bites.SessionStore { return newOwned(t) })
	bitetest.TestOwnerSessionStore(t, newOwned)
}

// appCtx returns a ctx that bites.Now reads as clk: the ctx of a Service's Start in an app on a
// bitetest.Clock.
func appCtx(t *testing.T) (context.Context, *bitetest.Clock) {
	t.Helper()
	clk := bitetest.NewClock()
	got := make(chan context.Context, 1)
	app := bites.New(bites.WithClock(clk))
	app.Service(bites.Service{Name: "clock", Start: func(c context.Context) error { got <- c; return nil }})
	bitetest.Start(t, app)
	select {
	case c := <-got:
		return c, clk
	case <-time.After(5 * time.Second):
		t.Fatal("the app did not start")
		return nil, nil
	}
}

func TestExpiryIsJudgedOnTheAppClockNotRedisClock(t *testing.T) {
	t.Parallel()
	st, c, prefix := newStore(t)
	actx, clk := appCtx(t) // 2026-01-01: long before the server's now
	if err := st.Save(actx, "a", []byte("x"), clk.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, found, err := st.Load(actx, "a"); err != nil || !found {
		t.Fatalf("a session live on the app's clock was not loaded: found=%v err=%v", found, err)
	}
	clk.Advance(2 * time.Hour)
	if _, found, _ := st.Load(actx, "a"); found {
		t.Fatal("a session past its expiry on the app's clock was loaded")
	}
	if n := len(keysOf(t, c, prefix)); n != 1 {
		t.Fatalf("%d keys; want the session still there, since Redis's own TTL has an hour to run", n)
	}
}

func TestTheKeyHasATTLOfTheExpiryAndATouchMovesIt(t *testing.T) {
	t.Parallel()
	st, c, prefix := newStore(t)
	if err := st.SaveOwned(ctx, "a", "ana", []byte("x"), time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	ttls := func() (session, owner time.Duration) {
		for _, k := range keysOf(t, c, prefix) {
			d := c.PTTL(ctx, k).Val()
			if strings.Contains(k, ":owner:") {
				owner = d
			} else {
				session = d
			}
		}
		return
	}
	if s, o := ttls(); s < 59*time.Second || s > time.Minute || o < 59*time.Second || o > time.Minute {
		t.Fatalf("TTLs after a Save for a minute: session %v, owner's set %v", s, o)
	}
	if ok, err := st.Touch(ctx, "a", time.Now().Add(time.Hour)); err != nil || !ok {
		t.Fatalf("Touch = %v, %v", ok, err)
	}
	if s, o := ttls(); s < 59*time.Minute || o < 59*time.Minute {
		t.Fatalf("TTLs after a Touch to an hour: session %v, owner's set %v", s, o)
	}
	if err := st.SaveOwned(ctx, "b", "ana", []byte("y"), time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, o := ttls(); o < 59*time.Minute {
		t.Fatalf("a shorter session shortened the TTL of its owner's set to %v: the set must outlive its longest session", o)
	}
}

func TestRedisDeletesASessionWhenItsTimeEnds(t *testing.T) {
	t.Parallel()
	st, c, prefix := newStore(t)
	if err := st.SaveOwned(ctx, "a", "ana", []byte("x"), time.Now().Add(700*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if n := len(keysOf(t, c, prefix)); n != 2 {
		t.Fatalf("%d keys after a Save, want the session and its owner's set", n)
	}
	time.Sleep(1200 * time.Millisecond)
	if keys := keysOf(t, c, prefix); len(keys) != 0 {
		t.Fatalf("keys %v remain after the session's expiry: Redis should have deleted them and nothing sweeps", keys)
	}
}

func TestAnExpiryInThePastKeepsNothingAndDeletesWhatTheIdHad(t *testing.T) {
	t.Parallel()
	st, c, prefix := newStore(t)
	st.SaveOwned(ctx, "a", "ana", []byte("x"), time.Now().Add(time.Hour))
	if err := st.SaveOwned(ctx, "a", "ana", []byte("y"), time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := st.Load(ctx, "a"); found {
		t.Fatal("a session saved already expired was found")
	}
	for _, k := range keysOf(t, c, prefix) {
		if !strings.Contains(k, ":owner:") {
			t.Fatalf("the session key %s remains after a Save with an expiry in the past", k)
		}
	}
}

func TestTheIdAndTheOwnerAreNotStoredInTheClear(t *testing.T) {
	t.Parallel()
	st, c, prefix := newStore(t)
	st.SaveOwned(ctx, "secret-session-id", "ana@example.com", []byte("payload"), time.Now().Add(time.Hour))
	keys := keysOf(t, c, prefix)
	if len(keys) != 2 {
		t.Fatalf("keys = %v, want the session and its owner's set", keys)
	}
	for _, k := range keys {
		if strings.Contains(k, "secret") || strings.Contains(k, "ana") {
			t.Fatalf("the key %q holds an id or an owner in the clear", k)
		}
		if strings.Contains(k, ":id:") {
			o, _ := c.HGet(ctx, k, "o").Result()
			if o == "" || strings.Contains(o, "ana") {
				t.Fatalf("the owner field is %q, want the hash of the owner", o)
			}
		} else if ms := c.ZRange(ctx, k, 0, -1).Val(); len(ms) != 1 || strings.Contains(ms[0], "secret") || len(ms[0]) != 43 {
			t.Fatalf("the owner's set holds %q, want one SHA-256", ms)
		}
	}
}

func TestEveryKeyOfAStoreSharesOneClusterHashTag(t *testing.T) {
	t.Parallel()
	st, c, prefix := newStore(t)
	st.SaveOwned(ctx, "a", "ana", []byte("x"), time.Now().Add(time.Hour))
	st.SaveOwned(ctx, "b", "budi", []byte("y"), time.Now().Add(time.Hour))
	keys := keysOf(t, c, prefix)
	if len(keys) != 4 {
		t.Fatalf("keys = %v, want two sessions and two owners' sets", keys)
	}
	for _, k := range keys {
		if !strings.HasPrefix(k, "{"+prefix+"}:") {
			t.Fatalf("key %q does not start with the prefix as a hash tag", k)
		}
	}
}

func TestADeletedSessionIsNeverCountedByItsOwnersRevoke(t *testing.T) {
	t.Parallel()
	st, c, prefix := newStore(t)
	st.SaveOwned(ctx, "a1", "ana", []byte("x"), time.Now().Add(time.Hour))
	st.SaveOwned(ctx, "a2", "ana", []byte("y"), time.Now().Add(time.Hour))
	st.SaveOwned(ctx, "a3", "ana", []byte("z"), time.Now().Add(time.Hour))
	st.Delete(ctx, "a1")
	st.SaveOwned(ctx, "a2", "budi", []byte("y"), time.Now().Add(time.Hour)) // a2 changes hands
	set := "{" + prefix + "}:owner:"
	var ana string
	for _, k := range keysOf(t, c, prefix) {
		if strings.HasPrefix(k, set) && c.ZCard(ctx, k).Val() == 3 {
			ana = k
		}
	}
	if ana == "" {
		t.Fatal("ana's set does not hold the three ids that were saved for her: a delete or a change of owner must be found lazily, not by a second write")
	}
	if n, err := st.DeleteOwner(ctx, "ana", ""); err != nil || n != 1 {
		t.Fatalf("DeleteOwner(ana) = %d, %v, want 1: only a3 is still hers", n, err)
	}
	if n := c.Exists(ctx, ana).Val(); n != 0 {
		t.Fatal("the stale members of ana's set were not removed by the DeleteOwner that found them")
	}
	if _, found, _ := st.Load(ctx, "a2"); !found {
		t.Fatal("DeleteOwner(ana) ended a2, which belongs to budi")
	}
}

func TestAnOwnersSetIsTrimmedOfExpiredSessionsWhenTheOwnerSavesAnother(t *testing.T) {
	t.Parallel()
	st, c, prefix := newStore(t)
	actx, clk := appCtx(t)
	for i := range 5 {
		st.SaveOwned(actx, fmt.Sprint("old-", i), "ana", []byte("x"), clk.Now().Add(time.Minute))
	}
	clk.Advance(2 * time.Minute)
	st.SaveOwned(actx, "new", "ana", []byte("y"), clk.Now().Add(time.Hour))
	for _, k := range keysOf(t, c, prefix) {
		if strings.Contains(k, ":owner:") {
			if n := c.ZCard(ctx, k).Val(); n != 1 {
				t.Fatalf("the owner's set holds %d ids, want only the live one: it would grow for as long as the owner signs in", n)
			}
		}
	}
}

func TestATouchedSessionIsNotTrimmedFromItsOwnersSetAndSoIsRevoked(t *testing.T) {
	t.Parallel()
	st, _, _ := newStore(t)
	actx, clk := appCtx(t)
	st.SaveOwned(actx, "slid", "ana", []byte("x"), clk.Now().Add(time.Minute))
	if ok, err := st.Touch(actx, "slid", clk.Now().Add(3*time.Hour)); err != nil || !ok {
		t.Fatalf("Touch = %v, %v", ok, err)
	}
	clk.Advance(2 * time.Minute) // past the expiry it was saved with
	st.SaveOwned(actx, "other", "ana", []byte("y"), clk.Now().Add(time.Hour))
	if n, err := st.DeleteOwner(actx, "ana", ""); err != nil || n != 2 {
		t.Fatalf("DeleteOwner(ana) = %d, %v, want 2: a session that slid is still hers, and a revoke that misses it leaves a stolen cookie working", n, err)
	}
	if _, found, _ := st.Load(actx, "slid"); found {
		t.Fatal("a session that slid survived the revoke of its owner")
	}
}

// changeOwnerOnce is a hook that, the first time a client reads a session's owner field, makes a
// second client save the session for another owner, so a Touch finds its read stale.
type changeOwnerOnce struct {
	other *redissession.Store
	done  atomic.Bool
}

func (h *changeOwnerOnce) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *changeOwnerOnce) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(c context.Context, cmd redis.Cmder) error {
		err := next(c, cmd)
		if cmd.Name() == "hget" && h.done.CompareAndSwap(false, true) {
			h.other.SaveOwned(ctx, "a", "budi", []byte("x"), time.Now().Add(2*time.Hour))
		}
		return err
	}
}
func (h *changeOwnerOnce) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestATouchThatReadAStaleOwnerReadsAgainAndMovesTheRightSet(t *testing.T) {
	t.Parallel()
	st, c, prefix := newStore(t)
	other := redissession.New(redissession.Options{Client: newClient(t), Prefix: prefix})
	st.SaveOwned(ctx, "a", "ana", []byte("x"), time.Now().Add(time.Hour))
	c.AddHook(&changeOwnerOnce{other: other})
	if ok, err := st.Touch(ctx, "a", time.Now().Add(3*time.Hour)); err != nil || !ok {
		t.Fatalf("Touch = %v, %v, want it to read the new owner and succeed", ok, err)
	}
	if n, err := other.DeleteOwner(ctx, "budi", ""); err != nil || n != 1 {
		t.Fatalf("DeleteOwner(budi) = %d, %v, want 1: the touch must have kept a in budi's set", n, err)
	}
}

func TestTwoStoresOnOneRedisShareTheirSessions(t *testing.T) {
	t.Parallel()
	a, _, prefix := newStore(t)
	b := redissession.New(redissession.Options{Client: newClient(t), Prefix: prefix}) // an instance has a client of its own
	a.SaveOwned(ctx, "a1", "ana", []byte("x"), time.Now().Add(time.Hour))
	if d, ok, err := b.Load(ctx, "a1"); err != nil || !ok || string(d) != "x" {
		t.Fatalf("an instance did not see a session another saved: %q, %v, %v", d, ok, err)
	}
	if n, err := b.DeleteOwner(ctx, "ana", ""); err != nil || n != 1 {
		t.Fatalf("DeleteOwner on the other instance = %d, %v", n, err)
	}
	if _, ok, _ := a.Load(ctx, "a1"); ok {
		t.Fatal("a session revoked on one instance was still loaded on another")
	}
}

func TestStoresWithDifferentPrefixesDoNotShareSessionsOrOwners(t *testing.T) {
	t.Parallel()
	a, c, prefix := newStore(t)
	b := redissession.New(redissession.Options{Client: c, Prefix: prefix + "_other"})
	t.Cleanup(func() { deleteAll(c, prefix+"_other") })
	a.SaveOwned(ctx, "a1", "ana", []byte("x"), time.Now().Add(time.Hour))
	b.SaveOwned(ctx, "b1", "ana", []byte("y"), time.Now().Add(time.Hour))
	if _, ok, _ := b.Load(ctx, "a1"); ok {
		t.Fatal("a Store on another prefix loaded a session of this one")
	}
	if n, _ := a.DeleteOwner(ctx, "ana", ""); n != 1 {
		t.Fatalf("DeleteOwner forgot %d sessions, want 1", n)
	}
	if _, ok, _ := b.Load(ctx, "b1"); !ok {
		t.Fatal("DeleteOwner on one prefix ended a session on another")
	}
}

func TestAnOwnerWithMoreSessionsThanOneBatchIsEndedWhole(t *testing.T) {
	t.Parallel()
	st, _, _ := newStore(t)
	const n = 1234
	for i := range n {
		if err := st.SaveOwned(ctx, fmt.Sprint("id-", i), "ana", []byte("x"), time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := st.DeleteOwner(ctx, "ana", "id-77"); err != nil || got != n-1 {
		t.Fatalf("DeleteOwner = %d, %v, want %d", got, err, n-1)
	}
	if _, ok, _ := st.Load(ctx, "id-77"); !ok {
		t.Fatal("the kept session was ended")
	}
	if _, ok, _ := st.Load(ctx, "id-1000"); ok {
		t.Fatal("a session of the second batch survived")
	}
}

func TestAStoreErrorIsWrappedWithTheOperation(t *testing.T) {
	t.Parallel()
	l, err := net.Listen("tcp", "127.0.0.1:0") // a port nothing listens on once it is closed
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	c := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: -1, DialTimeout: time.Second})
	defer c.Close()
	st := redissession.New(redissession.Options{Client: c})
	var op *net.OpError
	check := func(name string, err error) {
		t.Helper()
		if err == nil || !strings.HasPrefix(err.Error(), "redissession: "+name+": ") || !errors.As(err, &op) {
			t.Errorf("%s with no Redis = %v, want redissession: %s: wrapping the net error", name, err, name)
		}
	}
	_, _, err = st.Load(ctx, "a")
	check("load", err)
	check("save", st.Save(ctx, "a", nil, time.Now().Add(time.Hour)))
	_, err = st.Touch(ctx, "a", time.Now().Add(time.Hour))
	check("touch", err)
	check("delete", st.Delete(ctx, "a"))
	_, err = st.DeleteOwner(ctx, "ana", "")
	check("delete owner", err)
}

func TestNewRefusesABadOptionAndSaysWhatToChange(t *testing.T) {
	t.Parallel()
	c := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}) // lazy: nothing connects
	defer c.Close()
	for _, tc := range []struct {
		name string
		o    redissession.Options
		want string
	}{
		{"a nil client", redissession.Options{}, "Client is nil: give the application's client, such as redis.NewClient("},
		{"a prefix with an opening brace", redissession.Options{Client: c, Prefix: "my{app"}, `Prefix "my{app" contains a brace`},
		{"a prefix with a closing brace", redissession.Options{Client: c, Prefix: "{myapp}"}, `give a prefix such as "myapp" without '{' or '}'`},
	} {
		func() {
			defer func() {
				msg, _ := recover().(string)
				if !strings.Contains(msg, tc.want) || !strings.Contains(msg, "registered at redissession_test.go:") {
					t.Errorf("%s: panic = %q; want it to contain %q and the caller's line", tc.name, msg, tc.want)
				}
			}()
			redissession.New(tc.o)
			t.Errorf("%s: New did not panic", tc.name)
		}()
	}
}

type Login struct {
	UserID string `json:"user_id"`
}

func (l Login) SessionOwner() string { return l.UserID }

func TestARevokedOwnerIsSignedOutOnTheNextRequestThroughBites(t *testing.T) {
	t.Parallel()
	st, c, prefix := newStore(t)
	app := bites.New(bites.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	logins := bites.NewSessions[Login](bites.SessionOptions{Keys: [][]byte{bytes.Repeat([]byte{7}, bites.SessionKeyLen)}, Store: st})
	app.Provide(logins.Load)
	app.Post("/login/{user}", func(ctx context.Context, in struct {
		S    bites.Session[Login]
		Path struct{ User string }
	}) (bites.NoContent, error) {
		cookie, err := in.S.Renew(ctx, Login{UserID: in.Path.User})
		return bites.NoContent{Cookies: []bites.Cookie{cookie}}, err
	})
	app.Get("/me", func(_ context.Context, in struct{ S bites.Session[Login] }) (string, error) {
		if in.S.IsNew() {
			return "anonymous", nil
		}
		return in.S.Value.UserID, nil
	})
	srv := bitetest.Start(t, app)
	login := func(user string) string {
		sc := srv.Do(bitetest.Request{Method: "POST", Target: "/login/" + user}).Get("set-cookie")
		return strings.SplitN(sc, ";", 2)[0]
	}
	me := func(cookie string) string {
		r := srv.Do(bitetest.Request{Method: "GET", Target: "/me", Header: map[string][]string{"Cookie": {cookie}}})
		return strings.Trim(r.Text(), `"`)
	}
	laptop, phone, budi := login("ana"), login("ana"), login("budi")
	if me(laptop) != "ana" || me(phone) != "ana" || me(budi) != "budi" {
		t.Fatal("the sessions were not kept in Redis")
	}
	if n := len(keysOf(t, c, prefix)); n != 5 {
		t.Fatalf("%d keys; want 3 sessions and 2 owners' sets", n)
	}
	n, err := logins.RevokeOwner(ctx, "ana")
	if err != nil || n != 2 {
		t.Fatalf("RevokeOwner = %d, %v; want 2", n, err)
	}
	if me(laptop) != "anonymous" || me(phone) != "anonymous" {
		t.Fatal("a revoked session was still signed in on the next request")
	}
	if me(budi) != "budi" {
		t.Fatal("another owner was signed out")
	}
}

// commands counts the commands a client sends.
type commands struct{ n atomic.Int32 }

func (c *commands) DialHook(next redis.DialHook) redis.DialHook { return next }
func (c *commands) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error { c.n.Add(1); return next(ctx, cmd) }
}
func (c *commands) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error { c.n.Add(int32(len(cmds))); return next(ctx, cmds) }
}

func TestEachCallIsOneRoundTripExceptATouchWhichIsTwo(t *testing.T) {
	t.Parallel()
	st, c, _ := newStore(t)
	exp := time.Now().Add(time.Hour)
	st.SaveOwned(ctx, "warm", "ana", []byte("x"), exp) // the first use may send a script whole
	st.Touch(ctx, "warm", exp)
	st.DeleteOwner(ctx, "nobody", "")
	cmds := &commands{}
	c.AddHook(cmds)
	for _, tc := range []struct {
		name string
		want int32
		do   func() error
	}{
		{"SaveOwned", 1, func() error { return st.SaveOwned(ctx, "a", "ana", []byte("x"), exp) }},
		{"Save", 1, func() error { return st.Save(ctx, "b", []byte("x"), exp) }},
		{"Load", 1, func() error { _, _, err := st.Load(ctx, "a"); return err }},
		{"Touch", 2, func() error { _, err := st.Touch(ctx, "a", exp); return err }},
		{"Touch of an id with nothing", 1, func() error { _, err := st.Touch(ctx, "nothing", exp); return err }},
		{"Delete", 1, func() error { return st.Delete(ctx, "b") }},
		{"DeleteOwner of a few sessions", 2, func() error { _, err := st.DeleteOwner(ctx, "ana", ""); return err }},
	} {
		before := cmds.n.Load()
		if err := tc.do(); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if n := cmds.n.Load() - before; n != tc.want {
			t.Errorf("%s sent %d commands, want %d", tc.name, n, tc.want)
		}
	}
}
