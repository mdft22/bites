package natsbroker_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nevindra/bites"
	"github.com/nevindra/bites/bitetest"
	"github.com/nevindra/bites/modules/natsbroker"
)

// natsURL is the NATS server the integration tests use. They are skipped without it, with a
// message naming the variable, so a run with no server says what it left out.
func natsURL(t *testing.T) string {
	t.Helper()
	u := os.Getenv("BITES_NATS_URL")
	if u == "" {
		t.Skip("BITES_NATS_URL names no NATS server, so this test that needs one is skipped; for example BITES_NATS_URL=nats://127.0.0.1:54222")
	}
	return u
}

// logBuf is a log destination safe for the goroutines of the tests.
type logBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuf) Write(p []byte) (int, error) { l.mu.Lock(); defer l.mu.Unlock(); return l.b.Write(p) }
func (l *logBuf) String() string              { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }
func (l *logBuf) logger() *slog.Logger        { return slog.New(slog.NewTextHandler(l, nil)) }

var seq atomic.Int32

// prefixFor is a prefix of its own to the test, so tests that share a server do not hear each
// other; the Brokers of one instance set pass the same one.
func prefixFor(t *testing.T) string { return fmt.Sprintf("t%d_%d", os.Getpid(), seq.Add(1)) }

// newBroker returns a started Broker, with the short backoff the tests need.
func newBroker(t *testing.T, url, prefix string, o natsbroker.Options) *natsbroker.Broker {
	t.Helper()
	o.URL, o.Prefix = url, prefix
	if o.MinBackoff == 0 {
		o.MinBackoff, o.MaxBackoff = 20*time.Millisecond, 100*time.Millisecond
	}
	b := natsbroker.New(o)
	go b.Run(context.Background())
	t.Cleanup(func() { b.Close() })
	waitFor(t, "the broker to listen", b.Listening)
	return b
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

// proxy is a TCP forwarder in front of the NATS server that a test can cut, to make the client
// lose its connection without touching the server, and hold cut to keep it down.
type proxy struct {
	ln     net.Listener
	target string
	mu     sync.Mutex
	conns  []net.Conn
	paused bool
}

func newProxy(t *testing.T, natsURL string) *proxy {
	t.Helper()
	u, err := url.Parse(natsURL)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{ln: ln, target: u.Host}
	t.Cleanup(func() { ln.Close(); p.cut(false) })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			if p.paused {
				p.mu.Unlock()
				c.Close()
				continue
			}
			s, err := net.Dial("tcp", p.target)
			if err != nil {
				p.mu.Unlock()
				c.Close()
				continue
			}
			p.conns = append(p.conns, c, s)
			p.mu.Unlock()
			go func() { io.Copy(s, c); s.Close(); c.Close() }()
			go func() { io.Copy(c, s); s.Close(); c.Close() }()
		}
	}()
	return p
}

func (p *proxy) URL() string { return "nats://" + p.ln.Addr().String() }

// cut closes every connection through the proxy; hold also refuses new ones until resume.
func (p *proxy) cut(hold bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = hold
	for _, c := range p.conns {
		c.Close()
	}
	p.conns = nil
}

func (p *proxy) resume() { p.mu.Lock(); p.paused = false; p.mu.Unlock() }

// ---- no server needed

func TestNewRefusesAnOptionThatCannotWorkNamingTheLine(t *testing.T) {
	const ok = "nats://127.0.0.1:4222"
	for name, tc := range map[string]struct {
		o    natsbroker.Options
		want string
	}{
		"no url":          {natsbroker.Options{}, "natsbroker.New: URL is empty: give the NATS server, such as nats://127.0.0.1:4222"},
		"no scheme":       {natsbroker.Options{URL: "127.0.0.1:4222"}, `has no scheme: give nats://host:4222`},
		"a bad second":    {natsbroker.Options{URL: ok + ",http://x:1"}, `has the scheme "http"`},
		"no host":         {natsbroker.Options{URL: "nats://"}, "has no host: give nats://host:4222"},
		"long prefix":     {natsbroker.Options{URL: ok, Prefix: strings.Repeat("p", 65)}, "Prefix is 65 bytes, over 64"},
		"wildcard prefix": {natsbroker.Options{URL: ok, Prefix: "app.>"}, `Prefix "app.>" is not a NATS subject prefix: Prefix has '>' at byte 4, which NATS reads as a wildcard`},
		"space prefix":    {natsbroker.Options{URL: ok, Prefix: "my app"}, "Prefix has a space or control character"},
		"empty part":      {natsbroker.Options{URL: ok, Prefix: "a..b"}, "Prefix has an empty part"},
		"system prefix":   {natsbroker.Options{URL: ok, Prefix: "$SYS"}, `Prefix "$SYS" begins with a name NATS reserves`},
		"inbox prefix":    {natsbroker.Options{URL: ok, Prefix: "_INBOX.x"}, "reserves"},
		"backoff":         {natsbroker.Options{URL: ok, MinBackoff: time.Second, MaxBackoff: time.Millisecond}, "MaxBackoff 1ms is below MinBackoff 1s"},
		"negative":        {natsbroker.Options{URL: ok, KeepAlive: -1}, "a duration is negative"},
	} {
		func() {
			defer func() {
				r, _ := recover().(string)
				if !strings.Contains(r, tc.want) || !strings.Contains(r, "\n\tregistered at natsbroker_test.go:") {
					t.Errorf("%s: panic %q, want %q and the line", name, r, tc.want)
				}
			}()
			natsbroker.New(tc.o)
		}()
	}
}

func TestANameNatsCouldReadAsSomethingElseIsRefusedAndNeverEscaped(t *testing.T) {
	b := natsbroker.New(natsbroker.Options{URL: "nats://127.0.0.1:1"})
	defer b.Close()
	for name, want := range map[string]string{
		"":                       "the name is empty",
		"room.*":                 "wildcard",
		"room.>":                 "wildcard",
		">":                      "wildcard",
		"a b":                    "space or control character",
		"a\tb":                   "space or control character",
		"a\nb":                   "space or control character",
		"a\x00b":                 "space or control character",
		"a b":                    "space or control character",
		".room":                  "empty part",
		"room.":                  "empty part",
		"room..7":                "empty part",
		strings.Repeat("n", 256): "256 bytes, over 255",
		"bad\xffname":            "not valid UTF-8",
	} {
		err := b.Accepts(name)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("Accepts(%q) = %v, want it to say %q", name, err, want)
		}
	}
	for _, name := range []string{"room.7", "cache.sessions", "user:42", "a-b_c", "café.1", "k.a@b.com", strings.Repeat("n", 255), "$not.special.here"} {
		if err := b.Accepts(name); err != nil {
			t.Errorf("Accepts(%q) = %v, want nil", name, err)
		}
	}
	err := b.Accepts("room.*")
	if err == nil || !strings.Contains(err.Error(), "encode a key that comes from data") {
		t.Errorf("the refusal for a wildcard does not say what to change: %v", err)
	}
}

func TestAPublishBeforeRunHasConnectedIsNotConnectedAndSendsNothing(t *testing.T) {
	b := natsbroker.New(natsbroker.Options{URL: "nats://127.0.0.1:1"})
	defer b.Close()
	err := b.Publish(context.Background(), "room.1", []byte(`"x"`))
	if !errors.Is(err, natsbroker.ErrNotConnected) || !strings.Contains(err.Error(), "go broker.Run(ctx)") {
		t.Fatalf("Publish before Run returned %v, want ErrNotConnected saying to start Run", err)
	}
	if b.Listening() {
		t.Fatal("a Broker that never ran claims to listen")
	}
}

func TestAServerThatIsNotThereIsRetriedWithTheErrorAndThePauseLoggedAndPublishIsNotConnected(t *testing.T) {
	var logs logBuf
	var states []bool
	var mu sync.Mutex
	b := natsbroker.New(natsbroker.Options{
		URL: "nats://127.0.0.1:1", MinBackoff: 10 * time.Millisecond, MaxBackoff: 20 * time.Millisecond, Log: logs.logger(),
		OnState: func(up bool) { mu.Lock(); states = append(states, up); mu.Unlock() },
	})
	go b.Run(context.Background())
	defer b.Close()
	waitFor(t, "a retry to be logged", func() bool { return strings.Contains(logs.String(), "retry_in") })
	if !strings.Contains(logs.String(), "no connection to a NATS server, retrying") {
		t.Fatalf("the log says nothing of the retry:\n%s", logs.String())
	}
	if err := b.Publish(context.Background(), "room.1", []byte("1")); !errors.Is(err, natsbroker.ErrNotConnected) {
		t.Fatalf("Publish with no server returned %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(states) != 0 {
		t.Fatalf("OnState was called %v for a connection that never was live", states)
	}
}

func TestACloseStopsRunAndEverythingAfterItIsRefused(t *testing.T) {
	b := natsbroker.New(natsbroker.Options{URL: "nats://127.0.0.1:1"})
	ran := make(chan error, 1)
	go func() { ran <- b.Run(context.Background()) }()
	time.Sleep(20 * time.Millisecond)
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-ran:
		if err != nil {
			t.Fatalf("Run returned %v after Close, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Close")
	}
	if err := b.Close(); err != nil {
		t.Fatalf("a second Close returned %v", err)
	}
	if err := b.Publish(context.Background(), "x", nil); !errors.Is(err, natsbroker.ErrClosed) {
		t.Fatalf("Publish after Close returned %v, want ErrClosed", err)
	}
	b.Subscribe("x", func([]byte) { t.Error("a subscription after Close was delivered") }, func() {})() // a no-op
	late := natsbroker.New(natsbroker.Options{URL: "nats://127.0.0.1:1"})
	late.Close()
	if err := late.Run(context.Background()); !errors.Is(err, natsbroker.ErrClosed) {
		t.Fatalf("Run after Close returned %v", err)
	}
}

func TestARunCalledTwiceReturnsAnErrorNamingTheFix(t *testing.T) {
	b := natsbroker.New(natsbroker.Options{URL: "nats://127.0.0.1:1"})
	defer b.Close()
	go b.Run(context.Background())
	time.Sleep(20 * time.Millisecond)
	if err := b.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "one Broker has one connection") {
		t.Fatalf("a second Run returned %v", err)
	}
}

func TestACoreTopicRefusesABrokerNameItDoesNotCarry(t *testing.T) {
	b := natsbroker.New(natsbroker.Options{URL: "nats://127.0.0.1:1"})
	defer b.Close()
	defer func() {
		r := recover()
		if r == nil || !strings.Contains(fmt.Sprint(r), "the broker does not carry this name: the name has '*'") {
			t.Fatalf("panic %v", r)
		}
	}()
	bites.NewTopic[line]("room.*", b, bites.TopicOptions{})
}

func TestAKeyFromDataThatIsAWildcardIsAnErrorOfTheFamilyAndNothingIsSent(t *testing.T) {
	b := natsbroker.New(natsbroker.Options{URL: "nats://127.0.0.1:1"})
	defer b.Close()
	rooms := bites.NewTopics[line]("room.", b, bites.TopicsOptions{})
	for _, key := range []string{">", "*", "a b", "7.", "7..8"} {
		err := rooms.Publish(key, line{"x"})
		if err == nil || !strings.Contains(err.Error(), "the broker does not carry the key") {
			t.Errorf("Publish(%q) = %v, want the family to say the broker does not carry the key", key, err)
		}
	}
}

func TestAModuleImportsOnlyTheExportedCore(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip(err)
	}
	src, _ := os.ReadFile("natsbroker.go")
	if strings.Contains(string(src), "bites/internal") {
		t.Errorf("natsbroker.go imports the core's internal packages")
	}
}

type line struct {
	Text string `json:"text"`
}

// ---- against a real NATS server

func TestTheBrokerKeepsTheContractOfEveryBroker(t *testing.T) {
	url := natsURL(t)
	// The server's own limit is what the Broker reports, so the suite asks the server for it.
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	max := int(nc.MaxPayload())
	nc.Close()
	bitetest.TestBroker(t, bitetest.BrokerSuite{
		New: func(t *testing.T) (bites.Broker, bites.Broker) {
			p := prefixFor(t)
			return newBroker(t, url, p, natsbroker.Options{}), newBroker(t, url, p, natsbroker.Options{})
		},
		Name:       "conformance.topic",
		Refused:    []string{"", "a b", "a.*", "a.>", "a..b", ".a", "a.", "a\nb", strings.Repeat("n", 256), "bad\xffname"},
		MaxPayload: max,
	})
}

func TestANameIsTheSubjectUnderThePrefixAndInstancesOfOtherPrefixesNeverHearIt(t *testing.T) {
	url := natsURL(t)
	p := prefixFor(t)
	a := newBroker(t, url, p, natsbroker.Options{})
	other := newBroker(t, url, prefixFor(t), natsbroker.Options{})
	raw, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	wild, exact := make(chan *nats.Msg, 4), make(chan *nats.Msg, 4)
	raw.ChanSubscribe(p+".>", wild)
	raw.ChanSubscribe(p+".room.7", exact)
	raw.Flush()
	heardByOther := make(chan []byte, 1)
	other.Subscribe("room.7", func(b []byte) { heardByOther <- b }, func() {})
	if err := a.Publish(context.Background(), "room.7", []byte(`"hi"`)); err != nil {
		t.Fatal(err)
	}
	for _, c := range []chan *nats.Msg{wild, exact} {
		select {
		case m := <-c:
			if m.Subject != p+".room.7" || string(m.Data) != `"hi"` {
				t.Fatalf("the subject was %q with %q, want %q: the name under the prefix, unchanged", m.Subject, m.Data, p+".room.7")
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("a plain NATS client on %q heard nothing", p+".>")
		}
	}
	select {
	case b := <-heardByOther:
		t.Fatalf("an instance of another prefix heard %q", b)
	case <-time.After(150 * time.Millisecond):
	}
	// And what a plain client publishes under the prefix reaches a Broker's subscription.
	got := make(chan string, 1)
	a.Subscribe("raw.in", func(b []byte) { got <- string(b) }, func() {})
	raw.Publish(p+".raw.in", []byte(`1`))
	raw.Flush()
	select {
	case s := <-got:
		if s != "1" {
			t.Fatalf("heard %q", s)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the subscription did not hear a plain client")
	}
}

func TestAMessageForANameNobodyListensToIsDroppedAndALaterSubscriptionHearsOnlyLaterOnes(t *testing.T) {
	url := natsURL(t)
	p := prefixFor(t)
	a, b := newBroker(t, url, p, natsbroker.Options{}), newBroker(t, url, p, natsbroker.Options{})
	if err := a.Publish(context.Background(), "room.1", []byte(`"early"`)); err != nil {
		t.Fatalf("a publish to a name with no subscriber returned %v", err)
	}
	time.Sleep(100 * time.Millisecond) // the early message has reached b and been dropped there
	got := make(chan string, 4)
	defer b.Subscribe("room.1", func(x []byte) { got <- string(x) }, func() {})()
	// The subscription is live at once: the next message of a peer is heard, with no round trip to wait for.
	if err := a.Publish(context.Background(), "room.1", []byte(`"late"`)); err != nil {
		t.Fatal(err)
	}
	select {
	case x := <-got:
		if x != `"late"` {
			t.Fatalf("heard %s, want only the message published after the subscription", x)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the message published right after Subscribe was not heard")
	}
}

func TestAPayloadOverTheServersLimitIsAPayloadErrorNamingTheLimitAndSendsNothing(t *testing.T) {
	url := natsURL(t)
	b := newBroker(t, url, prefixFor(t), natsbroker.Options{})
	nc, _ := nats.Connect(url)
	max := int(nc.MaxPayload())
	nc.Close()
	var pe *bites.PayloadError
	err := b.Publish(context.Background(), "room.1", bytes.Repeat([]byte("x"), max+1))
	if !errors.As(err, &pe) || pe.Name != "room.1" || pe.Size != max+1 || pe.Limit != max {
		t.Fatalf("a payload one byte over returned %#v, want a *bites.PayloadError with Limit %d", err, max)
	}
	if !strings.Contains(err.Error(), "publish a reference") {
		t.Fatalf("the error does not say what to do: %v", err)
	}
	if err := b.Publish(context.Background(), "room.1", bytes.Repeat([]byte("x"), max)); err != nil {
		t.Fatalf("a payload of exactly the limit returned %v", err)
	}
}

func TestAPublishOnOneInstanceReachesBothInstancesInOrderAndTheOwnOnceEach(t *testing.T) {
	url := natsURL(t)
	p := prefixFor(t)
	a, b := newBroker(t, url, p, natsbroker.Options{}), newBroker(t, url, p, natsbroker.Options{})
	var mu sync.Mutex
	var onA, onB []string
	a.Subscribe("n", func(x []byte) { mu.Lock(); onA = append(onA, string(x)); mu.Unlock() }, func() {})
	b.Subscribe("n", func(x []byte) { mu.Lock(); onB = append(onB, string(x)); mu.Unlock() }, func() {})
	for i := range 50 {
		if err := a.Publish(context.Background(), "n", []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "both to hear 50", func() bool { mu.Lock(); defer mu.Unlock(); return len(onA) >= 50 && len(onB) >= 50 })
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	for i := range 50 {
		if onA[i] != fmt.Sprint(i) || onB[i] != fmt.Sprint(i) {
			t.Fatalf("message %d: a heard %q, b heard %q", i, onA[i], onB[i])
		}
	}
	if len(onA) != 50 || len(onB) != 50 {
		t.Fatalf("a heard %d and b %d, want 50 each", len(onA), len(onB))
	}
}

func TestSeveralSubscriptionsOfOneNameShareOneNatsSubscriptionAndEachGetsItsOwnBytes(t *testing.T) {
	url := natsURL(t)
	b := newBroker(t, url, prefixFor(t), natsbroker.Options{})
	got := make(chan []byte, 3)
	var stops []func()
	for range 3 {
		stops = append(stops, b.Subscribe("n", func(x []byte) { got <- x }, func() {}))
	}
	if err := b.Publish(context.Background(), "n", []byte("abc")); err != nil {
		t.Fatal(err)
	}
	var all [][]byte
	for range 3 {
		select {
		case x := <-got:
			all = append(all, x)
		case <-time.After(5 * time.Second):
			t.Fatal("a subscription heard nothing")
		}
	}
	all[0][0] = 'X'
	if string(all[1]) != "abc" || string(all[2]) != "abc" {
		t.Fatalf("a subscriber changing its bytes changed another's: %q", all)
	}
	stops[0]()
	stops[1]()
	if err := b.Publish(context.Background(), "n", []byte("def")); err != nil {
		t.Fatal(err)
	}
	if x := <-got; string(x) != "def" {
		t.Fatalf("the remaining subscription heard %q", x)
	}
	select {
	case x := <-got:
		t.Fatalf("a stopped subscription heard %q", x)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestALostConnectionIsReopenedLoggedAndTellsEverySubscriptionItsGapOnceAndOnStateFollowsIt(t *testing.T) {
	url := natsURL(t)
	px := newProxy(t, url)
	p := prefixFor(t)
	var logs logBuf
	var mu sync.Mutex
	var states []bool
	b := newBroker(t, px.URL(), p, natsbroker.Options{Log: logs.logger(), OnState: func(up bool) { mu.Lock(); states = append(states, up); mu.Unlock() }})
	peer := newBroker(t, url, p, natsbroker.Options{})
	var gaps atomic.Int32
	heard := make(chan string, 8)
	b.Subscribe("room.1", func(x []byte) { heard <- string(x) }, func() { gaps.Add(1) })
	b.Subscribe("room.2", func([]byte) {}, func() { gaps.Add(1) })
	if gaps.Load() != 0 {
		t.Fatalf("a subscription made while the connection was live was told of a gap")
	}
	if err := peer.Publish(context.Background(), "room.1", []byte(`"before"`)); err != nil {
		t.Fatal(err)
	}
	if got := <-heard; got != `"before"` {
		t.Fatalf("heard %q", got)
	}

	px.cut(true)
	waitFor(t, "the broker to notice", func() bool { return !b.Listening() })
	waitFor(t, "the loss to be logged", func() bool { return strings.Contains(logs.String(), "the NATS connection was lost") })
	// A message the peer publishes now is lost to the cut instance: at most once.
	if err := peer.Publish(context.Background(), "room.1", []byte(`"during"`)); err != nil {
		t.Fatal(err)
	}
	// And a publish from the cut instance is an error and is not kept for later.
	err := b.Publish(context.Background(), "room.1", []byte(`"unsent"`))
	if !errors.Is(err, natsbroker.ErrNotConnected) || !strings.Contains(err.Error(), "not sent and is not kept") {
		t.Fatalf("Publish while the connection was down returned %v", err)
	}
	// A subscription made while it is down is told of a gap too, once the connection is back.
	b.Subscribe("room.3", func([]byte) {}, func() { gaps.Add(1) })
	px.resume()
	waitFor(t, "the broker to listen again", b.Listening)
	waitFor(t, "three gaps", func() bool { return gaps.Load() == 3 })
	time.Sleep(100 * time.Millisecond)
	if n := gaps.Load(); n != 3 {
		t.Fatalf("the subscriptions were told %d gaps, want one each (3)", n)
	}
	if err := peer.Publish(context.Background(), "room.1", []byte(`"after"`)); err != nil {
		t.Fatal(err)
	}
	if got := <-heard; got != `"after"` {
		t.Fatalf("heard %q after the reconnect: the message of the gap is gone (at most once) and nothing unsent came late", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(states) != "[true false true]" {
		t.Fatalf("OnState was called with %v, want [true false true]", states)
	}
	if !strings.Contains(logs.String(), "retrying") || !strings.Contains(logs.String(), "retry_in") {
		t.Fatalf("the retry was not logged with its pause:\n%s", logs.String())
	}
}

func TestASubscriptionMadeBeforeRunConnectsIsDeliveredAfterAndToldTheGapOfTheFirstConnection(t *testing.T) {
	url := natsURL(t)
	px := newProxy(t, url)
	px.cut(true) // the server is not reachable yet
	p := prefixFor(t)
	b := natsbroker.New(natsbroker.Options{URL: px.URL(), Prefix: p, MinBackoff: 10 * time.Millisecond, MaxBackoff: 20 * time.Millisecond})
	t.Cleanup(func() { b.Close() })
	var gaps atomic.Int32
	heard := make(chan string, 1)
	b.Subscribe("room.1", func(x []byte) { heard <- string(x) }, func() { gaps.Add(1) })
	go b.Run(context.Background())
	time.Sleep(100 * time.Millisecond)
	px.resume()
	waitFor(t, "the broker to listen", b.Listening)
	waitFor(t, "the first gap", func() bool { return gaps.Load() == 1 })
	if err := b.Publish(context.Background(), "room.1", []byte(`"late"`)); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-heard:
		if got != `"late"` {
			t.Fatalf("heard %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a subscription made before the connection heard nothing after it")
	}
}

func TestASubscriptionThatFellBehindIsToldItsGapWhenNatsDropsItsMessages(t *testing.T) {
	url := natsURL(t)
	p := prefixFor(t)
	var logs logBuf
	b := newBroker(t, url, p, natsbroker.Options{Log: logs.logger()})
	var gaps atomic.Int32
	release := make(chan struct{})
	first := make(chan struct{}, 1)
	b.Subscribe("slow", func([]byte) {
		select {
		case first <- struct{}{}:
		default:
		}
		<-release
	}, func() { gaps.Add(1) })
	raw, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	defer close(release)
	// The first message holds the delivery; the next 65536 fill what NATS keeps for the subject and
	// the ones after are dropped.
	raw.Publish(p+".slow", []byte("1"))
	<-first
	for range 70000 {
		raw.Publish(p+".slow", []byte("x"))
	}
	raw.Flush()
	waitFor(t, "the gap of the dropped messages", func() bool { return gaps.Load() >= 1 })
	if !strings.Contains(logs.String(), "ran behind and NATS dropped messages") {
		t.Fatalf("the drop was not logged:\n%s", logs.String())
	}
}

func TestAConnectOptionOfTheCallerReachesTheClientAndTheBrokersOwnSettingsWin(t *testing.T) {
	url := natsURL(t)
	var named atomic.Bool
	b := newBroker(t, url, prefixFor(t), natsbroker.Options{ConnectOptions: []nats.Option{
		nats.Name("a name of the caller"),
		nats.NoEcho(), // the Broker's own settings win: it must hear itself
		nats.MaxReconnects(1),
		nats.ConnectHandler(func(*nats.Conn) { named.Store(true) }),
	}})
	heard := make(chan struct{}, 1)
	b.Subscribe("n", func([]byte) { heard <- struct{}{} }, func() {})
	if err := b.Publish(context.Background(), "n", []byte("1")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-heard:
	case <-time.After(5 * time.Second):
		t.Fatal("the Broker did not hear itself: a caller's nats.NoEcho took effect")
	}
}

func TestATopicOnTheBrokerReachesTheListenersOfEveryInstanceAndTellsAGap(t *testing.T) {
	url := natsURL(t)
	px := newProxy(t, url)
	p := prefixFor(t)
	type frame struct {
		Kind string `json:"kind"`
		Text string `json:"text,omitempty"`
	}
	br := [2]*natsbroker.Broker{newBroker(t, px.URL(), p, natsbroker.Options{}), newBroker(t, url, p, natsbroker.Options{})}
	var topics [2]*bites.Topic[frame]
	var got [2]chan frame
	for i := range topics {
		got[i] = make(chan frame, 8)
		topics[i] = bites.NewTopic[frame]("room.1", br[i], bites.TopicOptions{OnGap: func() { got[i] <- frame{Kind: "reload"} }})
		topics[i].Listen(func(f frame) { got[i] <- f })
	}
	waitFor(t, "both Topics to listen", func() bool { return br[0].Listening() && br[1].Listening() })
	if err := topics[1].Publish(frame{Kind: "message", Text: "hi"}); err != nil {
		t.Fatal(err)
	}
	for i := range got {
		select {
		case f := <-got[i]:
			if f.Text != "hi" {
				t.Fatalf("instance %d heard %+v", i, f)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("instance %d heard nothing", i)
		}
	}
	px.cut(false)
	select {
	case f := <-got[0]:
		if f.Kind != "reload" {
			t.Fatalf("instance 0 heard %+v after its connection was cut, want the OnGap", f)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnGap did not run after the connection was cut and came back")
	}
	select {
	case f := <-got[1]:
		t.Fatalf("the instance whose connection stayed up was told %+v", f)
	case <-time.After(150 * time.Millisecond):
	}
}

func TestAFamilyKeyReachesTheInstanceThatHoldsTheSocketFromAnInstanceThatHoldsNone(t *testing.T) {
	url := natsURL(t)
	p := prefixFor(t)
	a, b := newBroker(t, url, p, natsbroker.Options{}), newBroker(t, url, p, natsbroker.Options{})
	type frame struct {
		Text string `json:"text"`
	}
	fa := bites.NewTopics[frame]("user.", a, bites.TopicsOptions{})
	fb := bites.NewTopics[frame]("user.", b, bites.TopicsOptions{})
	app := bites.New()
	app.WebSocket("/ws", func(_ context.Context, _ struct{}, s *bites.Socket[frame]) (bites.Inbox[struct{}], error) {
		return bites.Inbox[struct{}]{}, fb.Subscribe("42", s)
	})
	c := bitetest.Start(t, app).DialWS(bitetest.WSRequest{Target: "/ws"})
	waitFor(t, "the socket to join", func() bool { return fb.Len("42") == 1 })
	if err := fa.Publish("42", frame{Text: "for you"}); err != nil {
		t.Fatal(err)
	}
	if got := c.ReadText(); got != `{"text":"for you"}` {
		t.Fatalf("the socket got %s", got)
	}
}

func TestConcurrentSubscribeStopAndPublishNeverRaceOrLoseTheBrokerOrLeakASubscription(t *testing.T) {
	url := natsURL(t)
	b := newBroker(t, url, prefixFor(t), natsbroker.Options{})
	var wg sync.WaitGroup
	var heard atomic.Int64
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := fmt.Sprintf("n.%d", w%3)
			for range 100 {
				stop := b.Subscribe(name, func([]byte) { heard.Add(1) }, func() {})
				b.Publish(context.Background(), name, []byte(`1`))
				stop()
				stop()
			}
		}()
	}
	wg.Wait()
	if heard.Load() == 0 {
		t.Fatal("no subscription heard anything")
	}
	// The broker still works.
	got := make(chan struct{}, 1)
	defer b.Subscribe("after", func([]byte) { got <- struct{}{} }, func() {})()
	if err := b.Publish(context.Background(), "after", []byte(`1`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("the Broker stopped delivering after the churn")
	}
}
