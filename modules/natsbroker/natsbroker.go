// Package natsbroker is a bites.Broker over core NATS publish and subscribe: a bites.Topic made
// with bites.NewTopic and this Broker reaches the WebSockets of every instance of an application
// that shares a NATS server or cluster (ADR 0094).
//
// Delivery is at most once. A message is delivered to each instance that is subscribed and
// connected when the server routes it, once, and to no one else: core NATS keeps nothing for an
// instance that is down, and drops what an instance too slow to read it would otherwise have to
// buffer. The Broker says so when it knows: a connection that dropped and came back, and a
// subscription NATS dropped messages for, each call the gap of the subscriptions they touch (a
// Topic's OnGap), so the application tells its clients to read their state again. A Publish made
// while the connection is down returns ErrNotConnected and sends nothing; it is never queued to
// be sent later and out of order. JetStream is not used: the Broker contract has no
// acknowledgement or replay, and durability is what an application's history route is for.
//
// A Topic's name becomes the NATS subject Prefix + "." + name, unchanged, so "room.7" under the
// default prefix is the subject "bites.room.7" and nats sub "bites.>" shows an application's
// traffic. Each instance holds one subscription to "bites.>" and keeps the names with a Topic
// listening, as pgnotify holds one LISTEN, so a Topic that starts listening hears the next message
// with no round trip to wait for; the price is that an instance is sent the messages of the
// names it has no Socket for, and drops them. A name NATS would read differently, one with a wildcard ("*" or ">"), a space or an
// empty part ("a..b"), is refused by Accepts and not escaped, so a key from data cannot reach
// another name's messages and the subject an operator sees is the name the code wrote.
//
// It depends on github.com/nats-io/nats.go. A Broker is started with Run, which blocks, and
// stopped with Close.
package natsbroker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/url"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/nats-io/nats.go"
	"github.com/nevindra/bites"
)

// maxName is the longest Topic name carried.
const maxName = 255

// maxPrefix is the longest Options.Prefix.
const maxPrefix = 64

// defaultPrefix is the subject prefix when Options.Prefix is empty.
const defaultPrefix = "bites"

// pendingMsgs and pendingBytes bound what NATS holds for one subject while its deliveries run
// behind: past either, NATS drops the messages that arrive and the Broker tells the subscriptions
// of that subject their gap. They are the nats.go defaults, written down so a library change does
// not change the memory an instance may hold for a subject.
const (
	pendingMsgs  = 65536
	pendingBytes = 64 << 20
)

// ErrClosed is returned by Publish after Close, or after Run ended.
var ErrClosed = errors.New("natsbroker: the Broker is closed")

// ErrNotConnected is returned by Publish while there is no live connection to a NATS server:
// before Run has connected, while it reconnects, or after the library gave up. Nothing is sent
// and nothing is kept to send later. It is wrapped with the connection's state, so a program
// finds it with errors.Is.
var ErrNotConnected = errors.New("natsbroker: not connected to a NATS server")

// Options are the settings of New.
type Options struct {
	// URL is the NATS server, such as nats://127.0.0.1:4222, or several of a cluster separated
	// by commas. The scheme is required: nats, tls, ws or wss. User and password or a token in
	// the URL are read by the client. Required.
	URL string
	// Prefix is put before every Topic name to make the NATS subject: Prefix + "." + name.
	// Instances that share Topics use the same one, and applications that share a NATS server use
	// different ones, so a subject permission such as "bites.>" fits one application. Empty means
	// "bites". It is at most 64 bytes, made of parts separated by dots, with no wildcard, space,
	// empty part or leading "$" (reserved by NATS).
	Prefix string
	// ConnectOptions are nats.go options applied to the connection before the Broker's own, for
	// what the Broker does not decide: TLS (nats.RootCAs, nats.ClientCert), credentials
	// (nats.UserCredentials, nats.Nkey, nats.UserInfo, nats.Token) and nats.Name. The Broker sets
	// reconnection (forever, with MinBackoff and MaxBackoff), publish buffering (off), echo (on),
	// ping interval and every handler after yours, so an option of yours for those is replaced.
	ConnectOptions []nats.Option
	// Log receives each lost connection and each retry. Nil means slog.Default().
	Log *slog.Logger
	// OnState is called when the connection becomes live (true) and when it is lost (false), on
	// a goroutine of the NATS client, one call at a time, in order. It is how an application fails
	// its readiness while the instance cannot hear: app.SetReady. It must not block.
	OnState func(listening bool)
	// MinBackoff and MaxBackoff bound the pause before a reconnect: it starts at MinBackoff,
	// doubles with each failed attempt up to MaxBackoff, and has 20% of jitter so instances that
	// lost the server together do not return together. Zero means 100ms and 10s.
	MinBackoff, MaxBackoff time.Duration
	// PublishTimeout bounds one Publish: the round trip to the server that confirms it has the
	// message. Zero means 5s.
	PublishTimeout time.Duration
	// KeepAlive is how often an idle connection is pinged, so a connection a firewall dropped
	// without a reset is found in at most three periods. Zero means 30s.
	KeepAlive time.Duration
}

// Broker implements bites.Broker over core NATS.
type Broker struct {
	opt    Options
	prefix string // opt.Prefix and the dot

	mu     sync.RWMutex
	names  map[string]*entry
	conn   *nats.Conn
	up     bool // the connection is live, as the client last said
	closed bool

	evMu     sync.Mutex // serializes OnState and gap calls, so they keep the order of the events
	reported bool       // what OnState was last told; guarded by evMu

	running   atomic.Bool
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	closeOnce sync.Once
}

// entry is one name with a subscriber.
type entry struct {
	name string
	subs map[*sub]struct{}
}

type sub struct {
	deliver func([]byte)
	gap     func()
	missed  bool // messages may have been lost since it subscribed
}

// New returns a Broker; it connects nothing until Run. It panics, naming the line that called it,
// when URL is empty or does not name a NATS server with a scheme, Prefix is not a valid subject
// prefix, or a duration is negative.
func New(o Options) *Broker {
	_, file, line, _ := runtime.Caller(1)
	where := fmt.Sprintf("%s:%d", filepath.Base(file), line)
	fail := func(format string, args ...any) {
		panic(fmt.Sprintf("natsbroker.New: %s\n\tregistered at %s", fmt.Sprintf(format, args...), where))
	}
	if o.URL == "" {
		fail("URL is empty: give the NATS server, such as nats://127.0.0.1:4222, or several of a cluster separated by commas")
	}
	for _, s := range strings.Split(o.URL, ",") {
		s = strings.TrimSpace(s)
		u, err := url.Parse(s)
		switch {
		case !strings.Contains(s, "://"):
			fail("URL %q has no scheme: give nats://host:4222, or tls://, ws:// or wss:// for those transports", s)
		case err != nil:
			fail("URL %q does not parse: %v", s, err)
		case u.Scheme != "nats" && u.Scheme != "tls" && u.Scheme != "ws" && u.Scheme != "wss":
			fail("URL %q has the scheme %q: give nats://host:4222, or tls://, ws:// or wss:// for those transports", s, u.Scheme)
		case u.Hostname() == "":
			fail("URL %q has no host: give nats://host:4222", s)
		}
	}
	if o.Prefix == "" {
		o.Prefix = defaultPrefix
	}
	if len(o.Prefix) > maxPrefix {
		fail("Prefix is %d bytes, over %d: give a shorter one, such as %q", len(o.Prefix), maxPrefix, defaultPrefix)
	}
	if err := checkSubject("Prefix", o.Prefix); err != nil {
		fail("Prefix %q is not a NATS subject prefix: %v", o.Prefix, err)
	}
	if strings.HasPrefix(o.Prefix, "$") || o.Prefix == "_INBOX" || strings.HasPrefix(o.Prefix, "_INBOX.") {
		fail("Prefix %q begins with a name NATS reserves for the server and JetStream (\"$\", \"_INBOX\"): give one such as %q", o.Prefix, defaultPrefix)
	}
	if o.MinBackoff < 0 || o.MaxBackoff < 0 || o.PublishTimeout < 0 || o.KeepAlive < 0 {
		fail("a duration is negative: give a positive time, or zero for the default")
	}
	if o.MinBackoff == 0 {
		o.MinBackoff = 100 * time.Millisecond
	}
	if o.MaxBackoff == 0 {
		o.MaxBackoff = 10 * time.Second
	}
	if o.MaxBackoff < o.MinBackoff {
		fail("MaxBackoff %v is below MinBackoff %v: give a maximum at least the minimum", o.MaxBackoff, o.MinBackoff)
	}
	if o.PublishTimeout == 0 {
		o.PublishTimeout = 5 * time.Second
	}
	if o.KeepAlive == 0 {
		o.KeepAlive = 30 * time.Second
	}
	if o.Log == nil {
		o.Log = slog.Default()
	}
	b := &Broker{opt: o, prefix: o.Prefix + ".", names: map[string]*entry{}, done: make(chan struct{})}
	b.ctx, b.cancel = context.WithCancel(context.Background())
	return b
}

// checkSubject reports why s cannot be a NATS subject, or part of one, in words that say what to
// change; what names the thing checked ("the name", "Prefix"). The rule is the same for a Topic's
// name and the Prefix, so a name that passes can never make an invalid subject under any Prefix.
func checkSubject(what, s string) error {
	switch {
	case s == "":
		return fmt.Errorf("%s is empty", what)
	case !utf8.ValidString(s):
		return fmt.Errorf("%s is not valid UTF-8", what)
	}
	for i, r := range s {
		switch {
		case unicode.IsSpace(r) || unicode.IsControl(r):
			return fmt.Errorf("%s has a space or control character (%q at byte %d), which a NATS subject cannot carry: use letters, digits and \".\", \"-\", \"_\", \":\"", what, r, i)
		case r == '*' || r == '>':
			return fmt.Errorf("%s has %q at byte %d, which NATS reads as a wildcard, so it would hear or send other names' messages: remove it, or encode a key that comes from data (hex, or base64.RawURLEncoding) before it is put in a name", what, r, i)
		}
	}
	if strings.HasPrefix(s, ".") || strings.HasSuffix(s, ".") || strings.Contains(s, "..") {
		return fmt.Errorf("%s has an empty part (a leading, trailing or doubled \".\"), which a NATS subject cannot have: write parts such as \"room.7\"", what)
	}
	return nil
}

// Accepts reports whether name can be carried: not empty, at most 255 bytes, valid UTF-8, with no
// space or control character, no "*" or ">" (NATS wildcards) and no empty part between dots. The
// check is deterministic and the name is never changed, so what Accepts refuses is a Registration
// panic for a Topic, and an error from the Subscribe and Publish of a Topics family for a key.
func (b *Broker) Accepts(name string) error {
	if len(name) > maxName {
		return fmt.Errorf("the name is %d bytes, over %d: use a shorter name, or a shorter key in the family", len(name), maxName)
	}
	return checkSubject("the name", name)
}

func (b *Broker) subject(name string) string { return b.prefix + name }

// Publish sends payload to every instance subscribed to name, this one included, and returns once
// the NATS server has confirmed it holds the message (a round trip, bounded by PublishTimeout), or
// the reason it did not. Delivery is at most once: an instance that is not subscribed and connected
// when the server routes the message never gets it. A payload over the server's max_payload
// (1 MiB unless the server is set otherwise) is a *bites.PayloadError whose Limit is that number,
// and nothing is sent. With no live connection it returns ErrNotConnected and sends nothing, and
// after Close, ErrClosed. Messages of one publisher to one name arrive in the order of its
// Publish calls; on one server every instance hears one order, and in a cluster only a publisher's
// own order is held.
func (b *Broker) Publish(ctx context.Context, name string, payload []byte) error {
	if err := b.Accepts(name); err != nil {
		return fmt.Errorf("natsbroker: %w", err)
	}
	b.mu.RLock()
	nc, closed := b.conn, b.closed
	b.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	if nc == nil {
		return fmt.Errorf("%w: Run has not connected yet: start it with go broker.Run(ctx) and wait for Listening", ErrNotConnected)
	}
	if limit := int(nc.MaxPayload()); limit > 0 && len(payload) > limit {
		return &bites.PayloadError{Name: name, Size: len(payload), Limit: limit}
	}
	if st := nc.Status(); st != nats.CONNECTED {
		return fmt.Errorf("%w (%s): the message for %q was not sent and is not kept; the client is reconnecting by itself, and OnState says when it is live", ErrNotConnected, st, name)
	}
	ctx, cancel := context.WithTimeout(ctx, b.opt.PublishTimeout)
	defer cancel()
	if err := nc.Publish(b.subject(name), payload); err != nil {
		if errors.Is(err, nats.ErrMaxPayload) {
			return &bites.PayloadError{Name: name, Size: len(payload), Limit: int(nc.MaxPayload())}
		}
		return fmt.Errorf("natsbroker: publish %q: %w", name, err)
	}
	if err := nc.FlushWithContext(ctx); err != nil {
		return fmt.Errorf("natsbroker: publish %q: the server did not confirm the message, which may or may not have been routed: %w", name, err)
	}
	return nil
}

// Subscribe registers deliver for name. Nothing is sent to NATS: the one subscription to every name
// of the Prefix is made when the connection is, and the Broker keeps the names with a subscriber,
// so a subscription made while the connection is up hears the next message published to the name.
// A subscription made while the connection is down, or that the connection was lost under, is told
// by gap once the connection is live, and every subscription is told when NATS dropped messages
// because the deliveries ran behind. Deliveries run one at a time on a goroutine of the NATS
// client, in the order the server sent them. A name Accepts refuses delivers nothing and is
// logged at Error, since a Topic never asks for one.
func (b *Broker) Subscribe(name string, deliver func([]byte), gap func()) func() {
	if err := b.Accepts(name); err != nil {
		b.opt.Log.Error("natsbroker: Subscribe for a name Accepts refuses, which delivers nothing; call Accepts first, as bites.NewTopic does", "name", name, "err", err)
		return func() {}
	}
	s := &sub{deliver: deliver, gap: gap}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return func() {}
	}
	s.missed = b.conn == nil || !b.up
	e := b.names[name]
	if e == nil {
		e = &entry{name: name, subs: map[*sub]struct{}{}}
		b.names[name] = e
	}
	e.subs[s] = struct{}{}
	b.mu.Unlock()
	return func() { b.stop(e, s) }
}

func (b *Broker) stop(e *entry, s *sub) {
	b.mu.Lock()
	delete(e.subs, s)
	if len(e.subs) == 0 && b.names[e.name] == e {
		delete(b.names, e.name)
	}
	b.mu.Unlock()
}

// dispatch hands a message to the subscribers of its name; one nobody listens to is dropped. The
// list is copied and the lock released first, because a delivery can close a slow Socket, which
// leaves its Topic, which calls stop. The first subscriber gets the client's own bytes and each
// other a copy, so every payload is its subscriber's to keep.
func (b *Broker) dispatch(m *nats.Msg) {
	name, ok := strings.CutPrefix(m.Subject, b.prefix)
	if !ok {
		return
	}
	b.mu.RLock()
	e := b.names[name]
	var list []*sub
	if e != nil {
		list = make([]*sub, 0, len(e.subs))
		for s := range e.subs {
			list = append(list, s)
		}
	}
	b.mu.RUnlock()
	for i, s := range list {
		p := m.Data
		if i > 0 {
			p = bytes.Clone(m.Data)
		}
		s.deliver(p)
	}
}

// Listening reports whether the connection to a NATS server is live and the subscriptions are
// made.
func (b *Broker) Listening() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.conn != nil && b.up
}

// Run connects to NATS and keeps the connection until ctx is done or Close is called, and then
// returns nil. A server that cannot be reached, at the start or later, is retried forever after a
// pause that grows from MinBackoff to MaxBackoff, each failure logged at Warn with the error and
// the pause, and each success at Info; Run does not wait for the first connection, so a program
// that must not serve before it hears waits for Listening or fails its readiness from OnState. It
// returns an error when called twice, when the options of ConnectOptions are invalid, and when the
// client library gives up for good (after two authentication failures, for one). Run it on its
// own goroutine.
func (b *Broker) Run(ctx context.Context) error {
	if !b.running.CompareAndSwap(false, true) {
		return errors.New("natsbroker: Run is already running, or has run: one Broker has one connection")
	}
	defer close(b.done)
	if b.isClosed() {
		return ErrClosed
	}
	closedc := make(chan struct{})
	opts := append(append([]nats.Option{}, b.opt.ConnectOptions...), b.options(closedc)...)
	nc, err := nats.Connect(b.opt.URL, opts...)
	if err != nil {
		return fmt.Errorf("natsbroker: the NATS client could not be started (check URL and ConnectOptions): %w", err)
	}
	defer b.shutdown(nc)
	// The one subscription to every name is made before the connection counts as live, and the
	// server is asked to confirm it, so a message published after Listening is true is heard.
	ns, err := nc.Subscribe(b.prefix+">", b.dispatch)
	if err != nil {
		return fmt.Errorf("natsbroker: the subscription to %q could not be made: %w", b.prefix+">", err)
	}
	_ = ns.SetPendingLimits(pendingMsgs, pendingBytes)
	if nc.Status() == nats.CONNECTED {
		fctx, cancel := context.WithTimeout(ctx, b.opt.PublishTimeout)
		_ = nc.FlushWithContext(fctx)
		cancel()
	}
	b.attach(nc)
	select {
	case <-ctx.Done():
	case <-b.ctx.Done():
	case <-closedc:
		if !b.isClosed() {
			return fmt.Errorf("natsbroker: the NATS client closed the connection for good and this instance hears nothing: %w", nc.LastError())
		}
	}
	return nil
}

// options are the settings the Broker decides, applied after the caller's.
func (b *Broker) options(closedc chan struct{}) []nats.Option {
	log := b.opt.Log
	var closedOnce sync.Once
	return []nats.Option{
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.CustomReconnectDelay(func(attempts int) time.Duration {
			pause := b.opt.MinBackoff
			for i := 1; i < attempts && pause < b.opt.MaxBackoff; i++ {
				pause *= 2
			}
			pause = min(pause, b.opt.MaxBackoff)
			wait := pause + time.Duration(rand.Int64N(int64(pause)/5+1))
			log.Warn("natsbroker: no connection to a NATS server, retrying", "attempt", attempts, "retry_in", wait)
			return wait
		}),
		nats.ReconnectBufSize(-1),
		nats.PingInterval(b.opt.KeepAlive),
		func(o *nats.Options) error { o.NoEcho = false; return nil },
		nats.ConnectHandler(func(nc *nats.Conn) {
			log.Info("natsbroker: connected", "server", nc.ConnectedUrlRedacted(), "prefix", b.opt.Prefix)
			b.setUp(true)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Info("natsbroker: connected", "server", nc.ConnectedUrlRedacted(), "prefix", b.opt.Prefix)
			b.setUp(true)
		}),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				if !b.isClosed() {
					log.Warn("natsbroker: the NATS connection was lost", "err", err)
				}
			}
			b.setUp(false)
		}),
		nats.ReconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				log.Warn("natsbroker: could not connect to a NATS server", "err", err)
			}
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			b.setUp(false)
			closedOnce.Do(func() { close(closedc) })
		}),
		nats.ErrorHandler(b.onError),
	}
}

// onError is the client's asynchronous error handler: NATS dropping messages because the deliveries
// ran behind is a gap for every subscription, since one subscription carries every name, and any
// other error (a permission violation for the subject, a bad message) is logged.
func (b *Broker) onError(_ *nats.Conn, ns *nats.Subscription, err error) {
	if ns == nil || !errors.Is(err, nats.ErrSlowConsumer) {
		b.opt.Log.Warn("natsbroker: NATS reported an error", "err", err)
		return
	}
	b.opt.Log.Warn("natsbroker: deliveries ran behind and NATS dropped messages; every Topic is told of a gap", "subject", ns.Subject, "pending_limit_messages", pendingMsgs, "pending_limit_bytes", pendingBytes)
	b.evMu.Lock()
	defer b.evMu.Unlock()
	b.mu.RLock()
	var gaps []func()
	for _, e := range b.names {
		for s := range e.subs {
			gaps = append(gaps, s.gap)
		}
	}
	b.mu.RUnlock()
	for _, g := range gaps {
		g()
	}
}

// attach hands the connection to the Broker, and tells what is owed a gap if the connection is
// already live.
func (b *Broker) attach(nc *nats.Conn) {
	b.evMu.Lock()
	defer b.evMu.Unlock()
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.conn = nc
	gaps, live := b.collectLocked()
	b.mu.Unlock()
	b.report(live, gaps)
}

// setUp records what the client says about the connection. A connection that dropped makes every
// subscription owed a gap, which is told when the connection is live again.
func (b *Broker) setUp(up bool) {
	b.evMu.Lock()
	defer b.evMu.Unlock()
	b.mu.Lock()
	b.up = up
	if !up {
		for _, e := range b.names {
			for s := range e.subs {
				s.missed = true
			}
		}
	}
	gaps, live := b.collectLocked()
	b.mu.Unlock()
	b.report(live, gaps)
}

func (b *Broker) collectLocked() (gaps []func(), live bool) {
	live = b.conn != nil && b.up
	if !live {
		return nil, false
	}
	for _, e := range b.names {
		for s := range e.subs {
			if s.missed {
				s.missed = false
				gaps = append(gaps, s.gap)
			}
		}
	}
	return gaps, true
}

// report calls OnState if the state changed and then the gaps. The caller holds evMu.
func (b *Broker) report(live bool, gaps []func()) {
	if live != b.reported {
		b.reported = live
		if b.opt.OnState != nil {
			b.opt.OnState(live)
		}
	}
	for _, g := range gaps {
		g()
	}
}

func (b *Broker) isClosed() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.closed
}

// shutdown ends the Broker when Run ends: Publish and Subscribe are refused from now on, the
// connection is closed and OnState hears false.
func (b *Broker) shutdown(nc *nats.Conn) {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	nc.Close()
	b.setUp(false)
}

// Close stops Run and waits for it to return, which closes the connection and so every
// subscription. Publish returns ErrClosed afterwards and Subscribe delivers nothing. It is safe to
// call more than once, and returns nil.
func (b *Broker) Close() error {
	b.closeOnce.Do(func() {
		b.mu.Lock()
		b.closed = true
		b.mu.Unlock()
		b.cancel()
		if b.running.Load() {
			<-b.done
		}
	})
	return nil
}

var _ bites.Broker = (*Broker)(nil)
