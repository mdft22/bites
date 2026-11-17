package bites

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"log/slog"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nevindra/bites/internal/h1"
)

// Idempotency-Key (ADR 0090): a write sent again with the same key is answered with the first
// answer, so a client that timed out and retries does not charge a card twice. The middleware
// claims the key in a store before the handler runs, keeps the answer when the handler succeeded,
// and replays it for the same request sent again.

// IdempotencyOptions configures an Idempotency. The zero value is a usable configuration: the
// header Idempotency-Key, optional, answers kept for 24 hours in a bounded in-memory store.
type IdempotencyOptions struct {
	// Header is the name of the request header that carries the key; "Idempotency-Key" when empty.
	// It must be an HTTP token.
	Header string

	// Required refuses a POST or PATCH that has no key with a 400 problem+json naming the header.
	// When false (the zero value) such a request goes on with no protection, which is what an
	// optional key means; a route that takes payments sets it. Requests of any other method never
	// need a key.
	Required bool

	// MaxKeyLength is the longest key accepted, in bytes; 255 when zero, at most 1024. A key is
	// printable ASCII with no space, quote or comma, one pair of double quotes around it (the
	// structured-field form of the IETF draft) being removed first; anything else is a 400
	// problem+json naming the header.
	MaxKeyLength int

	// Scope names who a key belongs to, so one client cannot replay another's key. It is called for
	// each request that has a key; two requests with the same key and different scopes are two
	// different keys. When nil the scope is what the request presents as credentials: its
	// Authorization header and its Cookie header, so two clients with different tokens or sessions
	// never share a key and the key is never visible to a client that does not hold the same
	// credentials. A middleware runs before the Providers of ADR 0048, so it cannot know the user's
	// id; a Scope that returns the id of a tenant header or the prefix of an API key replaces the
	// default, and a Scope that returns "" gives every request the same scope, which is for a route
	// whose callers are not authenticated (a webhook) and whose keys are unguessable.
	Scope func(Request) string

	// TTL is how long an answer is kept after it was produced; 24 hours when zero, at least a
	// second. A key sent again after that is a new request.
	TTL time.Duration

	// InFlight is how long a claim holds while the handler runs; one minute when zero, at least a
	// second and at most TTL. A request with the same key that arrives meanwhile is answered 409.
	// A handler that runs longer than InFlight loses its claim, and a duplicate may then run it
	// again: set it above the longest the route can take (a Timeout belongs on the route).
	InFlight time.Duration

	// MaxAnswer is the largest answer body kept, in bytes; 64 KiB when zero. The answer of a
	// request that did its work and is larger is not kept: the first request gets its answer, a
	// duplicate gets a 409 that says the request was handled and its answer was too large to keep,
	// and a line at Warn names the route and the size. The work is never run twice for it.
	MaxAnswer int

	// Store keeps the claims and the answers, so instances behind one balancer share them; the
	// in-memory store of this process when nil, which is per instance (two instances behind a
	// balancer each remember only what they served).
	Store IdempotencyStore

	// MaxKeys bounds the in-memory store: 10,000 keys when zero. It is refused together with Store.
	MaxKeys int

	// MaxBytes bounds the answers the in-memory store holds, headers and bodies counted: 16 MiB
	// when zero, at least twice MaxAnswer. It is refused together with Store.
	MaxBytes int
}

// IdempotencyStore keeps what an Idempotency remembers: for each key, a claim while a request
// runs, and the answer once it has finished. An implementation outside the core (Redis, Postgres)
// is held to this comment by bitetest.TestIdempotencyStore (ADR 0086). Every method must be safe
// to call from many goroutines, and decide with the now it is given, not with a clock of its own.
// A store error is the answer to the request (a 500): the middleware fails closed, because running
// the handler without the claim is the double charge it exists to prevent.
type IdempotencyStore interface {
	// Claim records that a request with fingerprint is about to run for key, if no request has.
	// It returns State Claimed when key had no live entry, and then holds the claim until
	// now+inFlight, and State Running with the claim's own Fingerprint (not the caller's) while
	// another claim is live, and State Done with the first request's Fingerprint and Answer once
	// Complete has stored one and until its time to live passes. A claim past its inFlight, and an
	// answer past its ttl, no longer exist. It must be atomic for a key: of several callers at
	// once, exactly one gets Claimed. A store that cannot take a new key returns an error that
	// matches ErrIdempotencyStoreFull.
	Claim(ctx context.Context, key, fingerprint string, now time.Time, inFlight time.Duration) (IdempotencyEntry, error)

	// Complete keeps answer for key, replacing a claim or an older answer, until now+ttl. It does
	// so whether or not the caller still holds the claim (it may have expired, or been dropped to
	// make room), because the work was done and its answer must not be lost. The Answer a later
	// Claim returns is equal to the one stored, field for field, headers in their order.
	Complete(ctx context.Context, key, fingerprint string, answer IdempotencyAnswer, now time.Time, ttl time.Duration) error

	// Release removes the claim of key so the next Claim gets Claimed, and nothing else: a key that
	// holds an answer keeps it, and a key that holds nothing is not an error.
	Release(ctx context.Context, key string) error
}

// IdempotencyState is what a store found for a key.
type IdempotencyState uint8

const (
	// IdempotencyClaimed means the key was free and now belongs to the caller, who runs the request.
	IdempotencyClaimed IdempotencyState = iota + 1
	// IdempotencyRunning means another request holds the key and has not finished.
	IdempotencyRunning
	// IdempotencyDone means a request finished and its answer is kept.
	IdempotencyDone
)

// IdempotencyEntry is what Claim found for a key. The zero value is not a result: a store always
// sets State.
type IdempotencyEntry struct {
	// State says whether the caller owns the key, another request is running, or one has finished.
	State IdempotencyState
	// Fingerprint is that of the request that holds or held the key; empty when State is Claimed.
	Fingerprint string
	// Answer is the kept answer when State is Done. The middleware never changes it; a store that
	// returns the slices it holds must not change them afterwards either.
	Answer IdempotencyAnswer
}

// IdempotencyAnswer is an answer kept to be sent again: what a request that did its work
// produced.
type IdempotencyAnswer struct {
	// Status is the status code, 200 to 399.
	Status int
	// ContentType is the Content-Type of the answer; empty when it had none.
	ContentType string
	// Headers are the answer's other headers in the order they were written (Location, Set-Cookie,
	// those of the handler and of the middleware inside Idempotency). The ones the engine adds when
	// it writes (Date, Content-Length, Connection) are not kept.
	Headers []IdempotencyHeader
	// Body is the answer body as the handler produced it, before any compression.
	Body []byte
	// TooLarge is true when the request did its work but its answer was over MaxAnswer and was not
	// kept: Status, ContentType, Headers and Body are then empty.
	TooLarge bool
}

// IdempotencyHeader is one header line of a kept answer.
type IdempotencyHeader struct{ Name, Value string }

// ErrIdempotencyStoreFull is what a store's Claim returns, wrapped or not, when it cannot take a
// new key because every key it holds belongs to a request still running. The middleware answers
// 503 with Retry-After and never runs the handler.
var ErrIdempotencyStoreFull = errors.New("bites: the idempotency store holds only requests still running and has no room for another key")

// Idempotency answers a write sent again with the same key with the answer of the first (ADR
// 0090). Its Middleware covers the routes it is given to with Use; a request with the key header
// and the method POST or PATCH is claimed in the store, run, and its answer kept when it is a 2xx
// or 3xx.
type Idempotency struct {
	header    string
	required  bool
	maxKey    int
	scope     func(Request) string
	ttl       time.Duration
	inFlight  time.Duration
	maxAnswer int
	store     IdempotencyStore
	mem       *memoryIdempotency
	lastWarn  atomic.Int64 // unix nanoseconds of the last eviction warning
}

const (
	defaultIdempotencyKey      = "Idempotency-Key"
	defaultIdempotencyTTL      = 24 * time.Hour
	defaultIdempotencyInFlight = time.Minute
	defaultIdempotencyAnswer   = 64 << 10
	defaultIdempotencyKeys     = 10_000
	defaultIdempotencyBytes    = 16 << 20
	maxIdempotencyKeyLength    = 1024
	replayedHeader             = "Idempotent-Replayed"
)

// NewIdempotency returns an Idempotency. It panics when Header is not an HTTP token, MaxKeyLength
// is negative or over 1024, TTL or InFlight is under a second, InFlight is over TTL, MaxAnswer,
// MaxKeys or MaxBytes is negative, MaxBytes is under twice MaxAnswer, or MaxKeys or MaxBytes is
// given with a Store.
func NewIdempotency(o IdempotencyOptions) *Idempotency {
	const fn = "NewIdempotency"
	if o.Header == "" {
		o.Header = defaultIdempotencyKey
	}
	if !isToken(o.Header) {
		refuseMiddleware(fn, "Header %q is not an HTTP header name: give a token such as %q", o.Header, defaultIdempotencyKey)
	}
	if o.MaxKeyLength < 0 || o.MaxKeyLength > maxIdempotencyKeyLength {
		refuseMiddleware(fn, "MaxKeyLength is %d: give zero for 255, or a length from 1 to %d", o.MaxKeyLength, maxIdempotencyKeyLength)
	}
	if o.MaxKeyLength == 0 {
		o.MaxKeyLength = 255
	}
	if o.TTL == 0 {
		o.TTL = defaultIdempotencyTTL
	}
	if o.TTL < time.Second {
		refuseMiddleware(fn, "TTL is %v: give zero for 24 hours, or how long an answer is kept, at least a second", o.TTL)
	}
	if o.InFlight == 0 {
		o.InFlight = min(defaultIdempotencyInFlight, o.TTL)
	}
	if o.InFlight < time.Second || o.InFlight > o.TTL {
		refuseMiddleware(fn, "InFlight is %v: give zero for a minute, or how long a claim holds while the handler runs, from a second to the TTL of %v", o.InFlight, o.TTL)
	}
	if o.MaxAnswer < 0 {
		refuseMiddleware(fn, "MaxAnswer is %d, which is not a size: give zero for 64 KiB, or the largest answer body kept in bytes", o.MaxAnswer)
	}
	if o.MaxAnswer == 0 {
		o.MaxAnswer = defaultIdempotencyAnswer
	}
	if o.MaxKeys < 0 {
		refuseMiddleware(fn, "MaxKeys is %d, which is not a bound: give zero for 10,000, or a positive number", o.MaxKeys)
	}
	if o.MaxBytes < 0 {
		refuseMiddleware(fn, "MaxBytes is %d, which is not a bound: give zero for 16 MiB, or a positive number of bytes", o.MaxBytes)
	}
	if (o.MaxKeys > 0 || o.MaxBytes > 0) && o.Store != nil {
		refuseMiddleware(fn, "MaxKeys or MaxBytes is given with a Store: they bound the in-memory store, which a Store replaces, so bound the Store's own memory instead")
	}
	d := &Idempotency{
		header: o.Header, required: o.Required, maxKey: o.MaxKeyLength, scope: o.Scope,
		ttl: o.TTL, inFlight: o.InFlight, maxAnswer: o.MaxAnswer, store: o.Store,
	}
	if d.store == nil {
		keys, size := o.MaxKeys, o.MaxBytes
		if keys == 0 {
			keys = defaultIdempotencyKeys
		}
		if size == 0 {
			size = max(defaultIdempotencyBytes, 2*o.MaxAnswer)
		}
		if size < 2*o.MaxAnswer {
			refuseMiddleware(fn, "MaxBytes is %d, under twice MaxAnswer of %d: the store must hold at least two of the largest answers, so give a larger MaxBytes or a smaller MaxAnswer", size, o.MaxAnswer)
		}
		d.mem = newMemoryIdempotency(keys, size)
		d.store = d.mem
	}
	return d
}

// Middleware covers the routes it is given to with Use, a route's, a group's, or UseEach's; the
// app's own Use is refused, because the answer is kept by route and the body limits are the
// route's. A POST or PATCH with the key header is claimed, run and answered; every other request
// passes through untouched. Put it inside RequestID, CORS and the rate limiter, so a replay still
// carries the request's id and counts against the limit.
func (d *Idempotency) Middleware() Middleware { return d.serve }

// serve is the middleware, a method value so that Registration can recognise it on a route.
func (d *Idempotency) serve(ctx context.Context, req Request, next Next) (Response, error) {
	if req.method != "POST" && req.method != "PATCH" {
		return next.Call(ctx), nil
	}
	n := countHeader(req, d.header)
	if n == 0 {
		if d.required {
			return Response{}, d.badKey("the " + d.header + " header is required: send a key that is the same for every retry of this request and new for a different one")
		}
		return next.Call(ctx), nil
	}
	key, why := d.readKey(req, n)
	if why != "" {
		return Response{}, d.badKey(why)
	}
	if !req.live() {
		return next.Call(ctx), nil // a Request kept past its request: nothing to claim
	}
	run := req.run
	if run.rt == nil {
		run.a.log.Error("bites: Idempotency ran outside a route; give it to a route or a group with Use")
		return Response{}, errors.New("idempotency: no route")
	}
	body, err := run.ex.ReadBody(run.rt.maxBody, run.rt.bodyTimeout)
	if err != nil {
		var he *h1.Error
		if errors.As(err, &he) {
			return Problem(he.Status, he.Reason), nil
		}
		return Problem(400, ""), nil
	}
	fp := d.fingerprint(req, body)
	sk := d.storeKey(req, key)
	now := Now(ctx)
	e, err := d.store.Claim(ctx, sk, fp, now, d.inFlight)
	if err != nil {
		if errors.Is(err, ErrIdempotencyStoreFull) {
			run.a.log.Error("bites: the Idempotency store is full of requests still running", "route", run.rt.pattern, "error", err)
			return Response{}, ServiceUnavailable("too many requests with an "+d.header+" are running; retry shortly").WithHeader("Retry-After", "1")
		}
		return Response{}, err
	}
	switch e.State {
	case IdempotencyClaimed:
	case IdempotencyRunning, IdempotencyDone:
		return d.duplicate(req, e, fp)
	default:
		run.a.log.Error("bites: an IdempotencyStore returned no State from Claim; it must return Claimed, Running or Done", "store", storeName(d.store))
		return Response{}, errors.New("idempotency: the store returned no state")
	}

	// The claim is ours. Whatever happens next, including a panic that unwinds through here, the
	// claim is either completed or released, so a retry never waits for InFlight to pass.
	settled := false
	store := context.WithoutCancel(ctx) // the client may be gone; the work is done and its answer is kept all the same
	defer func() {
		if !settled {
			if err := d.store.Release(store, sk); err != nil {
				run.a.log.Error("bites: Idempotency could not release a claim; a retry gets 409 until InFlight passes", "route", run.rt.pattern, "error", err)
			}
		}
	}()
	res := next.Call(ctx)
	ans, keep, why := d.answerOf(res, run)
	switch {
	case !keep:
		run.a.log.Debug("bites: Idempotency did not keep an answer", "route", run.rt.pattern, "status", res.Status(), "reason", why)
		return res, nil
	case why != "":
		run.a.log.Warn("bites: Idempotency did not keep an answer: "+why+"; a request sent again with this key is answered 409, and the work is not run twice", "route", run.rt.pattern, "status", res.Status(), "limit", d.maxAnswer)
	}
	if err := d.store.Complete(store, sk, fp, ans, Now(ctx), d.ttl); err != nil {
		run.a.log.Error("bites: Idempotency could not keep an answer; a retry may run the request again", "route", run.rt.pattern, "error", err)
		return res, nil
	}
	settled = true
	d.noteEvictions(run)
	return res, nil
}

// badKey is the 400 of a missing, repeated or malformed key, naming the header in the location of
// its error as every client error does (ADR 0012).
func (d *Idempotency) badKey(message string) error {
	type fault struct{ Location, Message string }
	return BadRequest(message).With("errors", []fault{{Location: "header." + d.header, Message: message}})
}

// readKey reads the key from the header, which n lines carry.
func (d *Idempotency) readKey(req Request, n int) (key, why string) {
	if n > 1 {
		return "", "the " + d.header + " header is sent " + strconv.Itoa(n) + " times: send it once"
	}
	v := req.Header(d.header)
	if len(v) >= 2 && v[0] == '"' && v[len(v)-1] == '"' {
		v = v[1 : len(v)-1]
	}
	if v == "" || len(v) > d.maxKey {
		return "", "the " + d.header + " header must be 1 to " + strconv.Itoa(d.maxKey) + " characters, printable ASCII with no space, quote or comma"
	}
	for i := 0; i < len(v); i++ {
		if c := v[i]; c <= ' ' || c > '~' || c == '"' || c == ',' {
			return "", "the " + d.header + " header must be 1 to " + strconv.Itoa(d.maxKey) + " characters, printable ASCII with no space, quote or comma"
		}
	}
	return v, ""
}

// duplicate answers a request whose key is held or was answered already: 422 when it is not the
// same request, 409 while the first still runs, and the first answer when it has finished.
func (d *Idempotency) duplicate(req Request, e IdempotencyEntry, fp string) (Response, error) {
	if e.Fingerprint != fp {
		req.AddLogAttrs(slog.String("idempotency", "mismatch"))
		return Response{}, UnprocessableEntity("this " + d.header + " was already used with a different request (another method, path, query or body): send a new key for a different request, and the same key only to repeat this one")
	}
	if e.State == IdempotencyRunning {
		req.AddLogAttrs(slog.String("idempotency", "in_flight"))
		return Response{}, Conflict("a request with this "+d.header+" is still running: retry after it has finished").WithHeader("Retry-After", "1")
	}
	a := e.Answer
	if a.TooLarge {
		req.AddLogAttrs(slog.String("idempotency", "too_large"))
		return Response{}, Conflict("a request with this " + d.header + " was handled, but its answer was too large to keep and cannot be sent again: read the resource it changed instead of repeating the request")
	}
	req.AddLogAttrs(slog.String("idempotency", "replayed"))
	res := NewResponse(a.Status)
	if a.ContentType != "" || len(a.Body) > 0 {
		res.SetBody(a.ContentType, string(a.Body))
	}
	for _, h := range a.Headers {
		res.AddHeader(h.Name, h.Value)
	}
	res.AddHeader(replayedHeader, "true")
	return res, nil
}

// fingerprint is a hash of what makes two requests the same one: the method, the path and the
// query as sent, and the body as received. Each part is length-prefixed so that a boundary cannot
// be moved between them.
func (d *Idempotency) fingerprint(req Request, body []byte) string {
	h := sha256.New()
	field(h, req.method)
	field(h, req.path)
	field(h, req.query)
	var n [binary.MaxVarintLen64]byte
	h.Write(n[:binary.PutUvarint(n[:], uint64(len(body)))])
	h.Write(body)
	var sum [sha256.Size]byte
	return hex.EncodeToString(h.Sum(sum[:0]))
}

// storeKey is the key the store sees: a hash of the route, the scope and the client's key, so a key
// is never shared by two routes or two clients, the store never holds a credential or the key
// itself, and a key has a fixed length whatever the client sent.
func (d *Idempotency) storeKey(req Request, key string) string {
	h := sha256.New()
	field(h, req.run.rt.pattern)
	if d.scope != nil {
		field(h, d.scope(req))
	} else {
		field(h, req.Header("authorization"))
		field(h, joinedHeader(req, "cookie"))
	}
	field(h, key)
	var sum [sha256.Size]byte
	return hex.EncodeToString(h.Sum(sum[:0])[:16])
}

func field(h hash.Hash, s string) {
	var n [binary.MaxVarintLen64]byte
	h.Write(n[:binary.PutUvarint(n[:], uint64(len(s)))])
	h.Write([]byte(s))
}

// joinedHeader is every line of a header, joined with a newline, since a Cookie header may be
// sent as several lines and Request.Header returns the first.
func joinedHeader(req Request, name string) string {
	var out string
	b := req.headers
	for len(b) > 0 {
		i := strings.IndexByte(b, '\n')
		if i < 0 {
			break
		}
		line := b[:i]
		b = b[i+1:]
		if len(line) <= 1 {
			break
		}
		if len(line) > len(name) && line[len(name)] == ':' && foldPrefix(line, name) {
			out += strings.TrimSpace(line[len(name)+1:]) + "\n"
		}
	}
	return out
}

// countHeader is how many lines of the head carry the header called name.
func countHeader(r Request, name string) int {
	n := 0
	b := r.headers
	for len(b) > 0 {
		i := strings.IndexByte(b, '\n')
		if i < 0 {
			return n
		}
		line := b[:i]
		b = b[i+1:]
		if len(line) <= 1 {
			return n
		}
		if len(line) > len(name) && line[len(name)] == ':' && foldPrefix(line, name) {
			n++
		}
	}
	return n
}

// answerOf turns what the chain produced into an answer to keep. keep is false when the request
// did not do its work (an error, a refusal, a stream) and the claim is released; keep with a
// non-empty why is an answer too large, kept as the marker.
func (d *Idempotency) answerOf(res Response, run *chainRun) (ans IdempotencyAnswer, keep bool, why string) {
	st := res.Status()
	if st < 200 || st >= 400 {
		return ans, false, "the status " + strconv.Itoa(st) + " is not a 2xx or a 3xx, so the request did not do its work"
	}
	if res.stream {
		return ans, false, "the answer is a stream"
	}
	var body []byte
	switch res.kind {
	case kindInner:
		p := run.ex.Pending()
		if p.BodyLen() > d.maxAnswer {
			return IdempotencyAnswer{TooLarge: true}, true, "the answer body is " + strconv.Itoa(p.BodyLen()) + " bytes, over MaxAnswer"
		}
		body = bytes.Clone(*p.Body())
		ans.Headers = appendHeaderBlock(ans.Headers, p.HeaderBlock())
	case kindFresh:
		if len(res.text) > d.maxAnswer {
			return IdempotencyAnswer{TooLarge: true}, true, "the answer body is " + strconv.Itoa(len(res.text)) + " bytes, over MaxAnswer"
		}
		body = []byte(res.text)
	default:
		return ans, false, "the answer is not one the chain produced"
	}
	ans.Status, ans.ContentType, ans.Body = st, res.ctype, body
	for _, h := range res.pairs() {
		ans.Headers = append(ans.Headers, IdempotencyHeader{h.name, h.value})
	}
	return ans, true, ""
}

// appendHeaderBlock parses the header lines the engine holds ("Name: value\r\n") into pairs,
// leaving out Content-Type, which the answer keeps beside them.
func appendHeaderBlock(dst []IdempotencyHeader, block []byte) []IdempotencyHeader {
	for len(block) > 0 {
		i := bytes.Index(block, []byte("\r\n"))
		if i < 0 {
			i = len(block)
		}
		line := block[:i]
		block = block[min(len(block), i+2):]
		c := bytes.IndexByte(line, ':')
		if c <= 0 {
			continue
		}
		name := string(line[:c])
		if foldEq(name, "content-type") {
			continue
		}
		dst = append(dst, IdempotencyHeader{name, strings.TrimLeft(string(line[c+1:]), " ")})
	}
	return dst
}

// noteEvictions says, at most every ten seconds, that the in-memory store dropped answers before
// their time to live to make room, since a retry of one of them runs again.
func (d *Idempotency) noteEvictions(run *chainRun) {
	if d.mem == nil {
		return
	}
	n := d.mem.takeEvicted()
	if n == 0 {
		return
	}
	now := run.a.clk.Now().UnixNano()
	last := d.lastWarn.Load()
	if now-last < int64(10*time.Second) || !d.lastWarn.CompareAndSwap(last, now) {
		return
	}
	run.a.log.Warn("bites: the in-memory Idempotency store dropped answers before their TTL to stay within MaxKeys and MaxBytes; a request sent again with one of those keys runs again: raise the bounds, put a rate limit in front, or use a shared Store",
		slog.Int64("dropped", n))
}

func storeName(s IdempotencyStore) string { return fmt.Sprintf("%T", s) }

// checkIdempotency refuses a route that cannot keep an answer or compare a body: one that returns a
// File or a stream, whose answer is written after the chain has returned, and one whose body is an
// Upload, a multipart form or a BodyReader, which is read part by part and never whole (ADR 0090).
func checkIdempotency(mw []Middleware, in *inputPlan, out *outPlan) error {
	on := false
	for _, m := range mw {
		on = on || sameCode(m, (*Idempotency)(nil).serve)
	}
	if !on {
		return nil
	}
	if out.wk >= wrapFile {
		return refusalf("Idempotency is on a route that returns a File or a stream, whose body is written after the middleware has returned, so there is no answer to keep: take Idempotency off this route, or return the answer whole")
	}
	if in.body != nil {
		switch in.body.kind {
		case bodyUpload, bodyMultipart, bodyReader:
			return refusalf("Idempotency is on a route whose body is an Upload, a multipart form or a BodyReader, which is read part by part and cannot be compared whole: take Idempotency off this route, or have the client create the resource with a JSON request and send the file to it")
		}
	}
	return nil
}
