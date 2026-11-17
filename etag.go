package bites

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/nevindra/bites/internal/engine"
	"github.com/nevindra/bites/internal/h1"
	"github.com/nevindra/bites/internal/wire"
)

// Validators for a JSON answer, and writes that check them (ADR 0089). A Tagged answers a GET
// with the version of what it sends and a 304 when the client already has it; an IfMatch input
// field carries the version a write was made against, and the handler checks it with the current
// one before it writes. File does the same for its own answers (ADR 0064).

// maxETagVersion is the longest Version an ETag may hold: a counter, a timestamp or a hex digest
// of a few dozen bytes fits, and the wire form is built in a buffer on the stack.
const maxETagVersion = 128

// ETag is the validator of one version of what an answer holds (RFC 9110 section 8.8.3): the
// opaque Version a client sends back in If-None-Match to ask "has it changed?" and in If-Match to
// say "change it only if it is still this". It is the ETag of a Tagged and the value an
// IfMatch.Check compares:
//
//	return bites.Tagged[Doc]{Body: d, ETag: bites.ETag{Version: strconv.FormatInt(d.Version, 10)}}, nil
//	...
//	err := in.Header.IfMatch.Check(strconv.FormatInt(cur.Version, 10))
//
// Version is a record's version counter, its update time, or a digest of what the answer shows:
// any text of 1 to 128 visible characters without a double quote, so "3", "2026-11-17T10:00:00Z"
// and "9f86d0" are all fine. The framework puts it in quotes. The zero ETag is a mistake and the
// answer is a logged 500; to have the framework make the tag from the bytes of the body, give
// ETagOfBody().
type ETag struct {
	// Version is the opaque text of the tag, which changes whenever what the answer holds does.
	// Empty is a logged 500, unless the ETag is ETagOfBody().
	Version string
	// Weak says that two answers with this tag mean the same thing and are not necessarily the
	// same bytes (W/"3"). The zero value is a strong tag, which is the one a write can be made
	// conditional on; a weak one is for a cache. The framework's own gzip weakens a strong tag it
	// compresses, whatever this says (ADR 0031).
	Weak bool
	body bool
}

// ETagOfBody asks the framework to make the tag from the JSON it writes: a weak tag of a SHA-256
// of the body, cut to 128 bits, the same on every instance and across restarts, which costs one
// pass over the body and no allocation (ADR 0089). It is the tag to give when the handler has no
// version of its own, for a client that revalidates a read. It cannot be the version of a write,
// because the handler does not know it: give the version, as ETag{Version: v}, when a client will
// send it back in If-Match.
func ETagOfBody() ETag { return ETag{body: true} }

// appendTo appends the wire form of the tag, W/ and quotes included. The version must have been
// checked with validVersion.
func (e ETag) appendTo(b []byte) []byte {
	if e.Weak {
		b = append(b, 'W', '/')
	}
	b = append(b, '"')
	b = append(b, e.Version...)
	return append(b, '"')
}

// validVersion reports whether v can be the opaque text of an entity-tag: 1 to maxETagVersion
// bytes, each a visible character (RFC 9110 etagc) and none a double quote.
func validVersion(v string) bool {
	if v == "" || len(v) > maxETagVersion {
		return false
	}
	for i := 0; i < len(v); i++ {
		if c := v[i]; c == '"' || c < 0x21 || c == 0x7f {
			return false
		}
	}
	return true
}

// Tagged is a 200 with a JSON body and the validator of what the body holds, in the ETag header
// (ADR 0089). On a GET or HEAD whose If-None-Match names the tag, the answer is a 304 with no
// body, so a client that already has the record does not read it again; on any other method it
// only sends the tag, so a PUT can answer with the new version of the record it changed. The
// handler gives the version it already knows (a counter or an update time of the row), or
// ETagOfBody() when it has none:
//
//	return bites.Tagged[Doc]{Body: d, ETag: bites.ETag{Version: strconv.FormatInt(d.Version, 10)}}, nil
//
// An empty ETag, one with a Version that cannot be in a tag, and an ETag in Header as well are
// each a logged 500 that says what to change. The API document states the ETag header as
// required on the 200, and on a GET or HEAD route a 304.
type Tagged[T any] struct {
	// Body is the value written as JSON.
	Body T
	// ETag is the validator of Body: required, and never empty (see ETag).
	ETag ETag
	// Header holds the response headers to send; Cache-Control, Content-Location, Expires and Vary
	// are also sent on the 304. An ETag here is a logged 500: the field above is the one place.
	Header Header
	// Cookies are the cookies the response sets.
	Cookies []Cookie
}

// writeTagged answers with a Tagged (ADR 0089): its tag is checked, a GET or HEAD whose
// If-None-Match names it is a 304 before anything is encoded when the version was given, and after
// the encode when the tag is made from the body. It is not inlined into write, so a route that
// does not use it carries no extra frame.
//
//go:noinline
func (p *outPlan) writeTagged(a *App, ex *engine.Exchange, src unsafe.Pointer) {
	tag := (*ETag)(unsafe.Add(src, p.etagOff))
	hdr := *(*Header)(unsafe.Add(src, p.hdrOff))
	cookies := *(*[]Cookie)(unsafe.Add(src, p.cookOff))
	switch {
	case !tag.body && tag.Version == "":
		a.internalError(ex, refusalf("a Tagged has an empty ETag: give the version of the record as ETag{Version: v}, or bites.ETagOfBody() to make the tag from the body"))
		return
	case !tag.body && !validVersion(tag.Version):
		a.internalError(ex, refusalf("a Tagged has the ETag version %q, which cannot be in an entity-tag: it must be 1 to %d visible characters and no double quote (hash it if it is longer)", clipText(tag.Version, 32), maxETagVersion))
		return
	case headerValue(hdr, "ETag") != "":
		a.internalError(ex, refusalf("a Tagged has an ETag in its Header as well as in its ETag field: give it once, in the ETag field"))
		return
	}
	hs := ex.Head().Headers
	method := ex.Head().Method
	read := string(method) == "GET" || string(method) == "HEAD"
	var buf [maxETagVersion + 8]byte
	var wireTag []byte
	if !tag.body {
		wireTag = tag.appendTo(buf[:0])
		if read && ifNoneMatchHits(hs, wireTag) {
			a.taggedNotModified(ex, wireTag, hdr, cookies)
			return
		}
	}
	r := ex.Respond(200)
	jsonType := true
	if err := writeHeaders(r, hdr, &jsonType); err != nil {
		a.internalError(ex, err)
		return
	}
	if !a.writeCookies(ex, r, cookies) {
		return
	}
	if jsonType {
		r.Header("Content-Type", "application/json")
	}
	if h := p.sizeHint.Load(); h > 0 {
		r.Reserve(int(h))
	}
	body := r.Body()
	start := len(*body)
	nb, err := p.enc.Append(*body, unsafe.Add(src, p.bodyOff))
	if err != nil {
		a.internalError(ex, err)
		return
	}
	*body = nb
	p.noteSize(len(nb) - start)
	if tag.body {
		sum := sha256.Sum256(nb[start:])
		wireTag = appendBodyTag(buf[:0], &sum)
		if read && ifNoneMatchHits(hs, wireTag) {
			a.taggedNotModified(ex, wireTag, hdr, cookies)
			return
		}
	}
	r.HeaderBytes("ETag", wireTag)
}

// appendBodyTag appends the weak tag of a body's digest: W/" and the first 16 bytes in unpadded
// base64url and a quote.
func appendBodyTag(b []byte, sum *[sha256.Size]byte) []byte {
	b = append(b, 'W', '/', '"')
	n := len(b)
	b = b[:n+base64.RawURLEncoding.EncodedLen(16)]
	base64.RawURLEncoding.Encode(b[n:], sum[:16])
	return append(b, '"')
}

// ifNoneMatchHits reports whether the request has an If-None-Match that names tag, by the weak
// comparison RFC 9110 section 13.1.2 gives it, or is "*". A header that does not parse names
// nothing.
func ifNoneMatchHits(hs []h1.Header, tag []byte) bool {
	v := validators{etag: tag}
	present, match := v.matchHeader(hs, "if-none-match", false)
	return present && match
}

// taggedNotModified is the 304 of a Tagged: the tag, the headers a cache updates its copy with,
// the cookies, and no body (RFC 9110 section 15.4.5).
func (a *App) taggedNotModified(ex *engine.Exchange, tag []byte, hdr Header, cookies []Cookie) {
	r := ex.Respond(304)
	needType := false
	writeFileHeaders(r, hdr, true, &needType)
	r.HeaderBytes("ETag", tag)
	a.writeCookies(ex, r, cookies)
}

// clipText cuts s to n bytes for a log line or a message that echoes a handler's own value.
func clipText(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// Limits of an If-Match value, on by default as every limit is (ADR 0024): a client names one
// version, and a list longer than this is an attack on the scan, not a request.
const (
	maxIfMatchBytes = 1024
	maxIfMatchTags  = 16
)

// IfMatch is the If-Match header of a write: the version of the record the client read and means to
// change (RFC 9110 section 13.1.1). It is a field of an Input's Header section, named IfMatch, and
// is how a PUT, a PATCH or a DELETE refuses to overwrite an edit the client has not seen:
//
//	type Update struct {
//		Path   struct{ ID int64 }
//		Header struct{ IfMatch bites.IfMatch }
//		Body   Doc
//	}
//
//	cur, err := load(ctx, in.Path.ID) // in a transaction, with the row locked
//	if err != nil { return Doc{}, err }
//	if err := in.Header.IfMatch.Check(strconv.FormatInt(cur.Version, 10)); err != nil {
//		return Doc{}, err // a 428 when the client sent no If-Match, a 412 when it is not the version
//	}
//
// The check and the write must be one atomic step in the store (a row lock, or an UPDATE whose
// WHERE names the version just checked): the framework cannot make them one. A handler that
// accepts a write with no If-Match asks Present first and writes unconditionally when it is false;
// one that never asks either is logged on every success, as a write that was not conditional.
// The zero IfMatch is a request that sent none. A header that is not "*" or a list of entity-tags,
// longer than 1024 bytes, or naming more than 16 tags is a 400 at header.If-Match; a second
// If-Match line is a 400, since the list is sent on one. IfMatch is read only from the Header
// section, the field must be named IfMatch, and it may not be an Opt or a list element: all are
// refused at Registration.
type IfMatch struct {
	list string // the header's value, or "" when the request sent none
	seen *byte  // set to 1 when the handler asks Present or Check; nil outside a request
}

// UnmarshalText reads an If-Match value: "*", or one or more entity-tags separated by commas. It
// is what the framework runs on the header, so a test of a handler that builds an IfMatch by hand
// reads it the same way. The text must outlive the IfMatch: it is kept, not copied.
func (m *IfMatch) UnmarshalText(b []byte) error {
	list, err := parseIfMatch(unsafe.String(unsafe.SliceData(b), len(b)))
	if err != nil {
		return err
	}
	m.list = list
	return nil
}

// Present reports whether the request sent an If-Match. A handler that accepts writes with no
// precondition asks this and skips Check when it is false; asking counts as looking at the header,
// so the write is not logged as unchecked.
func (m IfMatch) Present() bool {
	m.touch()
	return m.list != ""
}

func (m IfMatch) touch() {
	if m.seen != nil {
		*m.seen = 1
	}
}

// Check compares the version the client sent with version, the current one of the record the
// handler loaded, and returns nil when they are the same. It returns a 428 problem when the
// request sent no If-Match (the route changes a record only for a client that says which version
// it read), and a 412 when the header names other versions only. "*" matches, because the
// handler calls Check once the record exists; call it after the 404, not before. The comparison
// ignores W/: a proxy or the framework's own gzip weakens a strong tag, and the client sends back
// what it received, so a weak-looking tag must still name the version (ADR 0089 says why this
// departs from RFC 9110's strong comparison). Check never reads the body, and costs one scan of
// a header of at most 1024 bytes.
//
// version is the text of ETag.Version for the same record, and must be 1 to 128 visible characters
// with no double quote; any other is an error that is not a 412, which the handler returns and the
// client sees as a 500.
func (m IfMatch) Check(version string) error {
	m.touch()
	if !validVersion(version) {
		return fmt.Errorf("IfMatch.Check was given the version %q, which cannot be in an entity-tag: it must be 1 to %d visible characters and no double quote", clipText(version, 32), maxETagVersion)
	}
	if m.list == "" {
		return PreconditionRequired("this change is made only to the version you read: send If-Match with the ETag of the record (GET it to get the ETag)")
	}
	if m.list == "*" {
		return nil
	}
	for rest := unsafe.Slice(unsafe.StringData(m.list), len(m.list)); ; {
		tag, next, ok := nextETag(rest)
		if !ok {
			break
		}
		tag = bytes.TrimPrefix(tag, []byte("W/"))
		if len(tag) == len(version)+2 && string(tag[1:len(tag)-1]) == version {
			return nil
		}
		rest = next
	}
	return PreconditionFailed("the record is no longer the version named in If-Match: GET it again, apply the change to what you read, and send the new ETag")
}

// parseIfMatch checks an If-Match value (RFC 9110 section 13.1.1): "*", or entity-tags separated
// by commas, at most maxIfMatchBytes long and naming at most maxIfMatchTags. A value that is
// anything else is not ignored, as a precondition the recipient cannot read may be: ignoring it
// would make the write unconditional.
func parseIfMatch(s string) (string, error) {
	switch {
	case s == "*":
		return s, nil
	case len(s) > maxIfMatchBytes:
		return "", wire.NewTextError(fmt.Sprintf("expected at most %d bytes", maxIfMatchBytes))
	}
	n := 0
	rest := unsafe.Slice(unsafe.StringData(s), len(s))
	for {
		tag, next, ok := nextETag(rest)
		if !ok {
			break
		}
		if !validETag(unsafe.String(unsafe.SliceData(tag), len(tag))) {
			return "", errIfMatchSyntax
		}
		if n++; n > maxIfMatchTags {
			return "", wire.NewTextError(fmt.Sprintf("expected at most %d entity-tags", maxIfMatchTags))
		}
		rest = next
	}
	if n == 0 || len(bytes.Trim(rest, " \t,")) != 0 {
		return "", errIfMatchSyntax
	}
	return s, nil
}

var errIfMatchSyntax = wire.NewTextError(`expected * or entity-tags such as "3" or W/"3", separated by commas`)

// ifMatchType is the type compile gives its own decoder.
var ifMatchType = reflect.TypeFor[IfMatch]()

// armIfMatch gives each IfMatch field of the Input a byte of the request's slab to say the handler
// looked at it, so a handler that declares the field and never asks is told in the log.
func (p *inputPlan) armIfMatch(in unsafe.Pointer, slab *wire.Slab) {
	for _, f := range p.ifMatch {
		flag := slab.Reserve(1)
		flag[0] = 0
		(*IfMatch)(unsafe.Add(in, f.off)).seen = &flag[0]
	}
}

// warnUnchecked logs, for each IfMatch field the handler never asked about, that the write it
// just answered was not conditional (ADR 0089). The answer is not changed: the write has happened.
func (p *inputPlan) warnUnchecked(a *App, ex *engine.Exchange, in unsafe.Pointer) {
	for _, f := range p.ifMatch {
		if m := (*IfMatch)(unsafe.Add(in, f.off)); m.seen != nil && *m.seen == 0 {
			h := ex.Head()
			a.log.Warn("bites: the handler answered without calling Present or Check on "+f.goPath+", so this write was not conditional on the version the client read: call Check with the record's current version before writing",
				"method", string(h.Method), "path", string(h.Path))
		}
	}
}

// ifMatchField compiles an IfMatch field of a Header section: the field is the If-Match header,
// which a request may leave out (ask Present), decoded by the framework's own parser and not by
// UnmarshalText, so the value is kept as the slab holds it and costs no copy (ADR 0089).
func (c *compiler) ifMatchField(kind secKind, tf textField, name string) (textField, error) {
	switch {
	case kind != secHeader:
		where := map[secKind]string{secPath: "Path", secQuery: "Query", secCookie: "Cookie", secForm: "Form", secMultipart: "Multipart"}[kind]
		return tf, refusalf("%s is an IfMatch, which is the If-Match header, so it belongs in the Header section and not in a %s section", tf.goPath, where)
	case !equalFold([]byte(name), "if-match"):
		return tf, refusalf("%s is an IfMatch but its wire name is %q: an IfMatch reads the If-Match header, so name the field IfMatch and give it no other wire name", tf.goPath, name)
	}
	tf.required, tf.ifMatch = false, true
	tf.dec = func(dst unsafe.Pointer, s string) error {
		list, err := parseIfMatch(s)
		if err != nil {
			return err
		}
		(*IfMatch)(dst).list = list
		return nil
	}
	tf.typ = ifMatchType
	return tf, nil
}

// hasTagged reports whether the plan, or a variant of it, is a Tagged.
func (p *outPlan) hasTagged() bool {
	if p.wk == wrapTagged {
		return true
	}
	for i := range p.variants {
		if p.variants[i].plan.hasTagged() {
			return true
		}
	}
	for i := range p.alts {
		if p.alts[i].plan.hasTagged() {
			return true
		}
	}
	return false
}

// statedValidators adds to a route's responses what the validators of ADR 0089 send and answer:
// the ETag of a Tagged, required on its 200 (the framework sends it on every one), and a 304 on a
// GET or HEAD route; and the 412 and 428 that an IfMatch field makes the handler's Check return.
// The Check itself is the handler's, so the statuses are stated from the type of the field.
func statedValidators(responses map[string]any, method string, in *inputPlan, out *outPlan) {
	if out != nil && out.hasTagged() {
		etag := map[string]any{
			"description": "The version of the body: send it back in If-None-Match to ask whether it changed, and in If-Match to change this version only.",
			"required":    true,
			"schema":      map[string]any{"type": "string"},
		}
		if r, ok := responses["200"].(map[string]any); ok {
			hs, _ := r["headers"].(map[string]any)
			if hs == nil {
				hs = map[string]any{}
				r["headers"] = hs
			}
			hs["ETag"] = etag
		}
		if method == "GET" || method == "HEAD" {
			if _, ok := responses["304"]; !ok {
				responses["304"] = map[string]any{
					"description": "Not Modified: the If-None-Match names the current ETag, so the client's copy is the answer and no body is sent.",
					"headers":     map[string]any{"ETag": etag},
				}
			}
		}
	}
	if in != nil && len(in.ifMatch) > 0 {
		for _, st := range []string{"412", "428"} {
			if _, ok := responses[st]; !ok {
				responses[st] = map[string]any{"$ref": "#/components/responses/Error"}
			}
		}
	}
}
