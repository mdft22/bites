# Conditional requests

**A handler gives an answer a version with `Tagged`, so a client that already has it gets a 304 and no body, and a write checks the version the client read with `IfMatch`, so two people who edit one record cannot overwrite each other.**

**Reference:** [`Tagged`, `ETag`](../reference/responses.md#tagged), [`IfMatch`](../reference/input.md#ifmatch) · **Design:** [Responses](../design/responses.md)

## Letting a client ask "has it changed?"

**Return a [`Tagged`](../reference/responses.md#tagged) with the version of the record as its `ETag`, and a GET that already has that version is answered with a 304 and no body.** The version is the text a client will send back, so it is a counter, an update time or a digest, and the framework puts the quotes on ([ADR 0089](../adr/0089-an-answer-carries-a-validator-and-a-write-checks-the-version-it-read.md)). The body is not even encoded when the version matches.

<!-- compiles: decls -->
```go
type Room struct {
	ID      int64  `json:"id"`
	Name    string `json:"name"`
	Version int64  `json:"version"`
}

// Rooms is the store the examples of this page use: a map under a lock, where an application has
// a database.
type Rooms struct {
	mu   sync.Mutex
	byID map[int64]Room
}

type GetRoom struct{ Path struct{ ID int64 } }

func (s *Rooms) Get(ctx context.Context, in GetRoom) (bites.Tagged[Room], error) {
	s.mu.Lock()
	r, ok := s.byID[in.Path.ID]
	s.mu.Unlock()
	if !ok {
		return bites.Tagged[Room]{}, bites.NotFound("no such room")
	}
	return bites.Tagged[Room]{
		Body:   r,
		ETag:   bites.ETag{Version: strconv.FormatInt(r.Version, 10)},
		Header: bites.Header{"Cache-Control": {"no-cache"}},
	}, nil
}
```

```http
GET /rooms/7 HTTP/1.1

HTTP/1.1 200 OK
ETag: "3"
Cache-Control: no-cache
Content-Type: application/json

{"id":7,"name":"lobby","version":3}
```

```http
GET /rooms/7 HTTP/1.1
If-None-Match: "3"

HTTP/1.1 304 Not Modified
ETag: "3"
Cache-Control: no-cache
```

`If-None-Match` is compared as RFC 9110 says for a read: `W/` is ignored, `*` matches, and a list is read tag by tag. A 304 keeps the `ETag`, the `Cache-Control`, `Content-Location`, `Expires` and `Vary` of `Header` and the cookies, and drops everything else. A header that cannot be read is not an error for a read: the answer is a 200.

**An answer that differs by who asks must say so.** A 304 goes to anyone who holds the tag, so a record that differs by user carries `Cache-Control: private` or a `Vary` in `Header`, which the 304 keeps; the framework does not know that the record is private.

A `Tagged` on a PUT or POST only sends the tag, so a write can answer with the new version of the record it changed; `If-None-Match` is not evaluated there.

## A tag made from the body

**When the handler has no version, give `bites.ETagOfBody()` and the framework makes a weak tag from the JSON it writes.** It is the SHA-256 of the encoded body, kept to 128 bits, so it is the same on every instance and after a restart, and it costs one pass over the body (about 3 µs for 8 KB) and no allocation. The 304 saves the write and not the encode.

<!-- compiles: decls -->
```go
func (s *Rooms) List(ctx context.Context, in struct{}) (bites.Tagged[[]Room], error) {
	s.mu.Lock()
	rooms := make([]Room, 0, len(s.byID))
	for _, r := range s.byID {
		rooms = append(rooms, r)
	}
	s.mu.Unlock()
	slices.SortFunc(rooms, func(a, b Room) int { return int(a.ID - b.ID) })
	return bites.Tagged[[]Room]{Body: rooms, ETag: bites.ETagOfBody()}, nil
}
```

A body that changes on every encode, such as one with a server time in it, gets a new tag every time and never a 304. A tag made from the body is not a version a write can name: use it for reads only.

## Refusing a write to a version the client has not seen

**Put an [`IfMatch`](../reference/input.md#ifmatch) in the write's Header section, and call `Check` with the record's current version before the write.** A client that GETs a record, and PUTs it back with the `ETag` it received as `If-Match`, can change only what it read: if someone else changed the record in between, the client gets a 412 and nothing is written.

<!-- compiles: decls -->
```go
type UpdateRoom struct {
	Path   struct{ ID int64 }
	Header struct{ IfMatch bites.IfMatch }
	Body   struct{ Name bites.NonEmpty }
}

func (s *Rooms) Update(ctx context.Context, in UpdateRoom) (bites.Tagged[Room], error) {
	s.mu.Lock() // the check and the write are one step under the lock
	defer s.mu.Unlock()
	cur, ok := s.byID[in.Path.ID]
	if !ok {
		return bites.Tagged[Room]{}, bites.NotFound("no such room") // before the check
	}
	if err := in.Header.IfMatch.Check(strconv.FormatInt(cur.Version, 10)); err != nil {
		return bites.Tagged[Room]{}, err // a 428 with no If-Match, a 412 with a stale one
	}
	cur.Name, cur.Version = string(in.Body.Name), cur.Version+1
	s.byID[cur.ID] = cur
	return bites.Tagged[Room]{Body: cur, ETag: bites.ETag{Version: strconv.FormatInt(cur.Version, 10)}}, nil
}
```

```http
PUT /rooms/7 HTTP/1.1
If-Match: "2"
Content-Type: application/json

{"name":"lounge"}

HTTP/1.1 412 Precondition Failed
Content-Type: application/problem+json

{"title":"Precondition Failed","status":412,"detail":"the record is no longer the version named in If-Match: GET it again, apply the change to what you read, and send the new ETag"}
```

`Check` returns:

| the request | the answer |
|---|---|
| `If-Match` names the current version, in any spelling (`"3"`, `W/"3"`, `"2", "3"`) | nil: write |
| `If-Match: *` | nil: write (the record exists, since you answered the 404 first) |
| `If-Match` names only other versions | 412 problem+json |
| no `If-Match` | 428 problem+json |
| `If-Match` that is not `*` or entity-tags, over 1024 bytes, or naming more than 16 tags | 400 at `header.If-Match`, before the handler runs |

**The check and the write have to be one step in the store.** `Check` compares two values the handler already has, so a write that is not atomic with it can still lose an update in between. Either hold a lock across both, as above (in a database, a transaction that reads the row with `SELECT ... FOR UPDATE`), or write with the version in the `WHERE` and answer the 412 yourself when no row changed:

```go
n, err := res.RowsAffected() // UPDATE rooms SET name = $1, version = version + 1 WHERE id = $2 AND version = $3
if err == nil && n == 0 {
	return bites.Tagged[Room]{}, bites.PreconditionFailed("the record changed: GET it again and retry")
}
```

## Allowing a write with no If-Match

**A handler that accepts an unconditional write asks `Present()` first.** The zero `IfMatch` is a request that sent no header, and `Check` on it is a 428, which is right for a route that wants a precondition and wrong for one that does not.

<!-- compiles: decls -->
```go
func (s *Rooms) Touch(ctx context.Context, in UpdateRoom) (bites.NoContent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.byID[in.Path.ID]
	if !ok {
		return bites.NoContent{}, bites.NotFound("no such room")
	}
	if in.Header.IfMatch.Present() {
		if err := in.Header.IfMatch.Check(strconv.FormatInt(cur.Version, 10)); err != nil {
			return bites.NoContent{}, err
		}
	}
	cur.Version++
	s.byID[cur.ID] = cur
	return bites.NoContent{}, nil
}
```

**A handler that declares the field and never asks is logged.** A route with an `IfMatch` and no call to `Present` or `Check` writes unconditionally, which is the lost update this exists to stop, and nothing before the first request can see it. When such a handler returns without an error, the log says `the handler answered without calling Present or Check on UpdateRoom.Header.IfMatch, so this write was not conditional`, with the method and path; the answer is not changed, because the write has happened.

## Behind gzip and a proxy

**Compression and proxies weaken a strong tag, and `Check` accepts it.** With `WithGzip` the answer carries `W/"3"` for the strong tag `"3"`, and the client sends back `W/"3"`; `Check` ignores `W/`, so the write is accepted, and a proxy that strips the `W/` is accepted too. That departs from RFC 9110's strong comparison for `If-Match` on purpose, because the tag here names a version of a record and not its bytes ([ADR 0089](../adr/0089-an-answer-carries-a-validator-and-a-write-checks-the-version-it-read.md)). A file is different: a [`File`](./files.md#revalidation-etag-304-and-412) keeps the strong comparison.

## What the document says and what Registration refuses

**The API document states what the types say.** A `Tagged` has a required `ETag` header on its 200, and a GET or HEAD route a 304 that carries it. An `IfMatch` field is an optional `If-Match` header parameter, and the route has the 412 and 428 that `Check` answers.

Registration refuses an `IfMatch` outside a Header section, one that is not named `IfMatch`, an `Opt[IfMatch]` (a zero `IfMatch` is already absent) and a list of `IfMatch`. A `Tagged` with an empty `ETag`, a `Version` that cannot be in a tag (empty, over 128 bytes, a quote, a space) or an `ETag` in its `Header` as well is a logged 500 that names what to change.

## Testing a write

**Test the two clients, with [`bitetest`](./testing.md): read twice, write once with the first tag, then with the second.**

<!-- compiles: decls -->
```go
func TestTheSecondEditorIsRefused(t *testing.T) {
	rooms := &Rooms{byID: map[int64]Room{7: {ID: 7, Name: "lobby", Version: 1}}}
	app := bites.New()
	app.Get("/rooms/{id}", rooms.Get)
	app.Put("/rooms/{id}", rooms.Update)
	srv := bitetest.Start(t, app)
	a := srv.Do(bitetest.Request{Method: "GET", Target: "/rooms/7"})
	b := srv.Do(bitetest.Request{Method: "GET", Target: "/rooms/7"})
	put := func(tag string) bitetest.Response {
		return srv.Do(bitetest.Request{
			Method: "PUT", Target: "/rooms/7", Body: []byte(`{"name":"lounge"}`),
			Header: map[string][]string{"Content-Type": {"application/json"}, "If-Match": {tag}},
		})
	}
	if r := put(a.Get("ETag")); r.Status != 200 {
		t.Fatalf("the first editor: %d", r.Status)
	}
	if r := put(b.Get("ETag")); r.Status != 412 {
		t.Fatalf("the second editor read the old version and must be refused, got %d", r.Status)
	}
}
```
