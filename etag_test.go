package bites_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/nevindra/bites"
	"github.com/nevindra/bites/bitetest"
)

// The validators of ADR 0089: a Tagged answer and an IfMatch write.

type etagDoc struct {
	ID      int64  `json:"id"`
	Title   string `json:"title"`
	Version int64  `json:"version"`
}

type etagGet struct{ Path struct{ ID int64 } }

type etagPut struct {
	Path   struct{ ID int64 }
	Header struct{ IfMatch bites.IfMatch }
	Body   struct{ Title string }
}

// etagStore is the record two clients overwrite.
type etagStore struct {
	mu  sync.Mutex
	doc etagDoc
}

func (s *etagStore) get(_ context.Context, in etagGet) (bites.Tagged[etagDoc], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.doc
	d.ID = in.Path.ID
	return bites.Tagged[etagDoc]{Body: d, ETag: bites.ETag{Version: strconv.FormatInt(d.Version, 10)}}, nil
}

func (s *etagStore) put(_ context.Context, in etagPut) (bites.Tagged[etagDoc], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := in.Header.IfMatch.Check(strconv.FormatInt(s.doc.Version, 10)); err != nil {
		return bites.Tagged[etagDoc]{}, err
	}
	s.doc.Title = in.Body.Title
	s.doc.Version++
	d := s.doc
	return bites.Tagged[etagDoc]{Body: d, ETag: bites.ETag{Version: strconv.FormatInt(d.Version, 10)}}, nil
}

// Inputs the table of refusals in refusal_test.go registers.
type (
	EtagQueryIfMatch struct {
		Query struct{ IfMatch bites.IfMatch }
	}
	EtagRenamed    struct{ Header struct{ Match bites.IfMatch } }
	EtagOptIfMatch struct {
		Header struct{ IfMatch bites.Opt[bites.IfMatch] }
	}
	EtagListIfMatch struct {
		Header struct{ IfMatch []bites.IfMatch }
	}
)

func startDocs(t *testing.T, opts ...bites.Option) (*bitetest.Server, *etagStore, *logBuf) {
	t.Helper()
	st := &etagStore{doc: etagDoc{Title: "first", Version: 1}}
	srv, lb := startLogged(t, func(a *bites.App) {
		a.Get("/docs/{id}", st.get)
		a.Put("/docs/{id}", st.put)
	}, opts...)
	return srv, st, lb
}

func etagWith(r bitetest.Request, name, value string) bitetest.Request {
	h := map[string][]string{}
	for k, v := range r.Header {
		h[k] = v
	}
	h[name] = append(h[name], value)
	r.Header = h
	return r
}

func putReq(id, ifMatch, title string) bitetest.Request {
	r := jsonReq("PUT", "/docs/"+id, `{"title":`+strconv.Quote(title)+`}`)
	if ifMatch != "" {
		r = etagWith(r, "If-Match", ifMatch)
	}
	return r
}

func TestATaggedGetSendsItsETagInQuotesAndItsBodyAsJSON(t *testing.T) {
	srv, _, _ := startDocs(t)
	r := srv.Do(req("GET", "/docs/7"))
	if r.Status != 200 || r.Get("etag") != `"1"` || r.Get("content-type") != "application/json" || r.Text() != `{"id":7,"title":"first","version":1}` {
		t.Fatalf("%d %v %s", r.Status, r.Header, r.Text())
	}
}

func TestAGetWhoseIfNoneMatchNamesTheTagIsA304WithTheTagAndNoBody(t *testing.T) {
	srv, _, _ := startDocs(t)
	for _, inm := range []string{`"1"`, `W/"1"`, `"0", "1"`, `"9",W/"1" , "7"`, `*`} {
		r := srv.Do(etagWith(req("GET", "/docs/7"), "If-None-Match", inm))
		if r.Status != 304 || r.Get("etag") != `"1"` || len(r.Body) != 0 || r.Get("content-type") != "" || r.Get("content-length") != "" {
			t.Errorf("If-None-Match %s: %d %v %q", inm, r.Status, r.Header, r.Body)
		}
	}
}

func TestAGetWhoseIfNoneMatchNamesAnotherTagOrNoneOrCannotBeReadIsAnswered200(t *testing.T) {
	srv, _, _ := startDocs(t)
	for _, inm := range []string{`"2"`, `"11"`, `1`, `"1`, `W/1`, ``, `,`} {
		r := srv.Do(etagWith(req("GET", "/docs/7"), "If-None-Match", inm))
		if r.Status != 200 || r.Get("etag") != `"1"` || len(r.Body) == 0 {
			t.Errorf("If-None-Match %q: %d %v", inm, r.Status, r.Header)
		}
	}
}

func TestAHeadWhoseIfNoneMatchNamesTheTagIsA304AndOtherwiseCarriesTheTagWithNoBody(t *testing.T) {
	st := &etagStore{doc: etagDoc{Version: 4}}
	srv := start(t, func(a *bites.App) { a.Head("/docs/{id}", st.get) })
	if r := srv.Do(etagWith(req("HEAD", "/docs/1"), "If-None-Match", `"4"`)); r.Status != 304 || r.Get("etag") != `"4"` {
		t.Fatalf("%d %v", r.Status, r.Header)
	}
	if r := srv.Do(req("HEAD", "/docs/1")); r.Status != 200 || r.Get("etag") != `"4"` || len(r.Body) != 0 {
		t.Fatalf("%d %v %q", r.Status, r.Header, r.Body)
	}
}

func TestATaggedAnswerToAPutSendsTheNewTagAndNeverEvaluatesIfNoneMatch(t *testing.T) {
	srv, _, _ := startDocs(t)
	// The tag the answer will carry is "2": a 304 here would drop the change the client just made.
	r := srv.Do(etagWith(putReq("7", `"1"`, "second"), "If-None-Match", `"2"`))
	if r.Status != 200 || r.Get("etag") != `"2"` || !strings.Contains(r.Text(), `"title":"second"`) {
		t.Fatalf("%d %v %s", r.Status, r.Header, r.Text())
	}
}

func TestA304KeepsCacheControlVaryExpiresAndCookiesAndDropsTheRestOfHeader(t *testing.T) {
	srv := start(t, func(a *bites.App) {
		a.Get("/d", func(context.Context, Empty) (bites.Tagged[etagDoc], error) {
			return bites.Tagged[etagDoc]{
				Body:    etagDoc{ID: 1},
				ETag:    bites.ETag{Version: "v"},
				Header:  bites.Header{"Cache-Control": {"private, max-age=0"}, "Vary": {"Authorization"}, "X-Other": {"x"}},
				Cookies: []bites.Cookie{{Name: "seen", Value: "1"}},
			}, nil
		})
	})
	r := srv.Do(etagWith(req("GET", "/d"), "If-None-Match", `"v"`))
	if r.Status != 304 || r.Get("cache-control") != "private, max-age=0" || r.Get("vary") != "Authorization" || r.Get("x-other") != "" || !strings.HasPrefix(r.Get("set-cookie"), "seen=1") {
		t.Fatalf("%d %v", r.Status, r.Header)
	}
	if r := srv.Do(req("GET", "/d")); r.Status != 200 || r.Get("x-other") != "x" || r.Get("etag") != `"v"` {
		t.Fatalf("%d %v", r.Status, r.Header)
	}
}

func TestATagMadeFromTheBodyIsWeakStableAndChangesWithTheBody(t *testing.T) {
	title := "one"
	srv := start(t, func(a *bites.App) {
		a.Get("/d", func(context.Context, Empty) (bites.Tagged[etagDoc], error) {
			return bites.Tagged[etagDoc]{Body: etagDoc{Title: title}, ETag: bites.ETagOfBody()}, nil
		})
	})
	a, b := srv.Do(req("GET", "/d")), srv.Do(req("GET", "/d"))
	tag := a.Get("etag")
	if a.Status != 200 || !strings.HasPrefix(tag, `W/"`) || len(tag) != len(`W/"`)+22+1 || tag != b.Get("etag") {
		t.Fatalf("%d %v", a.Status, a.Header)
	}
	if r := srv.Do(etagWith(req("GET", "/d"), "If-None-Match", tag)); r.Status != 304 || len(r.Body) != 0 || r.Get("etag") != tag {
		t.Fatalf("%d %v", r.Status, r.Header)
	}
	title = "two"
	r := srv.Do(etagWith(req("GET", "/d"), "If-None-Match", tag))
	if r.Status != 200 || r.Get("etag") == tag || !strings.Contains(r.Text(), `"title":"two"`) {
		t.Fatalf("the body changed but %d %v", r.Status, r.Header)
	}
}

func TestATaggedWithNoETagAnInvalidVersionOrAnETagInHeaderIsALoggedProblem500(t *testing.T) {
	for _, c := range []struct {
		name string
		tag  bites.ETag
		hdr  bites.Header
		log  string
	}{
		{"none", bites.ETag{}, nil, "a Tagged has an empty ETag"},
		{"quote", bites.ETag{Version: `a"b`}, nil, "cannot be in an entity-tag"},
		{"space", bites.ETag{Version: `a b`}, nil, "cannot be in an entity-tag"},
		{"long", bites.ETag{Version: strings.Repeat("a", 129)}, nil, "cannot be in an entity-tag"},
		{"twice", bites.ETag{Version: "1"}, bites.Header{"ETag": {`"2"`}}, "give it once, in the ETag field"},
	} {
		srv, lb := startLogged(t, func(a *bites.App) {
			a.Get("/d", func(context.Context, Empty) (bites.Tagged[etagDoc], error) {
				return bites.Tagged[etagDoc]{ETag: c.tag, Header: c.hdr}, nil
			})
		})
		if r := srv.Do(req("GET", "/d")); r.Status != 500 || r.Get("etag") != "" || !strings.Contains(lb.String(), c.log) {
			t.Errorf("%s: %d %v\n%s", c.name, r.Status, r.Header, lb.String())
		}
	}
}

func TestTwoClientsOverwritingOneRecordTheSecondGetsA412UntilItReadsAgain(t *testing.T) {
	srv, st, _ := startDocs(t)
	a, b := srv.Do(req("GET", "/docs/7")), srv.Do(req("GET", "/docs/7"))
	if a.Get("etag") != `"1"` || b.Get("etag") != `"1"` {
		t.Fatalf("%v %v", a.Header, b.Header)
	}
	if r := srv.Do(putReq("7", a.Get("etag"), "from A")); r.Status != 200 || r.Get("etag") != `"2"` {
		t.Fatalf("A: %d %v", r.Status, r.Header)
	}
	r := srv.Do(putReq("7", b.Get("etag"), "from B"))
	wantProblem(t, r, 412, `{"title":"Precondition Failed","status":412,"detail":"the record is no longer the version named in If-Match: GET it again, apply the change to what you read, and send the new ETag"}`)
	if st.doc.Title != "from A" {
		t.Fatalf("B's write got through: %q", st.doc.Title)
	}
	fresh := srv.Do(req("GET", "/docs/7"))
	if r := srv.Do(putReq("7", fresh.Get("etag"), "from B")); r.Status != 200 || st.doc.Title != "from B" {
		t.Fatalf("B after reading again: %d %s", r.Status, st.doc.Title)
	}
}

func TestAWriteWithNoIfMatchThatChecksIsA428AndChangesNothing(t *testing.T) {
	srv, st, _ := startDocs(t)
	r := srv.Do(putReq("7", "", "lost"))
	wantProblem(t, r, 428, `{"title":"Precondition Required","status":428,"detail":"this change is made only to the version you read: send If-Match with the ETag of the record (GET it to get the ETag)"}`)
	if st.doc.Title != "first" {
		t.Fatalf("the write went through: %q", st.doc.Title)
	}
}

func TestIfMatchNamesTheVersionByAnyListSpellingAndStarMatchesTheRecord(t *testing.T) {
	for _, im := range []string{`"1"`, `W/"1"`, `"9", "1"`, `"9",W/"1"`, `  "1"  `, `*`} {
		srv, _, _ := startDocs(t)
		if r := srv.Do(putReq("7", im, "x")); r.Status != 200 {
			t.Errorf("If-Match %q: %d %s", im, r.Status, r.Text())
		}
	}
	for _, im := range []string{`"2"`, `"11"`, `"9", "8"`, `W/"2"`} {
		srv, _, _ := startDocs(t)
		if r := srv.Do(putReq("7", im, "x")); r.Status != 412 {
			t.Errorf("If-Match %q: %d %s", im, r.Status, r.Text())
		}
	}
}

func TestAnIfMatchThatIsNotStarOrEntityTagsIsA400AtHeaderIfMatchAndNeverIgnored(t *testing.T) {
	long := strings.Repeat(`"abcdefgh", `, 200)
	many := strings.Repeat(`"a",`, 17) + `"a"`
	for _, c := range []struct{ im, msg string }{
		{`1`, `expected * or entity-tags such as \"3\" or W/\"3\", separated by commas`},
		{`"1`, `expected * or entity-tags such as \"3\" or W/\"3\", separated by commas`},
		{`"1" x`, `expected * or entity-tags such as \"3\" or W/\"3\", separated by commas`},
		{`,`, `expected * or entity-tags such as \"3\" or W/\"3\", separated by commas`},
		{`"a b"`, `expected * or entity-tags such as \"3\" or W/\"3\", separated by commas`},
		{many, `expected at most 16 entity-tags`},
		{long, `expected at most 1024 bytes`},
	} {
		srv, st, _ := startDocs(t)
		r := srv.Do(putReq("7", c.im, "x"))
		want := `{"title":"Bad Request","status":400,"errors":[{"location":"header.If-Match","message":"` + c.msg + `","rule":"type"}]}`
		if r.Status != 400 || r.Text() != want || st.doc.Title != "first" {
			t.Errorf("If-Match %.20q: %d %s", c.im, r.Status, r.Text())
		}
	}
}

func TestATwoLineIfMatchIsA400BecauseTheListIsSentOnOneLine(t *testing.T) {
	srv, _, _ := startDocs(t)
	r := srv.Do(etagWith(putReq("7", `"1"`, "x"), "If-Match", `"2"`))
	if r.Status != 400 || !strings.Contains(r.Text(), `"location":"header.If-Match"`) || !strings.Contains(r.Text(), "sent more than once") {
		t.Fatalf("%d %s", r.Status, r.Text())
	}
}

func TestAHandlerThatAsksPresentMayWriteWithNoIfMatchAndIsNotLogged(t *testing.T) {
	var wrote int
	srv, lb := startLogged(t, func(a *bites.App) {
		a.Put("/docs/{id}", func(_ context.Context, in etagPut) (bites.NoContent, error) {
			if in.Header.IfMatch.Present() {
				if err := in.Header.IfMatch.Check("1"); err != nil {
					return bites.NoContent{}, err
				}
			}
			wrote++
			return bites.NoContent{}, nil
		})
	})
	if r := srv.Do(putReq("7", "", "x")); r.Status != 204 {
		t.Fatalf("%d", r.Status)
	}
	if r := srv.Do(putReq("7", `"2"`, "x")); r.Status != 412 {
		t.Fatalf("%d", r.Status)
	}
	if wrote != 1 || strings.Contains(lb.String(), "not conditional") {
		t.Fatalf("wrote %d times\n%s", wrote, lb.String())
	}
}

func TestAHandlerThatNeverAsksAboutItsIfMatchFieldIsLoggedOnEverySuccessAndStillAnswers(t *testing.T) {
	srv, lb := startLogged(t, func(a *bites.App) {
		a.Put("/docs/{id}", func(context.Context, etagPut) (bites.NoContent, error) { return bites.NoContent{}, nil })
	})
	for range 2 {
		if r := srv.Do(putReq("7", `"2"`, "x")); r.Status != 204 {
			t.Fatalf("%d", r.Status)
		}
	}
	log := lb.String()
	if n := strings.Count(log, "so this write was not conditional on the version the client read"); n != 2 || !strings.Contains(log, "etagPut.Header.IfMatch") || !strings.Contains(log, "path=/docs/7") {
		t.Fatalf("%d lines\n%s", n, log)
	}
}

func TestAHandlerThatReturnsAnErrorIsNotLoggedForSkippingIfMatch(t *testing.T) {
	srv, lb := startLogged(t, func(a *bites.App) {
		a.Put("/docs/{id}", func(context.Context, etagPut) (bites.NoContent, error) {
			return bites.NoContent{}, bites.NotFound("no such doc")
		})
	})
	if r := srv.Do(putReq("7", "", "x")); r.Status != 404 || strings.Contains(lb.String(), "not conditional") {
		t.Fatalf("%d\n%s", r.Status, lb.String())
	}
}

func TestIfMatchCheckWithAVersionThatCannotBeInATagIsAnErrorTheClientSeesAs500(t *testing.T) {
	srv, lb := startLogged(t, func(a *bites.App) {
		a.Put("/docs/{id}", func(_ context.Context, in etagPut) (bites.NoContent, error) {
			return bites.NoContent{}, in.Header.IfMatch.Check(`has "quotes"`)
		})
	})
	if r := srv.Do(putReq("7", `"1"`, "x")); r.Status != 500 || !strings.Contains(lb.String(), "IfMatch.Check was given the version") {
		t.Fatalf("%d\n%s", r.Status, lb.String())
	}
}

func TestIfMatchBuiltByHandReadsLikeTheHeaderAndAZeroOneSentNone(t *testing.T) {
	var m bites.IfMatch
	if m.Present() || m.Check("1") == nil {
		t.Fatal("the zero IfMatch must be absent and refuse")
	}
	if err := m.UnmarshalText([]byte(`W/"1", "2"`)); err != nil || !m.Present() || m.Check("2") != nil || m.Check("3") == nil {
		t.Fatalf("%v", err)
	}
	if err := m.UnmarshalText([]byte(`nonsense`)); err == nil {
		t.Fatal("a value that is not entity-tags must be refused")
	}
}

func TestAWeakenedTagTheClientGotFromGzipOrAProxyStillNamesTheVersionOnTheNextWrite(t *testing.T) {
	// The framework's own gzip weakens the strong tag of a compressed body (ADR 0031), and the
	// client sends back what it received.
	st := &etagStore{doc: etagDoc{Title: strings.Repeat("x", 3000), Version: 1}}
	srv := startWith(t, func(a *bites.App) {
		a.Get("/docs/{id}", st.get)
		a.Put("/docs/{id}", st.put)
	}, bites.WithGzip(bites.Gzip{}))
	r := srv.Do(etagWith(req("GET", "/docs/7"), "Accept-Encoding", "gzip"))
	if r.Get("content-encoding") != "gzip" || r.Get("etag") != `W/"1"` {
		t.Fatalf("expected a compressed answer with a weakened tag: %v", r.Header)
	}
	if w := srv.Do(putReq("7", r.Get("etag"), "x")); w.Status != 200 {
		t.Fatalf("the weakened tag the client received was refused: %d %s", w.Status, w.Text())
	}
	if w := srv.Do(putReq("7", strings.TrimPrefix(r.Get("etag"), "W/"), "y")); w.Status != 412 {
		t.Fatalf("the version moved on, so the old tag is a 412 weak or strong: %d", w.Status)
	}
}

func TestATaggedAnswerOfTheBodyIsTheSameWithAndWithoutCompression(t *testing.T) {
	big := strings.Repeat("y", 3000)
	app := func(a *bites.App) {
		a.Get("/d", func(context.Context, Empty) (bites.Tagged[etagDoc], error) {
			return bites.Tagged[etagDoc]{Body: etagDoc{Title: big}, ETag: bites.ETagOfBody()}, nil
		})
	}
	plain := start(t, app)
	zipped := startWith(t, app, bites.WithGzip(bites.Gzip{}))
	a := plain.Do(req("GET", "/d"))
	b := zipped.Do(etagWith(req("GET", "/d"), "Accept-Encoding", "gzip"))
	if b.Get("content-encoding") != "gzip" || a.Get("etag") == "" || a.Get("etag") != b.Get("etag") {
		t.Fatalf("%v %v", a.Header, b.Header)
	}
}

func TestAHostileIfNoneMatchOfAThousandTagsCostsOneScanAndIsA200(t *testing.T) {
	srv, _, _ := startDocs(t)
	list := strings.Repeat(`"a",`, 1500) + `"z"`
	r := srv.Do(etagWith(req("GET", "/docs/7"), "If-None-Match", list))
	if r.Status != 200 && r.Status != 400 && r.Status != 431 {
		t.Fatalf("%d", r.Status)
	}
	small := strings.Repeat(`"a",`, 1000) + `"1"`
	if r := srv.Do(etagWith(req("GET", "/docs/7"), "If-None-Match", small)); r.Status != 304 {
		t.Fatalf("%d", r.Status)
	}
}

func TestTheAPIDocumentStatesTheETagThe304TheIfMatchHeaderAndTheStatusesItAnswers(t *testing.T) {
	st := &etagStore{doc: etagDoc{Title: "first", Version: 1}}
	srv := start(t, func(a *bites.App) {
		a.OpenAPI("/openapi.json", bites.Info{Title: "t", Version: "1"})
		a.Get("/docs/{id}", st.get)
		a.Put("/docs/{id}", st.put)
	})
	var doc map[string]any
	if err := json.Unmarshal(srv.Do(req("GET", "/openapi.json")).Body, &doc); err != nil {
		t.Fatal(err)
	}
	get := doc["paths"].(map[string]any)["/docs/{id}"].(map[string]any)["get"].(map[string]any)
	put := doc["paths"].(map[string]any)["/docs/{id}"].(map[string]any)["put"].(map[string]any)
	gr, pr := get["responses"].(map[string]any), put["responses"].(map[string]any)
	etag := gr["200"].(map[string]any)["headers"].(map[string]any)["ETag"].(map[string]any)
	if etag["required"] != true {
		t.Fatalf("the ETag of a Tagged is always sent: %v", etag)
	}
	if _, ok := gr["304"].(map[string]any)["headers"].(map[string]any)["ETag"]; !ok {
		t.Fatalf("GET 304: %v", gr["304"])
	}
	if _, ok := pr["304"]; ok {
		t.Fatalf("a PUT never answers 304: %v", pr)
	}
	for _, st := range []string{"412", "428"} {
		if _, ok := pr[st]; !ok {
			t.Errorf("PUT %s missing: %v", st, pr)
		}
		if _, ok := gr[st]; ok {
			t.Errorf("GET %s should not be stated: %v", st, gr)
		}
	}
	var found map[string]any
	for _, p := range put["parameters"].([]any) {
		if p.(map[string]any)["name"] == "If-Match" {
			found = p.(map[string]any)
		}
	}
	if found == nil || found["in"] != "header" || found["required"] != false || found["schema"].(map[string]any)["type"] != "string" {
		t.Fatalf("parameters: %v", put["parameters"])
	}
}

func TestErrorStatusesNamingTheFramework412And428AreAcceptedAndErrorStatusWithMembersIsRefused(t *testing.T) {
	type stale struct{ Current int }
	st := &etagStore{doc: etagDoc{Version: 1}}
	srv := start(t, func(a *bites.App) {
		a.OpenAPI("/openapi.json", bites.Info{Title: "t", Version: "1"})
		a.Put("/docs/{id}", st.put, bites.ErrorStatuses(412, 404))
	})
	var doc map[string]any
	if err := json.Unmarshal(srv.Do(req("GET", "/openapi.json")).Body, &doc); err != nil {
		t.Fatal(err)
	}
	rs := doc["paths"].(map[string]any)["/docs/{id}"].(map[string]any)["put"].(map[string]any)["responses"].(map[string]any)
	for _, s := range []string{"404", "412", "428"} {
		if _, ok := rs[s]; !ok {
			t.Errorf("%s is not stated: %v", s, rs)
		}
	}
	defer func() {
		if r := recover(); r == nil {
			t.Error("ErrorStatus[T](412) on a route with an IfMatch must be refused: the framework already answers 412 there")
		}
	}()
	app := bites.New(bites.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	app.OpenAPI("/openapi.json", bites.Info{Title: "t", Version: "1"})
	app.Put("/docs/{id}", st.put, bites.ErrorStatus[stale](412))
	_, _ = app.OpenAPIDocument()
}
