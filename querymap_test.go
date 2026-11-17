package bites_test

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/nevindra/bites"
	"github.com/nevindra/bites/bitetest"
)

// ---- A map in the query: filter[name]=x, the deepObject style (ADR 0042) ----

type filterKey string

func (filterKey) Values() []filterKey { return []filterKey{"name", "age"} }

type shortFilter map[string]string

func (shortFilter) Members() (int, int) { return 1, 2 }

type (
	MapIn struct {
		Query struct {
			Filter map[string]string
			Page   int
		}
	}
	MapOptIn struct {
		Query struct{ Filter bites.Opt[map[string]int] }
	}
	MapKeysIn struct {
		Query struct{ Filter map[filterKey]string }
	}
	MapBoundIn struct {
		Query struct{ Filter shortFilter }
	}
	MapTypedIn struct {
		Query struct {
			Range bites.Opt[map[string]bites.Date]
		}
	}
	MapTwoIn struct {
		Query struct {
			Filter map[string]string
			Sort   map[string]string
		}
	}
)

// The inputs the refusal table of refusal_test.go registers for a map.
type (
	mapItems map[string]string
	MapPath  struct {
		Path struct{ ID map[string]string }
	}
	MapHeader struct {
		Header struct{ XFilter map[string]string }
	}
	MapCookie struct {
		Cookie struct{ Filter map[string]string }
	}
	MapForm struct {
		Body bites.Form[struct{ Filter map[string]string }]
	}
	MapIntKey struct {
		Query struct{ Filter map[int]string }
	}
	MapOfOpt struct {
		Query struct{ Filter map[string]bites.Opt[int] }
	}
	MapOfList struct {
		Query struct{ Filter map[string][]string }
	}
	MapOfStruct struct {
		Query struct{ Filter map[string]struct{ A int } }
	}
	MapDefault struct{ Query mapDefaultQuery }
	MapItems   struct{ Query struct{ Filter mapItems } }
	MapTooMany struct {
		Query struct{ A, B, C, D, E map[string]string }
	}
)

type mapDefaultQuery struct{ Filter map[string]string }

func (mapDefaultQuery) Defaults() map[string]string { return map[string]string{"Filter": "a"} }
func (mapItems) Items() (int, int)                  { return 1, 2 }

func mapSrv(t *testing.T, opts ...bites.Option) *bitetest.Server {
	return startWith(t, func(app *bites.App) {
		app.Get("/m", func(_ context.Context, in MapIn) (map[string]string, error) { return in.Query.Filter, nil })
		app.Get("/o", func(_ context.Context, in MapOptIn) (struct {
			N   int
			Set bool
			Sum int
		}, error) {
			m, ok := in.Query.Filter.Get()
			sum := 0
			for _, v := range m {
				sum += v
			}
			return struct {
				N   int
				Set bool
				Sum int
			}{len(m), ok, sum}, nil
		})
		app.Get("/k", func(_ context.Context, in MapKeysIn) (map[filterKey]string, error) { return in.Query.Filter, nil })
		app.Get("/b", func(_ context.Context, in MapBoundIn) (shortFilter, error) { return in.Query.Filter, nil })
		app.Get("/t", func(_ context.Context, in MapTypedIn) (int, error) { m, _ := in.Query.Range.Get(); return len(m), nil })
		app.Get("/2", func(_ context.Context, in MapTwoIn) (map[string]string, error) {
			out := map[string]string{}
			for k, v := range in.Query.Filter {
				out["f."+k] = v
			}
			for k, v := range in.Query.Sort {
				out["s."+k] = v
			}
			return out, nil
		})
	}, opts...)
}

func getMap(srv *bitetest.Server, target string) bitetest.Response {
	return srv.Do(bitetest.Request{Method: "GET", Target: target})
}

func TestAMapInTheQueryIsFilledFromNameAndKeyInBrackets(t *testing.T) {
	srv := mapSrv(t)
	r := getMap(srv, "/m?filter[name]=x&page=2&filter[age]=3")
	if r.Status != 200 || r.Text() != `{"age":"3","name":"x"}` {
		t.Fatalf("%d %s", r.Status, r.Text())
	}
}

func TestAPercentEncodedBracketAndAUnicodeKeyAreTheSameKeyAsWritten(t *testing.T) {
	srv := mapSrv(t)
	r := getMap(srv, "/m?filter%5Bname%5D=x&filter%5B%E5%90%8D%E5%89%8D%5D=%E5%A4%AA%E9%83%8E&filter[a+b]=c%20d&page=1")
	if r.Status != 200 || r.Text() != `{"a b":"c d","name":"x","名前":"太郎"}` {
		t.Fatalf("%d %s", r.Status, r.Text())
	}
}

func TestAMemberWithNoValueOrAnEmptyOneIsTheEmptyString(t *testing.T) {
	srv := mapSrv(t)
	r := getMap(srv, "/m?filter[a]&filter[b]=&page=1")
	if r.Status != 200 || r.Text() != `{"a":"","b":""}` {
		t.Fatalf("%d %s", r.Status, r.Text())
	}
}

func TestAMapNobodySentIsA400RequiredAndAnOptMapIsUnset(t *testing.T) {
	srv := mapSrv(t)
	wantProblem(t, getMap(srv, "/m?page=1"), 400,
		`{"title":"Bad Request","status":400,"errors":[{"location":"query.filter","message":"required","rule":"required"}]}`)
	r := getMap(srv, "/o")
	if r.Status != 200 || r.Text() != `{"n":0,"set":false,"sum":0}` {
		t.Fatalf("%d %s", r.Status, r.Text())
	}
	r = getMap(srv, "/o?filter[a]=2&filter[b]=5")
	if r.Status != 200 || r.Text() != `{"n":2,"set":true,"sum":7}` {
		t.Fatalf("%d %s", r.Status, r.Text())
	}
}

func TestAMemberIsDecodedByTheValuesTypeAndTheBadOneIsLocatedByItsKey(t *testing.T) {
	srv := mapSrv(t)
	wantProblem(t, getMap(srv, "/o?filter[a]=1&filter[b]=x&filter[c]=y"), 400,
		`{"title":"Bad Request","status":400,"errors":[`+
			`{"location":"query.filter[b]","message":"expected an integer","rule":"type"},`+
			`{"location":"query.filter[c]","message":"expected an integer","rule":"type"}]}`)
	wantProblem(t, getMap(srv, "/t?range[from]=2026-01-01&range[to]=2026-02-30"), 400,
		`{"title":"Bad Request","status":400,"errors":[{"location":"query.range[to]","message":"expected a date as YYYY-MM-DD","rule":"type"}]}`)
	if r := getMap(srv, "/t?range[from]=2026-01-01&range[to]=2026-02-03"); r.Text() != "2" {
		t.Fatalf("%d %s", r.Status, r.Text())
	}
}

func TestAMemberSentTwiceIsA400SentMoreThanOnceNamedOnce(t *testing.T) {
	srv := mapSrv(t)
	wantProblem(t, getMap(srv, "/m?filter[a]=1&filter[a]=2&filter[b]=3&filter[a]=4&page=1"), 400,
		`{"title":"Bad Request","status":400,"errors":[{"location":"query.filter[a]","message":"sent more than once"}]}`)
}

func TestAKeyTypeWithValuesListsTheKeysThatAreAllowed(t *testing.T) {
	srv := mapSrv(t)
	if r := getMap(srv, "/k?filter[name]=x&filter[age]=3"); r.Status != 200 || r.Text() != `{"age":"3","name":"x"}` {
		t.Fatalf("%d %s", r.Status, r.Text())
	}
	wantProblem(t, getMap(srv, "/k?filter[name]=x&filter[nmae]=y"), 400,
		`{"title":"Bad Request","status":400,"errors":[{"location":"query.filter[nmae]","message":"expected one of: name, age","rule":"values"}]}`)
}

func TestABoundOnTheMembersOfANamedMapIsA400BeforeAnythingIsStored(t *testing.T) {
	srv := mapSrv(t)
	wantProblem(t, getMap(srv, "/b?filter[a]=1&filter[b]=2&filter[c]=3"), 400,
		`{"title":"Bad Request","status":400,"errors":[{"location":"query.filter","message":"must have between 1 and 2 members","rule":"members","min":1,"max":2}]}`)
	if r := getMap(srv, "/b?filter[a]=1&filter[b]=2"); r.Status != 200 {
		t.Fatalf("%d %s", r.Status, r.Text())
	}
}

func TestAMapWithMoreMembersThanTheLimitIsA400WithNoMapBuiltAndThePairsOverTheHeadAre431(t *testing.T) {
	srv := mapSrv(t)
	var q strings.Builder
	for i := 0; i < 101; i++ {
		q.WriteString("&filter[k" + strconv.Itoa(i) + "]=v")
	}
	wantProblem(t, getMap(srv, "/m?page=1"+q.String()), 400,
		`{"title":"Bad Request","status":400,"errors":[{"location":"query.filter","message":"must have at most 100 members","rule":"members","min":0,"max":100}]}`)
	q.Reset()
	for i := 0; i < 10000; i++ {
		q.WriteString("&filter[k" + strconv.Itoa(i) + "]=v")
	}
	if r := getMap(srv, "/m?page=1"+q.String()); r.Status != 431 {
		t.Fatalf("10,000 members are %d, want the 431 of the head limit: %s", r.Status, r.Text())
	}
	big := mapSrv(t, bites.WithLimits(bites.Limits{MaxHead: 1 << 20}))
	r := getMap(big, "/m?page=1"+q.String())
	if r.Status != 400 || strings.Count(r.Text(), `"location"`) != 1 || !strings.Contains(r.Text(), "must have at most 100 members") {
		t.Fatalf("10,000 members in a big head are one error, got %d %s", r.Status, r.Text())
	}
}

func TestBracketsInTheWrongPlaceAreA400NamingTheKeyAndWhatToSend(t *testing.T) {
	srv := mapSrv(t)
	for _, c := range []struct{ target, loc, msg string }{
		{"/m?page=1&filter[=x", "query.filter[", "expected the form filter[key]"},
		{"/m?page=1&filter]=x", "query.filter]", "expected the form filter[key]"},
		{"/m?page=1&filter[]=x", "query.filter[]", "expected a key between the brackets, as filter[key]"},
		{"/m?page=1&filter[a][b]=x", "query.filter[a][b]", "nested keys are not supported: send one key in brackets, as filter[key]"},
		{"/m?page=1&filter[a]b=x", "query.filter[a]b", "expected the form filter[key]"},
		{"/m?page=1&filter[a[b]=x", "query.filter[a[b]", "expected the form filter[key]"},
		{"/m?page=1&filter=x", "query.filter", "expected the form filter[key]"},
		{"/m?page=1&filter", "query.filter", "expected the form filter[key]"},
		{"/m?page=1&filter%5Ba%5D%5Bb%5D=x", "query.filter[a][b]", "nested keys are not supported: send one key in brackets, as filter[key]"},
	} {
		r := getMap(srv, c.target)
		want := `{"title":"Bad Request","status":400,"errors":[{"location":"` + c.loc + `","message":"` + c.msg + `"}]}`
		if r.Status != 400 || r.Text() != want {
			t.Errorf("%s:\n got %d %s\nwant %s", c.target, r.Status, r.Text(), want)
		}
	}
}

func TestAKeyThatIsNotTheMapsNameIsAnUnknownKeyAsBefore(t *testing.T) {
	srv := mapSrv(t)
	if r := getMap(srv, "/m?page=1&filters[a]=x&[a]=y&other[b]=z&filter[a]=1"); r.Status != 200 || r.Text() != `{"a":"1"}` {
		t.Fatalf("%d %s", r.Status, r.Text())
	}
	strict := mapSrv(t, bites.WithRejectUnknownQueryKeys())
	wantProblem(t, getMap(strict, "/m?page=1&filter[a]=1&filters[a]=x"), 400,
		`{"title":"Bad Request","status":400,"errors":[{"location":"query.filters[a]","message":"unknown query parameter; this route takes filter[key], page"}]}`)
}

func TestAHostileKeyIsEchoedCutTo64BytesOnAValidRuneBoundary(t *testing.T) {
	srv := mapSrv(t)
	key := strings.Repeat("名", 40)
	r := getMap(srv, "/m?page=1&filter["+key+"][b]=x")
	var p struct{ Errors []struct{ Location string } }
	if err := json.Unmarshal([]byte(r.Text()), &p); err != nil || r.Status != 400 || len(p.Errors) != 1 {
		t.Fatalf("%d %s %v", r.Status, r.Text(), err)
	}
	if n := len(strings.TrimPrefix(p.Errors[0].Location, "query.")); n > 64 || strings.ContainsRune(p.Errors[0].Location, '\uFFFD') {
		t.Fatalf("the location is %d bytes: %q", n, p.Errors[0].Location)
	}
	r = getMap(srv, "/m?page=1&filter[%ff][%fe]=x")
	if err := json.Unmarshal([]byte(r.Text()), &p); err != nil || r.Status != 400 {
		t.Fatalf("a key that is not UTF-8 is still answered as JSON: %d %q %v", r.Status, r.Text(), err)
	}
	r = getMap(srv, "/o?filter[%ff%fe]=1&filter[%ff]=2")
	if r.Status != 200 || r.Text() != `{"n":2,"set":true,"sum":3}` {
		t.Fatalf("a key that is not UTF-8 is a key like another: %d %s", r.Status, r.Text())
	}
}

func TestTwoMapsInOneQueryAreFilledApart(t *testing.T) {
	srv := mapSrv(t)
	r := getMap(srv, "/2?filter[a]=1&sort[a]=2&filter[b]=3")
	if r.Status != 200 || r.Text() != `{"f.a":"1","f.b":"3","s.a":"2"}` {
		t.Fatalf("%d %s", r.Status, r.Text())
	}
	wantProblem(t, getMap(srv, "/2?filter[a]=1"), 400,
		`{"title":"Bad Request","status":400,"errors":[{"location":"query.sort","message":"required","rule":"required"}]}`)
}

func TestAMapIsDocumentedAsADeepObjectWithTheValuesSchemaTheKeysAndTheBound(t *testing.T) {
	srv := start(t, func(app *bites.App) {
		app.OpenAPI("/openapi.json", bites.Info{Title: "t", Version: "1"})
		app.Get("/m", func(context.Context, MapIn) (struct{}, error) { return struct{}{}, nil })
		app.Get("/k", func(context.Context, MapKeysIn) (struct{}, error) { return struct{}{}, nil })
		app.Get("/b", func(context.Context, MapBoundIn) (struct{}, error) { return struct{}{}, nil })
		app.Get("/o", func(context.Context, MapOptIn) (struct{}, error) { return struct{}{}, nil })
	})
	var doc struct {
		Paths map[string]map[string]struct{ Parameters []map[string]any }
	}
	if err := json.Unmarshal([]byte(srv.Do(bitetest.Request{Method: "GET", Target: "/openapi.json"}).Text()), &doc); err != nil {
		t.Fatal(err)
	}
	param := func(path string) map[string]any {
		for _, p := range doc.Paths[path]["get"].Parameters {
			if p["name"] == "filter" {
				return p
			}
		}
		t.Fatalf("no filter on %s", path)
		return nil
	}
	schema := func(p map[string]any) map[string]any { return p["schema"].(map[string]any) }
	m := param("/m")
	if m["style"] != "deepObject" || m["explode"] != true || m["required"] != true || m["in"] != "query" {
		t.Errorf("a map is a deepObject with explode: %v", m)
	}
	if s := schema(m); s["type"] != "object" || s["additionalProperties"].(map[string]any)["type"] != "string" || s["maxProperties"] != float64(100) || s["propertyNames"] != nil {
		t.Errorf("an object of strings, at most 100 members, any key: %v", s)
	}
	k := schema(param("/k"))
	if pn, _ := k["propertyNames"].(map[string]any); pn == nil || (pn["$ref"] == nil && pn["enum"] == nil) {
		t.Errorf("a key type with Values lists the keys: %v", k)
	}
	b := schema(param("/b"))
	if b["minProperties"] != float64(1) || b["maxProperties"] != float64(2) {
		t.Errorf("a Members bound is minProperties and maxProperties: %v", b)
	}
	o := param("/o")
	if o["required"] != false || schema(o)["additionalProperties"].(map[string]any)["type"] != "integer" {
		t.Errorf("an Opt map is optional: %v", o)
	}
}

func TestEveryMemberOfAMapSeenByTheHandlerLivesPastTheRequest(t *testing.T) {
	var kept map[string]string
	srv := startWith(t, func(app *bites.App) {
		app.Get("/m", func(_ context.Context, in MapIn) (int, error) { kept = in.Query.Filter; return len(kept), nil })
	})
	getMap(srv, "/m?page=1&filter[alpha]=one&filter[beta]=two")
	getMap(srv, "/m?page=1&filter[alpha]=XXX&filter[beta]=YYY")
	var keys []string
	for k, v := range kept {
		keys = append(keys, k+"="+v)
	}
	sort.Strings(keys)
	if strings.Join(keys, ",") != "alpha=XXX,beta=YYY" {
		t.Fatalf("%v", keys)
	}
}

var _ = bites.Opt[int]{}

type fixedFilterQuery struct {
	Name string
	Age  int
}

func (fixedFilterQuery) WireNames() map[string]string {
	return map[string]string{"Name": "filter[name]", "Age": "filter[age]"}
}

func TestKeysKnownInAdvanceWithTheirOwnTypesAreFieldsNamedWithTheBrackets(t *testing.T) {
	srv := startWith(t, func(app *bites.App) {
		app.Get("/f", func(_ context.Context, in struct{ Query fixedFilterQuery }) (fixedFilterQuery, error) {
			return in.Query, nil
		})
	})
	if r := getMap(srv, "/f?filter[name]=x&filter%5Bage%5D=3"); r.Status != 200 || r.Text() != `{"name":"x","age":3}` {
		t.Fatalf("%d %s", r.Status, r.Text())
	}
}
