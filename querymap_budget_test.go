package bites

import (
	"context"
	"strconv"
	"strings"
	"testing"
)

type budgetMapIn struct {
	Query struct{ Filter map[string]string }
}

type budgetMapIntIn struct {
	Query struct{ Filter map[string]int }
}

func TestAMapInTheQueryCostsAFixedNumberOfAllocationsWhateverItsSize(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector changes allocation counts")
	}
	get := func(target string) []byte { return []byte("GET " + target + " HTTP/1.1\r\nHost: x\r\n\r\n") }
	run := func(members int) float64 {
		var q strings.Builder
		for i := 0; i < members; i++ {
			if i > 0 {
				q.WriteByte('&')
			}
			q.WriteString("filter[k" + strconv.Itoa(i) + "]=v" + strconv.Itoa(i))
		}
		return keepAlive(t, memRig(t, func(a *App) {
			a.Get("/x", func(_ context.Context, in budgetMapIn) (budgetOut, error) {
				return budgetOut{Age: len(in.Query.Filter)}, nil
			})
		}), get("/x?"+q.String()), "HTTP/1.1 200 OK")
	}
	small, eight, nine, hundred := run(1), run(8), run(9), run(100)
	ints := keepAlive(t, memRig(t, func(a *App) {
		a.Get("/x", func(_ context.Context, in budgetMapIntIn) (budgetOut, error) {
			return budgetOut{Age: len(in.Query.Filter)}, nil
		})
	}), get("/x?filter[a]=1&filter[b]=2&filter[c]=3"), "HTTP/1.1 200 OK")
	t.Logf("allocations per request: map[string]string of 1 member %v, 8 members %v, 9 members %v, 100 members %v; map[string]int of 3 %v", small, eight, nine, hundred, ints)
	// The slab and the context, then the run's state, the scratch for a member, the map and its table.
	if small != 6 || eight != 6 || ints != 6 || nine != 8 || hundred != 8 {
		t.Fatalf("a map of up to 8 members costs 6 allocations and one of 9 to 100 costs 8, whatever its members hold: got %v, %v, %v, %v and %v", small, eight, ints, nine, hundred)
	}
}
