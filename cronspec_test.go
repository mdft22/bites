package bites

import (
	"strings"
	"testing"
	"time"
	_ "time/tzdata"
)

func cronZone(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Fatalf("time.LoadLocation(%q): %v", name, err)
	}
	return loc
}

func cronMustParse(t *testing.T, spec string) *cronSpec {
	t.Helper()
	s, err := parseCron(spec)
	if err != nil {
		t.Fatalf("parseCron(%q): %v", spec, err)
	}
	return s
}

// cronRunsFrom lists the next n instants of spec after from, in loc.
func cronRunsFrom(t *testing.T, spec string, loc *time.Location, from time.Time, n int) []time.Time {
	t.Helper()
	s := cronMustParse(t, spec)
	var out []time.Time
	for range n {
		next, ok := s.next(from, loc)
		if !ok {
			t.Fatalf("%q has no next time after %v", spec, from)
		}
		if !next.After(from) {
			t.Fatalf("%q: next(%v) = %v is not after it", spec, from, next)
		}
		out = append(out, next)
		from = next
	}
	return out
}

func cronShowRuns(ts []time.Time, loc *time.Location) string {
	var b strings.Builder
	for i, u := range ts {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(u.In(loc).Format("01-02 15:04 MST"))
	}
	return b.String()
}

func TestACronSpecRunsAtTheTimesItsFieldsName(t *testing.T) {
	utc := time.UTC
	at := func(s string) time.Time {
		u, err := time.ParseInLocation("2006-01-02 15:04:05", s, utc)
		if err != nil {
			t.Fatal(err)
		}
		return u
	}
	rows := []struct {
		spec  string
		after string
		want  []string
	}{
		{"*/15 * * * *", "2026-01-01 00:00:00", []string{"2026-01-01 00:15:00", "2026-01-01 00:30:00", "2026-01-01 00:45:00", "2026-01-01 01:00:00"}},
		{"*/15 * * * *", "2026-01-01 00:14:59", []string{"2026-01-01 00:15:00"}},
		{"30 2 * * *", "2026-01-01 02:30:00", []string{"2026-01-02 02:30:00", "2026-01-03 02:30:00"}},
		{"0 9 * * mon-fri", "2026-01-02 09:00:00", []string{"2026-01-05 09:00:00", "2026-01-06 09:00:00"}}, // 2026-01-02 is a Friday
		{"0 0 1 jan *", "2026-01-01 00:00:00", []string{"2027-01-01 00:00:00", "2028-01-01 00:00:00"}},
		{"0 0 29 2 *", "2026-01-01 00:00:00", []string{"2028-02-29 00:00:00", "2032-02-29 00:00:00"}},
		{"10-40/10 8,20 * * *", "2026-01-01 00:00:00", []string{"2026-01-01 08:10:00", "2026-01-01 08:20:00", "2026-01-01 08:30:00", "2026-01-01 08:40:00", "2026-01-01 20:10:00"}},
		{"0 0 * * 7", "2026-01-01 00:00:00", []string{"2026-01-04 00:00:00", "2026-01-11 00:00:00"}}, // 7 is Sunday
		{"0 0 * * 0", "2026-01-01 00:00:00", []string{"2026-01-04 00:00:00"}},
		{"0 0 13 * fri", "2026-02-01 00:00:00", []string{"2026-02-06 00:00:00", "2026-02-13 00:00:00", "2026-02-20 00:00:00", "2026-02-27 00:00:00", "2026-03-06 00:00:00", "2026-03-13 00:00:00"}}, // the 13th or a Friday
		{"0 0 */2 * mon", "2026-02-01 00:00:00", []string{"2026-02-09 00:00:00", "2026-02-23 00:00:00"}},                                                                                            // '*' first: the day must match both (odd days that are Mondays)
		{"@daily", "2026-01-01 05:00:00", []string{"2026-01-02 00:00:00"}},
		{"@midnight", "2026-01-01 05:00:00", []string{"2026-01-02 00:00:00"}},
		{"@hourly", "2026-01-01 05:10:00", []string{"2026-01-01 06:00:00", "2026-01-01 07:00:00"}},
		{"@weekly", "2026-01-01 05:10:00", []string{"2026-01-04 00:00:00"}},
		{"@monthly", "2026-01-31 05:10:00", []string{"2026-02-01 00:00:00", "2026-03-01 00:00:00"}},
		{"@yearly", "2026-01-31 05:10:00", []string{"2027-01-01 00:00:00"}},
		{"@annually", "2026-01-31 05:10:00", []string{"2027-01-01 00:00:00"}},
		{"0 12 * * *", "2026-01-01 12:00:00", []string{"2026-01-02 12:00:00"}}, // strictly after
		{"  5   4  *  *  *  ", "2026-01-01 00:00:00", []string{"2026-01-01 04:05:00"}},
		{"0 0 1 MAR,sep *", "2026-01-01 00:00:00", []string{"2026-03-01 00:00:00", "2026-09-01 00:00:00"}},
		{"0 22-2 * * *", "", nil}, // a range that runs backwards is refused, below
	}
	for _, r := range rows {
		if r.want == nil {
			if _, err := parseCron(r.spec); err == nil {
				t.Errorf("parseCron(%q) is nil; a range that runs backwards must be refused", r.spec)
			}
			continue
		}
		var want []time.Time
		for _, w := range r.want {
			want = append(want, at(w))
		}
		got := cronRunsFrom(t, r.spec, utc, at(r.after), len(want))
		for i := range want {
			if !got[i].Equal(want[i]) {
				t.Errorf("%q after %s: runs %s; want %s", r.spec, r.after, cronShowRuns(got, utc), cronShowRuns(want, utc))
				break
			}
		}
	}
}

func TestACronSpecIsReadInTheZoneItIsGiven(t *testing.T) {
	jkt := cronZone(t, "Asia/Jakarta") // UTC+7, no daylight saving
	from := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	got := cronRunsFrom(t, "0 9 * * *", jkt, from, 2)
	want := []time.Time{time.Date(2026, 5, 1, 2, 0, 0, 0, time.UTC), time.Date(2026, 5, 2, 2, 0, 0, 0, time.UTC)}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Fatalf("09:00 in Jakarta runs %s UTC; want %s", cronShowRuns(got, time.UTC), cronShowRuns(want, time.UTC))
		}
	}
}

// The daylight-saving rule of ADR 0092, one row each.
func TestATimeOfDayOnTheDayTheClocksGoForwardRunsOnceAtTheFirstInstantAfterTheGap(t *testing.T) {
	ny := cronZone(t, "America/New_York") // 2026-03-08 02:00 EST becomes 03:00 EDT
	from := time.Date(2026, 3, 7, 12, 0, 0, 0, ny)
	got := cronRunsFrom(t, "30 2 * * *", ny, from, 3)
	want := []time.Time{
		time.Date(2026, 3, 8, 3, 0, 0, 0, ny), // 02:30 does not exist: 03:00 EDT, 07:00 UTC
		time.Date(2026, 3, 9, 2, 30, 0, 0, ny),
		time.Date(2026, 3, 10, 2, 30, 0, 0, ny),
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Fatalf("30 2 * * * around the spring change runs %s; want %s", cronShowRuns(got, ny), cronShowRuns(want, ny))
		}
	}
	if got[0].UTC().Hour() != 7 {
		t.Fatalf("the skipped 02:30 ran at %v UTC; want 07:00, the instant 03:00 EDT begins", got[0].UTC())
	}
}

func TestTwoTimesInTheSkippedHourRunOnceNotTwice(t *testing.T) {
	ny := cronZone(t, "America/New_York")
	from := time.Date(2026, 3, 7, 12, 0, 0, 0, ny)
	got := cronRunsFrom(t, "0,30 2 * * *", ny, from, 3)
	want := []time.Time{time.Date(2026, 3, 8, 3, 0, 0, 0, ny), time.Date(2026, 3, 9, 2, 0, 0, 0, ny), time.Date(2026, 3, 9, 2, 30, 0, 0, ny)}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Fatalf("0,30 2 * * * around the spring change runs %s; want %s", cronShowRuns(got, ny), cronShowRuns(want, ny))
		}
	}
}

func TestATimeOfDayOnTheDayTheClocksGoBackRunsOnceAtItsFirstOccurrence(t *testing.T) {
	ny := cronZone(t, "America/New_York") // 2026-11-01 02:00 EDT becomes 01:00 EST: 01:30 happens twice
	from := time.Date(2026, 10, 31, 12, 0, 0, 0, ny)
	got := cronRunsFrom(t, "30 1 * * *", ny, from, 3)
	want := []time.Time{
		time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC), // 01:30 EDT, the first of the two
		time.Date(2026, 11, 2, 1, 30, 0, 0, ny),
		time.Date(2026, 11, 3, 1, 30, 0, 0, ny),
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Fatalf("30 1 * * * around the autumn change runs %s; want %s", cronShowRuns(got, ny), cronShowRuns(want, ny))
		}
	}
	// The run made, a second clock reading of 01:30 (EST) is after it and is not run again.
	second := time.Date(2026, 11, 1, 6, 30, 0, 0, time.UTC)
	next, _ := cronMustParse(t, "30 1 * * *").next(second.Add(-time.Minute), ny)
	if !next.Equal(time.Date(2026, 11, 2, 1, 30, 0, 0, ny)) {
		t.Fatalf("after the first 01:30 the next run is %v; want the next day, the repeated 01:30 does not run again", next.In(ny))
	}
}

func TestAScheduleThatRunsEveryHourSkipsTheHourThatDoesNotExistAndRepeatsTheOneThatDoes(t *testing.T) {
	ny := cronZone(t, "America/New_York")
	forward := cronRunsFrom(t, "@hourly", ny, time.Date(2026, 3, 8, 0, 30, 0, 0, ny), 4)
	if got, want := cronShowRuns(forward, ny), "03-08 01:00 EST, 03-08 03:00 EDT, 03-08 04:00 EDT, 03-08 05:00 EDT"; got != want {
		t.Fatalf("@hourly over the spring change runs %s; want %s", got, want)
	}
	back := cronRunsFrom(t, "*/30 * * * *", ny, time.Date(2026, 11, 1, 0, 15, 0, 0, ny), 8)
	if got, want := cronShowRuns(back, ny), "11-01 00:30 EDT, 11-01 01:00 EDT, 11-01 01:30 EDT, 11-01 01:00 EST, 11-01 01:30 EST, 11-01 02:00 EST, 11-01 02:30 EST, 11-01 03:00 EST"; got != want {
		t.Fatalf("*/30 * * * * over the autumn change runs %s; want %s", got, want)
	}
}

func TestADaylightSavingOfHalfAnHourIsFoundToo(t *testing.T) {
	lh := cronZone(t, "Australia/Lord_Howe") // 2026-10-04 02:00 +10:30 becomes 02:30 +11:00
	got := cronRunsFrom(t, "15 2 * * *", lh, time.Date(2026, 10, 3, 12, 0, 0, 0, lh), 2)
	want := []time.Time{time.Date(2026, 10, 4, 2, 30, 0, 0, lh), time.Date(2026, 10, 5, 2, 15, 0, 0, lh)}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Fatalf("15 2 * * * on Lord Howe's change runs %s; want %s", cronShowRuns(got, lh), cronShowRuns(want, lh))
		}
	}
}

func TestATimeOfDayRunsExactlyOncePerLocalDateThroughAYearOfDaylightSaving(t *testing.T) {
	zones := []struct {
		name string
		year int
	}{
		{"America/New_York", 2026}, {"Europe/London", 2026}, {"Australia/Sydney", 2026}, {"Australia/Lord_Howe", 2026},
		{"America/Sao_Paulo", 2018}, // clocks went forward at midnight: 00:00 did not exist on 2018-11-04
		{"Asia/Jakarta", 2026},
	}
	for _, z := range zones {
		loc := cronZone(t, z.name)
		for _, spec := range []string{"0 0 * * *", "30 0 * * *", "30 1 * * *", "30 2 * * *", "0 3 * * *", "59 23 * * *", "0 2,3 * * *"} {
			s := cronMustParse(t, spec)
			perDate := map[string]int{}
			from := time.Date(z.year, 1, 1, 0, 0, 0, 0, loc).Add(-time.Minute)
			end := time.Date(z.year+1, 1, 1, 0, 0, 0, 0, loc)
			prev := from
			for {
				next, ok := s.next(from, loc)
				if !ok || !next.Before(end) {
					break
				}
				if !next.After(prev) {
					t.Fatalf("%s %q: %v does not follow %v", z.name, spec, next.In(loc), prev.In(loc))
				}
				perDate[next.In(loc).Format("2006-01-02")]++
				prev, from = next, next
			}
			per := strings.Count(spec, ",") + 1
			if strings.HasPrefix(spec, "0 2,3") {
				per = 2
			}
			for d := time.Date(z.year, 1, 1, 12, 0, 0, 0, time.UTC); d.Year() == z.year; d = d.AddDate(0, 0, 1) {
				key := d.Format("2006-01-02")
				got := perDate[key]
				// The one day both 02:00 and 03:00 name the same instant, a gap makes 2,3 one run.
				if got != per && !(spec == "0 2,3 * * *" && got == 1) {
					t.Fatalf("%s %q ran %d times on %s; want %d", z.name, spec, got, key, per)
				}
			}
		}
	}
}

func TestAClockThatReadsEveryInstantOnlyEverMovesTheNextTimeForward(t *testing.T) {
	ny := cronZone(t, "America/New_York")
	for _, spec := range []string{"*/7 * * * *", "@hourly", "13 * * * *", "0 */2 * * *", "30 2 * * *"} {
		s := cronMustParse(t, spec)
		prev := time.Time{}
		// Every minute across both changes: next is after its argument and never earlier than the last.
		for u := time.Date(2026, 3, 7, 0, 0, 0, 0, ny); u.Before(time.Date(2026, 3, 10, 0, 0, 0, 0, ny)); u = u.Add(11 * time.Minute) {
			n, ok := s.next(u, ny)
			if !ok || !n.After(u) {
				t.Fatalf("%q: next(%v) = %v", spec, u.In(ny), n.In(ny))
			}
			if n.Before(prev) {
				t.Fatalf("%q: next(%v) = %v is before the answer for an earlier time, %v", spec, u.In(ny), n.In(ny), prev.In(ny))
			}
			prev = n
		}
	}
}

func TestACronSpecThatIsNotASchedulePanicsWithTheFieldAndWhatToWrite(t *testing.T) {
	rows := []struct{ spec, want string }{
		{"", "the spec is empty"},
		{"   ", "the spec is empty"},
		{"* * * *", "it has 4 fields, want 5 (minute hour day-of-month month day-of-week)"},
		{"0 0 * * * *", "a sixth field for seconds is not supported"},
		{"60 * * * *", `minute "60": 60 is outside 0-59`},
		{"* 24 * * *", `hour "24": 24 is outside 0-23`},
		{"* * 0 * *", `day of month "0": 0 is outside 1-31`},
		{"* * 32 * *", `day of month "32": 32 is outside 1-31`},
		{"* * * 13 *", `month "13": 13 is outside 1-12`},
		{"* * * 0 *", `month "0": 0 is outside 1-12`},
		{"* * * * 8", `day of week "8": 8 is outside 0-7`},
		{"* * * foo *", `month "foo": "foo" is not a number or a three-letter name`},
		{"* * * * sunday", `day of week "sunday": "sunday" is not a number or a three-letter name`},
		{"* * * july *", `month "july": "july" is not a number or a three-letter name`},
		{"a * * * *", `minute "a": "a" is not a number`},
		{"-1 * * * *", `minute "-1": an empty number`},
		{"1,,2 * * * *", `minute "1,,2": an empty item in the list`},
		{"1, * * * *", `minute "1,": an empty item in the list`},
		{"*/0 * * * *", `minute "*/0": the step: a step of 0 does nothing`},
		{"*/x * * * *", `minute "*/x": the step: "x" is not a number`},
		{"*/ * * * *", `minute "*/": the step: an empty number`},
		{"5/15 * * * *", `minute "5/15": a step needs a range or *: write 5-59/15`},
		{"22-2 * * * *", `minute "22-2": the range 22-2 runs backwards`},
		{"* 22-2 * * *", `hour "22-2": the range 22-2 runs backwards: write it as two ranges, such as 22-23,0-2`},
		{"1-70 * * * *", `minute "1-70": the range 1-70 is outside 0-59`},
		{"? * * * *", `minute "?": ? is not supported: write * for any value`},
		{"* * L * *", `day of month "L": "L" is not supported`},
		{"* * 5W * *", `day of month "5W": "5W" is not supported`},
		{"* * * * 1#2", `day of week "1#2": "1#2" is not supported`},
		{"@every 5m", `@every is not a cron spec: an interval is bites.Every(name, interval, fn)`},
		{"@reboot", `@reboot is not supported`},
		{"@daily 5", `"@daily 5" is not a shortcut`},
		{"@fortnightly", `"@fortnightly" is not a shortcut: the shortcuts are @yearly, @annually, @monthly, @weekly, @daily, @midnight and @hourly`},
		{"0 0 31 2 *", `it never matches a date: day 31 is not in month 2`},
		{"0 0 31 4,6,9,11 *", `it never matches a date`},
	}
	for _, r := range rows {
		_, err := parseCron(r.spec)
		if err == nil {
			t.Errorf("parseCron(%q) is nil; want an error with %q", r.spec, r.want)
			continue
		}
		if !strings.Contains(err.Error(), r.want) {
			t.Errorf("parseCron(%q) = %q; want it to contain %q", r.spec, err, r.want)
		}
	}
}
