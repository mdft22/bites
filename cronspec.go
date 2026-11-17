package bites

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// cronSpec is a parsed cron expression (ADR 0092): five fields as sets of bits, and the two
// facts the daylight-saving rule needs. It holds no time zone, which Cron gives separately.
type cronSpec struct {
	min    uint64 // bit n: minute n, 0 to 59
	hour   uint32 // bit n: hour n, 0 to 23
	dom    uint32 // bit n: day of month n, 1 to 31
	month  uint16 // bit n: month n, 1 to 12
	dow    uint8  // bit n: day of week n, 0 (Sunday) to 6
	domAny bool   // the day-of-month field starts with '*'
	dowAny bool   // the day-of-week field starts with '*'
	// everyHour is true when every one of the 24 hours is set. Such a schedule follows the
	// instants on the clock (a repeated hour runs twice, a skipped one not at all); one that
	// names hours is a time of day, run once however the zone's offset moves (cronNext).
	everyHour bool
}

const allHours = 1<<24 - 1

var cronShortcuts = map[string]string{
	"@yearly":   "0 0 1 1 *",
	"@annually": "0 0 1 1 *",
	"@monthly":  "0 0 1 * *",
	"@weekly":   "0 0 * * 0",
	"@daily":    "0 0 * * *",
	"@midnight": "0 0 * * *",
	"@hourly":   "0 * * * *",
}

var cronMonthNames = []string{"jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"}
var cronDayNames = []string{"sun", "mon", "tue", "wed", "thu", "fri", "sat"}

// parseCron parses spec. The error says which field is wrong and what to write instead; Cron puts
// the job's name and the spec before it.
func parseCron(spec string) (*cronSpec, error) {
	text := strings.TrimSpace(spec)
	if text == "" {
		return nil, fmt.Errorf("the spec is empty: give five fields (minute hour day-of-month month day-of-week) such as \"30 2 * * *\", or a shortcut such as \"@daily\"")
	}
	if strings.HasPrefix(text, "@") {
		word, rest, _ := strings.Cut(text, " ")
		switch {
		case word == "@every":
			return nil, fmt.Errorf("@every is not a cron spec: an interval is bites.Every(name, interval, fn), which has no clock time to line up and so cannot share one run between instances")
		case word == "@reboot":
			return nil, fmt.Errorf("@reboot is not supported: a job at start is bites.EveryOptions{AtStart: true} on bites.Every, or a bites.Service Start")
		}
		expanded, ok := cronShortcuts[strings.ToLower(word)]
		if !ok || strings.TrimSpace(rest) != "" {
			return nil, fmt.Errorf("%q is not a shortcut: the shortcuts are @yearly, @annually, @monthly, @weekly, @daily, @midnight and @hourly, each alone", text)
		}
		text = expanded
	}
	fields := strings.Fields(text)
	if len(fields) != 5 {
		extra := ""
		if len(fields) == 6 {
			extra = ": a sixth field for seconds is not supported, the smallest step is a minute; use bites.Every for a shorter interval"
		}
		return nil, fmt.Errorf("it has %d fields, want 5 (minute hour day-of-month month day-of-week)%s", len(fields), extra)
	}
	s := &cronSpec{}
	var err error
	var bits uint64
	if bits, err = parseCronField(fields[0], "minute", 0, 59, nil); err != nil {
		return nil, err
	}
	s.min = bits
	if bits, err = parseCronField(fields[1], "hour", 0, 23, nil); err != nil {
		return nil, err
	}
	s.hour = uint32(bits)
	if bits, err = parseCronField(fields[2], "day of month", 1, 31, nil); err != nil {
		return nil, err
	}
	s.dom = uint32(bits)
	if bits, err = parseCronField(fields[3], "month", 1, 12, cronMonthNames); err != nil {
		return nil, err
	}
	s.month = uint16(bits)
	if bits, err = parseCronField(fields[4], "day of week", 0, 7, cronDayNames); err != nil {
		return nil, err
	}
	if bits&(1<<7) != 0 { // 7 is Sunday too
		bits = bits&^(1<<7) | 1
	}
	s.dow = uint8(bits)
	s.domAny = strings.HasPrefix(fields[2], "*")
	s.dowAny = strings.HasPrefix(fields[4], "*")
	s.everyHour = s.hour == allHours
	// A date that never comes (the 31st of February) is a mistake, found here and not in a year.
	if _, ok := s.nextWall(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)); !ok {
		return nil, fmt.Errorf("it never matches a date: day %s is not in month %s, so no day of any year qualifies; check the day-of-month and month fields", fields[2], fields[3])
	}
	return s, nil
}

// parseCronField parses one field into bits lo..hi. names, when not nil, are the three-letter
// names that stand for lo, lo+1 and so on (months from 1, days from 0).
func parseCronField(field, label string, lo, hi int, names []string) (uint64, error) {
	bad := func(format string, args ...any) (uint64, error) {
		return 0, fmt.Errorf("%s %q: %s", label, field, fmt.Sprintf(format, args...))
	}
	number := func(word string) (int, error) {
		if word == "" {
			return 0, fmt.Errorf("an empty number")
		}
		if names != nil {
			for i, n := range names {
				if strings.EqualFold(word, n) {
					return lo + i, nil
				}
			}
		}
		for _, r := range word {
			if r < '0' || r > '9' {
				rest := strings.TrimLeft(word, "0123456789")
				switch {
				case word == "?":
					return 0, fmt.Errorf("? is not supported: write * for any value")
				case rest == "L" || rest == "W" || rest == "LW" || strings.Contains(word, "#"):
					return 0, fmt.Errorf("%q is not supported: the last day, the nearest weekday and the nth weekday are not part of the syntax", word)
				case names != nil:
					return 0, fmt.Errorf("%q is not a number or a three-letter name such as %q", word, names[1])
				}
				return 0, fmt.Errorf("%q is not a number", word)
			}
		}
		n, err := strconv.Atoi(word)
		if err != nil {
			return 0, fmt.Errorf("%q is too large", word)
		}
		return n, nil
	}
	var bits uint64
	for _, part := range strings.Split(field, ",") {
		if part == "" {
			return bad("an empty item in the list: remove the extra comma")
		}
		rng, stepText, hasStep := strings.Cut(part, "/")
		step := 1
		if hasStep {
			n, err := number(stepText)
			if err != nil || n < 1 {
				if err == nil {
					err = fmt.Errorf("a step of %d does nothing: give 1 or more", n)
				}
				return bad("the step: %v", err)
			}
			step = n
		}
		from, to := lo, hi
		switch {
		case rng == "*":
			if label == "day of week" {
				to = 6 // */n over the week is 0 to 6, not 7
			}
		case strings.Contains(rng, "-"):
			a, b, _ := strings.Cut(rng, "-")
			x, err := number(a)
			if err != nil {
				return bad("%v", err)
			}
			y, err := number(b)
			if err != nil {
				return bad("%v", err)
			}
			if x < lo || x > hi || y < lo || y > hi {
				return bad("the range %s is outside %d-%d", rng, lo, hi)
			}
			if x > y {
				return bad("the range %s runs backwards: write it as two ranges, such as %d-%d,%d-%d", rng, x, hi, lo, y)
			}
			from, to = x, y
		default:
			n, err := number(rng)
			if err != nil {
				return bad("%v", err)
			}
			if n < lo || n > hi {
				return bad("%d is outside %d-%d", n, lo, hi)
			}
			if hasStep {
				return bad("a step needs a range or *: write %d-%d/%s", n, hi, stepText)
			}
			from, to = n, n
		}
		for v := from; v <= to; v += step {
			bits |= 1 << v
		}
	}
	return bits, nil
}

// dayMatches is the day rule of cron: when both the day of month and the day of week name days
// (neither starts with '*'), a day matches either; otherwise it must match both fields.
func (s *cronSpec) dayMatches(t time.Time) bool {
	dom := s.dom&(1<<t.Day()) != 0
	dow := s.dow&(1<<int(t.Weekday())) != 0
	if !s.domAny && !s.dowAny {
		return dom || dow
	}
	return dom && dow
}

// nextWall is the first wall-clock minute after w that the fields name, ignoring the time zone:
// w and the result are calendar readings carried in UTC. false when none comes in nine years.
func (s *cronSpec) nextWall(w time.Time) (time.Time, bool) {
	t := time.Date(w.Year(), w.Month(), w.Day(), w.Hour(), w.Minute()+1, 0, 0, time.UTC)
	limit := t.AddDate(9, 0, 0)
	for t.Before(limit) {
		switch {
		case s.month&(1<<int(t.Month())) == 0:
			t = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, time.UTC)
		case !s.dayMatches(t):
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, time.UTC)
		case s.hour&(1<<t.Hour()) == 0:
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, time.UTC)
		case s.min&(1<<t.Minute()) == 0:
			t = t.Add(time.Minute)
		default:
			return t, true
		}
	}
	return time.Time{}, false
}

// wall is the calendar reading of instant u in loc, to the minute, carried in UTC.
func wall(u time.Time, loc *time.Location) time.Time {
	l := u.In(loc)
	return time.Date(l.Year(), l.Month(), l.Day(), l.Hour(), l.Minute(), 0, 0, time.UTC)
}

func offsetAt(u time.Time, loc *time.Location) int {
	_, off := u.In(loc).Zone()
	return off
}

// cronNext is the first instant after after at which the schedule runs in loc, or false when
// none comes. This is the one place the rule for daylight saving lives (ADR 0092):
//
//   - A schedule that names hours is a time of day. A time that does not exist that day (the
//     hour skipped when the clocks go forward) runs once, at the first instant after the gap. A
//     time that occurs twice (the hour repeated when they go back) runs once, at its first
//     occurrence.
//   - A schedule that runs every hour follows the clock: a time that does not exist is not run,
//     and one that occurs twice runs twice.
func (s *cronSpec) next(after time.Time, loc *time.Location) (time.Time, bool) {
	if s.everyHour {
		return s.nextByInstant(after, loc)
	}
	w := wall(after, loc)
	for range 64 {
		var ok bool
		if w, ok = s.nextWall(w); !ok {
			return time.Time{}, false
		}
		if u := resolveWall(w, loc); u.After(after) {
			return u, true
		}
	}
	return time.Time{}, false
}

// resolveWall maps a wall-clock minute to the one instant a time of day runs at: the only one
// when it occurs once, the first when it occurs twice, and the first instant after the gap when
// it does not exist.
func resolveWall(w time.Time, loc *time.Location) time.Time {
	before, after := w.Add(-36*time.Hour), w.Add(36*time.Hour)
	offA, offB := offsetAt(before, loc), offsetAt(after, loc)
	var first time.Time
	for _, off := range [2]int{offA, offB} {
		u := w.Add(-time.Duration(off) * time.Second)
		if !wall(u, loc).Equal(w) {
			continue
		}
		if first.IsZero() || u.Before(first) {
			first = u
		}
	}
	if !first.IsZero() {
		return first
	}
	if offA == offB { // no gap and no match cannot happen; fall back to the nearest reading
		return w.Add(-time.Duration(offA) * time.Second)
	}
	return firstWithOffset(before, after, offB, loc)
}

// firstWithOffset is the first second in (lo, hi] at which loc's offset is off, given that it is
// not at lo and is at hi.
func firstWithOffset(lo, hi time.Time, off int, loc *time.Location) time.Time {
	a, b := lo.Unix(), hi.Unix()
	for b-a > 1 {
		m := a + (b-a)/2
		if offsetAt(time.Unix(m, 0), loc) == off {
			b = m
		} else {
			a = m
		}
	}
	return time.Unix(b, 0).UTC()
}

// nextByInstant finds the next instant a schedule that runs every hour is due, following the
// clock through the zone's changes of offset.
func (s *cronSpec) nextByInstant(after time.Time, loc *time.Location) (time.Time, bool) {
	cur, inclusive := after, false
	for range 4096 {
		off := offsetAt(cur, loc)
		w := wall(cur, loc)
		var ok bool
		if inclusive && cur.In(loc).Second() == 0 && cur.Nanosecond() == 0 { // a change of offset lands on a whole minute
			w, ok = s.nextWall(w.Add(-time.Minute))
		} else {
			w, ok = s.nextWall(w)
		}
		if !ok {
			return time.Time{}, false
		}
		u := w.Add(-time.Duration(off) * time.Second)
		t, moved := firstOffsetChange(cur, u, off, loc)
		if !moved {
			return u, true
		}
		cur, inclusive = t, true
	}
	return time.Time{}, false
}

// firstOffsetChange is the first instant in (from, to] at which loc's offset is not off, found by
// looking once a day and then halving, so a zone's change is never stepped over.
func firstOffsetChange(from, to time.Time, off int, loc *time.Location) (time.Time, bool) {
	prev := from
	for probe := from.Add(24 * time.Hour); ; probe = probe.Add(24 * time.Hour) {
		if probe.After(to) {
			probe = to
		}
		if offsetAt(probe, loc) != off {
			return firstWithOffset(prev, probe, offsetAt(probe, loc), loc), true
		}
		if !probe.Before(to) {
			return time.Time{}, false
		}
		prev = probe
	}
}
