package bites

import (
	"bytes"
	"reflect"
	"unsafe"

	"github.com/nevindra/bites/internal/wire"
)

// mapField is what a text field that is a map in a Query adds (ADR 0042): `filter[name]=x` fills
// map[K]V, the OpenAPI deepObject style. The key is decoded by K's own text decoding and the value
// by V's, so a K with Values lists the keys that are allowed. The zero value is every other field.
type mapField struct {
	mapT, keyT, valT reflect.Type
	mapIdx           int // the index in the section's maps
	keyDec           wire.TextDecoder
	members          *wire.Members // the bound on the members: the type's Members, or MaxFields
	pairT            reflect.Type  // struct{ K; V }: one scratch for a member's key and value
	pairV            uintptr       // where V is in pairT
}

// mapRun is the state of a Query's maps for one request. It is made when the first key of a map
// arrives, so a request with no map member allocates nothing for it.
type mapRun struct {
	counts [maxMaps]int32          // members sent for each map, counted in the first pass
	bad    uint8                   // maps whose count broke the bound: their members are not stored
	pair   [maxMaps]unsafe.Pointer // the scratch key and value of each map, made at its first member
}

// mapPair offers one query key to the maps of the section. It reports whether the key belongs to
// one: its name, up to the first bracket, is a map's wire name. Such a key is never an unknown key,
// even when its brackets are wrong, since the client plainly meant the map and silence would look
// like a filter that worked. kb is the key percent-decoded, in the slab when inSlab and otherwise
// still the request head's own bytes.
func (r *textRun) mapPair(kb []byte, inSlab bool, value []byte) bool {
	i := bytes.IndexAny(kb, "[]")
	if i <= 0 {
		return false
	}
	idx, ok := r.sec.lookup(kb[:i])
	if !ok || r.sec.fields[idx].mapT == nil {
		return false
	}
	f := &r.sec.fields[idx]
	rest := kb[i:]
	inner, good := mapMember(rest)
	if r.mr == nil {
		r.mr = new(mapRun)
	}
	if r.phase == 0 {
		if good {
			r.mr.counts[f.mapIdx]++
		}
		return true
	}
	r.seen |= uint64(1) << idx
	if !good {
		r.mapMalformed(f, kb, rest)
		return true
	}
	var ks string
	if inSlab {
		ks = unsafe.String(&inner[0], len(inner))
	} else {
		ks = r.slab.Copy(inner)
	}
	r.mapMember(f, ks, value)
	return true
}

// mapMember cuts the key out of `[key]`: one name in one pair of brackets, never empty, never
// holding a bracket of its own. A nested key (`[a][b]`) is not a member.
func mapMember(rest []byte) (inner []byte, ok bool) {
	n := len(rest)
	if n < 3 || rest[0] != '[' || rest[n-1] != ']' {
		return nil, false
	}
	inner = rest[1 : n-1]
	if bytes.IndexAny(inner, "[]") >= 0 {
		return nil, false
	}
	return inner, true
}

// mapMalformed is the 400 for a key of a map written wrongly. It names the key as sent, cut to 64
// bytes (ADR 0012), and what to send instead.
func (r *textRun) mapMalformed(f *textField, kb, rest []byte) {
	if len(r.errs.List) >= maxErrors {
		r.errs.Truncated = true
		return
	}
	want := f.wire + "[key]"
	var msg string
	switch {
	case bytes.Contains(rest, []byte("][")):
		msg = "nested keys are not supported: send one key in brackets, as " + want
	case len(rest) == 2 && rest[0] == '[':
		msg = "expected a key between the brackets, as " + want
	default:
		msg = "expected the form " + want
	}
	addError(r.errs, "query."+clipKey(string(kb)), msg)
}

// mapBare is the 400 for the name of a map sent with no brackets, `?filter=x`.
func (r *textRun) mapBare(f *textField, idx int) {
	if r.phase == 0 {
		return
	}
	bit := uint64(1) << idx
	if r.seen&bit != 0 {
		return
	}
	r.seen |= bit
	addError(r.errs, f.loc, "expected the form "+f.wire+"[key]")
}

// mapMember adds one member to its map, decoding the key and the value by their types. The map is
// made at its first member, sized to the count of the first pass, and only after that count has
// been held to the bound, so a client that sends ten thousand members costs a count and one error.
func (r *textRun) mapMember(f *textField, ks string, value []byte) {
	mr := r.mr
	bit := uint8(1) << f.mapIdx
	if mr.bad&bit != 0 {
		return
	}
	m := reflect.NewAt(f.mapT, unsafe.Add(r.base, f.listOff)).Elem()
	if m.IsNil() {
		if n := int(mr.counts[f.mapIdx]); !f.members.OK(n) {
			mr.bad |= bit
			addRule(r.errs, f.loc, f.members.Message, wire.RuleMembers, int64(f.members.Min), int64(f.members.Max))
			return
		} else {
			m.Set(reflect.MakeMapWithSize(f.mapT, n))
		}
		if f.hasFlag {
			*(*bool)(unsafe.Add(r.base, f.flagOff)) = true
		}
		mr.pair[f.mapIdx] = reflect.New(f.pairT).UnsafePointer()
	}
	if len(r.errs.List) >= maxErrors {
		r.errs.Truncated = true
		return
	}
	kp := mr.pair[f.mapIdx]
	vp := unsafe.Add(kp, f.pairV)
	key := reflect.NewAt(f.keyT, kp).Elem()
	val := reflect.NewAt(f.valT, vp).Elem()
	key.SetZero()
	val.SetZero()
	if err := f.keyDec(kp, ks); err != nil {
		addText(r.errs, f.mapLoc(ks), err)
		return
	}
	v, err := r.slab.Unescape(value, true)
	if err != nil {
		addError(r.errs, f.mapLoc(ks), msgEscape)
		return
	}
	if err := f.dec(vp, v); err != nil {
		addText(r.errs, f.mapLoc(ks), err)
		return
	}
	n := m.Len()
	m.SetMapIndex(key, val)
	if m.Len() == n {
		// A key sent twice is refused, never first-wins or last-wins (ADR 0042), and said once.
		loc := f.mapLoc(ks)
		for i := range r.errs.List {
			if r.errs.List[i].Location == loc {
				return
			}
		}
		addError(r.errs, loc, msgRepeated)
	}
}

// mapLoc is where a member is in a 400: query.filter[name], the key cut to 64 bytes (ADR 0012).
func (f *textField) mapLoc(key string) string {
	return f.loc + "[" + clipKey(key) + "]"
}
