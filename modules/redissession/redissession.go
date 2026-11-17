// Package redissession is a bites.OwnerSessionStore over Redis or Valkey: an application that runs
// Redis keeps its sessions there, and ends every session of a User with Sessions.RevokeOwner,
// without writing a store of its own (ADR 0093, ADR 0056).
//
// The session id is kept only as its SHA-256, so a dump of the keyspace hands out no id that a
// cookie could be forged from. A session is one hash key holding the data, the SHA-256 of its owner and
// the expiry, and each owner has a sorted set of its session hashes, so ending an owner's sessions reads one set
// and does not scan the keyspace. Each change is one Lua script, which Redis runs without running
// anything else, so a session and its owner's set never disagree for the length of a request. Several
// instances share one Redis without coordinating. The package depends on github.com/redis/go-redis/v9
// and works with Valkey as it does with Redis.
//
// A session's key carries a TTL that Redis enforces, so an expired session is deleted by Redis and
// nothing sweeps: there is no Service to register. Whether a session has expired is judged on the
// application's clock, bites.Now(ctx), never on Redis's: the expiry is kept in the session and
// compared with the application's now, and the TTL only frees the space.
//
// Every key of a Store starts with "{prefix}", a Redis Cluster hash tag, so one Store lives on one
// shard and its scripts work on a cluster. Give each Store its own Prefix to spread several over a
// cluster.
//
//	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	store := redissession.New(redissession.Options{Client: client})
//	carts := bites.NewSessions[Login](bites.SessionOptions{Keys: keys, Store: store})
package redissession

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/nevindra/bites"
	"github.com/redis/go-redis/v9"
)

// DefaultPrefix is the prefix used when Options.Prefix is empty. A session's key is
// "{bites:session}:id:" and the base64url of a SHA-256.
const DefaultPrefix = "bites:session"

// batch is how many sessions of an owner one DeleteOwner script ends, so a user with many sessions
// does not hold Redis for one long script.
const batch = 500

// touchTries is how many times Touch reads the session's owner and runs its script before it gives
// up: the script refuses to run when another call changed the owner in between.
const touchTries = 3

var (
	_ bites.OwnerSessionStore = (*Store)(nil)
)

// Options are the settings of New.
type Options struct {
	// Client is the application's client. Required. It is any redis.UniversalClient: a *redis.Client
	// for one server (Redis or Valkey), a Sentinel client from redis.NewFailoverClient, a
	// *redis.ClusterClient or a *redis.Ring. The Store never closes it.
	Client redis.UniversalClient
	// Prefix is what every key of this Store starts with, inside braces so a cluster keeps them on
	// one shard. Empty means "bites:session". It must not contain '{' or '}'. Applications that
	// share a Redis and want separate sessions use different prefixes; two Sessions types over one
	// prefix share owners (ADR 0056).
	Prefix string
}

// Store implements bites.OwnerSessionStore over Redis or Valkey. Make one with New; the zero value
// is not usable. It is safe for concurrent use, and any number of instances may share a Redis.
type Store struct {
	opt Options
	tag string // "{prefix}:"
}

// save writes a session. KEYS: the session, the owner's set (the session again when the owner is
// empty). ARGV: the owner's hash (empty for none), expiry in microseconds, TTL in milliseconds,
// data, now in microseconds, the session's hash. A TTL that is not positive deletes the session,
// which is what a Save with an expiry in the past means. The owner's set is a sorted set of session hashes scored by their expiry,
// and it is trimmed of the sessions that have expired on every save of that owner. A session that
// changed owner stays in the old owner's set until DeleteOwner or the trim finds it there: DeleteOwner
// ends only the sessions whose own owner is the one it was given.
var save = redis.NewScript(`
local ttl = tonumber(ARGV[3])
if ttl <= 0 then
	redis.call('DEL', KEYS[1])
	return 0
end
redis.call('HSET', KEYS[1], 'e', ARGV[2], 'o', ARGV[1], 'd', ARGV[4])
redis.call('PEXPIRE', KEYS[1], ttl)
if ARGV[1] ~= '' then
	redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', ARGV[5])
	redis.call('ZADD', KEYS[2], ARGV[2], ARGV[6])
	if redis.call('PTTL', KEYS[2]) < ttl then
		redis.call('PEXPIRE', KEYS[2], ttl)
	end
end
return 1
`)

// touch moves a live session's expiry and keeps its owner and data. KEYS as in save. ARGV: the owner's
// hash the caller read, expiry in microseconds, TTL in milliseconds, now in microseconds, the session's
// hash. It answers 0 when the session has nothing or has expired (it writes nothing, so it never
// creates one), -1 when the session's owner is no longer the one the caller read (the caller reads
// again, so the owner's set is the right one), and 1 when it moved the expiry.
var touch = redis.NewScript(`
local e = redis.call('HGET', KEYS[1], 'e')
if not e or tonumber(e) <= tonumber(ARGV[4]) then
	return 0
end
if redis.call('HGET', KEYS[1], 'o') ~= ARGV[1] then
	return -1
end
local ttl = tonumber(ARGV[3])
if ttl <= 0 then
	redis.call('DEL', KEYS[1])
	return 1
end
redis.call('HSET', KEYS[1], 'e', ARGV[2])
redis.call('PEXPIRE', KEYS[1], ttl)
if ARGV[1] ~= '' then
	redis.call('ZADD', KEYS[2], ARGV[2], ARGV[5])
	if redis.call('PTTL', KEYS[2]) < ttl then
		redis.call('PEXPIRE', KEYS[2], ttl)
	end
end
return 1
`)

// deleteOwner ends the sessions of an owner among a batch of its set's members. KEYS: the owner's
// set, then the session keys of the members. ARGV: the owner's hash, the key to keep (empty keeps none),
// then the members' hashes in the order of the keys. A session whose own owner is another, or that
// is gone, is a stale member: it is removed from the set and not counted. It answers how many
// sessions it deleted.
var deleteOwner = redis.NewScript(`
local n = 0
for i = 2, #KEYS do
	if KEYS[i] ~= ARGV[2] then
		if redis.call('HGET', KEYS[i], 'o') == ARGV[1] then
			redis.call('DEL', KEYS[i])
			n = n + 1
		end
		redis.call('ZREM', KEYS[1], ARGV[i + 1])
	end
end
return n
`)

// New returns a Store; it connects nothing and creates nothing. It panics, naming the line that
// called it, when Client is nil or Prefix contains a brace.
func New(o Options) *Store {
	_, file, line, _ := runtime.Caller(1)
	where := fmt.Sprintf("%s:%d", filepath.Base(file), line)
	fail := func(format string, args ...any) {
		panic(fmt.Sprintf("redissession.New: %s\n\tregistered at %s", fmt.Sprintf(format, args...), where))
	}
	if o.Client == nil {
		fail("Client is nil: give the application's client, such as redis.NewClient(&redis.Options{Addr: \"localhost:6379\"})")
	}
	if o.Prefix == "" {
		o.Prefix = DefaultPrefix
	}
	if strings.ContainsAny(o.Prefix, "{}") {
		fail("Prefix %q contains a brace: the Store puts the prefix inside braces, a Redis Cluster hash tag, so give a prefix such as %q without '{' or '}', or leave it empty for %q", o.Prefix, strings.NewReplacer("{", "", "}", "").Replace(o.Prefix), DefaultPrefix)
	}
	return &Store{opt: o, tag: "{" + o.Prefix + "}:"}
}

// hash is what an id or an owner is stored as: the id is never written to Redis.
func hash(s string) string {
	h := sha256.Sum256([]byte(s))
	return base64.RawURLEncoding.EncodeToString(h[:])
}

// idKey and ownerKey take the hashes of an id and of an owner.
func (s *Store) idKey(h string) string    { return s.tag + "id:" + h }
func (s *Store) ownerKey(h string) string { return s.tag + "owner:" + h }

// ttl is the milliseconds from now to expires, rounded up, and 0 when expires is not after now.
func ttl(expires, now time.Time) int64 {
	d := expires.Sub(now)
	if d <= 0 {
		return 0
	}
	return int64((d + time.Millisecond - 1) / time.Millisecond)
}

// Load returns the data saved for id, and false when there is none or it has expired. Whether a
// session has expired is judged on the application's clock, bites.Now(ctx), which is the same clock
// that computed the expiry and that checks the one sealed in the cookie, and never on Redis's: a
// server whose clock drifts from the application's, or an application on a test clock, does not
// disagree with the cookie. A ctx that carries no app (a test's context.Background) reads time.Now().
func (s *Store) Load(ctx context.Context, id string) ([]byte, bool, error) {
	v, err := s.opt.Client.HMGet(ctx, s.idKey(hash(id)), "e", "d").Result()
	if err != nil {
		return nil, false, fmt.Errorf("redissession: load: %w", err)
	}
	e, _ := v[0].(string)
	d, _ := v[1].(string)
	if e == "" {
		return nil, false, nil
	}
	exp, err := strconv.ParseInt(e, 10, 64)
	if err != nil {
		return nil, false, fmt.Errorf("redissession: load: the expiry of the session is %q, not a number: was the key written by another program?", e)
	}
	if exp <= bites.Now(ctx).UnixMicro() {
		return nil, false, nil
	}
	return []byte(d), true, nil
}

// Save keeps data for id until expires with no owner, replacing what id had, owner included. The
// framework calls SaveOwned, not Save, for a session type that states an owner.
func (s *Store) Save(ctx context.Context, id string, data []byte, expires time.Time) error {
	return s.SaveOwned(ctx, id, "", data, expires)
}

// SaveOwned is Save that also records owner for id, replacing the owner id had. An empty owner is
// kept as an empty owner: the session is kept and DeleteOwner never finds it. It is one script, and
// the key is given a TTL of expires minus bites.Now(ctx) so Redis deletes the session when it ends;
// an expires that is not after now keeps nothing and deletes what id had.
func (s *Store) SaveOwned(ctx context.Context, id, owner string, data []byte, expires time.Time) error {
	now := bites.Now(ctx)
	h, oh := hash(id), ""
	ownerKey := s.idKey(h)
	if owner != "" {
		oh = hash(owner)
		ownerKey = s.ownerKey(oh)
	}
	err := save.Run(ctx, s.opt.Client, []string{s.idKey(h), ownerKey},
		oh, expires.UnixMicro(), ttl(expires, now), data, now.UnixMicro(), h).Err()
	if err != nil {
		return fmt.Errorf("redissession: save: %w", err)
	}
	return nil
}

// Touch moves the expiry of id to expires and changes nothing else, owner and data included, and
// the key's TTL with it. It writes only to a session that is live on bites.Now(ctx), never creates
// one, so a session that was deleted or has expired stays gone and Touch returns false; the framework
// calls it to slide a session, so a slide that races Sessions.RevokeOwner cannot bring the session
// back. It is two round trips, one to read the session's owner and one script that moves the expiry
// only if the owner is still that (and reads again when it is not), since the owner's set is a key
// the script must be given.
func (s *Store) Touch(ctx context.Context, id string, expires time.Time) (bool, error) {
	now := bites.Now(ctx)
	h := hash(id)
	key := s.idKey(h)
	for range touchTries {
		oh, err := s.opt.Client.HGet(ctx, key, "o").Result()
		if errors.Is(err, redis.Nil) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("redissession: touch: %w", err)
		}
		ownerKey := key
		if oh != "" {
			ownerKey = s.ownerKey(oh)
		}
		r, err := touch.Run(ctx, s.opt.Client, []string{key, ownerKey}, oh, expires.UnixMicro(), ttl(expires, now), now.UnixMicro(), h).Int()
		if err != nil {
			return false, fmt.Errorf("redissession: touch: %w", err)
		}
		if r >= 0 {
			return r == 1, nil
		}
	}
	return false, fmt.Errorf("redissession: touch: the owner of the session changed %d times while it was being touched: is the same id saved by several requests at once?", touchTries)
}

// Delete forgets id. Deleting an id that has nothing is not an error. The id stays in its owner's set
// until the set is trimmed or DeleteOwner passes it, and is never counted there.
func (s *Store) Delete(ctx context.Context, id string) error {
	if err := s.opt.Client.Del(ctx, s.idKey(hash(id))).Err(); err != nil {
		return fmt.Errorf("redissession: delete: %w", err)
	}
	return nil
}

// DeleteOwner forgets every session of owner but the one whose id is keep, expired sessions that
// Redis has not yet deleted included, and returns how many it deleted. An empty keep keeps none. An
// owner with no session, and the empty owner, which names no one, are (0, nil), the empty owner
// without a call to Redis. It reads the owner's set and ends its sessions in scripts of 500, so a
// session saved for the owner after the read is not seen by this call: the later of a revoke and a
// save wins, as ADR 0056 says.
func (s *Store) DeleteOwner(ctx context.Context, owner, keep string) (int, error) {
	if owner == "" {
		return 0, nil
	}
	oh := hash(owner)
	ownerKey := s.ownerKey(oh)
	members, err := s.opt.Client.ZRange(ctx, ownerKey, 0, -1).Result()
	if err != nil {
		return 0, fmt.Errorf("redissession: delete owner: %w", err)
	}
	keepKey := ""
	if keep != "" {
		keepKey = s.idKey(hash(keep))
	}
	total := 0
	for len(members) > 0 {
		n := min(batch, len(members))
		keys := make([]string, 1, n+1)
		args := make([]any, 2, n+2)
		keys[0], args[0], args[1] = ownerKey, oh, keepKey
		for _, m := range members[:n] {
			keys = append(keys, s.idKey(m))
			args = append(args, m)
		}
		members = members[n:]
		c, err := deleteOwner.Run(ctx, s.opt.Client, keys, args...).Int()
		if err != nil {
			return total, fmt.Errorf("redissession: delete owner: %w", err)
		}
		total += c
	}
	return total, nil
}
