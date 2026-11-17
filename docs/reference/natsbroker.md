# natsbroker

**The module that carries a Topic between the instances of an application over core NATS, with delivery at most once, a gap signal, a payload limit that is an error and a name that is its subject.**

**Guide:** [WebSockets across several instances](../guide/websockets.md#across-several-instances) · **Design:** [WebSockets](../design/websocket.md)

`github.com/nevindra/bites/modules/natsbroker` is a module of its own ([ADR 0070](../adr/0070-optional-features-are-modules-under-modules-each-with-its-own-go-mod.md)), so the core carries none of it. It depends on `github.com/nats-io/nats.go` at v1.53.1. Its names are not checked by the documentation tests, which read the root, `bitetest` and `stdhttp`; its own tests are run with `cd modules/natsbroker && go test ./...`, and the tests that need a server run when `BITES_NATS_URL` names one (for example `nats://127.0.0.1:54222` for a throwaway `docker run -p 54222:4222 nats:2`) and are skipped, with a message naming it, otherwise. The decision is [ADR 0094](../adr/0094-a-nats-broker-is-a-module-with-at-most-once-delivery-and-a-name-is-its-subject.md).

**Delivery is at most once.** A message reaches each instance that is subscribed and connected when the server routes it, once, and no one else: NATS core keeps nothing for an instance that is down and drops messages for one that reads too slowly. The Broker tells the Topics when it knows it lost some, through their `gap` (a Topic's `OnGap`): when the connection dropped and came back, and when NATS dropped messages because the deliveries ran behind. The application keeps a place to read what was missed (a history route), as it does with [`pgnotify`](./pgnotify.md). A `Publish` that returns nil says the server has the message, not that an instance heard it.

**A Topic's name is its subject under a prefix.** `room.7` under the default `Prefix` is the subject `bites.room.7`, unchanged, so `nats sub "bites.>"` shows the traffic. One subscription to `Prefix.>` carries every name of an instance, and a name with no Topic listening is dropped on arrival, so a Topic that starts listening hears the next message with no round trip to wait for.

```go
broker := natsbroker.New(natsbroker.Options{URL: "nats://127.0.0.1:4222", Prefix: "myapp"})
go broker.Run(ctx)
defer broker.Close()
room := bites.NewTopic[Frame]("room.7", broker, bites.TopicOptions{})
```

### `natsbroker.New`

```go
func New(o Options) *Broker
```

New returns a Broker and connects nothing until `Run`. It panics, `natsbroker.New: <reason>` and `registered at file.go:N` on the next line, when `URL` is empty, has no scheme or one other than `nats`, `tls`, `ws` and `wss`, or has no host; when `Prefix` is over 64 bytes, has a wildcard (`*`, `>`), a space or control character, an empty part, a leading `$`, or is `_INBOX` or begins with `_INBOX.`; when a duration is negative; or when `MaxBackoff` is below `MinBackoff`.

### `natsbroker.Options`

```go
type Options struct {
	URL            string
	Prefix         string
	ConnectOptions []nats.Option
	Log            *slog.Logger
	OnState        func(listening bool)
	MinBackoff     time.Duration
	MaxBackoff     time.Duration
	PublishTimeout time.Duration
	KeepAlive      time.Duration
}
```

`URL` is the NATS server, such as `nats://127.0.0.1:4222`, or several servers of a cluster separated by commas; user and password or a token in the URL are read by the client. It is required. `Prefix` is put before every Topic name to make the subject, `Prefix + "." + name`: instances that share Topics use the same one, applications that share a server use different ones, so a subject permission `bites.>` fits one application. Empty means `bites`. `ConnectOptions` are `nats.Option`s applied before the Broker's own, for what the Broker does not decide: TLS (`nats.RootCAs`, `nats.ClientCert`), credentials (`nats.UserCredentials`, `nats.Nkey`, `nats.UserInfo`, `nats.Token`) and `nats.Name`. The Broker sets reconnection (forever, with `MinBackoff` and `MaxBackoff`), publish buffering (off), echo (on, so it hears its own publishes), the ping interval and every handler after yours, so an option of yours for those is replaced. `Log` is `slog.Default()` when nil. `OnState` is called when the connection becomes live (true) and when it is lost (false), on a goroutine of the NATS client, one call at a time and in order: it is how an application fails its readiness, `app.SetReady`, while it cannot hear; it must not block. The reconnect pause starts at `MinBackoff` (100 ms), doubles on each failed attempt up to `MaxBackoff` (10 s) and has 20% jitter. `PublishTimeout` (5 s) bounds one Publish, the round trip that confirms the server has the message. `KeepAlive` (30 s) is how often an idle connection is pinged, so one dropped without a reset is found in at most three periods.

### `natsbroker.Broker`

```go
type Broker struct { /* unexported */ }
```

Broker implements [`bites.Broker`](./websocket.md#broker). Make one with `New`; the zero value is not usable. It is safe for concurrent use.

### `natsbroker.Broker.Run`

```go
func (b *Broker) Run(ctx context.Context) error
```

Run connects to NATS and keeps the connection until `ctx` is done or `Close` is called, and then returns nil. A server that cannot be reached, at the start or later, is retried forever; each failure is logged at Warn with the error, the attempt and `retry_in`, each connection at Info. `Run` does not wait for the first connection: a program that must not serve before it hears waits for `Listening` or fails its readiness from `OnState`. When the connection is live again after it dropped, the subscriptions that were live, and those made while it was down or before the first connection, are told through their `gap` once. `Run` returns an error when called twice, when `ConnectOptions` are invalid, and when the client gives up for good; `ErrClosed` when called after `Close`. Run it on its own goroutine; under a lifecycle Service it is `Start: go broker.Run(ctx)` and `Stop: broker.Close` ([ADR 0081](../adr/0081-a-topic-reaches-other-instances-through-a-broker-and-postgres-is-a-module.md)).

### `natsbroker.Broker.Close`

```go
func (b *Broker) Close() error
```

Close stops `Run`, waits for it to return and closes the connection. `Publish` returns `ErrClosed` afterwards and `Subscribe` delivers nothing. Safe to call more than once; it returns nil.

### `natsbroker.Broker.Listening`

```go
func (b *Broker) Listening() bool
```

Listening reports whether the connection to a NATS server is live and the subscription to the prefix is made.

### `natsbroker.Broker.Publish`

```go
func (b *Broker) Publish(ctx context.Context, name string, payload []byte) error
```

Publish sends `payload` to every instance subscribed to `name`, this one included, and returns once the server has confirmed it holds the message, within `PublishTimeout`. Delivery is at most once. A payload over the server's `max_payload` (1 MiB unless the server is set otherwise) is a [`*bites.PayloadError`](./websocket.md#payloaderror) whose `Limit` is that number, found with `errors.As`, and nothing is sent; NATS counts the payload only, so nothing is taken off for the name. With no live connection (before `Run` has connected, or while it reconnects) it returns an error that `errors.Is` finds as `ErrNotConnected`, says the state of the client and that the message was not sent and is not kept, and sends nothing: a message is never queued to be sent after the gap. A name `Accepts` refuses is an error. Messages of one publisher to one name arrive in the order of its Publish calls; on one server every instance hears one order for a name, and in a cluster only a publisher's own order is held. A Topic calls this; an application calls `Topic.Publish`.

### `natsbroker.Broker.Subscribe`

```go
func (b *Broker) Subscribe(name string, deliver func([]byte), gap func()) (stop func())
```

Subscribe registers `deliver` for `name`. Nothing is sent to NATS: the one subscription to every name of the prefix is made with the connection, so a subscription made while the connection is up hears the next message published to the name. Deliveries run one at a time on a goroutine of the NATS client, in the order the server sent them; each payload is the subscriber's to keep. `gap` is called once when the connection came back after a drop, for a subscription made while it was down, and for every subscription when NATS dropped messages because the deliveries ran behind (65536 messages or 64 MiB pending); it must not block. A name `Accepts` refuses delivers nothing and is logged at Error. A Topic calls this; an application does not.

### `natsbroker.Broker.Accepts`

```go
func (b *Broker) Accepts(name string) error
```

Accepts reports whether `name` can be carried: not empty, at most 255 bytes, valid UTF-8, with no space or control character, no `*` or `>` (NATS wildcards) and no empty part (a leading, trailing or doubled dot). A name is never escaped or changed: `bites.NewTopic` calls `Accepts`, so such a name is a Registration panic, and a [`Topics`](./websocket.md#topics) family returns the error from `Subscribe` and `Publish` for a key from data, whose text says what to change (remove the character, or encode the key with hex or `base64.RawURLEncoding` before it is put in a name).

### `natsbroker.ErrClosed`

```go
var ErrClosed = errors.New("natsbroker: the Broker is closed")
```

ErrClosed is what `Publish` returns after `Close` or after `Run` ended, and what `Run` returns when called after `Close`.

### `natsbroker.ErrNotConnected`

```go
var ErrNotConnected = errors.New("natsbroker: not connected to a NATS server")
```

ErrNotConnected is what `Publish` wraps while there is no live connection to a server: nothing was sent and nothing is kept to send later. Find it with `errors.Is`.
