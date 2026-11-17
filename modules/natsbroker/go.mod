module github.com/nevindra/bites/modules/natsbroker

go 1.27

require (
	github.com/nats-io/nats.go v1.53.1
	github.com/nevindra/bites v0.0.0
)

require (
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/nats-io/nkeys v0.4.15 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.49.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
)

// While the modules live beside the core, they build against the core beside them. A release tags
// the core first and replaces this line with its version (ADR 0070).
replace github.com/nevindra/bites => ../..
