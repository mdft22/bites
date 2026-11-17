module github.com/nevindra/bites/modules/redissession

go 1.27

require (
	github.com/nevindra/bites v0.0.0
	github.com/redis/go-redis/v9 v9.23.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

// While the modules live beside the core, they build against the core beside them. A release tags
// the core first and replaces this line with its version (ADR 0070).
replace github.com/nevindra/bites => ../..
