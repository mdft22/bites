package bites

// MemoryIdempotencyStore is the in-memory store of an Idempotency with room for a test's keys, so
// the conformance suite of bitetest holds the store the middleware uses by default to the contract
// it states for every IdempotencyStore (ADR 0086, ADR 0090).
func MemoryIdempotencyStore() IdempotencyStore { return newMemoryIdempotency(1000, 1<<20) }
