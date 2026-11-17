# What a tag made from the body costs, and what the validators cost the request path

Taken on 2026-11-17, Go 1.27.4, on an AMD Ryzen 7 9700X (8 cores, 16 threads), at commit `1a466f6` with the change of [ADR 0089](../../adr/0089-an-answer-carries-a-validator-and-a-write-checks-the-version-it-read.md) in the working tree, pinned to cores 2,3,10,11 with `taskset`. The numbers are medians of the runs listed, with their spread; the machine was not otherwise idle, which shows in the large spread of the 8 KB rows.

## The hash a body tag is made of

[`hash/hash_test.go`](hash/hash_test.go) is its own module and hashes a buffer of each size with four hashes, three runs each (raw output [`hash.txt`](hash.txt)), no allocation in any:

| hash | 256 B | 4 KiB | 64 KiB | 1 MiB | tag |
|---|---|---|---|---|---|
| SHA-256 (`crypto/sha256`, SHA-NI) | 150 ns | 1.6 µs | 24.7 µs | 405 µs | 128 bits kept |
| FNV-1a 64 (`hash/fnv`) | 190 ns | 3.0 µs | 48.6 µs | 779 µs | 64 bits |
| CRC-64 ECMA (`hash/crc64`) | 110 ns | 1.45 µs | 23.5 µs | 400 µs | 64 bits |
| CRC-32C (`hash/crc32`, hardware) | 12 ns | 97 ns | 1.6 µs | 27 µs | 32 bits |

SHA-256 is as fast as CRC-64 and FNV-1a is slower than either, so the choice is by what the tag must hold: 64 bits of a non-cryptographic hash make a stale 304 a 1-in-2^64 event for a changed body that is not adversarial, but a body that contains attacker-chosen text can be given a collision for a CRC or FNV by anyone who can write it, and a collision here is a client served its stale copy. SHA-256 at 2.5 GB/s has no such input and costs 1.6 µs for a 4 KiB answer. CRC-32C is 16 times faster and has 32 bits, which is the one a changed body collides with once in four billion.

## The request path

`BenchmarkTaggedAnswers` in `etag_budget_test.go` answers one list over the in-memory connection four ways. Five runs of 50,000 requests for the body of 0.8 KB ([`tagged.txt`](tagged.txt)), seven runs of 100,000 for the body of 8 KB ([`tagged-200.txt`](tagged-200.txt)). The harness itself costs 3 allocations a request, so only the differences count.

| answer | 0.8 KB, median (range) | 8 KB, median (range) |
|---|---|---|
| a plain `T` | 1.79 µs (1.71 to 1.97) | 5.95 µs (5.76 to 6.03) |
| `Tagged` with a version | 1.77 µs (1.61 to 1.92) | 5.87 µs (5.77 to 6.06) |
| `Tagged` with `ETagOfBody()` | 2.38 µs (2.28 to 2.49) | 10.5 µs (9.07 to 12.9) |
| 304 of the version (`If-None-Match` names it) | 1.09 µs (1.03 to 1.24) | 2.3 µs (1.09 to 4.8) |

A tag given by the handler costs nothing measurable. A tag made from the body adds its hash: +0.6 µs at 0.8 KB, and at 8 KB the lowest run is +3.1 µs, which is the 3.3 µs the table above gives for SHA-256 over 8 KB; the upper runs are the machine's noise. The 304 of a version is decided before the body is encoded, so it costs about the head, and the larger the body the more it saves; its 8 KB row has the widest spread of the four and is read as "about 1 to 2 µs, under half of the answer it replaces".

Allocations are held by `TestATaggedGetItsMatching304AndATagMadeFromTheBodyAllocateTwice` and `TestAPutThatChecksIfMatchAndAnswersATaggedAllocatesTwice`: **2** for each shape, the number of a typed route ([ADR 0003](../../adr/0003-the-trade-budget.md)). Throughput and p99 of a route that uses none of this were not measured: its added cost is one comparison of the output plan's kind in `write`, one nil check of the input plan's `ifMatch` in `decode` and one in `call`.
