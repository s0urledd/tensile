// Package probe reads the blobs the scanner found — the layer that makes a
// Fibre publication into an actual observation.
//
// # Reading
//
// Each blob is read once, EndReadOffset (10 minutes) before its
// must_serve_until, the way celestia-app's Fibre client downloads it
// (blobread.go): the whole validator set, in the client's own order
// (validator.Set.Select), the next one asked while the rows still wanted
// outnumber the rows on their way, 15 s per request, one re-dial after a
// failed dial or an unreachable or timed-out peer, every row verified
// against the commitment by one Reconstructor, and the reading done at K
// distinct verified rows. It ends as the client's Download does: Available,
// or Unavailable with the client's error ("no shards retrieved", "not
// enough shards to reconstruct blob").
//
// The queue of readings is never stored. The Prober re-derives it every cycle
// from publications.jsonl and the existing measurements.jsonl, so a restart
// resumes exactly. Every wait is bounded, and a SIGINT/SIGTERM stops it
// cleanly.
//
// # Request
//
// Run makes one request to one validator, timing and judging each layer on
// its own: L1 DNS resolution, L2 TCP connect, L3 TLS 1.3 handshake (raw), L3
// identity (fibre-tlsverify consensus-key binding on the peer cert), L4
// retrievability (DownloadShard, the rows verified against the commitment
// with pkg/rsema1d and against the recomputed fibre-assign assignment).
//
// # Measurement
//
// A reading ends with one Measurement per validator it asked, appended
// together to measurements.jsonl: the vantage, the time, every layer's
// duration and result, the identity verdict, the rows returned and their
// verification, the raw error text, and where the answer sat in the reading
// (ReadInfo). A validator the reading did not need to ask has no row.
//
// # Taxonomy
//
// Classify keeps the error classes apart. An assigned, attested validator
// whose identity verified and that answers NOT_FOUND (or returns rows that do
// not verify) before must_serve_until is a FAULT; unreachable is UNREACHABLE
// and an identity failure is IDENTITY_MISMATCH or IDENTITY_EXPIRED. The
// class says what happened on the wire. What counts is simpler and decided
// from the whole reading (observer/verdict): a validator whose rows came
// back verified served; an endorsing validator whose rows did not come back
// is not served only when the blob was Unavailable. A validator that did not
// endorse is asked like the rest and is never counted (UNATTESTED).
package probe
