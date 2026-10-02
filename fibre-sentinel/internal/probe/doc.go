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
// With Config.AskEveryEndorser (sentinel-probe -end-read-all, the default)
// the reading is a full one (FullReadLabel, fullread.go): every validator
// that endorsed the promise is asked for its own rows, whatever the rows
// already held, and one whose answer did not serve is asked again, up to
// FullReadRetries times, Config.RetrySpacing after its last answer, while
// the request can start Config.RequestStartMargin before must_serve_until
// (retry.go; the row says when, Measurement.NextAttemptDue). The later
// attempts hold neither the reading's blob slot nor its Reconstructor, are
// one request each with at most one in flight to a validator, and each
// writes a row of its own (Measurement.Attempt); an endpoint that fails
// before any blob is asked for answers every attempt of its validator that
// is waiting (Measurement.SharedFrom). A failure of a full reading that
// this observer cannot pin on the validator (its network, its resolver,
// its clock) is rewritten as its own gap (ownside.go). Without
// AskEveryEndorser the reading stops once the rows are enough
// (EnoughReadLabel).
//
// Every request waits for room under this observer's own limits: requests
// and shard bytes in flight, and the rate shard bytes are let go at
// (Config.MaxReadMbps, ceiling.go), so the observer never takes its shared
// port from the work beside it. The wait only delays a request, up to its
// start cutoff (past it the request is not made: NOT_PROBED, this
// observer's gap), and is recorded on the row (LoadInfo), never in the
// request's own time.
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
// (ReadInfo). A validator the reading did not need to ask has no row; a
// later attempt of a full reading appends its own.
//
// # Taxonomy
//
// Classify keeps the error classes apart. An assigned, attested validator
// whose identity verified and that answers NOT_FOUND (or returns rows that do
// not verify) before must_serve_until is a FAULT; unreachable is UNREACHABLE
// and an identity failure is IDENTITY_MISMATCH or IDENTITY_EXPIRED. The
// class says what happened on the wire. What counts is decided in
// observer/verdict. At a full reading each endorser is judged on its own
// answers: served when one of them served (FullServed), this observer's gap
// when one was (FullGap: NOT_PROBED, PROBE_ERROR, or rows that are not its
// own and that no settled promise explains), when an attempt it was owed is
// not on record, or when the reading reached no server; not served
// otherwise, by its last answer. At a reading that stopped once
// the rows were enough, a validator whose rows came back verified served,
// and an endorsing validator whose rows did not come back is not served only
// when the blob was Unavailable. A validator that did not endorse is never
// counted (UNATTESTED), and a full reading does not ask it.
package probe
