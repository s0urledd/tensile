/**
 * The API reference: the documented routes, in the order the page lists
 * them, each with its parameters and an example response. The page renders
 * from this file alone (page.tsx, reference.tsx).
 *
 * Each parameter's type, allowed values, range and default are the ones
 * observer-api enforces (fibre-sentinel/observer/api). The example
 * responses are real answers of the Mocha API (mocha-5), taken on 30
 * September 2026 with the Try it values below; all but the ones marked
 * `whole` are trimmed to a few fields. Nothing is added and no value is
 * changed.
 *
 * In descriptions and error lines, `backticks` mark a field or parameter.
 */

export type Param = {
  name: string;
  in: "path" | "query";
  /** the type as the table prints it */
  type: "string" | "integer" | "RFC 3339";
  /** the only values the route accepts; Try it offers them as a list */
  values?: string[];
  /** "1–500" */
  range?: string;
  default?: string;
  required?: boolean;
  desc: string;
  /** the value Try it starts with; none leaves the parameter out */
  example?: string;
};

export type Endpoint = {
  /** the row's anchor on the page */
  id: string;
  path: string;
  /** twelve words at most */
  summary: string;
  /** two short sentences at most, only where the summary is not enough */
  desc?: string;
  params: Param[];
  /** the route's own error answers, beyond the ones every route shares */
  errors?: string;
  example: string;
  /** the example is the whole answer, not a trimmed one */
  whole?: boolean;
  /** what the route answers with, when it is not JSON */
  format?: "atom" | "text";
};

export type Group = { id: string; title: string; endpoints: Endpoint[] };

// ---- parameters several routes share ----

const HUGINN = "e4401aea8b1f8359fe58216d70d78a402689a2a4";
const PUBLISHER = "celestia1las83d0dt9gew3faq2mxp2gtupq5drclee9snr";
const NAMESPACE = "00000000000000000000000000000000000000736f762d6e696b6f2d61";
const BLOB = "36f68ba9a781754e80037357ebf485d25e471332f436596904467099cfda2417";

const windowParam: Param = {
  name: "window", in: "query", type: "string", values: ["24h", "7d", "30d", "all"], default: "24h",
  desc: "The period the figures cover.", example: "7d",
};
const asOf: Param = {
  name: "as_of", in: "query", type: "RFC 3339",
  desc: "End the period at this time instead of now.",
};
const validatorAddr: Param = {
  name: "addr", in: "path", type: "string", required: true,
  desc: "The validator's consensus, operator or account address.", example: HUGINN,
};
const rows: Param = {
  name: "rows", in: "query", type: "string", values: ["0", "1"], default: "0",
  desc: "1 adds each reading's `row_indices` and `rows_sha256`.",
};

// ---- example responses ----

const EX_VALIDATORS = `{
  "window": {"name": "7d"},
  "computed_at": "2026-09-30T05:58:48.392022076Z",
  "validators": [
    {
      "address": "e4401aea8b1f8359fe58216d70d78a402689a2a4",
      "moniker": "Huginn",
      "operator_address": "celestiavaloper1d2ktc37cme7ydk30ylzhamutcynhdvyet7nt3x",
      "host": "174.138.180.150:7980",
      "voting_power": 2100001,
      "endpoint_state": "reachable",
      "identity_status": "verified",
      "obligations": {
        "served": 1918,
        "broken": 0,
        "rate": {"num": 1918, "den": 1918, "value": 1}
      },
      "signing": {"assigned": 8625, "signed": 7916},
      "hosting": {"provider": "InterServer", "country": "US"}
    }
  ]
}`;

const EX_VALIDATOR = `{
  "window": {"name": "7d"},
  "validator": {
    "address": "e4401aea8b1f8359fe58216d70d78a402689a2a4",
    "moniker": "Huginn",
    "endpoint_state": "reachable",
    "obligations": {
      "served": 1918,
      "broken": 0,
      "not_counted": 5998,
      "rate": {"num": 1918, "den": 1918, "value": 1}
    },
    "signing": {"assigned": 8625, "signed": 7916},
    "load": {"bytes": 4756213248, "stored_bytes": 0, "rows_per_blob": 148}
  },
  "windows": [
    {"window": {"name": "24h"}, "obligations": {"served": 0, "broken": 0}},
    {"window": {"name": "7d"}, "obligations": {"served": 1918, "broken": 0}}
  ],
  "network_reference": {"median_rate": 1, "validators": 69},
  "recent_probes_truncated": true
}`;

const EX_STATUS = `{
  "address": "e4401aea8b1f8359fe58216d70d78a402689a2a4",
  "moniker": "Huginn",
  "host": "174.138.180.150:7980",
  "window": {"name": "7d"},
  "jailed": false,
  "bond_status": "BOND_STATUS_BONDED",
  "endpoint_state": "reachable",
  "identity_status": "verified",
  "obligations": {
    "served": 1918,
    "broken": 0,
    "rate": {"num": 1918, "den": 1918, "value": 1}
  },
  "signing": {
    "assigned": 8625,
    "signed": 7916,
    "last_endorsed_at": "2026-09-28T20:48:38.760812205Z"
  },
  "last_endpoint_check": {
    "at": "2026-09-30T05:59:40.968398993Z",
    "outcome": "REACHABLE"
  }
}`;

const EX_PROBES = `{
  "limit": 1,
  "truncated": true,
  "next_before": "2026-09-29T00:48:20.592635297Z",
  "probes": [
    {
      "promise_hash": "8a4aa311920cc5694ff88eee3e3928919ef97c4d4d057a946ee738ba9debe9e0",
      "validator_address": "e4401aea8b1f8359fe58216d70d78a402689a2a4",
      "started_at": "2026-09-29T00:48:20.592635297Z",
      "outcome": "SERVED_OK",
      "classification": "HEALTHY",
      "rows_returned": 148,
      "rows_expected": 148,
      "total_duration_ms": 506,
      "service": "served"
    }
  ]
}`;

const EX_BLOBS = `{
  "blobs": [
    {
      "promise_hash": "36f68ba9a781754e80037357ebf485d25e471332f436596904467099cfda2417",
      "namespace": "00000000000000000000000000000000000000736f762d6e696b6f2d61",
      "blob_size": 16777216,
      "publisher": "celestia1las83d0dt9gew3faq2mxp2gtupq5drclee9snr",
      "settlement_height": 1204085,
      "settlement_time": "2026-09-28T20:48:38.760812205Z",
      "reconstructable": {
        "status": "yes",
        "served_distinct_rows": 8051,
        "needed_rows": 4096
      },
      "charge": {"fee_utia": 3530000}
    }
  ],
  "limit": 2,
  "offset": 0,
  "total": 8467,
  "truncated": true,
  "next_before_height": 1204085,
  "next_before_tx_index": 1
}`;

const EX_BLOB = `{
  "blob": {
    "promise_hash": "36f68ba9a781754e80037357ebf485d25e471332f436596904467099cfda2417",
    "blob_size": 16777216,
    "publisher": "celestia1las83d0dt9gew3faq2mxp2gtupq5drclee9snr",
    "must_serve_until": "2026-09-29T00:48:35.807567083Z",
    "reconstructable": {
      "status": "yes",
      "served_distinct_rows": 8051,
      "needed_rows": 4096
    },
    "charge": {"fee_utia": 3530000}
  },
  "assignments": [
    {
      "validator_address": "f345f91cd3c36238f550a024800c0a2cd0d7d49c",
      "moniker": "devops",
      "row_count": 1451,
      "attested": true,
      "service": "served"
    }
  ],
  "probes": [
    {
      "validator_address": "f345f91cd3c36238f550a024800c0a2cd0d7d49c",
      "outcome": "SERVED_OK",
      "classification": "HEALTHY",
      "rows_returned": 1451,
      "total_duration_ms": 1849
    }
  ],
  "probes_truncated": false
}`;

const EX_NAMESPACES = `{
  "namespaces": [
    {
      "namespace": "00000000000000000000000000000000000000736f762d6e696b6f2d61",
      "blobs": 8467,
      "bytes": 142052687872,
      "blobs_24h": 0,
      "bytes_24h": 0,
      "accounts": 1,
      "first_seen": "2026-09-28T12:47:32.406462187Z",
      "last_blob": "2026-09-28T20:48:38.760812205Z"
    }
  ],
  "limit": 3,
  "truncated": true
}`;

const EX_PUBLISHERS = `{
  "window": {"name": "7d"},
  "count": 10,
  "publishers": [
    {
      "publisher": "celestia1las83d0dt9gew3faq2mxp2gtupq5drclee9snr",
      "settlements": 8467,
      "bytes": 142052687872,
      "fees_utia": 29888510000,
      "fees_share": 0.9882393979672135,
      "paid_per_mib_utia": 220625,
      "escrow": {"balance_utia": 111490000, "available_utia": 111490000}
    }
  ]
}`;

const EX_PUBLISHER = `{
  "window": {"name": "7d"},
  "publisher": {
    "publisher": "celestia1las83d0dt9gew3faq2mxp2gtupq5drclee9snr",
    "settlements": 8467,
    "fees_utia": 29888510000,
    "timeouts": 0,
    "escrow": {"balance_utia": 111490000, "available_utia": 111490000},
    "pending_withdrawals": {"count": 0, "utia": 0}
  },
  "recent_payments": [
    {
      "kind": "settlement",
      "height": 1204085,
      "time": "2026-09-28T20:48:38.760812205Z",
      "promise_hash": "36f68ba9a781754e80037357ebf485d25e471332f436596904467099cfda2417",
      "blob_size": 16777216,
      "amount_utia": 3530000
    }
  ],
  "recent_blobs_truncated": true
}`;

const EX_MARKET = `{
  "window": {"name": "7d"},
  "settlements": 8625,
  "blobs": 8623,
  "fees_settled_utia": 30244200000,
  "bytes": 143526461440,
  "publishers_active": 7,
  "timeouts": 0,
  "escrow_total_utia": 207465000,
  "daily": [
    {
      "day": "2026-09-28",
      "settlements": 8472,
      "fees_utia": 29896305000,
      "bytes": 142079164416
    }
  ],
  "top_publishers": [
    {
      "publisher": "celestia1las83d0dt9gew3faq2mxp2gtupq5drclee9snr",
      "fees_share": 0.9882393979672135,
      "bytes_share": 0.9897316943982758
    }
  ]
}`;

const EX_NETWORK = `{
  "window": {"name": "7d"},
  "registered_endpoints": 73,
  "reachability": {"num": 72, "den": 73, "value": 0.9863013698630136},
  "obligations": {
    "served": 284544,
    "broken": 0,
    "not_counted": 174576,
    "rate": {"num": 284544, "den": 284544, "value": 1}
  },
  "reconstructable": {
    "recoverable": {"num": 1999, "den": 1999, "value": 1},
    "yes": 1999,
    "no": 0,
    "not_read": 191,
    "publications_examined": 2000
  }
}`;

const EX_SIGNING = `{
  "window": {"name": "7d"},
  "threshold": {"num": 2, "den": 3},
  "promises": 8625,
  "meets_threshold": {"num": 8625, "den": 8625, "value": 1},
  "buckets": [
    {"key": "q_70", "label": "⅔ – 70%", "count": 6250},
    {"key": "70_75", "label": "70 – 75%", "count": 1704}
  ],
  "signers_median": 53
}`;

const EX_HOSTING = `{
  "summary": {
    "registered_hosts": 73,
    "resolved_hosts": 73,
    "by_provider": [
      {"key": "Scaleway", "hosts": 3, "stake_share": 0.14928847580443982}
    ],
    "nakamoto_third": {
      "provider": {
        "count": 3,
        "entities": ["Scaleway", "Cherry Servers", "Hetzner"],
        "share": 0.40274827648451667
      }
    }
  }
}`;

const EX_PARAMS = `{
  "current": {
    "shard_retention_s": 14400,
    "payment_promise_timeout_s": 3600,
    "withdrawal_delay_s": 86400,
    "source": "seed"
  },
  "derived": {"must_serve_window_s": 14400},
  "changes": 0,
  "protocol": {
    "max_blob_size_bytes": 134217728,
    "min_rows_per_validator": 148,
    "max_rows_per_validator": 4096
  },
  "price_formula": {
    "base_gas": 650000,
    "gas_per_chunk": 45000,
    "chunk_bytes": 262144,
    "utia_per_gas": 1
  }
}`;

const EX_EXPORTS = `{
  "exports": [
    {
      "name": "tensile-ut-1-2026-09-29.tar.gz",
      "bytes": 30490344,
      "sha256": "0ea0eda3f14487eae44f44cd10ccd5329a09efe4186a1aebb95eb9b5de6177b6",
      "day": "2026-09-29",
      "methodology_version": "2026-09-29.2",
      "signature": {
        "algorithm": "ed25519",
        "key_fingerprint": "sha256:8eb4c98fd59e067ce8651f0edec06bcccc6d92b49b2ddd8161929ec3826a5412"
      }
    }
  ]
}`;

const EX_PUBKEY = `{
  "signed": true,
  "current": {
    "algorithm": "ed25519",
    "key_fingerprint": "sha256:8eb4c98fd59e067ce8651f0edec06bcccc6d92b49b2ddd8161929ec3826a5412",
    "public_key": "VgeY7esoNK5uSXdt0DRLdS0K74K3hneAD2Gx5T1vd8s=",
    "first_day": "2026-09-24",
    "last_day": "2026-09-29"
  }
}`;

const EX_TIP = `{
  "height": 1245968,
  "block_time": "2026-09-30T06:03:27.867738453Z",
  "fibre_active": true,
  "server_time": "2026-09-30T06:03:38.225648922Z"
}`;

const EX_HEALTH = `{
  "status": "ok",
  "checks": [
    {"name": "prober", "ok": true, "detail": "alive"},
    {
      "name": "chain_liveness",
      "ok": true,
      "detail": "newest block 47s old (2026-09-30T06:02:50Z)"
    }
  ],
  "pin_status": "matches",
  "server_time": "2026-09-30T06:03:38.231100213Z"
}`;

const EX_VALIDATOR_FEED = `<feed xmlns="http://www.w3.org/2005/Atom">
  <title type="text">Tensile · Huginn · Fibre endpoint events</title>
  <updated>2026-09-25T06:31:19Z</updated>
  <entry>
    <id>tag:tensile.huginn.tech,2026:tensile/mocha-5/e4401aea8b1f8359fe58216d70d78a402689a2a4/registration/1095061-1</id>
    <title type="text">Huginn: registered Fibre host 174.138.180.150:7980</title>
    <updated>2026-09-25T06:30:25Z</updated>
    <category term="registered"></category>
  </entry>
</feed>`;

const EX_FEED = `<feed xmlns="http://www.w3.org/2005/Atom">
  <title type="text">Tensile · mocha-5 · Fibre network events</title>
  <updated>2026-09-29T18:10:13Z</updated>
  <entry>
    <id>tag:tensile.huginn.tech,2026:tensile/mocha-5/5282878ddcbbe0d2e0ca42d54cb64711c901ba75/bonded-joined/20260929T181013Z</id>
    <updated>2026-09-29T18:10:13Z</updated>
    <category term="bonded-joined"></category>
    <summary type="text">First seen in AllBondedFibreProviders at height 1230995 with host 158.51.121.62:7980.</summary>
  </entry>
</feed>`;

const EX_EXPORT = `0ea0eda3f14487eae44f44cd10ccd5329a09efe4186a1aebb95eb9b5de6177b6  tensile-ut-1-2026-09-29.tar.gz`;

// ---- the reference ----

export const GROUPS: Group[] = [
  {
    id: "validators",
    title: "Validators",
    endpoints: [
      {
        id: "validators",
        path: "/v1/validators",
        summary: "Every validator on record, with service, endorsements and hosting.",
        params: [windowParam, asOf],
        example: EX_VALIDATORS,
      },
      {
        id: "validator",
        path: "/v1/validators/{addr}",
        summary: "One validator's service, endorsements, load and newest readings.",
        desc: "`windows` repeats the service counts for 24h, 7d, 30d and all; `recent_probes` holds the newest 50 readings, not-probed ones left out.",
        params: [validatorAddr, windowParam, asOf],
        errors: "404 when no validator is on record at the address.",
        example: EX_VALIDATOR,
      },
      {
        id: "validator-status",
        path: "/v1/validators/{addr}/status",
        summary: "One validator's endpoint state, service, endorsements and last check.",
        params: [validatorAddr, windowParam],
        errors: "400 when `as_of` is given; 404 when no validator is on record at the address.",
        example: EX_STATUS,
      },
      {
        id: "validator-feed",
        path: "/v1/validators/{addr}/feed.atom",
        summary: "Atom feed of one validator's registration and endpoint events.",
        desc: "The newest 50 entries of the last 30 days. A reachability or certificate change is published once three checks in a row agree.",
        params: [validatorAddr],
        errors: "404 when no validator is on record at the address.",
        example: EX_VALIDATOR_FEED,
        format: "atom",
      },
      {
        id: "probes",
        path: "/v1/probes",
        summary: "Tensile's readings, newest first, by validator, blob, class or time.",
        params: [
          { name: "validator", in: "query", type: "string", desc: "A validator's consensus, operator or account address.", example: HUGINN },
          { name: "blob", in: "query", type: "string", desc: "A promise hash, 64 hex characters." },
          { name: "class", in: "query", type: "string", desc: "A classification, such as HEALTHY, FAULT or UNREACHABLE.", example: "HEALTHY" },
          { name: "served", in: "query", type: "string", values: ["no"], desc: "Only the readings counted as not served." },
          { name: "since", in: "query", type: "RFC 3339", desc: "Readings started at or after this time." },
          { name: "before", in: "query", type: "RFC 3339", desc: "Readings started before this time: pass `next_before` for the next page." },
          { name: "at", in: "query", type: "RFC 3339", desc: "Readings scheduled at this time." },
          { name: "limit", in: "query", type: "integer", range: "1–1000; 1–200 with rows=1", default: "100", desc: "Readings per page.", example: "1" },
          rows,
        ],
        example: EX_PROBES,
      },
    ],
  },
  {
    id: "blobs",
    title: "Blobs",
    endpoints: [
      {
        id: "blobs",
        path: "/v1/blobs",
        summary: "Settled blobs, newest first, by namespace, commitment or publisher.",
        desc: "Filters combine. `total` counts every blob the filters and the cursor select.",
        params: [
          { name: "namespace", in: "query", type: "string", desc: "A namespace, 58 hex characters.", example: NAMESPACE },
          { name: "commitment", in: "query", type: "string", desc: "A blob commitment, 64 hex characters." },
          { name: "publisher", in: "query", type: "string", desc: "The celestia1… account whose escrow paid." },
          { name: "limit", in: "query", type: "integer", range: "1–500", default: "50", desc: "Blobs per page.", example: "2" },
          { name: "before_height", in: "query", type: "integer", desc: "Blobs settled before this height: pass `next_before_height`." },
          { name: "before_tx_index", in: "query", type: "integer", default: "0", desc: "With `before_height`: pass `next_before_tx_index`." },
          { name: "offset", in: "query", type: "integer", range: "0–100000", default: "0", desc: "Blobs to skip, for numbered pages." },
        ],
        example: EX_BLOBS,
      },
      {
        id: "blob",
        path: "/v1/blobs/{hash}",
        summary: "One blob: availability, charge, validators with rows, and readings.",
        desc: "`reconstructable.status` is yes (Available), no (Unavailable), pending, not_read or unknown. Each assignment's `service` is served, not_served, in_retention_window or deadline_unverified.",
        params: [
          { name: "hash", in: "path", type: "string", required: true, desc: "The blob's promise hash, 64 hex characters.", example: BLOB },
          rows,
        ],
        errors: "404 when no blob has this promise hash.",
        example: EX_BLOB,
      },
      {
        id: "namespaces",
        path: "/v1/namespaces",
        summary: "Namespaces by newest settlement, with blob counts, bytes and accounts.",
        params: [
          { name: "limit", in: "query", type: "integer", range: "1–500", default: "100", desc: "Namespaces per answer.", example: "3" },
        ],
        example: EX_NAMESPACES,
      },
    ],
  },
  {
    id: "publishers",
    title: "Publishers",
    endpoints: [
      {
        id: "publishers",
        path: "/v1/publishers",
        summary: "Publishers with escrow movements in the period: fees, bytes, escrow.",
        params: [windowParam, asOf],
        example: EX_PUBLISHERS,
      },
      {
        id: "publisher",
        path: "/v1/publishers/{addr}",
        summary: "One publisher: fees, escrow, withdrawals, recent payments and blobs.",
        desc: "`window` and `as_of` apply to the `publisher` object only; the rest of the answer is as of now.",
        params: [
          { name: "addr", in: "path", type: "string", required: true, desc: "The publisher's celestia1… account.", example: PUBLISHER },
          windowParam, asOf,
        ],
        errors: "404 when the account has no escrow movement on record.",
        example: EX_PUBLISHER,
      },
      {
        id: "market",
        path: "/v1/market",
        summary: "Blob market totals: settlements, fees, bytes, escrow, daily figures, top publishers.",
        params: [windowParam, asOf],
        example: EX_MARKET,
      },
    ],
  },
  {
    id: "network",
    title: "Network",
    endpoints: [
      {
        id: "network",
        path: "/v1/network",
        summary: "Network service rate, availability and endpoint reachability.",
        desc: "`exclude` recomputes the figures without the named validators; availability still counts every validator.",
        params: [
          windowParam, asOf,
          { name: "exclude", in: "query", type: "string", desc: "Up to 8 consensus addresses, comma-separated or repeated." },
        ],
        example: EX_NETWORK,
      },
      {
        id: "signing",
        path: "/v1/signing",
        summary: "Endorsements per settled promise, against the ⅔ quorum.",
        params: [windowParam, asOf],
        example: EX_SIGNING,
      },
      {
        id: "hosting",
        path: "/v1/hosting",
        summary: "Where registered Fibre hosts run: by hosting provider, country and AS.",
        desc: "`nakamoto_third` is the fewest hosting providers, AS numbers or countries that together hold more than a third of the hosts' stake.",
        params: [],
        errors: "400 when `as_of` is given.",
        example: EX_HOSTING,
      },
      {
        id: "params",
        path: "/v1/params",
        summary: "x/fibre parameters and their history, protocol constants, the fee formula.",
        desc: "`derived.must_serve_window_s` is how long endorsing validators owe a blob's rows: the longer of shard retention and the promise timeout.",
        params: [],
        example: EX_PARAMS,
      },
      {
        id: "feed",
        path: "/v1/feed.atom",
        summary: "Network Atom feed: registrations, host changes, bonded list changes, first not-served readings.",
        desc: "The newest 50 entries of the last 30 days.",
        params: [],
        example: EX_FEED,
        format: "atom",
      },
    ],
  },
  {
    id: "exports",
    title: "Exports",
    endpoints: [
      {
        id: "exports",
        path: "/v1/exports",
        summary: "Daily archives of the record, one per UTC day, with digests.",
        params: [],
        example: EX_EXPORTS,
      },
      {
        id: "exports-pubkey",
        path: "/v1/exports/pubkey",
        summary: "The ed25519 keys that sign archive manifests, with the days each signed.",
        params: [],
        errors: "404 when the archives are not signed.",
        example: EX_PUBKEY,
      },
      {
        id: "export",
        path: "/v1/exports/{name}",
        summary: "One archive, or its .sha256 digest or .sig signature.",
        desc: "Archives are gzip tarballs and take HTTP byte ranges.",
        params: [
          { name: "name", in: "path", type: "string", required: true, desc: "A name from /v1/exports, optionally ending in .sha256 or .sig.", example: "tensile-ut-1-2026-09-29.tar.gz.sha256" },
        ],
        errors: "404 when there is no such export; 416, as plain text, for a byte range past the end.",
        example: EX_EXPORT,
        whole: true,
        format: "text",
      },
    ],
  },
  {
    id: "status",
    title: "Status",
    endpoints: [
      {
        id: "tip",
        path: "/v1/tip",
        summary: "The chain's newest block as Tensile last saw it.",
        desc: "`block_time` is absent while Tensile catches up to the tip.",
        params: [],
        example: EX_TIP,
        whole: true,
      },
      {
        id: "health",
        path: "/v1/health",
        summary: "Tensile's status, ok, degraded or down, with each check behind it.",
        params: [],
        errors: "503 when the status is not ok, with the same body.",
        example: EX_HEALTH,
      },
    ],
  },
];
