// Real answers of the Mocha API, taken on 29 September 2026, each cut down
// to the fields its question on the API page reads. Nothing is added and no
// value is changed: the figures move, the shapes do not.

export const status = `{
  "address": "e4401aea8b1f8359fe58216d70d78a402689a2a4",
  "cons_address": "celestiavalcons1u3qp465tr7p4nljcy9khp4u2gqngng4yyy9knf",
  "operator_address": "celestiavaloper1d2ktc37cme7ydk30ylzhamutcynhdvyet7nt3x",
  "moniker": "Huginn",
  "window": {"name": "24h"},
  "computed_at": "2026-09-29T16:05:38.375473449Z",
  "jailed": false,
  "bond_status": "BOND_STATUS_BONDED",
  "endpoint_state": "reachable",
  "reachable": true,
  "identity_status": "verified",
  "last_reachable_at": "2026-09-29T14:39:28.956167578Z",
  "obligations": {
    "served": 769,
    "broken": 0,
    "rate": {"num": 769, "den": 769, "value": 1}
  },
  "signing": {
    "assigned": 5659,
    "signed": 5568,
    "last_endorsed_at": "2026-09-28T20:48:38.760812205Z"
  },
  "last_endpoint_check": {
    "at": "2026-09-29T14:39:28.956167578Z",
    "outcome": "REACHABLE"
  }
}`;

export const serving = `{
  "window": {"name": "24h"},
  "validator": {
    "address": "e4401aea8b1f8359fe58216d70d78a402689a2a4",
    "moniker": "Huginn",
    "obligations": {
      "total": 5568,
      "served": 769,
      "broken": 0,
      "held_param_unverified": 0,
      "not_counted": 4799,
      "pending": 0,
      "rate": {"num": 769, "den": 769, "value": 1}
    }
  },
  "windows": [
    {
      "window": {"name": "24h"},
      "obligations": {
        "served": 769,
        "broken": 0,
        "rate": {"num": 769, "den": 769, "value": 1}
      }
    },
    {
      "window": {"name": "7d"},
      "obligations": {
        "served": 1918,
        "broken": 0,
        "rate": {"num": 1918, "den": 1918, "value": 1}
      }
    }
  ],
  "network_reference": {
    "median_rate": 1,
    "validators": 50,
    "pooled_rate": {"num": 178002, "den": 178002, "value": 1}
  }
}`;

export const failed = `{
  "limit": 1,
  "next_before": "2026-09-29T00:31:59.597460134Z",
  "probes": [
    {
      "promise_hash": "f3a254dd4025c391d42c73ea2c4bf421508fef9d757298c3971930811a4e64fd",
      "validator_address": "0db46fed54d395de21761113588645bfcc28aee9",
      "started_at": "2026-09-29T00:31:59.597460134Z",
      "phase": "in_window",
      "outcome": "TLS_HANDSHAKE_FAIL",
      "classification": "UNREACHABLE",
      "rows_returned": 0,
      "rows_expected": 0,
      "total_duration_ms": 10207,
      "raw_error": "context deadline exceeded"
    }
  ],
  "truncated": true
}`;

export const endorsing = `{
  "validator": {
    "moniker": "Huginn",
    "signing": {
      "assigned": 5659,
      "signed": 5568,
      "unknown": 0,
      "no_host": 0,
      "last_endorsed_at": "2026-09-28T20:48:38.760812205Z",
      "recent": {"assigned": 20, "endorsed": 20}
    }
  }
}`;

export const holding = `{
  "validator": {
    "moniker": "devops",
    "load": {
      "promises": 6296,
      "bytes": 36895180352,
      "stored_bytes": 0,
      "rows_per_blob": 1451
    },
    "serve_bytes_per_second": 3511891,
    "serve_throughput_sample": 6066
  },
  "in_retention_window": 0
}`;

export const blob = `{
  "blob": {
    "promise_hash": "36f68ba9a781754e80037357ebf485d25e471332f436596904467099cfda2417",
    "commitment": "3c6ead4767ad64b8ceaf869bb0a62e0ec4e0197729039681c2e48413c3be4766",
    "namespace": "00000000000000000000000000000000000000736f762d6e696b6f2d61",
    "blob_size": 16777216,
    "publisher": "celestia1las83d0dt9gew3faq2mxp2gtupq5drclee9snr",
    "settlement_height": 1204085,
    "settlement_time": "2026-09-28T20:48:38.760812205Z",
    "must_serve_until": "2026-09-29T00:48:35.807567083Z",
    "attested_voting_power": 259615879,
    "total_voting_power": 371539663,
    "reconstructable": {
      "status": "yes",
      "point_at": "2026-09-29T00:38:35.807567083Z",
      "served_distinct_rows": 8051,
      "needed_rows": 4096,
      "total_rows": 16384,
      "served_by_validators": 30,
      "probed_validators": 30
    },
    "charge": {
      "fee_utia": 3530000,
      "gas_units": 3530000,
      "settled": true,
      "timed_out": false
    }
  },
  "params": {
    "shard_retention_s": 14400,
    "payment_promise_timeout_s": 3600
  },
  "assignments": [
    {
      "validator_address": "f345f91cd3c36238f550a024800c0a2cd0d7d49c",
      "moniker": "devops",
      "voting_power": 43855666,
      "row_count": 1451,
      "attested": true,
      "service": "served"
    }
  ]
}`;

export const commitment = `{
  "blobs": [
    {
      "promise_hash": "737473c134aa7efa4199c6c364f230d347353732ee5d119523f68b5c8d68861a",
      "commitment": "a6488bbe560dd24aab75d902fc2975b51066ae3fae74f689218523e2bd49797f",
      "blob_size": 262144,
      "publisher": "celestia184zvjvnvfg5kuu7qrql8wn673h9jk808m40y2c",
      "settlement_height": 1115369,
      "settlement_time": "2026-09-25T22:31:58.933004495Z",
      "reconstructable": {"status": "yes"}
    },
    {
      "promise_hash": "e103897e2b6e6ac266b1edee467b58e083829d6770829fb41f064870684538d8",
      "commitment": "a6488bbe560dd24aab75d902fc2975b51066ae3fae74f689218523e2bd49797f",
      "blob_size": 262144,
      "publisher": "celestia184zvjvnvfg5kuu7qrql8wn673h9jk808m40y2c",
      "settlement_height": 1115285,
      "settlement_time": "2026-09-25T22:27:59.940518206Z",
      "reconstructable": {"status": "yes"}
    }
  ],
  "total": 2,
  "commitment": "a6488bbe560dd24aab75d902fc2975b51066ae3fae74f689218523e2bd49797f"
}`;

export const namespace = `{
  "blobs": [
    {
      "promise_hash": "36f68ba9a781754e80037357ebf485d25e471332f436596904467099cfda2417",
      "commitment": "3c6ead4767ad64b8ceaf869bb0a62e0ec4e0197729039681c2e48413c3be4766",
      "blob_size": 16777216,
      "settlement_height": 1204085,
      "settlement_tx_index": 2,
      "settlement_time": "2026-09-28T20:48:38.760812205Z",
      "reconstructable": {"status": "yes"},
      "charge": {"fee_utia": 3530000}
    },
    {
      "promise_hash": "bee11ce49993d6263668bd1e883cab12450416c4aef50942fb169de235752a50",
      "commitment": "f46203b3c7662718d9a467f5837533e3c210404a816d7bcc3b8c2190bdc30101",
      "blob_size": 16777216,
      "settlement_height": 1204085,
      "settlement_tx_index": 1,
      "settlement_time": "2026-09-28T20:48:38.760812205Z",
      "reconstructable": {"status": "yes"},
      "charge": {"fee_utia": 3530000}
    }
  ],
  "limit": 2,
  "next_before_height": 1204085,
  "next_before_tx_index": 1,
  "offset": 0,
  "total": 8467,
  "truncated": true
}`;

export const paidBy = `{
  "blobs": [
    {
      "promise_hash": "36f68ba9a781754e80037357ebf485d25e471332f436596904467099cfda2417",
      "commitment": "3c6ead4767ad64b8ceaf869bb0a62e0ec4e0197729039681c2e48413c3be4766",
      "namespace": "00000000000000000000000000000000000000736f762d6e696b6f2d61",
      "blob_size": 16777216,
      "publisher": "celestia1las83d0dt9gew3faq2mxp2gtupq5drclee9snr",
      "settlement_height": 1204085,
      "settlement_tx_index": 2,
      "reconstructable": {"status": "yes"},
      "charge": {"fee_utia": 3530000}
    }
  ],
  "next_before_height": 1204085,
  "next_before_tx_index": 2,
  "publisher": "celestia1las83d0dt9gew3faq2mxp2gtupq5drclee9snr",
  "total": 8467,
  "truncated": true
}`;

export const publisher = `{
  "window": {"name": "7d"},
  "publisher": {
    "publisher": "celestia1las83d0dt9gew3faq2mxp2gtupq5drclee9snr",
    "settlements": 8467,
    "bytes": 142052687872,
    "fees_utia": 29888510000,
    "paid_per_mib_utia": 220625,
    "timeouts": 0,
    "timed_out_utia": 0,
    "escrow": {
      "found": true,
      "balance_utia": 111490000,
      "available_utia": 111490000
    },
    "pending_withdrawals": {"count": 0, "utia": 0, "next_available_at": null}
  },
  "withdrawals": {"count": 0, "utia": 0, "pending": []},
  "recent_payments": [
    {
      "kind": "settlement",
      "height": 1204085,
      "time": "2026-09-28T20:48:38.760812205Z",
      "promise_hash": "36f68ba9a781754e80037357ebf485d25e471332f436596904467099cfda2417",
      "blob_size": 16777216,
      "amount_utia": 3530000
    }
  ]
}`;

export const params = `{
  "current": {
    "withdrawal_delay_s": 86400,
    "payment_promise_timeout_s": 3600,
    "shard_retention_s": 14400
  },
  "derived": {"must_serve_window_s": 14400},
  "protocol": {
    "max_blob_size_bytes": 134217728,
    "min_rows_per_validator": 148,
    "max_rows_per_validator": 4096
  },
  "price_formula": {
    "base_gas": 650000,
    "gas_per_chunk": 45000,
    "chunk_bytes": 262144,
    "utia_per_gas": 1,
    "note": "fee = (base_gas + gas_per_chunk × ⌈blob_size / chunk_bytes⌉) × utia_per_gas; the same charge whether the promise is settled or timed out"
  }
}`;

export const tip = `{
  "height": 1228475,
  "block_time": "2026-09-29T16:10:16.841850249Z",
  "fibre_active": true,
  "server_time": "2026-09-29T16:10:22.180934396Z"
}`;

export const validators = `{
  "window": {"name": "24h"},
  "computed_at": "2026-09-29T16:03:50.37406568Z",
  "validators": [
    {
      "address": "f345f91cd3c36238f550a024800c0a2cd0d7d49c",
      "moniker": "devops",
      "jailed": false,
      "bond_status": "BOND_STATUS_BONDED",
      "voting_power": 43855666,
      "endpoint_state": "reachable",
      "signing": {"assigned": 5696, "signed": 3764},
      "hosting": {"country": "FR", "provider": "Scaleway"}
    }
  ]
}`;

export const hosting = `{
  "summary": {
    "registered_hosts": 72,
    "basis": "stake",
    "by_provider": [
      {
        "key": "Other",
        "hosts": 22,
        "stake_share": 0.2281931466373274
      },
      {
        "key": "Scaleway",
        "hosts": 3,
        "stake_share": 0.1497057868521188
      },
      {
        "key": "Cherry Servers",
        "hosts": 4,
        "stake_share": 0.1409126191117233
      }
    ],
    "nakamoto_third": {
      "provider": {
        "count": 3,
        "entities": ["Scaleway", "Cherry Servers", "Hetzner"],
        "share": 0.4038740921531743
      }
    }
  },
  "sources": {
    "country_db": {
      "name": "DB-IP IP to Country Lite",
      "license": "CC BY 4.0",
      "attribution": "IP Geolocation by DB-IP"
    }
  }
}`;

export const market = `{
  "window": {"name": "7d"},
  "settlements": 8625,
  "blobs": 8623,
  "fees_settled_utia": 30244200000,
  "bytes": 143526461440,
  "publishers_active": 7,
  "paid_per_mib_utia": 220958.15601541524,
  "timeouts": 0,
  "escrow_total_utia": 207465000,
  "daily": [
    {
      "day": "2026-09-28",
      "fees_utia": 29896305000,
      "bytes": 142079164416,
      "settlements": 8472,
      "timeouts": 0,
      "timed_out_utia": 0
    }
  ],
  "top_publishers": [
    {
      "publisher": "celestia1las83d0dt9gew3faq2mxp2gtupq5drclee9snr",
      "fees_utia": 29888510000,
      "fees_share": 0.9882393979672135,
      "bytes_share": 0.9897316943982758
    }
  ]
}`;

export const signing = `{
  "window": {"name": "7d"},
  "threshold": {"num": 2, "den": 3},
  "promises": 8625,
  "meets_threshold": {"num": 8625, "den": 8625, "value": 1},
  "buckets": [
    {"key": "below", "label": "below ⅔", "count": 0},
    {"key": "q_70", "label": "⅔ – 70%", "count": 6250},
    {"key": "70_75", "label": "70 – 75%", "count": 1704},
    {"key": "75_80", "label": "75 – 80%", "count": 671}
  ],
  "signers_median": 53
}`;

export const network = `{
  "window": {"name": "7d"},
  "registered_endpoints": 72,
  "reachability": {"num": 71, "den": 72, "value": 0.9861111111111112},
  "reachability_window": {"num": 77945, "den": 78275, "value": 0.9957840945384862},
  "obligations": {
    "served": 284544,
    "broken": 0,
    "not_counted": 174576,
    "rate": {"num": 284544, "den": 284544, "value": 1}
  },
  "reconstructable": {
    "recoverable": {"num": 1999, "den": 1999, "value": 1},
    "not_read": 191,
    "publications_examined": 2000,
    "sample_limit": 2000
  }
}`;

export const exports = `{
  "exports": [
    {
      "name": "tensile-ut-1-2026-09-28.tar.gz",
      "bytes": 598241098,
      "sha256": "c50c0201f5e1bdfc4bd9149164998d9afb6f5a5e821d0e5482a568c951d43b6c",
      "day": "2026-09-28",
      "methodology_version": "2026-09-27.1",
      "signature": {
        "algorithm": "ed25519",
        "manifest_sha256": "631b6982582ee5d0d22e3183ca870fda814c536100b915ab8c0c4565ac5ac680",
        "key_fingerprint": "sha256:8eb4c98fd59e067ce8651f0edec06bcccc6d92b49b2ddd8161929ec3826a5412"
      }
    }
  ],
  "signing": {
    "signed": true,
    "current": {
      "algorithm": "ed25519",
      "key_fingerprint": "sha256:8eb4c98fd59e067ce8651f0edec06bcccc6d92b49b2ddd8161929ec3826a5412"
    }
  }
}`;

// one entry of a validator's Atom feed, as the site serves it
export const feed = `<entry>
  <id>tag:tensile.huginn.tech,2026:tensile/mocha-5/96a4f561df2c45a2fe0607f7a8e1c493251aedec/first-reachable</id>
  <title type="text">PPBCN Validator: Fibre endpoint reachable for the first time</title>
  <updated>2026-09-25T16:57:31Z</updated>
  <published>2026-09-25T16:57:31Z</published>
  <link rel="alternate" type="text/html" href="/validator/?addr=96a4f561df2c45a2fe0607f7a8e1c493251aedec"></link>
  <category term="first-reachable"></category>
  <summary type="text">The first heartbeat on record that completed a TLS handshake with this validator&#39;s registered Fibre host.</summary>
</entry>`;
