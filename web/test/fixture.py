#!/usr/bin/env python3
"""Generate a design-evaluation fixture for the Fibre observer store.

Purpose: the devnet has four validators and two publications, which is far too
little to judge a dashboard on. Real questions the design must survive -- does a
moniker column hold a 34-character name, does the worst-first sort surface the
right rows, does an eleven-class legend stay readable when nine of the classes
are actually present -- only show up at scale.

This writes into a COPY of the schema the real store creates, never into a live
database, and every row is synthetic. It is a local design aid; nothing it
produces is ever published.
"""
import hashlib, json, os, random, shutil, sqlite3, sys
from datetime import datetime, timedelta, timezone

# The schema is created by the real collector rather than copied from a
# database lying around, so this can never drift from the shipped migrations:
# `observer-collector -once` applies the baseline and every migration, then
# exits. Point OBSERVER_COLLECTOR at the binary if it is not on PATH or in the
# repo's bin directory.
def make_schema(path):
    import subprocess
    here = os.path.dirname(os.path.abspath(__file__))
    candidates = [os.environ.get("OBSERVER_COLLECTOR"),
                  os.path.join(here, "..", "..", "fibre-sentinel", "bin", "observer-collector"),
                  shutil.which("observer-collector")]
    binary = next((c for c in candidates if c and os.path.exists(c)), None)
    if not binary:
        sys.exit("observer-collector not found; run `make build` or set OBSERVER_COLLECTOR")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # An unreachable RPC is deliberate: the collector logs the failure, applies
    # the schema and exits 0. Nothing here should touch a real chain.
    subprocess.run([binary, "-once", "-db", path, "-data-dir", os.path.dirname(path),
                    "-rpc", "http://127.0.0.1:1"], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=120)

OUT = sys.argv[1] if len(sys.argv) > 1 else "/tmp/fibre-fixture/observer.db"
rnd = random.Random(51)   # CIP-51. Deterministic: same fixture every run.

# Every timestamp is placed relative to NOW, and the API reads its windows off
# the real clock, so NOW has to be the moment the fixture is generated. It used
# to be a fixed date, and a week later the 24h and 7d windows were empty and
# every page rendered its "nothing in this period" state over a full store.
# FIXTURE_NOW (RFC 3339, e.g. 2026-09-16T10:30:00Z) pins it when two runs have
# to be compared byte for byte; the random draws are seeded either way, so an
# unpinned run differs from another only by where its clock started.
def _now():
    pinned = os.environ.get("FIXTURE_NOW", "").strip()
    if pinned:
        return datetime.fromisoformat(pinned.replace("Z", "+00:00")).astimezone(timezone.utc)
    # Whole minutes, so the heartbeats land on the same seconds as the
    # schedule a real prober keeps.
    return datetime.now(timezone.utc).replace(second=0, microsecond=0)
NOW = _now()
def ts(dt): return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

# Moniker set chosen for SHAPE, not realism: the design has to survive a
# three-character name and a thirty-four-character one in the same column, plus
# non-ASCII, digits, punctuation and inconsistent casing, because real validator
# sets contain all of those.
MONIKERS = [
    "P2P", "Kiln", "Figment", "Chorus One", "Everstake", "Luganodes",
    "Stakin Institutional Operations", "Cosmostation", "Imperator.co",
    "polkachu", "NODEJUMPER", "Lavender.Five Nodes 🐝", "StakeLab",
    "Nodes.Guru", "Forbole", "Citadel.one", "A41", "DSRV", "B-Harvest",
    "Stakewolle | Community Node", "Enigma", "宇宙节点 Cosmos Node",
    "01node", "Blockscope", "Crosnest", "Golden Ratio Staking", "Huginn Tech",
    "KalpaTech", "Lunar Digital Assets", "Mandragora", "Nocturnal Labs",
    "Ontropy", "Orbital Command", "POSTHUMAN ꝏ DVS", "Qubelabs", "RHINO",
    "Simply Staking", "SmartStake", "Stake&Relax 🦥", "Staketab", "Synergy",
    "TheGrandSlam", "Umbrella ☔", "Validatus", "WhisperNode 🤐", "Witval",
    "ZKV", "brochain", "chainflow", "cryptech", "d-collective", "easy2stake",
    "freshSTAKING", "genznodes", "hashkey", "itrocket", "jabbey", "kjnodes",
    "moonli.me", "nodeist",
]
N = len(MONIKERS)

def cons_addr(i):
    return hashlib.sha256(f"fixture-cons-{i}".encode()).hexdigest()[:40]

# Real bech32, because the API validates publisher addresses before it looks
# them up and a made-up string would 400 on the publisher page. Twenty bytes
# drawn from the seed, encoded the way the SDK does (BIP-173).
B32 = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
def _b32_polymod(values):
    g = [0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3]
    chk = 1
    for v in values:
        b = chk >> 25
        chk = ((chk & 0x1ffffff) << 5) ^ v
        for i in range(5):
            chk ^= g[i] if (b >> i) & 1 else 0
    return chk
def _b32_hrp_expand(hrp):
    return [ord(c) >> 5 for c in hrp] + [0] + [ord(c) & 31 for c in hrp]
def _convertbits(data, frombits, tobits):
    acc, bits, ret, maxv = 0, 0, [], (1 << tobits) - 1
    for v in data:
        acc = (acc << frombits) | v
        bits += frombits
        while bits >= tobits:
            bits -= tobits
            ret.append((acc >> bits) & maxv)
    if bits:
        ret.append((acc << (tobits - bits)) & maxv)
    return ret
def bech32(prefix, raw20):
    data = _convertbits(raw20, 8, 5)
    poly = _b32_polymod(_b32_hrp_expand(prefix) + data + [0] * 6) ^ 1
    chk = [(poly >> 5 * (5 - i)) & 31 for i in range(6)]
    return prefix + "1" + "".join(B32[d] for d in data + chk)
def bech32ish(prefix, i):
    return bech32(prefix, hashlib.sha256(f"account-{i}".encode()).digest()[:20])

# --- validator population -------------------------------------------------
# Voting power follows a steep power law, the way a real active set does: the
# top validator holds roughly two orders of magnitude more than the tail. That
# matters because assigned rows scale with power, so the table's "rows" column
# spans a wide range and the design has to keep it aligned.
vals = []
for i in range(N):
    power = int(9_000_000 * (0.90 ** i)) + rnd.randint(1000, 60_000)
    vals.append({
        "i": i, "moniker": MONIKERS[i], "cons": cons_addr(i),
        "operator": bech32ish("celestiavaloper", i),
        # The same twenty bytes as `cons`: endpoint rows are keyed by the
        # bech32 form and probe rows by the hex form, and the API joins them.
        "consbech": bech32("celestiavalcons", bytes.fromhex(cons_addr(i))),
        "power": power,
        "jailed": i in (37, 52),
        "identity": hashlib.sha256(f"kb{i}".encode()).hexdigest()[:16].upper(),
        "host": f"fibre{i:02d}.example.net:7980",
    })
total_power = sum(v["power"] for v in vals)

# Behaviour classes. Deliberately skewed to healthy: an observer whose fixture
# is half-broken teaches the designer to optimise for a network that does not
# exist. The interesting cases are rare and must still be findable.
# hours before NOW at which each impairment began; None means "always".
OUTAGE_SINCE = {5: 30.0, 38: 9.0, 28: 40.0, 33: 18.0}

def impaired(i, at):
    """True if validator i's impairment was already in effect at time `at`."""
    if i not in OUTAGE_SINCE:
        return True
    since = OUTAGE_SINCE[i]
    if since is None:
        return True
    return at >= NOW - timedelta(hours=since)

BEHAVIOUR = {}
for v in vals:
    i = v["i"]
    if i == 23:                 BEHAVIOUR[i] = "faulty"        # real, repeated FAULT
    elif i in (14, 44):         BEHAVIOUR[i] = "flaky"         # occasional FAULT
    elif i in (5, 38):          BEHAVIOUR[i] = "unreachable"   # outage
    elif i == 47:               BEHAVIOUR[i] = "unregistered"  # no host on chain
    elif i == 28:               BEHAVIOUR[i] = "identity"      # certificate lapsed
    elif i == 41:               BEHAVIOUR[i] = "unattested"    # never signed
    elif i == 33:               BEHAVIOUR[i] = "prunes_early"  # drops late in window
    elif i == 19:               BEHAVIOUR[i] = "slow"          # serves everything, slowly
    elif i == 31:               BEHAVIOUR[i] = "erroring"      # reachable, answers 500 to every download
    elif i == 26:               BEHAVIOUR[i] = "throttling"    # reachable, rate-limits most downloads
    elif i in (37, 52):         BEHAVIOUR[i] = "jailed"
    else:                       BEHAVIOUR[i] = "healthy"

shutil.rmtree(os.path.dirname(OUT), ignore_errors=True)
make_schema(OUT)
db = sqlite3.connect(OUT)
db.execute("PRAGMA journal_mode=DELETE")
for t in ("publications","assignments","probes","reachability","endpoints",
          "validator_identities","observer_runs","meta","params_history","ingest_cursors"):
    db.execute(f"DELETE FROM {t}")

PAYMENT_TIMEOUT, RETENTION = 3600, 14400   # 1h / 4h, the spec's default shape
# One day, as the rows below have always used; params_history says the same,
# so a delay computed from the queue matches the parameter it ran under.
WITHDRAWAL_DELAY = timedelta(days=1)
JAILED_AT = NOW - timedelta(hours=6)       # when the jailed validators left the bonded list
MSU = max(PAYMENT_TIMEOUT, RETENTION)

# Every INSERT below names its columns. Positional inserts broke each time a
# migration added a column (params_history gained effective_from_time and
# escrow_accounts the withdrawal read in schema 21), and the failure was a
# crash at generation rather than anything that pointed at the migration.
db.execute("""INSERT INTO params_history
    (effective_from_height, effective_from_tx_index, source, withdrawal_delay_s,
     payment_promise_timeout_s, payment_promise_height_window, shard_retention_s,
     full_stake_storage_budget, effective_from_time)
    VALUES (?,?,?,?,?,?,?,?,?)""",
           (1, 0, "genesis", int(WITHDRAWAL_DELAY.total_seconds()), PAYMENT_TIMEOUT, 100, RETENTION, 1<<40,
            ts(NOW - timedelta(days=60))))

for v in vals:
    db.execute("""INSERT INTO validator_identities
        (cons_address, operator_address, moniker, identity, website, tokens,
         jailed, status, first_seen_at, updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (v["cons"], v["operator"], v["moniker"], v["identity"],
         f"https://{v['moniker'].split()[0].lower().strip('.')}.example",
         v["power"], 1 if v["jailed"] else 0,
         "BOND_STATUS_BONDED", ts(NOW - timedelta(days=30)), ts(NOW)))
    if BEHAVIOUR[v["i"]] == "jailed":
        # Jailing drops the provider from the bonded list while the chain
        # keeps the x/valaddr entry: the endpoint row closes with the reason
        # the collector records, and heartbeats stop from that moment.
        db.execute("""INSERT INTO endpoints
            (validator_cons_address, host, first_seen_at, first_seen_height,
             last_seen_at, last_seen_height, closed_at, closed_height, closed_reason)
            VALUES (?,?,?,?,?,?,?,?,'left_bonded_provider_list')""",
            (v["consbech"], v["host"], ts(NOW - timedelta(days=30)), 100,
             ts(JAILED_AT), 899_000, ts(JAILED_AT), 899_000))
    elif BEHAVIOUR[v["i"]] != "unregistered":
        db.execute("""INSERT INTO endpoints
            (validator_cons_address, host, first_seen_at, first_seen_height,
             last_seen_at, last_seen_height, closed_at, closed_height, closed_reason)
            VALUES (?,?,?,?,?,?,NULL,NULL,'')""",
            (v["consbech"], v["host"], ts(NOW - timedelta(days=30)), 100,
             ts(NOW), 900_000))

# --- publications ---------------------------------------------------------
# Spread over seven days so the 24h / 7d / 30d window switcher has something to
# switch between, and so a "last 24h" figure differs from an all-time one.
# Publications to write. The default is enough to judge a design; set it higher
# to measure how the API scales — reconstructSample caps the network summary at
# 2000, so that is the number that shows whether an endpoint holds up.
PUBS = int(os.environ.get("FIXTURE_PUBS", "260"))
pubs = []
for p in range(PUBS):
    age_h = rnd.random() ** 1.7 * 168          # skewed toward recent
    created = NOW - timedelta(hours=age_h)
    settled = created + timedelta(seconds=rnd.randint(2, 20))
    msu = created + timedelta(seconds=MSU)
    # Blob sizes cluster small with a long tail, like real DA traffic.
    size = rnd.choice([128, 256, 512] * 6 + [1024, 2048] * 3 + [4096, 8192, 16384]) * 1024
    ph = hashlib.sha256(f"promise-{p}".encode()).hexdigest()
    cm = hashlib.sha256(f"commit-{p}".encode()).hexdigest()
    ns = "00000000000000000000000000000000000000" + hashlib.sha256(
        f"ns-{p % 9}".encode()).hexdigest()[:26]
    # Assignment: clamp(ceil(4096 * power * 3 / total), 148, 4096), the real rule.
    import math
    assigned = []
    for v in vals:
        if BEHAVIOUR[v["i"]] == "unregistered" and rnd.random() < 0.5:
            pass
        rows = min(4096, max(148, math.ceil(4096 * v["power"] * 3 / total_power)))
        assigned.append((v, rows))
    sigma = sum(r for _, r in assigned)
    # Attestation: a validator marked "unattested" is missing from the signature
    # set, which is exactly the case the taxonomy must hold out of the rate.
    # A validator with no registered Fibre host never received an upload, so it
    # cannot have signed the promise. Classify still calls it NOT_REGISTERED
    # (OutcomeNoHost is judged before the attestation check in classify.go), but
    # it is correctly absent from the signature set, which is what keeps it out
    # of the reconstructability denominator.
    def could_attest(v):
        b = BEHAVIOUR[v["i"]]
        if b in ("unattested", "unregistered"):
            return False
        if b in ("unreachable", "identity") and impaired(v["i"], created):
            return False   # already dark at upload: it never got the shard
        return True

    # The two-thirds quorum, modelled rather than assumed away.
    #
    # The publisher uploads to everyone at once and collects signatures as they
    # arrive, stopping the moment it holds two thirds of total voting power
    # (fibre/validator/signature_set.go). Deliveries already in flight finish
    # afterwards, off chain, where nothing records them. So on a real network a
    # third of the set has no signature on any given blob and is neither down
    # nor at fault, and a fixture where everyone signs hides the single class
    # most likely to be misread on the dashboard.
    #
    # Arrival order is drawn per publication and is independent of stake, which
    # is what makes quorum membership a race rather than a list: the same
    # validator is inside on one blob and outside on the next.
    order = [v for v, _ in assigned]
    random.Random(f"quorum-{p}").shuffle(order)
    need, got, stopped = total_power * 2 // 3, 0, False
    attested = {}
    for v in order:
        ok = (not stopped) and could_attest(v)
        attested[v["cons"]] = ok
        if ok:
            got += v["power"]
            if got >= need:
                stopped = True
    sigcount = sum(1 for a in attested.values() if a)
    pubs.append(dict(ph=ph, cm=cm, ns=ns, created=created, settled=settled,
                     msu=msu, size=size, assigned=assigned, attested=attested,
                     # one blob in forty is lost: no validator serves it, so it is
                     # unavailable and its endorsing validators are not served
                     lost=rnd.random() < 0.025))
    att_rows = sum(r for v, r in assigned if attested[v["cons"]])
    att_power = sum(v["power"] for v, _ in assigned if attested[v["cons"]])
    db.execute("""INSERT INTO publications (
        promise_hash, commitment, blob_version, blob_size, namespace, chain_id,
        promise_height, creation_timestamp, signer, signer_public_key,
        validator_signature_count, settlement_height, settlement_time,
        settlement_tx_hash, settlement_tx_index, settlement_tx_code,
        must_serve_until, must_serve_until_basis, shard_retention_s,
        payment_promise_timeout_s, assignment_error,
        protocol_params_fingerprint, pinned_celestia_app, validator_set_height,
        total_voting_power, sigma_rows, distinct_rows, wrap_overlaps,
        validators_with_rows, recorded_at, raw_json, attested_with_rows,
        attested_voting_power, signature_entries, signatures_verified,
        signatures_unmatched, signatures_out_of_position
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
        ph, cm, 0, size, ns, "mocha-5", 800_000 + p, ts(created),
        bech32ish("celestia", p % 17), hashlib.sha256(f"pk{p}".encode()).hexdigest(),
        sigcount, 800_000 + p + 1, ts(settled),
        hashlib.sha256(f"tx{p}".encode()).hexdigest().upper(), 0, 0,
        ts(msu), "shard_retention", RETENTION, PAYMENT_TIMEOUT, "",
        "fp", "fa5b523b", 800_000 + p, total_power, sigma, min(sigma, 16384), 0,
        len(assigned), ts(settled), json.dumps({"assignment": {"protocol_params": {"original_rows": 4096, "total_rows": 16384}}}),
        sum(1 for v, _ in assigned if attested[v["cons"]]), att_power,
        sigcount, sigcount, 0, 0))
    cursor = 0
    idxs = {}
    for v, rows in assigned:
        idx = [(cursor + k) % 16384 for k in range(rows)]
        cursor = (cursor + rows) % 16384
        idxs[v["cons"]] = idx
        db.execute("""INSERT INTO assignments
            (promise_hash, validator_address, voting_power, row_count, rows_json, attested, settlement_height)
            VALUES (?,?,?,?,?,?,?)""",
            (ph, v["cons"], v["power"], rows, json.dumps(idx), 1 if attested[v["cons"]] else 0,
             # the publication's settlement height, as the store copies it
             # (migration 22): the validators table finds each one's newest
             # assignment by it
             800_000 + p + 1))
    pubs[-1]["idx"] = idxs

# --- readings -------------------------------------------------------------
# Each blob is read once, 10 minutes before must_serve_until, as the prober
# reads it: every validator with rows, endorsing or not, largest stake first,
# until 4096 distinct rows came back; when they did not, everyone who did not
# serve is asked again, and the blob is unavailable if the rows are still
# short. A validator that did not endorse is asked like the rest, and its rows
# count toward the blob; it is never counted either way.
READ_OFFSET = timedelta(minutes=10)
K = 4096
counts = {}
# Probe rows are buffered and inserted in start-time order for the same reason
# the heartbeats are: rowid order stands in for time in several of the API's
# "latest row per validator" queries, and a fixture that inserts out of order
# makes those queries answer with an arbitrary row.
PROBE_ROWS = []

# How long a probe took, in milliseconds.
#
# The old fixture wrote a flat 400 for every probe, which made the median and
# the 95th percentile the same number for every validator and the latency
# columns untestable. A real duration is dominated by the transfer, so it scales
# with the validator's assigned rows, sits on a per-validator floor (its own
# path and disk), and has a long right tail: most probes are near the floor and
# a few are several times it. utku's measurement against a local devnet — p50
# ~14ms, p95 ~24ms for dial plus DownloadShard plus full row verification — is
# the shape this imitates, scaled up for a public network.
def duration_ms(v, rows, ok):
    if not ok:
        return rnd.randint(20, 250)          # a failure takes time too
    base = 25 + (v["i"] % 7) * 6             # this validator's own floor
    if BEHAVIOUR[v["i"]] == "slow":
        base *= 9
    transfer = rows * 0.42                   # dominated by the shard's size
    jitter = rnd.random() ** 4 * base * 12   # long tail, rarely hit
    return int(base + transfer + jitter) + 1

def add_probe(pub, v, rows, label, at, phase, outcome, cls, **kw):
    if at > NOW:
        return
    counts[cls] = counts.get(cls, 0) + 1
    # rows that came back and verified: an endorser's, or one that owes nothing
    ok = cls == "HEALTHY" or (cls == "UNATTESTED" and outcome == "SERVED_OK")
    idx = json.dumps(pub["idx"][v["cons"]]) if ok else None
    key = hashlib.sha256(f"{pub['ph']}{v['cons']}{label}".encode()).hexdigest()
    ms = kw["ms"] if "ms" in kw else duration_ms(v, rows, ok)
    PROBE_ROWS.append((at, (
        key, "eu1", pub["ph"], pub["cm"], 0, ts(pub["msu"]), 800_000,
        v["cons"], kw.get("host", v["host"]), 1, rows, label, ts(kw.get("sched", at)), ts(at),
        ts(at + timedelta(milliseconds=ms)), rnd.randint(0, 900),
        kw.get("dns", 1), rnd.randint(1, 30), kw.get("tcp", 1), rnd.randint(4, 60),
        kw.get("tls", 1), rnd.randint(8, 90), "TLS1.3" if kw.get("tls", 1) else "",
        hashlib.sha256(f"cert{v['i']}".encode()).hexdigest() if kw.get("tls", 1) else "",
        kw.get("idok", 1), kw.get("idreason", ""), 1 if ok else 0,
        int(ms * 0.8) if ok else 0, rows if ok else kw.get("got", 0), rows,
        1 if ok or kw.get("cv") else 0, 1 if ok else 0,
        phase, outcome, cls, kw.get("reason", ""), kw.get("err", ""),
        ms, "{}", kw.get("attested", 1),
        # 512 B a row. One record in fifty predates the byte count, as every
        # record on a store from before schema 8 does; it must fall out of the
        # throughput sample and never read as zero bytes.
        (rows * 512) if ok and rnd.random() >= 0.02 else None, idx)))

# What actually happened on the wire for validator v at time `at`, independent
# of whether the chain proves it was obliged. Attestation decides the class, not
# the wire, and keeping the two separate here is what stops the fixture from
# recording a clean TLS handshake for an endpoint that was refusing
# connections: the heartbeat and the probe history are two views of the same
# minute, and they have to agree.
def wire(b, v, at, frac):
    if b == "unreachable" and impaired(v["i"], at):
        return ("TCP_REFUSED", dict(tcp=0, tls=0, idok=0, ms=0,
                                    err=f"dial tcp {v['host']}: connect: connection refused"))
    if b == "identity" and impaired(v["i"], at):
        # The reason is the verifier's machine code (fibre-tlsverify/errors.go),
        # which is what the prober stores. A prose reason fell through the API's
        # identityStatus switch to "mismatch" and put a lapsed renewal on the
        # page as another validator's key.
        return ("IDENTITY_FAIL", dict(idok=0, idreason="cert_expired"))
    if b == "prunes_early" and impaired(v["i"], at):
        return ("NOT_FOUND", {})
    if b == "erroring":
        return ("SERVER_ERROR", dict(err="rpc error: code = Internal desc = shard store: read failed"))
    if b == "throttling" and rnd.random() < 0.8:
        return ("RPC_THROTTLED", dict(err="rpc error: code = ResourceExhausted desc = too many requests"))
    if frac == "lost":
        return ("NOT_FOUND", {})
    if b == "faulty" and rnd.random() < 0.30:
        return ("NOT_FOUND", {})
    if b == "flaky" and rnd.random() < 0.05:
        return ("NOT_FOUND", {})
    if rnd.random() < 0.003:
        return ("TCP_TIMEOUT", dict(tcp=0, tls=0, idok=0, err="dial tcp: i/o timeout"))
    return ("SERVED_OK", {})

# A port of probe.Classify, branch for branch, over the outcomes this fixture
# emits. Written out rather than approximated because the ORDER is the whole
# point: identity is judged before attestation, so an unattested validator with
# a lapsed certificate is IDENTITY_EXPIRED and not UNATTESTED, and a fixture
# that gets that backwards teaches the design to expect a class mix the real
# taxonomy never produces.
REACH_FAIL = {"DNS_FAIL", "TCP_REFUSED", "TCP_TIMEOUT", "TCP_UNREACHABLE",
              "TLS_HANDSHAKE_FAIL", "RPC_UNAVAILABLE", "RPC_ERROR", "RPC_TIMEOUT"}

def classify(outcome, phase, attested):
    if outcome == "IDENTITY_FAIL":
        return "IDENTITY_EXPIRED"            # this fixture only lapses certificates
    if outcome == "NO_REGISTERED_HOST":
        return "NOT_REGISTERED"
    if not attested:
        return "UNATTESTED"
    if phase == "in_window":
        if outcome == "SERVED_OK":     return "HEALTHY"
        if outcome == "NOT_FOUND":     return "FAULT"
        if outcome == "SERVER_ERROR":  return "SERVER_ERROR"
        if outcome == "RPC_THROTTLED": return "THROTTLED"
        if outcome in REACH_FAIL:      return "UNREACHABLE"
        return "PROBE_ERROR"
    return "PROBE_ERROR"

REASONS = {
    "UNATTESTED": "the settled promise carries no verified signature from this validator",
    "FAULT": "no such shard while the promise still held",
    "NOT_REGISTERED": "no Fibre host registered in x/valaddr when the probe ran",
    "SERVER_ERROR": "the endpoint answered with an application error instead of the shard",
    "THROTTLED": "the endpoint refused the download with a rate limit",
}

def emit(pub, v, rows, label, at, phase, outcome, kw, attested):
    cls = classify(outcome, phase, attested)
    kw = dict(kw, attested=1 if attested else 0)
    if cls in REASONS:
        kw.setdefault("reason", REASONS[cls])
    add_probe(pub, v, rows, label, at, phase, outcome, cls, **kw)

for pub in pubs:
    at = pub["msu"] - READ_OFFSET
    endorsing = sorted((v for v, _ in pub["assigned"] if pub["attested"][v["cons"]]), key=lambda v: -v["power"])
    everyone = sorted((v for v, _ in pub["assigned"]), key=lambda v: -v["power"])
    rows_of = {v["cons"]: r for v, r in pub["assigned"]}
    # A lost blob: its largest endorsing validators lost the shard, until the
    # rest hold too few rows to rebuild it, and the validators that did not
    # endorse never stored it. Fewer than half of the endorsers, so the reading
    # is not taken for Tensile's own failure and set aside.
    lost = set(v["cons"] for v in everyone if pub["lost"] and not pub["attested"][v["cons"]])
    if pub["lost"]:
        for v in endorsing:
            if 2 * (len(lost & {x["cons"] for x in endorsing}) + 1) >= len(endorsing):
                break
            lost.add(v["cons"])
            rest = set().union(*(pub["idx"][x["cons"]] for x in endorsing if x["cons"] not in lost))
            if len(rest) < K:
                break
    have, answers = set(), []
    for n, v in enumerate(everyone):
        if len(have) >= K:
            break                                  # the rest are not asked
        t = at + timedelta(milliseconds=40 * n)
        outcome, kw = wire(BEHAVIOUR[v["i"]], v, t, "lost" if v["cons"] in lost else 0.9)
        answers.append((v, t, outcome, kw))
        if outcome == "SERVED_OK":
            have.update(pub["idx"][v["cons"]])
    if len(have) < K:
        # the second pass, a minute later, of those that did not serve
        again = []
        for v, t, outcome, kw in answers:
            if outcome != "SERVED_OK":
                t = t + timedelta(minutes=1)
                outcome, kw = wire(BEHAVIOUR[v["i"]], v, t, "lost" if v["cons"] in lost else 0.9)
                if outcome == "SERVED_OK":
                    have.update(pub["idx"][v["cons"]])
            again.append((v, t, outcome, kw))
        answers = again
    for v, t, outcome, kw in answers:
        # every row of a reading carries the reading's own time, as the prober writes it
        emit(pub, v, rows_of[v["cons"]], "end", t, "in_window", outcome, dict(kw, sched=at), pub["attested"][v["cons"]])

PROBE_ROWS.sort(key=lambda r: r[0])
db.executemany("""INSERT INTO probes (
    dedupe_key, vantage, promise_hash, commitment, blob_version,
    must_serve_until, validator_set_height, validator_address,
    validator_host, assigned, assigned_row_count, schedule_label,
    scheduled_at, started_at, finished_at, lateness_ms, dns_ok, dns_ms,
    tcp_ok, tcp_ms, tls_ok, tls_ms, tls_version, peer_cert_sha256,
    identity_ok, identity_reason, download_ok, download_ms, rows_returned,
    rows_expected, commitment_verified, assignment_verified, phase,
    outcome, classification, classification_reason, raw_error,
    total_duration_ms, raw_json, attested, bytes_returned, row_indices
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
    [r[1] for r in PROBE_ROWS])

# --- reachability heartbeats ---------------------------------------------
# The real heartbeat dials every registered endpoint every five minutes without
# downloading, for as long as the endpoint is registered. That is 288 samples a
# day per validator regardless of what the chain assigned or attested, which is
# why it is the one stability figure on the site whose coverage does not depend
# on a quorum — and it is the reason the history is generated here at its true
# cadence over the whole span the fixture covers, rather than as a dozen rows
# an hour deep. Twelve rows put every validator under the twenty-observation
# floor, so the uptime column showed "under floor" for the entire set and the
# design was never actually exercised.
HEARTBEAT_EVERY = timedelta(minutes=5)
HEARTBEAT_SPAN = timedelta(days=7)
# A couple of otherwise healthy endpoints flap, because a fixture where uptime
# is 100.0% or 0.0% and nothing between never shows what a real reading looks
# like.
FLAPPY = {9: 0.004, 26: 0.02}
beats = int(HEARTBEAT_SPAN / HEARTBEAT_EVERY)
for v in vals:
    i = v["i"]
    b = BEHAVIOUR[i]
    if b == "unregistered":
        continue
    flap = random.Random(f"flap{i}")
    # Oldest first. The API finds the latest heartbeat with MAX(rowid) GROUP BY
    # validator rather than a correlated MAX(started_at), because the real
    # collector ingests in write order and write order is chronological. A
    # fixture that inserts newest-first quietly hands every reader the OLDEST
    # heartbeat as "reachable (latest)", which is how a validator that had been
    # down for thirty hours came out of the API as reachable: yes.
    for r in reversed(range(beats)):
        at = NOW - r * HEARTBEAT_EVERY
        if b == "jailed" and at >= JAILED_AT:
            continue
        dark = b == "unreachable" and impaired(i, at)
        up = not dark and not (i in FLAPPY and flap.random() < FLAPPY[i])
        # The certificate is endorsed unless this validator's endorsement has
        # lapsed, and it only lapsed from its own start time onward.
        endorsed = up and not (b == "identity" and impaired(i, at))
        db.execute("""INSERT INTO reachability
            (dedupe_key, vantage, validator_address, validator_host, height, scheduled_at,
             started_at, dns_ok, tcp_ok, tcp_ms, tls_ok, tls_ms, peer_cert_sha256,
             identity_ok, identity_reason, outcome, raw_error, total_duration_ms, raw_json)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
            hashlib.sha256(f"reach{i}-{r}".encode()).hexdigest(), "eu1",
            v["cons"], v["host"], 900_000 - r, ts(at), ts(at),
            1, 1 if up else 0, rnd.randint(4, 40), 1 if up else 0, rnd.randint(8, 90),
            hashlib.sha256(f"cert{i}".encode()).hexdigest() if up else "",
            1 if endorsed else 0,
            "cert_expired" if (up and not endorsed) else "",
            "OK" if up else "TCP_REFUSED",
            "" if up else f"dial tcp {v['host']}: connect: connection refused",
            rnd.randint(20, 200), "{}"))

# --- payments: the escrow side of x/fibre ----------------------------------
# One settlement per publication at the module's own charge, so the Publishers
# page shows the fee curve a real network has: many small blobs paying ~0.7 TIA,
# a few large ones paying tens. Seventeen publishers (the signer set above),
# skewed by drawing most publications from a handful of them, so the share bar
# has a "top 5 + other" shape. A few timeouts, submitted by validator operator
# accounts, and one deposit and one withdrawal per publisher, so every kind the
# API knows is present.
def fee(size):
    return 650_000 + 45_000 * ((size + 262_143) // 262_144)

def payment_row(key, kind, height, at, txh, pub, proc, ph, ns, size, amount, avail=None):
    return (key, kind, height, ts(at), txh, 0 if txh else -1, 0, pub, proc, ph, ns, size,
            fee(size) if size else 0, "utia", amount, ts(avail) if avail else None, "{}")

PAYMENTS = []
QUEUE = []
publishers = {}
for p, pub in enumerate(pubs):
    signer = bech32ish("celestia", p % 17)
    publishers.setdefault(signer, NOW - timedelta(days=8))
    txh = hashlib.sha256(f"tx{p}".encode()).hexdigest().upper()
    PAYMENTS.append(payment_row(f"{txh}:0", "settlement", 800_000 + p + 1, pub["settled"], txh,
                                signer, signer, pub["ph"], pub["ns"], pub["size"], fee(pub["size"])))
# Six abandoned promises, reported by three different operator accounts. The
# operator address shares bytes with the validator's valoper address; the
# fixture's addresses are not real bech32, so timeouts_enforced stays zero here
# and is exercised by the Go tests instead.
for k in range(6):
    at = NOW - timedelta(hours=3 + 11 * k)
    signer = bech32ish("celestia", k % 3)
    size = [262144, 1 << 20, 4 << 20][k % 3]
    PAYMENTS.append(payment_row(f"T{k}:0", "timeout", 850_000 + k, at, hashlib.sha256(f"to{k}".encode()).hexdigest().upper(),
                                signer, bech32ish("celestia", 40 + k % 3), hashlib.sha256(f"abandoned{k}".encode()).hexdigest(),
                                "00" * 18 + hashlib.sha256(f"ns{k}".encode()).hexdigest()[:20], size, fee(size)))
for i, (signer, since) in enumerate(publishers.items()):
    PAYMENTS.append(payment_row(f"D{i}:0", "deposit", 790_000 + i, since, hashlib.sha256(f"dep{i}".encode()).hexdigest().upper(),
                                signer, "", "", "", 0, 6_000_000_000))
    if i % 4 == 0:
        at = NOW - timedelta(days=2, hours=i)
        PAYMENTS.append(payment_row(f"W{i}:0", "withdrawal_request", 880_000 + i, at, hashlib.sha256(f"wr{i}".encode()).hexdigest().upper(),
                                    signer, "", "", "", 0, 500_000_000, avail=at + WITHDRAWAL_DELAY))
        PAYMENTS.append(payment_row(f"h{881_000+i}:executed:0", "withdrawal_executed", 881_000 + i, at + WITHDRAWAL_DELAY, "",
                                    signer, "", "", "", 0, 500_000_000))
        # The queue as the collector reads it (store/withdrawals.go): seen
        # while queued, gone at the payout block, and attributed to exactly
        # that payout, which is the only way a request-to-payout delay is
        # ever published.
        QUEUE.append((signer, ts(at), ts(at + WITHDRAWAL_DELAY), "utia", 500_000_000, 500_000_000,
                      880_000 + i, ts(at), 880_900 + i, ts(at + WITHDRAWAL_DELAY - timedelta(minutes=5)),
                      881_000 + i, ts(at + WITHDRAWAL_DELAY), "executed", "",
                      f"h{881_000+i}:executed:0", 881_000 + i, ts(at + WITHDRAWAL_DELAY), 500_000_000, ts(NOW)))
    elif i % 4 == 2:
        # Still queued: requested ten hours ago, payable in fourteen. A
        # fixture whose every withdrawal has already been paid never shows
        # the pending side of the escrow panel.
        at = NOW - timedelta(hours=10 + i)
        PAYMENTS.append(payment_row(f"W{i}:0", "withdrawal_request", 899_000 + i, at, hashlib.sha256(f"wr{i}".encode()).hexdigest().upper(),
                                    signer, "", "", "", 0, 250_000_000, avail=at + WITHDRAWAL_DELAY))
        QUEUE.append((signer, ts(at), ts(at + WITHDRAWAL_DELAY), "utia", 250_000_000, 250_000_000,
                      899_000 + i, ts(at), 900_000, ts(NOW), None, None, None, "", None, None, None, None, ts(NOW)))
db.executemany("""INSERT INTO payments (dedupe_key, kind, height, time, tx_hash, tx_index, msg_index, publisher,
    processor, promise_hash, namespace, blob_size, gas_units, denom, amount_utia, available_at, raw_json)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", PAYMENTS)
db.executemany("""INSERT INTO withdrawal_queue (publisher, requested_at, available_at, denom, first_amount_utia,
    amount_utia, first_seen_height, first_seen_at, last_seen_height, last_seen_at, gone_height, gone_at, outcome,
    outcome_reason, paid_key, paid_height, paid_at, paid_utia, recorded_at)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", QUEUE)
for i, signer in enumerate(publishers):
    spent = sum(r[14] for r in PAYMENTS if r[7] == signer and r[1] in ("settlement", "timeout"))
    withdrawn = sum(r[14] for r in PAYMENTS if r[7] == signer and r[1] == "withdrawal_executed")
    pending = sum(q[5] for q in QUEUE if q[0] == signer and q[10] is None)
    bal = 6_000_000_000 - spent - withdrawn
    found = i != 16   # one publisher emptied and closed its escrow
    # available is balance minus what the queue has locked, and the queue
    # read is taken at the same height as the balance, so the API's
    # balance - available = pending check holds on every row.
    db.execute("""INSERT INTO escrow_accounts (publisher, found, denom, balance_utia, available_utia, height,
        updated_at, withdrawals_height, withdrawals_at, pending_utia) VALUES (?,?,?,?,?,?,?,?,?,?)""",
               (signer, 1 if found else 0, "utia" if found else "", bal if found else 0,
                bal - pending if found else 0, 900_000, ts(NOW), 900_000, ts(NOW), pending if found else 0))

for comp in ("collector", "prober", "api", "heartbeat"):
    db.execute("""INSERT INTO observer_runs
        (component, vantage, version, started_at, last_heartbeat_at, stopped_at, stop_reason)
        VALUES (?,?,?,?,?,NULL,NULL)""",
        (comp, "eu1", "0.1.0", ts(NOW - timedelta(days=7)), ts(NOW)))

# The same meta keys a real collector writes. app_version and fibre_active in
# particular: without them the site cannot tell "Fibre is not live on this
# chain" from "the collector has not reached a node yet", and a fixture that
# omits them renders the pre-activation copy over a store full of measurements.
for k, val in (("chain_id","mocha-5"), ("last_scanned_height","900000"),
               ("chain_height","900000"), ("app_version","10"),
               ("fibre_active","yes"), ("fibre_app_version","10"),
               ("pinned_celestia_app","fa5b523b7e3b2b83bd16bc072a45cbd3819fa369"),
               ("protocol_params_fingerprint","fp")):
    db.execute("INSERT INTO meta VALUES (?,?,?)", (k, val, ts(NOW)))

# Hosting lookups (observer/hosting), as the collector would have stored them
# from the heartbeat's addresses and the two database files. The mix leans on
# Hetzner and OVH the way real validator sets do, so the concentration panel
# has something to say; one host is unresolved and one resolves into two
# networks, the two cases a design must not hide. Addresses are documentation
# ranges; the AS numbers and names are the real ones from providers.go.
HOSTING = [("Hetzner", 24940, "HETZNER-AS", "DE"), ("Hetzner", 24940, "HETZNER-AS", "FI"),
           ("OVH", 16276, "OVH", "FR"), ("AWS", 16509, "AMAZON-02", "US"), ("Google Cloud", 396982, "GOOGLE-CLOUD-PLATFORM", "DE"),
           ("DigitalOcean", 14061, "DIGITALOCEAN-ASN", "NL"), ("Contabo", 51167, "CONTABO", "DE"), ("Vultr", 20473, "AS-VULTR", "JP"),
           ("Other", 64512, "LATITUDE-SH", "US"), ("Other", 64513, "CHERRYSERVERS1-AS", "LT"), ("Other", 64514, "LEASEWEB-NL-AMS-01", "NL")]
hrnd = random.Random(16)
for v in vals:
    if BEHAVIOUR[v["i"]] in ("jailed", "unregistered"):
        continue
    ip = f"203.0.113.{v['i']}"
    if v["i"] == 38:
        row = ("unresolved", "", 0, "", "", "", "", "Unknown", "[]")
    else:
        prov, asn, org, cc = HOSTING[0] if v["i"] < 4 else hrnd.choice(HOSTING + HOSTING[:3])
        addrs = [{"ip": ip, "asn": asn, "as_org": org, "country": cc, "provider": prov, "connected": True}]
        if v["i"] == 11:
            addrs.append({"ip": "2001:db8::11", "asn": 16276, "as_org": "OVH", "country": "FR", "provider": "OVH"})
        row = ("ok", ip, asn, org, cc, cc, "geolocation", prov, json.dumps(addrs))
    db.execute("""INSERT INTO endpoint_hosting (validator_address, host, status, ip, asn, as_org, as_country, country,
        country_basis, provider, addresses_json, resolved_at, resolved_by, looked_up_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (v["cons"], v["host"]) + row + (ts(NOW), "heartbeat", ts(NOW)))
for k, val in (("hosting_enabled", "yes"), ("hosting_asn_db", "ip2asn-combined.tsv.gz"),
               ("hosting_asn_db_modified", ts(NOW - timedelta(days=1))), ("hosting_country_db", "dbip-country-lite.csv.gz"),
               ("hosting_country_db_modified", ts(NOW - timedelta(days=20))), ("hosting_looked_up_at", ts(NOW))):
    db.execute("INSERT OR REPLACE INTO meta VALUES (?,?,?)", (k, val, ts(NOW)))

db.commit()
n = lambda t: db.execute(f"select count(*) from {t}").fetchone()[0]
print(f"validators {n('validator_identities')}  publications {n('publications')}  "
      f"assignments {n('assignments')}  probes {n('probes')}  reach {n('reachability')}  "
      f"payments {n('payments')}  escrow {n('escrow_accounts')}")
print("verdict mix:")
for cls, c in sorted(counts.items(), key=lambda x: -x[1]):
    print(f"  {cls:26} {c:7,}")
db.close()
