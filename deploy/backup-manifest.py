#!/usr/bin/env python3
"""
backup-manifest: a consistent cut of the observer's record, and the proof
that a copy of it came back whole.

The record is append-only JSONL written by several processes, plus
state.json, which the scanner rewrites at every checkpoint. A backup taken
by copying files one after another is not one moment: publications copied
at 03:00:00 and measurements at 03:00:04 disagree about what existed, and a
state.json copied last points past the records copied first. This tool
takes one cut, in this order:

  1. state.json, read whole, first. The scanner fsyncs its record files
     before it replaces state.json, so a record file read after the state
     holds everything the state's checkpoint covers.
  2. the byte length of every record file, dependents before what they
     refer to, each cut back to the end of its last complete line — a
     writer may be in the middle of a line at that instant.
  3. the SHA-256 of exactly those bytes, with every line in them parsed as
     a JSON record and counted. A line that is not one (a writer that died
     mid-line on a full disk, its restart's line glued to the fragment) is
     counted and its number listed (bad_lines, bad_count), not refused:
     the file is append-only, the line stays in it for good, and a cut
     that failed on it would stop every copy after it.

The manifest carries the hashes, the counts, the checkpoint, and the bytes
of state.json as they were. The copy may then be taken at leisure and may
be longer than the cut — the files only grow — and `verify` trims each
restored file back to the manifest's length, checks the hash, parses and
counts the records, and puts the cut's own state.json in place of the
copy's. The copy's state.json was taken later and points past the records
the cut holds; a scanner resuming from it would skip the blocks in between
for good, and pulling only its height back would leave every other field
(gaps, param history, host history, reconcile cursors) from a later
moment. Anything missing, shorter, different or unparseable fails, and so
does a line that is not a record where the cut has none.

A copy whose live file observer-archive rotated after the cut (a night
whose copy stopped part-way, or whose manifest did not reach the remote,
leaves the remote's manifest older than its live files) still holds the
cut: its live bytes are in the segments that rotation wrote and the start
of the newer live file. `verify` reads them back from there into the live
file's place, checked against the cut's hash like any other.

The manifest comes back from the remote with the copy, so `verify` acts
only on the names a cut can hold (RECORD_FILES, and
vantages/<name>/reachability.jsonl under a name vantage-pull accepts), on
segment and export names with no directory part, and on nothing a link
in the copy leads out of it to: it trims and writes the files it names.

The sampling master key must never be in a copy; `verify` fails if it is.

A file observer-archive has rotated is its archive plus its live file:
archive/<file>/ holds gzip segments of the older lines and index.json,
which says where the live file starts in the record (its base) by the
SHA-256 of its first line. The cut carries, per such file, the base and
every segment up to it (name, logical range, lines, digests of the lines
and of the gzip file); `records` stays the live file's own count and
`archived_records` is the rest. The cut and the copy hold archive/.lock
shared, so no rotation happens under them; segments never change once
written. `verify` checks every segment the cut names, whole, and that the
restored index places the live file at the cut's base.

A segment observer-archive -retire has retired has no file on the host:
its bytes are proven to be in the daily exports, and index.json names
the exports that hold them ("retired"). retired.json beside it keeps a
second copy of each such record, which is read when index.json lacks
one: an observer-archive from before retirement rewrites index.json
without the records it does not know, and never touches retired.json.
The cut lists a retired segment with its record and reads nothing of it
from the disk. `verify` checks it by its file when the copy has one, and
otherwise reads its bytes back from the copy's exports by the rule
internal/record reads them by: each named tarball and its member of the
file against the exports' index.json (sizes and SHA-256 digests), the
members covering the segment's range in order without a gap, and the
range's length, lines and SHA-256 against the segment. A segment is
retired only after a backup has copied its file, so the remote has every
retired segment's file as well.
`cat` reads a retired segment the same way, and hands out no byte of it
before all of it is proven; `snapshot` carries the exports it names,
and retired.json.

Each other vantage's heartbeats, vantages/<name>/reachability.jsonl,
are cut like the observer's own files; their archive is
vantages/<name>/archive/, under its own lock.

  write    <data-dir> <manifest.json>      cut + manifest, files untouched
  snapshot <data-dir> <dest-dir>           cut + manifest + trimmed copies
  verify   <restored-dir> [manifest.json]  trim, hash, parse, count, state;
                                           exit 1 on any fault
  show     <manifest.json>                 one line per file
  cat      <data-dir> <file>               the whole record of one file,
                                           archived lines first, to stdout
  end      <data-dir> <file>               its logical length (base + live)
"""
import fcntl
import gzip
import hashlib
import json
import os
import re
import shutil
import sys
import tarfile
import time
import zlib

# Dependents first: a line in measurements.jsonl names a promise that
# publications.jsonl must already hold once both cuts are taken. A file
# whose lines name nothing another file holds goes before payments and
# publications all the same. The list is exactly the record files the
# daily export carries (observer/export.Files; a Go test holds the two to
# each other).
RECORD_FILES = [
    "measurements.jsonl",
    # a publication the prober's load policy sampled out, once (it names a
    # promise, as a measurement does)
    "sampling_decisions.jsonl",
    "reachability.jsonl",
    "amendments.jsonl",
    "corrections.jsonl",
    "sampling-secrets.jsonl",
    "registry.jsonl",
    "runs.jsonl",
    "host_history.jsonl",
    "param_uncertainty.jsonl",
    # the transactions that failed in a block while carrying a Fibre
    # message: no line names a payment or a publication
    "failed_txs.jsonl",
    "payments.jsonl",
    "publications.jsonl",
]
STATE = "state.json"
FORBIDDEN = ["sampling-master.key"]
MANIFEST = "manifest.json"
# 4: a segment may carry "retired", and other vantages' heartbeats are cut
# 5: a file may carry bad_lines and bad_count
VERSION = 5
CHUNK = 1 << 20
ARCHIVE = "archive"
VANTAGES = "vantages"
EXPORTS_INDEX = "index.json"
# beside an archive's index.json: the second copy of its retired records
RETIRED = "retired.json"
# the vantage names vantage-pull accepts, and so the only ones a cut holds
VANTAGE_NAME = re.compile(r"[a-z0-9-]+")
# the lines that are not a record, listed by number up to this many per
# file (bad_count counts them all), so a file of garbage keeps a small
# manifest
BAD_LINES_LISTED = 100


class RecordError(Exception):
    """A record file whose cut cannot be taken or checked: shorter than
    the cut, cut inside a line, or an archive that does not add up."""


def record_name(name):
    """Whether name is one a cut lists: a file of RECORD_FILES, or another
    vantage's heartbeats under a name vantage-pull accepts. verify takes
    the names from a manifest that came back from the remote, and trims and
    writes the files they name: any other name could lead out of the copy
    (an absolute path, a "..")."""
    if name in RECORD_FILES:
        return True
    parts = name.split("/") if isinstance(name, str) else []
    return (len(parts) == 3 and parts[0] == VANTAGES and parts[2] == "reachability.jsonl"
            and VANTAGE_NAME.fullmatch(parts[1]) is not None)


def plain_name(x):
    """A file name with no directory part: a segment's, an export's."""
    return isinstance(x, str) and x not in ("", ".", "..") and not any(c in x for c in "/\\\0")


def inside(root, p):
    """Whether p, every link on its way followed, is under root."""
    r = os.path.realpath(root)
    return os.path.commonpath([os.path.realpath(p), r]) == r


def is_record(line):
    """A line (newline excluded) that parses as a JSON object."""
    try:
        return isinstance(json.loads(line), dict)
    except (ValueError, RecursionError):
        return False


def complete_length(path):
    """The length of the file up to and including its last newline: the
    bytes that hold only complete lines. A writer can be mid-line at the
    instant the cut is taken; those bytes belong to the next cut."""
    size = os.path.getsize(path)
    with open(path, "rb") as f:
        end = size
        while end > 0:
            start = max(0, end - CHUNK)
            f.seek(start)
            b = f.read(end - start)
            i = b.rfind(b"\n")
            if i >= 0:
                return start + i + 1
            end = start
    return 0


def sha_records(name, path, length):
    """SHA-256 of the first `length` bytes, the number of JSON records in
    them, and the lines that are not one: (digest, records, bad, bad_count),
    bad being the first BAD_LINES_LISTED of their line numbers (from 1). A
    line that does not parse as a JSON object (empty, cut short, glued to
    the next) is counted there, not refused: it is in the record's bytes for
    good. The cut must end on a line boundary, and the file must hold
    `length` bytes; anything else raises RecordError."""
    h = hashlib.sha256()
    n = 0
    bad, nbad = [], 0
    left = length
    rest = b""
    with open(path, "rb") as f:
        while left > 0:
            b = f.read(min(CHUNK, left))
            if not b:
                break
            h.update(b)
            left -= len(b)
            lines = (rest + b).split(b"\n")
            rest = lines.pop()
            for line in lines:
                n += 1
                if not is_record(line):
                    nbad += 1
                    if len(bad) < BAD_LINES_LISTED:
                        bad.append(n)
    if left > 0:
        raise RecordError(f"{name}: wanted {length} bytes, file is {length - left}")
    if rest:
        raise RecordError(f"{name}: the cut ends inside record {n + 1}, not on a line boundary")
    return h.hexdigest(), n - nbad, bad, nbad


def bad_label(bad, nbad):
    """The lines that are not records, for a message."""
    if not nbad:
        return "none"
    more = f" and {nbad - len(bad)} more" if nbad > len(bad) else ""
    return f"{nbad} (line {', '.join(str(x) for x in bad)}{more})"


def check_cut(name, info, digest, records, bad, nbad):
    """The problems with one file's bytes read back against its cut: the
    digest, the records, and the lines that are not records, which must be
    the cut's own (none, for a manifest from before they were listed)."""
    if digest != info["sha256"]:
        return [f"{name}: sha256 differs over the first {info['bytes']} bytes (content changed or not this backup)"]
    if records != info["records"]:
        return [f"{name}: {records} records, manifest says {info['records']}"]
    want = info.get("bad_lines") or []
    wantn = info.get("bad_count", len(want))
    if nbad != wantn or bad != want:
        return [f"{name}: lines that are not a JSON record: {bad_label(bad, nbad)}; the manifest says {bad_label(want, wantn)}"]
    return []


def read_state(path):
    try:
        with open(path, "rb") as f:
            return f.read()
    except OSError:
        return None


def checkpoint_of(raw):
    if raw is None:
        return None
    try:
        s = json.loads(raw)
    except ValueError:
        return None
    return {
        "last_scanned_height": int(s.get("last_scanned_height") or 0),
        "last_scanned_time": s.get("last_scanned_time"),
        "gaps": len(s.get("gaps") or []),
    }


def head_sha(path):
    """SHA-256 of the file's first line, newline included; "" when it has
    no complete line. It is how index.json names a live file."""
    h = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            while True:
                b = f.read(CHUNK)
                if not b:
                    return ""
                i = b.find(b"\n")
                if i >= 0:
                    h.update(b[:i + 1])
                    return h.hexdigest()
                h.update(b)
    except FileNotFoundError:
        return ""


def vantage_files(data_dir):
    """Each other vantage's heartbeats, vantages/<name>/reachability.jsonl,
    by name: vantage-pull appends to them, and observer-archive rotates and
    retires them as it does the observer's own files."""
    d = os.path.join(data_dir, VANTAGES)
    try:
        names = sorted(os.listdir(d))
    except (FileNotFoundError, NotADirectoryError):
        return []
    # only the names vantage-pull pulls to (VANTAGE_NAME), which are the
    # names verify accepts back
    return [f"{VANTAGES}/{n}/reachability.jsonl" for n in names
            if VANTAGE_NAME.fullmatch(n) and os.path.isfile(os.path.join(d, n, "reachability.jsonl"))]


def record_files(data_dir):
    """RECORD_FILES in their order, with each other vantage's heartbeats
    right after the observer's own: a heartbeat names nothing that another
    file must already hold."""
    out = []
    for name in RECORD_FILES:
        out.append(name)
        if name == "reachability.jsonl":
            out.extend(vantage_files(data_dir))
    return out


def archive_dir(data_dir, name):
    """Where one file's segments and index.json are: archive/<file>/ in the
    file's own directory, as internal/record.ArchiveDir has it, so
    vantages/<n>/archive/reachability.jsonl/ for another vantage's file."""
    return os.path.join(data_dir, os.path.dirname(name), ARCHIVE, os.path.basename(name))


def index_label(name):
    """index.json of one file, relative to the data dir, for messages."""
    return os.path.join(os.path.dirname(name), ARCHIVE, os.path.basename(name), "index.json")


def archive_index(data_dir, name):
    """One file's index.json, None for a file never archived. A segment it
    lists without a "retired" record gets the one retired.json keeps for
    the same segment (name, range and digest), as internal/record.LoadIndex
    gives it: an older observer-archive drops the records from index.json
    whenever it saves it, and never touches retired.json."""
    d = archive_dir(data_dir, name)
    try:
        with open(os.path.join(d, "index.json")) as f:
            idx = json.load(f)
    except FileNotFoundError:
        return None
    try:
        with open(os.path.join(d, RETIRED)) as f:
            kept = json.load(f).get("segments") or []
    except FileNotFoundError:
        kept = []
    for s in idx.get("segments") or []:
        if s.get("retired"):
            continue
        for k in kept:
            if k.get("retired") and all(k.get(x) == s.get(x) for x in ("name", "from", "to", "sha256")):
                s["retired"] = k["retired"]
    return idx


def live_base(data_dir, name, idx=None):
    """The logical offset the live file starts at: the base of the newest
    generation whose first line is the live file's, 0 for a file never
    archived. None when the index has generations and none is this file."""
    idx = idx if idx is not None else archive_index(data_dir, name)
    gens = (idx or {}).get("generations") or []
    if not gens:
        return 0
    head = head_sha(os.path.join(data_dir, name))
    for g in reversed(gens):
        if head and g.get("head_sha256") == head:
            return int(g["base"])
    return None


def archive_cut(data_dir, name):
    """The archived part of one file at the cut: the live file's base and
    every segment below it, in order, without a gap. None for a file never
    archived. A retired segment carries its "retired" record: its file may
    be gone, and the record says which exports hold its bytes."""
    idx = archive_index(data_dir, name)
    if idx is None or not idx.get("generations"):
        return None
    base = live_base(data_dir, name, idx)
    if base is None:
        raise RecordError(f"{name}: the live file's first line matches no generation in {index_label(name)}")
    segs, at = [], 0
    for s in sorted(idx.get("segments") or [], key=lambda s: s["from"]):
        if s["to"] > base:
            continue  # written by a run that never swapped; the next run drops it
        if not plain_name(s.get("name")):
            raise RecordError(f"{name}: archive segment {s.get('name')!r} in {index_label(name)} is not a file name")
        if s["from"] != at:
            raise RecordError(f"{name}: archive segments leave a gap at logical byte {at}")
        at = s["to"]
        seg = {k: s[k] for k in ("name", "from", "to", "lines", "sha256", "gz_sha256", "gz_bytes")}
        if s.get("retired"):
            seg["retired"] = s["retired"]
        segs.append(seg)
    if at != base:
        raise RecordError(f"{name}: archive segments end at {at}, the live file starts at {base}")
    return {"base": base, "segments": segs, "records": sum(s["lines"] for s in segs)}


class ArchiveLock:
    """archive/.lock held shared, and each other vantage's
    vantages/<name>/archive/.lock: observer-archive holds a file's lock
    exclusively for a run, rotating and retiring, so neither happens under
    a cut or a copy. A missing archive directory is made (owned like the
    data dir), so a first rotation cannot begin under a cut either."""

    def __init__(self, data_dir):
        self.data_dir, self.fs = data_dir, []

    def __enter__(self):
        dirs = [self.data_dir] + [os.path.join(self.data_dir, os.path.dirname(v)) for v in vantage_files(self.data_dir)]
        for d in dirs:
            f = self._lock(os.path.join(d, ARCHIVE))
            if f:
                self.fs.append(f)
        return self

    def _lock(self, d):
        if not os.path.isdir(d):
            try:
                os.makedirs(d, exist_ok=True)
                st = os.stat(self.data_dir)
                if os.geteuid() == 0:
                    os.chown(d, st.st_uid, st.st_gid)
            except OSError:
                return None  # a read-only copy: nothing rotates it
        p = os.path.join(d, ".lock")
        try:
            new = not os.path.exists(p)
            f = open(p, "a")
            if new and os.geteuid() == 0:
                st = os.stat(self.data_dir)
                os.chown(p, st.st_uid, st.st_gid)
            fcntl.flock(f, fcntl.LOCK_SH)
        except OSError:
            return None
        return f

    def __exit__(self, *exc):
        for f in self.fs:
            f.close()


# Reading a retired segment back from the daily exports, by the rule
# internal/record reads it by (exports.go). The export reads each file
# from where the previous one stopped, so the member of a file in
# consecutive exports holds consecutive logical bytes of it,
# [source_from, source_to) in exports/index.json; a retired segment's
# bytes are the parts of the members its "retired" record names that fall
# inside [from, to). Every tarball is read to its last byte and every
# member used whole, each against its size and digest in the index, and
# the range must come to the segment's length, lines and SHA-256.


def exports_dir_of(data_dir, name, r):
    """The exports directory a retired segment names, relative to its
    archive directory, so a restored copy reads its own exports."""
    ed = r.get("exports_dir") or ""
    if not ed or os.path.isabs(ed) or ed.startswith("/"):
        raise RecordError(f"{name}: exports_dir {ed!r} is not a path relative to the archive directory")
    return os.path.normpath(os.path.join(archive_dir(data_dir, name), ed))


def plan_retired(label, data_dir, name, s, cache):
    """The exports s's bytes are read from, each with its index entry and
    its member of the file, checked before a byte is read: the members cover
    [from, to) in order without a gap, each adds to it, and each is a copy
    of its source range byte for byte. An empty member that starts where
    the ones before it end adds nothing and is not read. cache holds the
    exports index already read, by directory."""
    r = s.get("retired") or {}
    if not r.get("exports"):
        raise RecordError(f"{label}: names no exports")
    if not r.get("member"):
        raise RecordError(f"{label}: names no member")
    edir = exports_dir_of(data_dir, name, r)
    ip = os.path.join(edir, EXPORTS_INDEX)
    if edir not in cache:
        try:
            with open(ip) as f:
                cache[edir] = json.load(f)
        except (OSError, ValueError) as e:
            raise RecordError(f"{label}: {e}")
    index = cache[edir]
    parts, at, to = [], s["from"], s["to"]
    try:
        for x in r["exports"]:
            if not x or x in (".", "..") or os.path.basename(x) != x or "/" in x or "\\" in x:
                raise RecordError(f"{label}: export {x!r} is not a file name")
            es = [e for e in index if e.get("name") == x]
            if not es:
                raise RecordError(f"{label}: export {x} is not in {ip}")
            if len(es) > 1:
                raise RecordError(f"{label}: export {x} is listed {len(es)} times in {ip}")
            e = es[0]
            ms = [m for m in e.get("files") or [] if m.get("name") == r["member"]]
            if not ms:
                raise RecordError(f"{label}: export {x} has no member {r['member']}")
            if len(ms) > 1:
                raise RecordError(f"{label}: export {x} lists member {r['member']} twice")
            m = ms[0]
            mf, mt, mb = int(m["source_from"]), int(m["source_to"]), int(m["bytes"])
            if mt - mf != mb:
                raise RecordError(f"{label}: export {x}: member {r['member']} holds {mb} bytes for source bytes [{mf}, {mt}), so it is not a copy of them")
            if mb == 0 and mf == at:
                continue  # nothing of the range depends on this tarball
            if at >= to:
                raise RecordError(f"{label}: export {x} is past the segment's end: the exports before it hold all of [{s['from']}, {to})")
            if mf > at:
                raise RecordError(f"{label}: the exports leave logical bytes [{at}, {min(mf, to)}) out: {x}'s member {r['member']} starts at {mf}")
            if mt <= at:
                raise RecordError(f"{label}: export {x} holds nothing of [{at}, {to}): its member {r['member']} ends at {mt}")
            p = os.path.join(edir, x)
            if not os.path.isfile(p):
                raise RecordError(f"{label}: export {x} is not in {edir}")
            parts.append((p, e, m))
            at = min(mt, to)
    except (KeyError, TypeError, ValueError, AttributeError) as e:
        raise RecordError(f"{label}: {ip} is not an exports index: {e!r}")
    if at < to:
        raise RecordError(f"{label}: the exports end at logical byte {at}, the segment at {to}")
    return parts


class _Tee:
    """A file read through it is hashed and counted, so the tarball's
    digest covers every byte whoever reads them (tarfile reads ahead)."""

    def __init__(self, f):
        self.f, self.h, self.n = f, hashlib.sha256(), 0

    def read(self, size=-1):
        b = self.f.read(size)
        self.h.update(b)
        self.n += len(b)
        return b


def read_parts(parts, s):
    """The bytes of s's range that parts hold, in order. A difference from
    the exports index raises RecordError once the member or tarball it is
    in has been read, which may be after some of its bytes were yielded:
    callers hand nothing out before a first pass came through whole."""
    at, to = s["from"], s["to"]
    for p, e, m in parts:
        x, member, mf, mb = e["name"], m["name"], int(m["source_from"]), int(m["bytes"])
        found = False
        try:
            f = open(p, "rb")
        except OSError as ex:
            raise RecordError(f"export {x}: {ex}")
        with f:
            tee = _Tee(f)
            try:
                with tarfile.open(fileobj=tee, mode="r|gz") as tf:
                    for ti in tf:
                        if ti.name != member:
                            continue
                        if found:
                            # a tool that extracts the tarball and this reader could read different bytes
                            raise RecordError(f"export {x}: member {member} appears twice")
                        found = True
                        if not ti.isreg():
                            raise RecordError(f"export {x}: member {member} is not a regular file")
                        if ti.size != mb:
                            raise RecordError(f"export {x}: member {member} is {ti.size} bytes, the index says {mb}")
                        mh, pos = hashlib.sha256(), mf
                        src = tf.extractfile(ti)
                        for b in iter(lambda: src.read(CHUNK), b""):
                            mh.update(b)
                            lo, hi = max(at, pos), min(to, pos + len(b))
                            if lo < hi:
                                yield b[lo - pos:hi - pos]
                                at = hi
                            pos += len(b)
                        if pos - mf != mb:
                            raise RecordError(f"export {x}: member {member}: read {pos - mf} bytes, the index says {mb}")
                        if mh.hexdigest() != m["sha256"]:
                            raise RecordError(f"export {x}: member {member}: sha256 {mh.hexdigest()}, the index says {m['sha256']}")
                # the rest of the file after the archive's end, so the
                # tarball's digest covers every byte of it
                for _ in iter(lambda: tee.read(CHUNK), b""):
                    pass
            except (tarfile.TarError, OSError, EOFError, zlib.error) as ex:
                raise RecordError(f"export {x}: {ex}")
        if not found:
            raise RecordError(f"export {x}: no member {member} in the tarball")
        if tee.n != int(e["bytes"]):
            raise RecordError(f"export {x}: tarball is {tee.n} bytes, the index says {e['bytes']}")
        if tee.h.hexdigest() != e["sha256"]:
            raise RecordError(f"export {x}: tarball sha256 {tee.h.hexdigest()}, the index says {e['sha256']}")


def blocks(chunks):
    """chunks cut into CHUNK-sized blocks, the last one shorter: two reads
    of the same range cut it at the same offsets, so their blocks pair up."""
    buf = bytearray()
    for b in chunks:
        buf += b
        while len(buf) >= CHUNK:
            yield bytes(buf[:CHUNK])
            del buf[:CHUNK]
    if buf:
        yield bytes(buf)


def prove_retired(label, parts, s):
    """The first pass: s's range read from parts without handing out a
    byte, checked whole against the segment (length, lines, SHA-256). It
    returns the SHA-256 of each block, for the second pass to match."""
    h, n, lines, sums = hashlib.sha256(), 0, 0, []
    try:
        for b in blocks(read_parts(parts, s)):
            h.update(b)
            n += len(b)
            lines += b.count(b"\n")
            sums.append(hashlib.sha256(b).digest())
    except RecordError as e:
        raise RecordError(f"{label}: {e}")
    total = s["to"] - s["from"]
    if n != total:
        raise RecordError(f"{label}: the exports gave {n} of the segment's {total} bytes")
    if h.hexdigest() != s["sha256"]:
        raise RecordError(f"{label}: the bytes differ from the segment's sha256")
    if lines != s["lines"]:
        raise RecordError(f"{label}: {lines} lines, the index says {s['lines']}")
    return sums


def retired_label(name, s):
    return f"{name}: archive segment {s['name']} from the exports"


def check_retired(data_dir, name, s, cache):
    """Read s back from the exports whole and require its length, lines and
    digest: the first pass alone, since nothing needs the bytes."""
    label = retired_label(name, s)
    prove_retired(label, plan_retired(label, data_dir, name, s, cache), s)


def iter_retired(data_dir, name, s, cache):
    """s's bytes from the exports. No byte comes out before all of them are
    proven (a reader may act on each line as it comes), so the exports are
    read twice: the first pass proves the range and keeps each block's
    digest, the second hands each block out once its digest is the first
    pass's, and ends with an error at the first that is not."""
    label = retired_label(name, s)
    parts = plan_retired(label, data_dir, name, s, cache)
    sums = prove_retired(label, parts, s)
    k = 0
    try:
        for b in blocks(read_parts(parts, s)):
            if k >= len(sums) or hashlib.sha256(b).digest() != sums[k]:
                raise RecordError(f"the exports changed while they were read: logical bytes from {s['from'] + k * CHUNK} differ from the first pass")
            k += 1
            yield b
    except RecordError as e:
        raise RecordError(f"{label}: {e}")
    if k != len(sums):
        raise RecordError(f"{label}: the exports changed while they were read: {k} of the range's {len(sums)} blocks came back")


def iter_record(data_dir, name, live_length=None):
    """The bytes of one file's whole record: its archived segments up to the
    live file's base (a retired one's from the exports once its file is
    gone), then the live file (its first live_length bytes when given)."""
    a = archive_cut(data_dir, name)
    cache = {}
    for s in (a or {}).get("segments", []):
        p = os.path.join(archive_dir(data_dir, name), s["name"])
        if not os.path.exists(p):
            if not s.get("retired"):
                raise RecordError(f"{name}: archive segment {s['name']} is gone and was never retired")
            yield from iter_retired(data_dir, name, s, cache)
            continue
        with gzip.open(p, "rb") as z:
            while True:
                b = z.read(CHUNK)
                if not b:
                    break
                yield b
    p = os.path.join(data_dir, name)
    if not os.path.exists(p):
        return
    left = live_length
    with open(p, "rb") as f:
        while left is None or left > 0:
            b = f.read(CHUNK if left is None else min(CHUNK, left))
            if not b:
                break
            if left is not None:
                left -= len(b)
            yield b


def gz_blocks(p):
    """A gzip file's bytes, CHUNK at a time."""
    with gzip.open(p, "rb") as z:
        while True:
            b = z.read(CHUNK)
            if not b:
                return
            yield b


def iter_range(data_dir, name, lo, hi):
    """Logical bytes [lo, hi) of one file's record, from where they are:
    the archived segments below the live file's base (a retired one's from
    the exports), then the live file. Nothing below lo is read. Raises
    RecordError when the record does not hold all of them."""
    a = archive_cut(data_dir, name)
    base = a["base"] if a else 0
    at, cache = lo, {}
    for s in (a or {}).get("segments", []):
        if at >= hi:
            break
        if s["to"] <= at:
            continue
        p = os.path.join(archive_dir(data_dir, name), s["name"])
        if os.path.exists(p):
            src = gz_blocks(p)
        elif s.get("retired"):
            src = iter_retired(data_dir, name, s, cache)
        else:
            raise RecordError(f"{name}: archive segment {s['name']} is gone and was never retired")
        pos = s["from"]
        try:
            for b in src:
                lo2, hi2 = max(at, pos), min(hi, pos + len(b))
                if lo2 < hi2:
                    yield b[lo2 - pos:hi2 - pos]
                    at = hi2
                pos += len(b)
                if pos >= hi:
                    break
        finally:
            src.close()
        if at < min(hi, s["to"]):
            raise RecordError(f"{name}: archive segment {s['name']} ends at logical byte {pos}, the index says {s['to']}")
    if at < hi and at >= base:
        with open(os.path.join(data_dir, name), "rb") as f:
            f.seek(at - base)
            while at < hi:
                b = f.read(min(CHUNK, hi - at))
                if not b:
                    break
                at += len(b)
                yield b
    if at < hi:
        raise RecordError(f"{name}: the record ends at logical byte {at}, the cut at {hi}")


def cut_from_rotated(restored, name, info):
    """The cut's live file put back in a copy whose live file is a later
    generation. observer-archive rotated the file after the cut, and the
    copy took the rotated one: a night whose copy stopped part-way, or
    whose manifest did not reach the remote, leaves the remote's manifest
    older than its live files. The cut's live bytes are logical bytes
    [base, base + bytes) of the copy's record, in the segments that rotation
    wrote and the start of the newer live file; they are read back, checked
    against the cut, and put in the live file's place, so the copy is the
    cut again. The copy's index lists the cut's generation (generations are
    only ever added), so it places the file at the cut's base, and the
    newer segments, above that base, are not read. Returns the problems."""
    a = info.get("archive")
    lo, n = (a["base"] if a else 0), info["bytes"]
    p = os.path.join(restored, name)
    tmp = p + ".cut.tmp"
    try:
        try:
            with open(tmp, "wb") as o:
                for b in iter_range(restored, name, lo, lo + n):
                    o.write(b)
        except (RecordError, OSError, EOFError, zlib.error) as e:
            return [f"{name}: the copy's live file was rotated after the cut, and the copy's record does not give the cut's bytes back: {e}"]
        try:
            digest, records, bad, nbad = sha_records(name, tmp, n)
        except RecordError as e:
            return [str(e)]
        problems = check_cut(name, info, digest, records, bad, nbad)
        if not problems:
            os.replace(tmp, p)
        return problems
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def verify_archive(restored, name, info):
    """Every segment the cut names is in the copy, whole: the gzip file's
    digest and size, and what it decompresses to (digest, length, lines);
    and the copy's index places the live file at the cut's base. A segment
    the cut lists as retired is checked by its file when the copy has one,
    and otherwise read back from the copy's exports, whole, as the record
    reads it (check_retired)."""
    a = info.get("archive")
    if not a:
        return []
    problems = []
    cache = {}
    for s in a["segments"]:
        if not plain_name(s.get("name")):
            problems.append(f"{name}: archive segment {s.get('name')!r} is not a file name; not read")
            continue
        p = os.path.join(archive_dir(restored, name), s["name"])
        if not inside(restored, p):
            problems.append(f"{name}: archive segment {s['name']} is a link out of the copy; not read")
            continue
        if not os.path.exists(p):
            if s.get("retired"):
                try:
                    # the exports it is read from, as the manifest names
                    # them, must be the copy's own
                    if not inside(restored, exports_dir_of(restored, name, s["retired"])):
                        raise RecordError(f"{retired_label(name, s)}: exports_dir {s['retired'].get('exports_dir')!r} leads out of the copy")
                    check_retired(restored, name, s, cache)
                except RecordError as e:
                    problems.append(str(e))
                continue
            problems.append(f"{name}: archive segment {s['name']} missing")
            continue
        gh = hashlib.sha256()
        with open(p, "rb") as f:
            for b in iter(lambda: f.read(CHUNK), b""):
                gh.update(b)
        if gh.hexdigest() != s["gz_sha256"] or os.path.getsize(p) != s["gz_bytes"]:
            problems.append(f"{name}: archive segment {s['name']} differs from the cut (gzip digest or size)")
            continue
        h, n, lines = hashlib.sha256(), 0, 0
        try:
            with gzip.open(p, "rb") as z:
                for b in iter(lambda: z.read(CHUNK), b""):
                    h.update(b)
                    n += len(b)
                    lines += b.count(b"\n")
        except (OSError, EOFError) as e:
            problems.append(f"{name}: archive segment {s['name']}: {e}")
            continue
        if h.hexdigest() != s["sha256"] or n != s["to"] - s["from"] or lines != s["lines"]:
            problems.append(f"{name}: archive segment {s['name']} does not decompress to the lines the cut names")
    if problems:
        return problems
    try:
        base = live_base(restored, name)
    except (OSError, ValueError) as e:
        return [f"{name}: {index_label(name)}: {e}"]
    if base != a["base"]:
        problems.append(f"{name}: the copy's index places the live file at {base}, the cut at {a['base']}")
    return problems


def cut(data_dir):
    """The consistent cut: state first, then every file's length in order,
    then hashed and parsed. Raises RecordError on a record that is not one."""
    with ArchiveLock(data_dir):
        return cut_locked(data_dir)


def cut_locked(data_dir):
    state_raw = read_state(os.path.join(data_dir, STATE))
    lengths = []
    for name in record_files(data_dir):
        p = os.path.join(data_dir, name)
        if os.path.exists(p):
            lengths.append((name, complete_length(p)))
    files = {}
    for name, length in lengths:
        digest, records, bad, nbad = sha_records(name, os.path.join(data_dir, name), length)
        files[name] = {"bytes": length, "sha256": digest, "records": records}
        if nbad:
            files[name]["bad_lines"] = bad
            files[name]["bad_count"] = nbad
        a = archive_cut(data_dir, name)
        if a:
            files[name]["archive"] = a
            files[name]["archived_records"] = a["records"]
    state = None
    if state_raw is not None:
        state = {"bytes": len(state_raw), "sha256": hashlib.sha256(state_raw).hexdigest()}
    m = {
        "version": VERSION,
        "taken_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "data_dir": os.path.abspath(data_dir),
        "files": files,
        "state": state,
        "checkpoint": checkpoint_of(state_raw),
    }
    m["id"] = hashlib.sha256(json.dumps({"files": files, "state": state, "checkpoint": m["checkpoint"]}, sort_keys=True).encode()).hexdigest()[:16]
    if state_raw is not None:
        # the bytes themselves, so a restore can put this state back beside
        # the records it was cut with
        m["state_raw"] = state_raw.decode("utf-8")
    return m


def dump(m, path):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(m, f, indent=1, sort_keys=True)
        f.write("\n")
    os.replace(tmp, path)


def write(data_dir, out):
    m = cut(data_dir)
    dump(m, out)
    return m


def snapshot(data_dir, dest):
    os.makedirs(dest, exist_ok=True)
    with ArchiveLock(data_dir):
        m = cut_locked(data_dir)
        copy_cut(data_dir, dest, m)
    dump(m, os.path.join(dest, MANIFEST))
    return m


def copy_cut(data_dir, dest, m):
    done = set()
    for name, info in m["files"].items():
        a = info.get("archive")
        if a:
            # segments never change once written: copied whole, with the
            # index that places the live file at the cut's base
            sdir, ddir = archive_dir(data_dir, name), archive_dir(dest, name)
            os.makedirs(ddir, exist_ok=True)
            for s in a["segments"]:
                if s.get("retired"):
                    # its bytes are in the exports it names, which go
                    # along; its file too while the host still has it
                    copy_exports(data_dir, dest, name, s["retired"], done)
                    if not os.path.exists(os.path.join(sdir, s["name"])):
                        continue
                shutil.copyfile(os.path.join(sdir, s["name"]), os.path.join(ddir, s["name"]))
            shutil.copyfile(os.path.join(sdir, "index.json"), os.path.join(ddir, "index.json"))
            # the second copy of the retired records goes along: a reader of
            # the copy whose index.json an older build rewrote finds them there
            if os.path.exists(os.path.join(sdir, RETIRED)):
                shutil.copyfile(os.path.join(sdir, RETIRED), os.path.join(ddir, RETIRED))
        src = os.path.join(data_dir, name)
        dst = os.path.join(dest, name)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        with open(src, "rb") as i, open(dst, "wb") as o:
            left = info["bytes"]
            while left > 0:
                b = i.read(min(CHUNK, left))
                if not b:
                    break
                o.write(b)
                left -= len(b)
    if m.get("state_raw") is not None:
        # the state as it was at the cut, not as it is now
        with open(os.path.join(dest, STATE), "w") as f:
            f.write(m["state_raw"])


def copy_exports(data_dir, dest, name, r, done):
    """The exports a retired segment is read from, and their index, to the
    same place under dest, so the copy reads the segment as the host does.
    done holds what this copy has copied already: the segments of several
    files name the same day's tarball."""
    src = exports_dir_of(data_dir, name, r)
    dst = exports_dir_of(dest, name, r)
    root = os.path.abspath(dest)
    if os.path.commonpath([os.path.abspath(dst), root]) != root:
        raise RecordError(f"{name}: exports_dir {r.get('exports_dir')!r} leads out of the data directory")
    os.makedirs(dst, exist_ok=True)
    for x in [EXPORTS_INDEX] + list(r.get("exports") or []):
        if not x or os.path.basename(x) != x or x in (".", ".."):
            raise RecordError(f"{name}: export {x!r} is not a file name")
        if os.path.join(dst, x) in done:
            continue
        try:
            shutil.copyfile(os.path.join(src, x), os.path.join(dst, x))
        except OSError as e:
            raise RecordError(f"{name}: a retired segment's export: {e}")
        done.add(os.path.join(dst, x))


def verify(restored, manifest_path=None):
    manifest_path = manifest_path or os.path.join(restored, MANIFEST)
    problems = []
    notes = []
    try:
        m = json.load(open(manifest_path))
    except Exception as e:
        return [f"manifest {manifest_path}: {e}"], None
    if not isinstance(m, dict) or not isinstance(m.get("files"), dict):
        return [f"manifest {manifest_path}: not a manifest (no files)"], None
    for bad in FORBIDDEN:
        if os.path.exists(os.path.join(restored, bad)):
            problems.append(f"{bad} is in the copy: it must never leave the host")
    trimmed = 0
    for name, info in sorted(m["files"].items()):
        # The manifest came back from the remote with the copy, and the
        # files it names are trimmed and replaced below: only a name a cut
        # can hold, at a path that stays in the copy, is touched.
        if not record_name(name):
            problems.append(f"{name!r}: not the name of a record file; nothing is done with it")
            continue
        if not isinstance(info, dict) or not isinstance(info.get("bytes"), int) or info["bytes"] < 0:
            problems.append(f"{name}: the manifest gives no length for it")
            continue
        p = os.path.join(restored, name)
        if os.path.islink(p) or not inside(restored, p):
            problems.append(f"{name}: a link in the copy, not the file; not touched")
            continue
        if not os.path.exists(p):
            problems.append(f"{name}: missing (manifest has {info['bytes']} bytes, {info['records']} records)")
            continue
        # A live file observer-archive rotated after the cut: the copy's
        # index places it past the cut's base, and the cut's live bytes are
        # in the copy's newer segments (cut_from_rotated).
        a = info.get("archive")
        try:
            copy_base = live_base(restored, name)
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            copy_base = None
        if copy_base is not None and copy_base > (a["base"] if a else 0):
            got = cut_from_rotated(restored, name, info)
            if got:
                problems.extend(got)
                continue
            notes.append(f"{name}: the copy's live file was rotated after the cut; the cut's {info['bytes']} bytes were read back from the copy's archive and put in its place")
            problems.extend(verify_archive(restored, name, info))
            continue
        size = os.path.getsize(p)
        if size < info["bytes"]:
            problems.append(f"{name}: truncated, {size} bytes of {info['bytes']}")
            continue
        if size > info["bytes"]:
            # the copy was taken after the cut; keep exactly the cut
            with open(p, "r+b") as f:
                f.truncate(info["bytes"])
            trimmed += size - info["bytes"]
        try:
            digest, records, bad, nbad = sha_records(name, p, info["bytes"])
        except RecordError as e:
            problems.append(str(e))
            continue
        got = check_cut(name, info, digest, records, bad, nbad)
        if got:
            problems.extend(got)
        else:
            problems.extend(verify_archive(restored, name, info))
    cp = m.get("checkpoint")
    sp = os.path.join(restored, STATE)
    if m.get("state_raw") is not None and (os.path.islink(sp) or not inside(restored, sp)):
        problems.append(f"{STATE}: a link in the copy, not the file; not touched")
    elif m.get("state_raw") is not None:
        # The cut's own state.json goes beside the cut's records, whatever
        # the copy carried: the copy's was read later and points past
        # them, and only the whole file is consistent with the cut.
        raw = m["state_raw"].encode("utf-8")
        st = m.get("state") or {}
        if hashlib.sha256(raw).hexdigest() != st.get("sha256") or checkpoint_of(raw) != cp:
            problems.append(f"{STATE}: the manifest's copy does not match its own hash or checkpoint (manifest altered)")
        else:
            have = read_state(sp)
            if have is None:
                with open(sp, "wb") as f:
                    f.write(raw)
                notes.append(f"{STATE}: not in the copy; written from the cut (height {cp['last_scanned_height'] if cp else '?'})")
            elif have != raw:
                hc = checkpoint_of(have)
                with open(sp, "wb") as f:
                    f.write(raw)
                notes.append(f"{STATE}: the copy's (height {hc['last_scanned_height'] if hc else '?'}) is not the cut's; replaced with the cut's (height {cp['last_scanned_height'] if cp else '?'})")
    elif cp:
        # a manifest from before the state was carried: it can check the
        # copy's state.json but not repair it, and a state past the cut
        # would resume the scanner past records this cut does not hold
        rc = checkpoint_of(read_state(sp))
        if rc is None:
            problems.append(f"{STATE}: missing or unreadable (manifest checkpoint at height {cp['last_scanned_height']})")
        elif rc["last_scanned_height"] != cp["last_scanned_height"]:
            problems.append(f"{STATE}: checkpoint {rc['last_scanned_height']} is not the manifest's {cp['last_scanned_height']}; this manifest carries no state.json to restore, take the backup again with the current tool")
    m["_trimmed_bytes"] = trimmed
    m["_notes"] = notes
    return problems, m


def show(m):
    print(f"manifest {m['id']} taken {m['taken_at']} from {m['data_dir']} (v{m.get('version', 1)})")
    if m.get("checkpoint"):
        st = m.get("state") or {}
        carried = f", {st['bytes']} bytes carried" if st else ""
        print(f"  checkpoint: height {m['checkpoint']['last_scanned_height']} ({m['checkpoint'].get('last_scanned_time')}), {m['checkpoint']['gaps']} gap(s){carried}")
    for name, info in sorted(m["files"].items()):
        if not record_name(name) or not isinstance(info, dict):
            continue  # verify says what is wrong with it
        archived = ""
        if info.get("archive"):
            a = info["archive"]
            retired = sum(1 for s in a["segments"] if s.get("retired"))
            retired = f", {retired} retired to the exports" if retired else ""
            archived = f" + {a['records']} archived in {len(a['segments'])} segment(s){retired} (base {a['base']})"
        print(f"  {name:<24} {info.get('bytes', '?'):>12} bytes {info.get('records', '?'):>9} records {str(info.get('sha256', ''))[:16]}{archived}")
        if info.get("bad_count"):
            # kept in the record's bytes for good, and in every copy
            print(f"  {'':<24} lines that are not a JSON record: {bad_label(info.get('bad_lines') or [], info['bad_count'])}")


def main(argv):
    if len(argv) < 3:
        print(__doc__.strip(), file=sys.stderr)
        return 2
    cmd = argv[1]
    try:
        if cmd == "write":
            m = write(argv[2], argv[3])
            show(m)
        elif cmd == "snapshot":
            m = snapshot(argv[2], argv[3])
            show(m)
        elif cmd == "verify":
            problems, m = verify(argv[2], argv[3] if len(argv) > 3 else None)
            if m:
                show(m)
                if m.get("_trimmed_bytes"):
                    print(f"  trimmed {m['_trimmed_bytes']} bytes appended after the cut")
                for n in m.get("_notes", []):
                    print(f"  {n}")
            for p in problems:
                print(f"  FAIL {p}")
            if problems:
                return 1
            print("verify: every file matches the manifest")
        elif cmd == "show":
            show(json.load(open(argv[2])))
        elif cmd == "cat" and len(argv) > 3:
            try:
                with ArchiveLock(argv[2]):
                    p = os.path.join(argv[2], argv[3])
                    length = complete_length(p) if os.path.exists(p) else None
                    for b in iter_record(argv[2], argv[3], length):
                        sys.stdout.buffer.write(b)
            except RecordError as e:
                # stdout is the record: the reason goes to stderr, and the
                # exit status says the record did not come out whole
                print(f"cat: {e}", file=sys.stderr)
                return 1
        elif cmd == "end" and len(argv) > 3:
            p = os.path.join(argv[2], argv[3])
            base = live_base(argv[2], argv[3])
            print((base or 0) + (os.path.getsize(p) if os.path.exists(p) else 0))
        else:
            print(__doc__.strip(), file=sys.stderr)
            return 2
    except RecordError as e:
        print(f"  FAIL {e}")
        print(f"{cmd}: no cut of the record could be taken; nothing written")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
