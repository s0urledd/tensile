"use client";
import { Suspense, useCallback, useState, type ReactNode } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { useApi, type Meta, type Tip, type Window, blobFee, bytes, fmtShare, int, nsDisplay, tia, utcWord, TIP_MS } from "@/lib/api";
import type { MarketWithQueue, Params, PublisherWithQueue } from "@/lib/withdrawals";
import { useWindow, WindowSwitch, periodName } from "@/lib/window";
import { openRow } from "@/lib/row";
import PreLive, { notLiveOf } from "@/components/PreLive";
import { unit } from "@/components/Unit";
import Pager, { usePage } from "@/components/Pager";
import Picker, { type Choice } from "@/components/Picker";
import Ident from "@/components/Ident";
import { Who } from "@/components/Ledger";
import PublishersTop from "@/components/PublishersTop";
import { age, monthDayTime } from "@/components/BlobsDeck";

/** rows per page of the publisher list */
const SIZE = 25;

const FIND_ICON = <svg width="14" height="14" viewBox="0 0 16 16" aria-hidden="true"><circle cx="7" cy="7" r="4.75" fill="none" stroke="currentColor" strokeWidth="1.5" /><path d="m10.5 10.5 3.5 3.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" /></svg>;
const ARROW = <svg className="ar" viewBox="0 0 10 10" aria-hidden="true"><path d="M5 1.5v7M2 5.6 5 8.6l3-3" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" /></svg>;

type SortKey = "last" | "settlements" | "bytes" | "fees" | "escrow";
type Sort = { k: SortKey; desc: boolean };
/** the time its column shows: its newest blob, or before the API gave that its newest escrow movement of any kind */
const lastAt = (p: PublisherWithQueue) => (p.last_settlement_at !== undefined ? p.last_settlement_at : p.last_seen_at);
/** what each column that sorts orders by; an account with no escrow found goes below an empty one */
const BY: Record<SortKey, (p: PublisherWithQueue) => number> = {
  last: (p) => Date.parse(lastAt(p) ?? "") || 0,
  settlements: (p) => p.settlements,
  bytes: (p) => p.bytes,
  fees: (p) => p.fees_utia,
  escrow: (p) => (p.escrow?.found ? p.escrow.available_utia : -1),
};

/** a publisher's namespaces in the period: the most used, as the Blobs list names it and linked to its blobs, and how many more there are */
function Namespaces({ p }: { p: PublisherWithQueue }) {
  const nss = p.namespaces ?? [];
  if (nss.length === 0) return null;
  const more = (p.namespaces_total ?? nss.length) - 1;
  const rest = (p.namespaces_total ?? nss.length) - nss.length;
  const title = nss.map((x) => `${nsDisplay(x.namespace)} (${int(x.settlements)})`).join(", ") + (rest > 0 ? ` and ${int(rest)} more` : "");
  return (
    <span className="nsw">
      <Link className="nsl" href={`/blobs/?namespace=${nss[0].namespace}`} title={title}>{nsDisplay(nss[0].namespace)}</Link>
      {more > 0 && <span className="more" title={title}>+{int(more)}</span>}
    </span>
  );
}

/**
 * A column head that sorts: the word is the button. The order in force is
 * marked by an arrow just before the word, in the head's own padding, so no
 * word moves when the order changes.
 */
function SortHead({ k, sort, onSort, children }: { k: SortKey; sort: Sort; onSort: (k: SortKey) => void; children: ReactNode }) {
  const on = sort.k === k;
  return (
    <button type="button" className="sort" aria-pressed={on} data-dir={on && !sort.desc ? "asc" : "desc"} onClick={() => onSort(k)}>
      {ARROW}{children}
    </button>
  );
}

const plural = (n: number, w: string) => `${int(n)} ${w}${n === 1 ? "" : "s"}`;

/**
 * Publishers: the accounts that pay for blobs from escrow. A framed top of
 * the period's figures in three short ledgers (components/PublishersTop.tsx),
 * then the publishers in the Blobs list's own rows: what each posted and paid
 * in the period, and what its escrow can still spend, in a lane of its own.
 * Every figure is the chain's, read through the API; none was measured by
 * Tensile.
 */
function Page() {
  const router = useRouter();
  // 24h, as on the overview and the Blobs page
  const [win, setWinRaw] = useWindow("24h");
  const [page, setPage] = usePage();
  const [sort, setSortRaw] = useState<Sort>({ k: "fees", desc: true });
  // another period or order is another list: start from its first page
  const setWin = (w: typeof win) => { setWinRaw(w); setPage(1); };
  const onSort = (k: SortKey) => { setSortRaw((s) => (s.k === k ? { k, desc: !s.desc } : { k, desc: true })); setPage(1); };

  const { data: meta } = useApi<Meta>("/v1/meta");
  // Before activation every market figure is a zero of a module that does not exist yet: one line says so, the figures show a dash.
  const pre = notLiveOf(meta);
  const { data: m, error } = useApi<MarketWithQueue>(`/v1/market?window=${win}`);
  const { data: list } = useApi<{ publishers: PublisherWithQueue[]; count: number; window?: Window }>(`/v1/publishers?window=${win}`);
  // every account on record: what each posts as a rule (its average blob over all of them), and the choices of Find
  const { data: all } = useApi<{ publishers: PublisherWithQueue[] }>("/v1/publishers?window=all");
  const pf = useApi<Params>("/v1/params", 0).data?.price_formula;
  const tip = useApi<Tip>("/v1/tip", TIP_MS); // the header's stream: no request of its own
  const skew = tip.data?.server_time && tip.fetchedAt ? Date.parse(tip.data.server_time) - Date.parse(tip.fetchedAt) : 0;
  const now = Date.now() + skew;

  const pubs = list?.publishers ?? [];
  // The namespaces and the newest blob, where the API gives them; an API from before them gives the newest escrow
  // movement of any kind, and the list shows that as it did.
  const blobs = pubs.some((p) => p.last_settlement_at !== undefined);
  // the API's order is by fees, then blob size; any other keeps an account with no blobs last (in the period, or by
  // the newest blob ever when that is the order), unless the order is the escrow's, which is a balance now whatever
  // the period
  const none = (p: PublisherWithQueue) => (sort.k === "last" && blobs ? !p.last_settlement_at : !p.settlements);
  const rows = sort.k === "fees" && sort.desc ? pubs : pubs.map((p, i) => ({ p, i })).sort((a, b) => {
    if (sort.k !== "escrow") { const z = (none(a.p) ? 1 : 0) - (none(b.p) ? 1 : 0); if (z) return z; }
    const d = BY[sort.k](a.p) - BY[sort.k](b.p);
    return (sort.desc ? -d : d) || a.i - b.i;
  }).map((r) => r.p);
  const shown = rows.slice((page - 1) * SIZE, page * SIZE);
  // a Timed out column only while a publisher in the period has a promise that timed out
  const timeouts = pubs.some((p) => p.timeouts > 0);
  const cols = 8 + (timeouts ? 1 : 0) + (blobs ? 1 : 0);

  // A publisher's usual blob: its average size over every blob it posted. Its escrow is short when what it can spend
  // now does not pay for one more of those; an account that never posted has no usual blob, so it is never short.
  const usual = new Map((all?.publishers ?? []).filter((p) => p.settlements > 0 && p.avg_blob_bytes).map((p) => [p.publisher, p.avg_blob_bytes!]));

  // a figure of the period names it in its label: the period of the answer shown (the last one stays while
  // another loads), else the one selected
  const per = periodName(m?.window?.name ?? win);

  const findChoices: Choice[] | null = all
    ? [...all.publishers].sort((a, b) => b.settlements - a.settlements || (b.escrow?.available_utia ?? 0) - (a.escrow?.available_utia ?? 0)).map((p) => ({
      value: p.publisher,
      label: p.label ? <><Ident addr={p.publisher} />{p.label}</> : <><Ident addr={p.publisher} /><span className="hd">celestia •••</span><span className="mono">{p.publisher.slice(-4)}</span></>,
      count: p.settlements || undefined,
      find: `${p.publisher} ${p.label ?? ""}`.toLowerCase(),
    }))
    : null;
  const open = useCallback((e: React.MouseEvent, href: string) => router.push(href), [router]);

  return (
    <>
      <div className="page-head lg-head pl-head">
        <h1>Publishers</h1>
        <WindowSwitch value={win} onChange={setWin} />
      </div>
      <PreLive meta={meta} />

      {error && <div className="note hold"><span className="label">Observer</span><p>Cannot reach the observer API: {error}. Nothing below is current.</p></div>}

      {/* the period's three ledgers: activity, blob size with the largest publisher's part, fees and escrow */}
      <PublishersTop m={pre ? null : m} win={win} list={list} all={all} now={now} pre={pre} onWin={setWin} />

      <section id="list" className="listing pl-list">
        {/* the search over the table's right, above the escrow lane */}
        <div className="lg-bar pl-bar">
          <span className="pl-find">
            <Picker name="Find a publisher" icon={FIND_ICON} value="" text={null} choices={findChoices} find
              accept={(s) => (/^celestia1[0-9a-z]{38}$/.test(s) ? s : null)} placeholder="Address" onPick={(a) => { if (a) router.push(`/publisher/?addr=${a}`); }} />
          </span>
        </div>
        <div className="lg-tw">
          <table className={`lg-t pl-t${timeouts ? " has-to" : ""}${blobs ? " has-ns" : ""}`}>
            <thead>
              <tr>
                <th className="c-pub">Publisher</th>
                {blobs && <th className="c-ns" title="The namespaces it posted to in the period, the most used first">Namespaces</th>}
                {blobs
                  ? <th className="c-t" title="When its newest blob settled, whatever the period"><SortHead k="last" sort={sort} onSort={onSort}>Last blob <span className="per">(UTC)</span></SortHead></th>
                  : <th className="c-t" title="Its newest escrow movement of any kind, whatever the period: a blob's fee, a deposit or a withdrawal"><SortHead k="last" sort={sort} onSort={onSort}>Last activity <span className="per">(UTC)</span></SortHead></th>}
                <th className="c-n num" title="Settlements paid in the period: one per blob paid for"><SortHead k="settlements" sort={sort} onSort={onSort}>Settlements</SortHead></th>
                <th className="c-sz num" title="The size of the blobs it paid for in the period"><SortHead k="bytes" sort={sort} onSort={onSort}>Blob size</SortHead></th>
                <th className="c-sh num" title="Its part of all the blob size posted in the period">Share</th>
                <th className="c-fee num" title="Fees its escrow paid in the period"><SortHead k="fees" sort={sort} onSort={onSort}>Fees paid</SortHead></th>
                {timeouts && <th className="c-to num" title="Payment promises not settled in time in the period; each is charged as a blob">Timed out</th>}
                <th className="gap" aria-hidden="true" />
                <th className="c-esc" title="What it can spend from its escrow now, whatever the period"><SortHead k="escrow" sort={sort} onSort={onSort}>Escrow available</SortHead></th>
              </tr>
            </thead>
            <tbody>
              {!list && <tr className="lg-empty"><td colSpan={cols}>Loading…</td></tr>}
              {list && pubs.length === 0 && <tr className="lg-empty"><td colSpan={cols}>No publisher posted a blob or moved escrow in this period.</td></tr>}
              {shown.map((p) => {
                const href = `/publisher/?addr=${p.publisher}`;
                const has = p.settlements > 0;
                // its newest blob, whatever the period; none when it never posted one
                const last = lastAt(p);
                const never = blobs && !p.last_settlement_at;
                const e = p.escrow;
                const avg = usual.get(p.publisher);
                const need = avg != null && pf ? blobFee(pf, avg) : null;
                const low = !!e?.found && need != null && e.available_utia < need;
                let escTitle = e ? "No escrow account on the chain" : "Not read yet";
                if (e?.found) {
                  escTitle = low
                    ? `Not enough for one more ${bytes(Math.round(avg!))} blob, its average size (${tia(need)}).`
                    : `As of block #${int(e.height)}, ${utcWord(e.updated_at)}.`;
                  if (e.balance_utia !== e.available_utia) escTitle += ` Balance ${tia(e.balance_utia)}; ${tia(e.balance_utia - e.available_utia)} is queued to withdraw.`;
                }
                const toTitle = p.timeouts > 0 ? `${plural(p.timeouts, "payment promise")} timed out in the period; ${tia(p.timed_out_utia)} charged` : undefined;
                return (
                  // The whole row opens the publisher; its chip keeps its own link.
                  <tr key={p.publisher} className={`row${p.timeouts > 0 ? " to" : ""}`} onClick={(ev) => openRow(ev, href, open)} onAuxClick={(ev) => openRow(ev, href, open)}>
                    {/* under 1080 px the most used namespace follows the chip, as the column is gone */}
                    <td className="c-pub"><Who addr={p.publisher} />{blobs && has && <Namespaces p={p} />}</td>
                    {blobs && <td className="c-ns"><Namespaces p={p} /></td>}
                    <td className="c-t">{last
                      ? <span title={utcWord(last)}><span className="tm">{monthDayTime(last)}</span><span className="ag">{age(now - Date.parse(last))}</span></span>
                      : <span className="none">No blobs</span>}</td>
                    {has ? <>
                      <td className="c-n num">{int(p.settlements)}</td>
                      <td className="c-sz num">{unit(bytes(p.bytes))}</td>
                      <td className="c-sh num">{fmtShare(p.bytes_share)}</td>
                      <td className="c-fee num"><span title={p.paid_per_mib_utia != null && p.avg_blob_bytes != null ? `${tia(p.paid_per_mib_utia)} per MiB · average blob ${bytes(Math.round(p.avg_blob_bytes))}` : undefined}>{unit(tia(p.fees_utia))}</span></td>
                    </> : never
                      // never posted: the time column says so, and the period's figures are left empty
                      ? <><td className="c-n" /><td className="c-sz" /><td className="c-sh" /><td className="c-fee" /></>
                      : <td className="c-nb" colSpan={4}>{blobs ? `No blobs in ${per}` : "No blobs"}</td>}
                    {timeouts && <td className="c-to num">{p.timeouts > 0 && <span className="tox" title={toTitle}>{int(p.timeouts)}</span>}</td>}
                    <td className="gap" aria-hidden="true" />
                    <td className="c-esc">
                      <span className="kp">Escrow</span>
                      {e?.found ? <span className={low ? "low" : undefined} title={escTitle}>{unit(tia(e.available_utia))}</span> : <span className="none" title={escTitle}>—</span>}
                    </td>
                    <td className="c-m">
                      {has
                        ? <><span className="n-set">{plural(p.settlements, "settlement")}<span className="sep">·</span></span><span className="n-sz">{bytes(p.bytes)}<span className="sep">·</span></span>{tia(p.fees_utia)}<span className="w">paid</span></>
                        : <span className="none">{blobs && !never ? `No blobs in ${per}` : "No blobs"}</span>}
                      {p.timeouts > 0 && <><span className="sep">·</span><span className="tox" title={toTitle}>{int(p.timeouts)} timed out</span></>}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        {list && pubs.length > 0 && <Pager total={pubs.length} page={page} size={SIZE} onPage={setPage} noun={pubs.length === 1 ? "publisher" : "publishers"} />}
      </section>
    </>
  );
}

export default function PublishersPage() {
  return <Suspense fallback={<p className="muted">Loading…</p>}><Page /></Suspense>;
}
