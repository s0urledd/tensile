"use client";
import { Suspense } from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { shortMid } from "@/lib/api";
import { blobKey } from "@/lib/blobkey";
import TxDetail from "@/components/TxDetail";

/**
 * One Fibre transaction by its hash: /tx/?hash=<64 hex>, either case, with or without 0x. Links use lower case; the
 * page prints it in upper case. Every row of a publisher's transactions and of a validator's endpoint history opens it.
 */
function Page() {
  const asked = (useSearchParams().get("hash") ?? "").trim();
  const k = asked ? blobKey(asked) : null;
  if (!asked) return <p className="notice">Open a transaction from a publisher&rsquo;s or a validator&rsquo;s page, or add <code>?hash=&lt;transaction hash&gt;</code> to the address.</p>;
  if (k?.kind !== "hash") {
    return (
      <>
        <div className="head"><div><p className="crumb"><Link href="/blobs/">Blobs</Link> › …</p><h1>Transaction</h1></div></div>
        <p className="notice"><span className="mono">{shortMid(asked, 10, 6)}</span> is not a transaction hash: one is 64 hex characters.</p>
      </>
    );
  }
  return <TxDetail key={k.hex} hex={k.hex} />;
}

export default function TxPage() {
  return <Suspense fallback={<p className="crumb" style={{ paddingTop: 22 }}>Loading…</p>}><Page /></Suspense>;
}
