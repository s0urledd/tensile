"use client";
import { useEffect, useState } from "react";
import Copy from "@/components/Copy";
import { netName } from "@/components/Chrome";
import { useApi, type Meta, API_BASE } from "@/lib/api";
import { API_URL, API_URL_FIXED } from "@/lib/site";

/**
 * The parts of the API page that depend on which site serves it. One static
 * export serves every network, each site with its own API on its own /api,
 * so the address the commands print and the network the page names are read
 * where the page runs: the address from the page's origin, unless
 * NEXT_PUBLIC_API_URL fixed one at build time, and the network from
 * /v1/meta. Until then the page shows the public deployment's address.
 */
function useApiUrl(): string {
  const [url, setUrl] = useState(API_URL);
  useEffect(() => {
    if (API_URL_FIXED) return;
    try { setUrl(new URL(`${API_BASE}/v1`, window.location.origin).href.replace(/\/$/, "")); } catch { /* keep the default */ }
  }, []);
  return url;
}

/** One call as a curl command, with a button that copies it. */
export function Call({ path }: { path: string }) {
  const cmd = `curl -s '${useApiUrl()}${path}'`;
  return (
    <div className="api-call">
      <code>{cmd}</code>
      <Copy text={cmd} label="the curl command" />
    </div>
  );
}

export function BaseUrl() {
  const url = useApiUrl();
  return (
    <div className="api-call api-base">
      <code>{url}</code>
      <Copy text={url} label="the base URL" />
    </div>
  );
}

/** What Celestia calls its public test networks, by chain-id stem. */
const KIND: Record<string, string> = { mocha: "testnet", arabica: "devnet" };

/** The network this site's API answers for, as /v1/meta names it. */
export function Network() {
  const { data: meta } = useApi<Meta>("/v1/meta", 30000);
  const id = meta?.chain_id;
  if (!id) return <span>Celestia</span>;
  if (netName(id) === "Mainnet") return <span>Celestia <b>Mainnet</b></span>;
  const stem = id.replace(/-\d+$/, "");
  return <span>Celestia <b>{stem.charAt(0).toUpperCase() + stem.slice(1)}</b>{KIND[stem] ? ` ${KIND[stem]}` : ""}, <code>{id}</code></span>;
}
