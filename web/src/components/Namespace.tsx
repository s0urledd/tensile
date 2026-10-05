import { nsName } from "@/lib/api";

/** a namespace's significant hex, without the leading zero padding */
export const nsHex = (ns: string) => ns.replace(/^(00)+/, "");

/** a namespace as a list names it: its text, or its hex in the mono face when it is not text */
export function NsName({ ns }: { ns: string }) {
  const { text, hex } = nsName(ns);
  return hex ? <span className="mono">{text}</span> : <>{text}</>;
}

/** the Namespace filter's mark */
export const NS_ICON = <svg width="14" height="14" viewBox="0 0 16 16" aria-hidden="true"><rect x="2" y="2.5" width="12" height="3.2" rx="1.2" fill="none" stroke="currentColor" strokeWidth="1.4" /><rect x="2" y="10.3" width="12" height="3.2" rx="1.2" fill="none" stroke="currentColor" strokeWidth="1.4" /></svg>;
