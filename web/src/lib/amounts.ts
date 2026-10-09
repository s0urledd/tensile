/**
 * A coin as the chain printed it ("8000utia", several joined by commas), as the transaction page and the blob page write
 * it. No runtime import, so amounts.test.mjs loads it on its own; every page takes these from @/lib/api.
 */

/** a digit string with its thousands grouped by commas, exact at any length (a Number is exact to 15 digits only) */
const grouped = (n: string) => n.replace(/\B(?=(\d{3})+(?!\d))/g, ",");

/** the fee as the chain printed it ("800utia", coins joined by commas), with each amount's digits grouped and a space before its denomination */
export const coins = (fee: string) =>
  fee.split(",").map((c) => c.replace(/^(\d+)(\D.*)$/, (_, n: string, d: string) => `${grouped(n)} ${d}`)).join(", ");

/**
 * the fee in TIA, as the site writes amounts, but always down to the utia ("2500utia" is 0.0025 TIA, where tia()
 * would round it to 0.003): a transaction fee is small and paid exactly; another denomination as coins() prints it
 */
export const feeTia = (fee: string) =>
  fee.split(",").map((c) => {
    const m = /^(\d{1,15})utia$/.exec(c.trim());
    return m ? `${(Number(m[1]) / 1e6).toFixed(6).replace(/\.?0+$/, "")} TIA` : coins(c);
  }).join(", ");

/**
 * an amount the chain printed, exact, its trailing zeros trimmed: "1,000,000,000 TIA", "0.000001 TIA"; never rounded to a
 * page's precision, so a request of 1 utia never reads "0.000 TIA". A coin that is not utia stays as coins() gives it.
 */
export function exactCoin(c: string): string {
  const m = /^(\d+)utia$/.exec(c.trim());
  if (!m) return coins(c);
  const s = m[1].replace(/^0+(?=\d)/, "");
  const whole = s.length > 6 ? s.slice(0, -6) : "0";
  const frac = s.padStart(6, "0").slice(-6).replace(/0+$/, "");
  return `${grouped(whole)}${frac ? `.${frac}` : ""} TIA`;
}
