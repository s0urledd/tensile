/**
 * The host map's geometry: country outlines and label points built offline
 * by web/scripts/build-map.mjs from Natural Earth 1:50m (small countries
 * also at 1:10m, with the coast round them from build-coast.mjs), and the
 * Equal Earth forward projection that places a host in the same map units.
 */
import world from "./world.json";

/** the frame in map units: x right, y down */
export const FRAME = { w: world.w, h: world.h };

/** every country as [ISO alpha-2 or "", SVG path in map units] */
export const COUNTRIES = world.countries as [string, string][];

/** small countries' 1:10m outlines: middle x, y and larger side e in map units, the path on a grid where e is 100 */
export const TINY = world.tiny as unknown as Record<string, [number, number, number, string]>;

/**
 * the 1:10m land round each country of TINY (coast.json, built by web/scripts/build-coast.mjs): per country, every
 * country's land inside its patch as [ISO alpha-2 or "", SVG path], the country itself last, on a grid of g units
 * across its e and about its middle; a patch reaches hx across and hy down from that middle
 */
export type Coast = { g: number; hx: number; hy: number; patches: Record<string, [string, string][]> };
/** the coast patches, fetched apart from the page: only a map with a small hosted country draws them */
export const loadCoast = (): Promise<Coast> => import("./coast.json").then((m) => m.default as unknown as Coast);

const A1 = 1.340264, A2 = -0.081106, A3 = 0.000893, A4 = 0.003796, M = Math.sqrt(3) / 2;

/** lon/lat in degrees -> map units */
export function project(lon: number, lat: number): [number, number] {
  const l = (lon * Math.PI) / 180, p = (lat * Math.PI) / 180;
  const t = Math.asin(M * Math.sin(p)), t2 = t * t, t6 = t2 * t2 * t2;
  const x = (l * Math.cos(t)) / (M * (A1 + 3 * A2 * t2 + t6 * (7 * A3 + 9 * A4 * t2)));
  const y = t * (A1 + A2 * t2 + t6 * (A3 + A4 * t2));
  return [world.tx + world.k * x, world.ty - world.k * y];
}

const points = world.points as unknown as Record<string, [number, number]>;

/** a country's label point as [lon, lat], by ISO 3166-1 alpha-2 */
export function countryPoint(cc: string | undefined): [number, number] | null {
  if (!cc) return null;
  return points[cc.toUpperCase()] ?? null;
}
