/**
 * Whether the site shows the service figures: Service rate, Not served, In
 * retention window and the "not read by Tensile" count on the validator page,
 * the per-validator "not served" words on the blob page, and every not-served
 * count, red mark and served=no link elsewhere.
 *
 * Off for now. The API still judges a validator by its own end reading, so a
 * validator whose reading got no rows counts as not served even when the blob
 * was read in full from the others: a validator at 100% reachability shows
 * shards "not served" on blobs that were all available. That is wrong data,
 * so it is not shown. Flip this back to true when the protocol-level reading
 * (PR #138) ships and the API counts service that way.
 *
 * Reachability, endorsements, blob availability and everything else stay on.
 */
export const SHOW_SERVICE: boolean = false;
