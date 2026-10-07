package store

import (
	"context"
	"strings"
	"time"
)

// A failed avatar refresh, kept apart from the picture.
//
// PutAvatar records whatever one resolution found, and the collector used it
// for failures too: an "error" row with no data replaced the picture held
// for the identity. Every identity falls due on the same day, so one Keybase
// outage (an HTTP 5xx or 429, a timeout, a status that is not 0) during the
// daily batch took every validator's picture off the site, and the next try
// was a whole max age later, because the error row was dated like a
// success.
//
// A failure now leaves a held picture as it was, checked_at included: the
// picture is still the last one Keybase gave, and an identity whose check
// is old stays due, so the next run (hourly) tries it again rather than the
// next day. Only an identity with no picture held gets the error row, and
// FailedAvatarsDue hands those back sooner than the max age. An explicit
// "no picture" from Keybase, or a new picture, still replaces a held one
// through PutAvatar.

// PutAvatarError records a failed resolution of identity, reason being why.
// kept is true when a picture was held, in which case nothing is written:
// the failure is the caller's to report.
func (s *Store) PutAvatarError(identity, reason string, now time.Time) (kept bool, err error) {
	identity = strings.ToUpper(identity)
	// One statement, so nothing can land between the test and the write:
	// the update applies only where no picture is held.
	res, err := s.db.Exec(`INSERT INTO validator_avatars (identity, url, content_type, data, status, checked_at)
		VALUES (?, ?, '', NULL, 'error', ?)
		ON CONFLICT(identity) DO UPDATE SET url = excluded.url, content_type = excluded.content_type,
			data = excluded.data, status = excluded.status, checked_at = excluded.checked_at
		WHERE NOT (validator_avatars.status = 'ok' AND COALESCE(LENGTH(validator_avatars.data), 0) > 0)`,
		identity, reason, ts(now))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 0, nil
}

// FailedAvatarsDue lists the identities of known validators whose last
// resolution failed with no picture held, last tried before now - retryAfter,
// at most limit of them, oldest attempt first. AvatarsDue still hands every
// identity back after the max age; this is the sooner retry for failures.
func (s *Store) FailedAvatarsDue(ctx context.Context, now time.Time, retryAfter time.Duration, limit int) ([]string, error) {
	// The same well-formed-suffix test and case-insensitive join as
	// AvatarsDue.
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT vi.identity, a.checked_at
		FROM validator_identities vi JOIN validator_avatars a ON UPPER(a.identity) = UPPER(vi.identity)
		WHERE vi.identity GLOB '[0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f]'
		  AND a.status = 'error' AND a.checked_at < ?
		ORDER BY a.checked_at ASC LIMIT ?`, ts(now.Add(-retryAfter)), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id, checked string
		if err := rows.Scan(&id, &checked); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
