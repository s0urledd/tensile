package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/collect"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/keybase"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// Validator pictures: the identity field on chain is a Keybase key suffix,
// and the picture behind it is fetched here, once a day per identity, into
// the store, so the site can show it without a reader ever contacting
// Keybase. Sequential and spaced, because Keybase is somebody else's API.
//
// On a goroutine of its own, with a time budget per batch. It ran inside
// the collector's one loop, between passes, at up to two Keybase requests of
// fifteen seconds each per identity, and every identity falls due on the
// same day: a Keybase that hung connections held the ingest up for most of
// an hour (a mainnet-sized validator set for longer), with the status file
// still fresh. Here it only waits on Keybase; the store, which has one
// connection, is touched by short calls that hold nothing open across a
// request (AvatarsDue reads its rows whole before returning).
//
// A refresh that fails keeps the picture already held (store.PutAvatarError)
// and is tried again on the next run rather than a day later; the failure is
// counted on the status file (detail "avatars") and in the log. Keybase being
// down is not this observer's fault and alerts nobody; a store write that
// fails is, and goes to the work list.

// avatarSource is the part of keybase.Client the resolver uses.
type avatarSource interface {
	Lookup(ctx context.Context, identity string) (string, error)
	Fetch(ctx context.Context, pictureURL string) (contentType string, data []byte, err error)
}

// Bounds of one batch. A full batch is followed by the next a minute later,
// so a first install with a few hundred identities fills in minutes, not
// days; anything left over waits for the next run.
const (
	avatarBatch   = 40
	avatarBudget  = 2 * time.Minute
	avatarSpacing = 300 * time.Millisecond
	avatarCatchUp = time.Minute
)

type avatarResolver struct {
	st   *store.Store
	kb   avatarSource
	logf logf
	live liveStatus
	work *collect.WorkErrors
	// maxAge is -avatar-max-age; retryAfter how long a failed identity
	// waits before it is tried again (half of -avatars-every, so the next
	// run takes it).
	maxAge, retryAfter time.Duration
	// batch, budget and spacing are the constants above; tests shorten them.
	batch   int
	budget  time.Duration
	spacing time.Duration
	now     func() time.Time
}

func newAvatarResolver(st *store.Store, kb avatarSource, every, maxAge time.Duration, logf logf, live liveStatus, work *collect.WorkErrors) *avatarResolver {
	return &avatarResolver{st: st, kb: kb, logf: logf, live: live, work: work,
		maxAge: maxAge, retryAfter: every / 2,
		batch: avatarBatch, budget: avatarBudget, spacing: avatarSpacing, now: time.Now}
}

// avatarRun is what one batch did.
type avatarRun struct {
	Due, Checked, Pictures, None, Failed, Kept int
	// Cut: the budget ran out (or the collector is stopping) before the
	// batch was done.
	Cut bool
	// LastError is the latest Keybase failure, for the status file.
	LastError string
	// StoreErr is the first store failure, for the work list.
	StoreErr error
}

// loop runs a batch now and then every every (or avatarCatchUp after a full
// batch) until ctx ends; done is closed when it has returned.
func (r *avatarResolver) loop(ctx context.Context, every time.Duration, done chan<- struct{}) {
	defer close(done)
	for {
		res := r.run(ctx)
		wait := every
		if !res.Cut && res.Failed == 0 && res.StoreErr == nil && res.Due >= r.batch {
			wait = avatarCatchUp
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

// run is one batch.
func (r *avatarResolver) run(ctx context.Context) avatarRun {
	var out avatarRun
	now := r.now()
	due, err := r.due(ctx, now)
	if err != nil {
		if ctx.Err() == nil {
			r.logf("avatars: %v", err)
			r.work.Report("avatars", err, now)
		}
		return out
	}
	out.Due = len(due)
	bctx, cancel := context.WithTimeout(ctx, r.budget)
	defer cancel()
	storeErr := func(id string, err error) {
		r.logf("avatars: store %s: %v", id, err)
		if out.StoreErr == nil {
			out.StoreErr = fmt.Errorf("store %s: %w", id, err)
		}
	}
	failed := func(id, reason string) {
		out.Failed++
		out.LastError = id + ": " + reason
		kept, err := r.st.PutAvatarError(id, reason, r.now())
		if err != nil {
			storeErr(id, err)
		} else if kept {
			out.Kept++
		}
	}
	for i, id := range due {
		if i > 0 && !sleepCtx(bctx, r.spacing) {
			out.Cut = true
			break
		}
		u, ct, data, err := r.resolve(bctx, id)
		if bctx.Err() != nil {
			// The budget, or the collector stopping: not this identity's
			// failure, and it stays due.
			out.Cut = true
			break
		}
		out.Checked++
		switch {
		case errors.Is(err, keybase.ErrNoPicture):
			// An explicit answer: the picture held, if any, goes.
			out.None++
			if err := r.st.PutAvatar(id, "none", "", "", nil, r.now()); err != nil {
				storeErr(id, err)
			}
		case err != nil:
			failed(id, err.Error())
		default:
			if err := r.st.PutAvatar(id, "ok", u, ct, data, r.now()); err != nil {
				storeErr(id, err)
			} else {
				out.Pictures++
			}
		}
	}
	if ctx.Err() != nil {
		return out // stopping: nothing more to say
	}
	if out.Due > 0 {
		cut := ""
		if out.Cut {
			cut = fmt.Sprintf(" (stopped at the %s budget; the rest stay due)", r.budget)
		}
		r.logf("avatars: %d identities checked: %d pictures, %d without one, %d failed (%d kept the picture held)%s",
			out.Checked, out.Pictures, out.None, out.Failed, out.Kept, cut)
		if out.LastError != "" {
			r.logf("avatars: last failure: %s", out.LastError)
		}
	}
	r.live.Set("avatars", map[string]any{
		"run_at": now.UTC().Format(time.RFC3339), "due": out.Due, "checked": out.Checked,
		"pictures": out.Pictures, "none": out.None, "failed": out.Failed, "kept": out.Kept,
		"cut": out.Cut, "last_error": out.LastError,
	})
	r.work.Report("avatars", out.StoreErr, now)
	// Polled: Keybase answered at least one lookup, or the batch ran to its
	// end without a failure (a run with nothing due is a poll too). A run
	// where every lookup failed, or that the budget cut before any answer,
	// says nothing about the pictures held.
	if out.Pictures+out.None > 0 || (!out.Cut && out.Failed == 0) {
		if err := r.st.SetMeta("avatars_polled_at", store.TS(now), now); err != nil {
			r.logf("avatars: meta: %v", err)
		}
	}
	return out
}

// resolve looks one identity up and fetches its picture: the picture's URL,
// type and bytes, or why there is none (keybase.ErrNoPicture, or a failure
// naming the URL when the fetch failed).
func (r *avatarResolver) resolve(ctx context.Context, id string) (u, ct string, data []byte, err error) {
	if u, err = r.kb.Lookup(ctx, id); err != nil {
		return "", "", nil, err
	}
	if ct, data, err = r.kb.Fetch(ctx, u); err != nil {
		return u, "", nil, fmt.Errorf("%s: %w", u, err)
	}
	return u, ct, data, nil
}

// due is the batch: identities due by age first (never resolved, then the
// oldest check), then failures due a retry, each once, at most r.batch.
func (r *avatarResolver) due(ctx context.Context, now time.Time) ([]string, error) {
	byAge, err := r.st.AvatarsDue(ctx, now, r.maxAge, r.batch)
	if err != nil {
		return nil, err
	}
	failed, err := r.st.FailedAvatarsDue(ctx, now, r.retryAfter, r.batch)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var out []string
	for _, id := range append(byAge, failed...) {
		if len(out) >= r.batch {
			break
		}
		if k := strings.ToUpper(id); !seen[k] {
			seen[k] = true
			out = append(out, id)
		}
	}
	return out, nil
}

// sleepCtx waits d, or until ctx ends; false when it ended.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
