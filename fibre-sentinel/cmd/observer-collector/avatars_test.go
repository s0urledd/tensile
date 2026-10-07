package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/collect"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/keybase"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// fakeKeybase answers lookups from a table; down fails every request the
// way an overloaded Keybase does, hang holds every request until its
// context ends.
type fakeKeybase struct {
	mu      sync.Mutex
	picture map[string]string // identity -> picture bytes; absent = no picture
	down    bool
	hang    bool
	asked   int
}

func (k *fakeKeybase) Lookup(ctx context.Context, id string) (string, error) {
	k.mu.Lock()
	k.asked++
	down, hang := k.down, k.hang
	pic, ok := k.picture[id]
	k.mu.Unlock()
	switch {
	case hang:
		<-ctx.Done()
		return "", ctx.Err()
	case down:
		return "", errors.New("keybase lookup: HTTP 503")
	case !ok:
		return "", keybase.ErrNoPicture
	}
	return "https://s3.amazonaws.com/keybase/" + id + "/" + pic, nil
}

func (k *fakeKeybase) Fetch(_ context.Context, u string) (string, []byte, error) {
	for id, pic := range k.picture {
		if u == "https://s3.amazonaws.com/keybase/"+id+"/"+pic {
			return "image/jpeg", []byte(pic), nil
		}
	}
	return "", nil, fmt.Errorf("picture: HTTP 404")
}

func newResolver(t *testing.T, kb avatarSource, ids ...string) (*avatarResolver, *store.Store, *fakeLive, *time.Time) {
	t.Helper()
	st := openStore(t)
	var vs []scan.ValidatorIdentity
	for i, id := range ids {
		vs = append(vs, scan.ValidatorIdentity{ConsAddressHex: fmt.Sprintf("%02x", i+1), Identity: id, Status: "BOND_STATUS_BONDED"})
	}
	clock := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	if _, err := st.UpsertValidatorIdentities(vs, clock); err != nil {
		t.Fatal(err)
	}
	live := &fakeLive{}
	r := newAvatarResolver(st, kb, time.Hour, 24*time.Hour, quiet, live, collect.NewWorkErrors(live))
	r.spacing = 0
	r.now = func() time.Time { return clock }
	return r, st, live, &clock
}

// The daily refresh meets a Keybase outage: every picture already held stays
// on the site, and the next run (an hour later, not a day) tries again and
// takes the new pictures.
func TestAKeybaseOutageKeepsThePictures(t *testing.T) {
	const a, b = "D27EE330254D4F6A", "A1B2C3D4E5F60718"
	kb := &fakeKeybase{picture: map[string]string{a: "a1", b: "b1"}}
	r, st, live, clock := newResolver(t, kb, a, b)
	ctx := context.Background()
	if res := r.run(ctx); res.Pictures != 2 {
		t.Fatalf("first run: %+v", res)
	}
	if v, _ := st.Meta("avatars_polled_at"); v != store.TS(*clock) {
		t.Fatalf("avatars_polled_at = %q", v)
	}

	// A day on, Keybase is down for the batch.
	*clock = clock.Add(25 * time.Hour)
	kb.down = true
	res := r.run(ctx)
	if res.Failed != 2 || res.Kept != 2 {
		t.Fatalf("outage run: %+v", res)
	}
	for _, id := range []string{a, b} {
		if _, data, _, ok, _ := st.Avatar(ctx, id); !ok || len(data) == 0 {
			t.Fatalf("%s: the outage took the picture off the site", id)
		}
	}
	if v, _ := st.Meta("avatars_polled_at"); v == store.TS(*clock) {
		t.Fatalf("a run where every lookup failed counts as polled")
	}
	if got, _ := live.get("avatars").(map[string]any); got == nil || got["failed"] != 2 || got["last_error"] == "" {
		t.Fatalf("the failure is not on the status file: %v", live.get("avatars"))
	}
	if w, _ := live.get(collect.WorkErrorsKey).(map[string]any); len(w) != 0 {
		t.Fatalf("a Keybase outage went on the work list: %v", w)
	}

	// An hour later Keybase is back and the pictures changed: both are due
	// again (their check is still the old one) and both are taken.
	*clock = clock.Add(time.Hour)
	kb.down = false
	kb.picture = map[string]string{a: "a2", b: "b2"}
	if res := r.run(ctx); res.Pictures != 2 || res.Due != 2 {
		t.Fatalf("run after the outage: %+v", res)
	}
	if _, data, _, ok, _ := st.Avatar(ctx, a); !ok || string(data) != "a2" {
		t.Fatalf("new picture not taken: %q", data)
	}
	// And an explicit "no picture" still removes one.
	*clock = clock.Add(25 * time.Hour)
	delete(kb.picture, b)
	r.run(ctx)
	if _, _, _, ok, _ := st.Avatar(ctx, b); ok {
		t.Fatalf("a picture Keybase no longer has is still served")
	}
}

// An identity that never had a picture and failed is tried again on the
// next run, not after the max age.
func TestAFailedIdentityIsRetriedOnTheNextRun(t *testing.T) {
	const a = "D27EE330254D4F6A"
	kb := &fakeKeybase{picture: map[string]string{a: "a1"}, down: true}
	r, st, _, clock := newResolver(t, kb, a)
	ctx := context.Background()
	if res := r.run(ctx); res.Failed != 1 || res.Kept != 0 {
		t.Fatalf("first run: %+v", res)
	}
	*clock = clock.Add(time.Hour)
	kb.down = false
	if res := r.run(ctx); res.Due != 1 || res.Pictures != 1 {
		t.Fatalf("the next run did not retry the failure: %+v", res)
	}
	if _, _, _, ok, _ := st.Avatar(ctx, a); !ok {
		t.Fatal("picture not held after the retry")
	}
}

// A Keybase that hangs costs one batch its budget and nothing else: the
// batch stops, records no failure for what it never heard back about, and
// the store stays free for the ingest the whole time.
func TestAHangingKeybaseHoldsOnlyItsOwnGoroutine(t *testing.T) {
	const a, b = "D27EE330254D4F6A", "A1B2C3D4E5F60718"
	kb := &fakeKeybase{picture: map[string]string{a: "a1", b: "b1"}, hang: true}
	r, st, _, _ := newResolver(t, kb, a, b)
	r.budget = 300 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	start := time.Now()
	go r.loop(ctx, time.Hour, done)

	// While the batch waits on Keybase, the store answers at once.
	time.Sleep(50 * time.Millisecond)
	for i := 0; i < 20; i++ {
		t0 := time.Now()
		if err := st.SetMeta("ingest_probe", fmt.Sprint(i), time.Now()); err != nil {
			t.Fatal(err)
		}
		if _, err := st.Count(context.Background()); err != nil {
			t.Fatal(err)
		}
		if d := time.Since(t0); d > 200*time.Millisecond {
			t.Fatalf("a store call waited %s on the avatar batch", d)
		}
	}
	// The batch gives up at its budget; the loop then waits for the next
	// run, and stops when told.
	deadline := time.Now().Add(5 * time.Second)
	for {
		var n int
		if err := st.DB().QueryRow(`SELECT COUNT(*) FROM validator_avatars`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Fatalf("a request that never answered was recorded as a failure (%d row(s))", n)
		}
		kb.mu.Lock()
		asked := kb.asked
		kb.mu.Unlock()
		if asked >= 1 && time.Since(start) > r.budget+100*time.Millisecond {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the batch did not stop at its budget")
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the resolver did not stop")
	}
	if v, _ := st.Meta("avatars_polled_at"); v != "" {
		t.Fatalf("a batch cut before any answer counts as polled: %s", v)
	}
}

// A batch takes what is due by age first, then failures due a retry, each
// identity once, at most the batch size.
func TestTheAvatarBatchIsBounded(t *testing.T) {
	var ids []string
	for i := 0; i < 7; i++ {
		ids = append(ids, fmt.Sprintf("%016X", i+1))
	}
	kb := &fakeKeybase{picture: map[string]string{}}
	r, _, _, _ := newResolver(t, kb, ids...)
	r.batch = 3
	res := r.run(context.Background())
	if res.Due != 3 || res.Checked != 3 || res.None != 3 {
		t.Fatalf("batch: %+v", res)
	}
	if res := r.run(context.Background()); res.Due != 3 {
		t.Fatalf("second batch: %+v", res)
	}
	if res := r.run(context.Background()); res.Due != 1 {
		t.Fatalf("last batch: %+v", res)
	}
}
