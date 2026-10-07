package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// A refresh that fails keeps the picture held, and its date: the identity
// stays due, so the next run tries it again. An identity with no picture
// held gets the error row, and is handed back after the retry interval
// rather than the max age.
func TestAFailedAvatarRefreshKeepsThePicture(t *testing.T) {
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	const held, never = "D27EE330254D4F6A", "A1B2C3D4E5F60718"
	if _, err := st.UpsertValidatorIdentities([]scan.ValidatorIdentity{
		{ConsAddressHex: "aa", Identity: held, Status: "BOND_STATUS_BONDED"},
		{ConsAddressHex: "bb", Identity: never, Status: "BOND_STATUS_BONDED"},
	}, now); err != nil {
		t.Fatal(err)
	}
	if err := st.PutAvatar(held, "ok", "https://x/pic.jpg", "image/jpeg", []byte("jpeg"), now); err != nil {
		t.Fatal(err)
	}

	// A day on, the daily refresh fails for both.
	day := now.Add(25 * time.Hour)
	if kept, err := st.PutAvatarError(held, "keybase lookup: HTTP 503", day); err != nil || !kept {
		t.Fatalf("held picture: kept=%v err=%v", kept, err)
	}
	if kept, err := st.PutAvatarError(never, "keybase lookup: HTTP 503", day); err != nil || kept {
		t.Fatalf("no picture held: kept=%v err=%v", kept, err)
	}
	ct, data, checked, ok, err := st.Avatar(ctx, held)
	if err != nil || !ok || ct != "image/jpeg" || string(data) != "jpeg" || !checked.Equal(now) {
		t.Fatalf("after a failed refresh the avatar is %q %q %s ok=%v err=%v; the picture and its date must stay", ct, data, checked, ok, err)
	}
	var served int
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM validator_avatars WHERE status = 'ok'`).Scan(&served); err != nil || served != 1 {
		t.Fatalf("rows the API serves as has_avatar: %d (%v)", served, err)
	}

	// The held one is still due an hour later (its check is the old one);
	// the failed one is not due by the max age...
	hour := day.Add(time.Hour + time.Minute)
	due, err := st.AvatarsDue(ctx, hour, 24*time.Hour, 100)
	if err != nil || len(due) != 1 || due[0] != held {
		t.Fatalf("due by age = %v (%v), want only the held picture", due, err)
	}
	// ...but by the retry interval.
	failed, err := st.FailedAvatarsDue(ctx, hour, time.Hour, 100)
	if err != nil || len(failed) != 1 || failed[0] != never {
		t.Fatalf("failed due = %v (%v), want the identity with no picture", failed, err)
	}
	if failed, _ := st.FailedAvatarsDue(ctx, day.Add(30*time.Minute), time.Hour, 100); len(failed) != 0 {
		t.Fatalf("a failure retried before the interval: %v", failed)
	}

	// A later success replaces the error row; an explicit "no picture"
	// still replaces a held one.
	if err := st.PutAvatar(never, "ok", "https://x/b.jpg", "image/png", []byte("png"), hour); err != nil {
		t.Fatal(err)
	}
	if _, _, _, ok, _ := st.Avatar(ctx, never); !ok {
		t.Fatal("a picture fetched after a failure is not served")
	}
	if failed, _ := st.FailedAvatarsDue(ctx, hour.Add(2*time.Hour), time.Hour, 100); len(failed) != 0 {
		t.Fatalf("a resolved identity is still a failure: %v", failed)
	}
	if err := st.PutAvatar(held, "none", "", "", nil, hour); err != nil {
		t.Fatal(err)
	}
	if _, _, _, ok, _ := st.Avatar(ctx, held); ok {
		t.Fatal("a picture Keybase no longer has is still served")
	}
}
