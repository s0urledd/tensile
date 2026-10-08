package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// A full verdict cache gives up the verdict used longest ago. It used to
// start again from nothing once full, so a walk through more publications
// than it holds emptied it over and over, and the first page's verdicts,
// which every reader asks for, went with the rest.
func TestAFullBlobCacheKeepsTheVerdictsInUse(t *testing.T) {
	c := newBlobCacheOf(4)
	c.put("hot", blobVerdict{fp: "1"})
	for i := 0; i < 10; i++ {
		if _, ok := c.get("hot", "1"); !ok {
			t.Fatalf("after %d other verdicts the one in use was given up", i)
		}
		c.put(fmt.Sprintf("walk%d", i), blobVerdict{fp: "1"})
	}
	if n := len(c.m); n != 4 {
		t.Errorf("%d verdicts kept, want the bound, 4", n)
	}
	if _, ok := c.get("walk9", "1"); !ok {
		t.Error("the verdict put last is not kept")
	}
	if _, ok := c.get("walk0", "1"); ok {
		t.Error("the verdict used longest ago is still kept in a full cache")
	}
}

// The verdicts /v1/blobs computes take one of blobWorkSlots each, whatever
// asks for them: with every slot taken, a page with a verdict to compute
// waits for one. Before, every page computed its verdicts at once on its
// own request, and deep pages walked by a crawler held every connection of
// the pool.
func TestBlobVerdictsAreComputedInTheirSlots(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	now := time.Now().UTC().Truncate(time.Second)
	// in its window still, so its verdict is never cached: computed on
	// every read
	pub := scan.Publication{SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: "open1", SettlementHeight: 100,
		SettlementTime: now.Add(-time.Hour), MustServeUntil: now.Add(time.Hour), SettlementTxHash: "tx1",
		Promise: scan.PromiseFields{Commitment: "cc", Height: 99, CreationTimestamp: now.Add(-time.Hour)}}
	pub.Assignment.ProtocolParams.OriginalRows, pub.Assignment.ProtocolParams.TotalRows = 4, 16
	raw, _ := json.Marshal(pub)
	if _, err := st.UpsertPublication(pub, raw); err != nil {
		t.Fatal(err)
	}
	s := NewWithVantage(st, VantageInfo{Name: "test"}, nil)
	defer s.Close()

	for i := 0; i < blobWorkSlots; i++ {
		s.blobWork <- struct{}{}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if rows, err := s.blobRowsAt(ctx, "", 10, 0); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("with every slot taken a page with a verdict to compute was answered: %d rows, %v", len(rows), err)
	}
	for i := 0; i < blobWorkSlots; i++ {
		<-s.blobWork
	}
	rows, err := s.blobRowsAt(context.Background(), "", 10, 0)
	if err != nil || len(rows) != 1 || rows[0].Reconstructable == nil {
		t.Fatalf("with the slots free: %d rows, %v", len(rows), err)
	}
}
