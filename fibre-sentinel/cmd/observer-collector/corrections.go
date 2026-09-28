package main

import (
	"log"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// bumpHoldsRevision tells the API that a hold was raised or lifted, or a
// verdict moved. Its cached aggregates run to a fifteen-minute TTL, and a
// withheld fault republished for minutes after the hold landed is the
// accusation the hold exists to stop.
//
// It goes through the store's counter, like every other path that moves
// this key. Writing now.UnixNano() here gave two bumps in one collector
// pass the same token, because the pass stamps one time.Now() and threads
// it through everything it ingests — and a token that does not change does
// not invalidate the snapshot holding the withdrawn fault.
func bumpHoldsRevision(st *store.Store, now time.Time) {
	if err := st.BumpParamHoldsRev(now); err != nil {
		log.Printf("param holds revision: %v", err)
	}
}
