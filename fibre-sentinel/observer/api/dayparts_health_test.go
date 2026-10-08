package api

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The partials in /v1/health say that their load or their last audit
// failed, and how many times, never the error: a load's names the file it
// was loading under the data directory, and either carries the store's or
// the system's own text. The journal has it, as it has a failed write's.
func TestDayPartsHealthPublishesNoLoadOrAuditError(t *testing.T) {
	dp := &dayParts{}
	now := time.Now()
	file := filepath.Join("/var/lib/fibre-observer/mocha/snapshots", dayPartsFile)
	for i := 0; i < 2; i++ {
		err := &os.PathError{Op: "open", Path: file, Err: errors.New("permission denied")}
		dp.fail("load", now, loadRetryBase, loadRetryCeiling, fmt.Errorf("loading %s: %w (every window is read whole meanwhile)", file, err))
	}
	h, _ := dp.health(now)
	if h.State != "loading" || h.Why != partsLoadFailed(2) {
		t.Errorf("a load that failed twice: state %q, why %q", h.State, h.Why)
	}
	seal := filepath.Join("/var/lib/fibre-observer/mocha/snapshots", dayPartsDir, "rows-2026-10-01.json")
	dp.noteAudit(auditResult{}, &os.PathError{Op: "read", Path: seal, Err: errors.New("input/output error")}, now)
	h, _ = dp.health(now)
	if h.LastAudit != partsAuditFailed || h.LastAuditAt == nil {
		t.Errorf("an audit that failed: %q at %v", h.LastAudit, h.LastAuditAt)
	}
	for _, leak := range []string{"/var/lib", dayPartsFile, dayPartsDir, "permission denied", "input/output"} {
		if strings.Contains(h.Why, leak) || strings.Contains(h.LastAudit, leak) {
			t.Errorf("/v1/health carries %q: why %q, last audit %q", leak, h.Why, h.LastAudit)
		}
	}
}
