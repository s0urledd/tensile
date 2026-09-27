package policy

import (
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
)

// assumeNothing is the policy with sampling and the per-validator byte caps
// off: what is left assumes nothing about what a validator can serve.
func assumeNothing() Config {
	cfg := Default()
	cfg.Sampling.Enabled = false
	cfg.Caps.PerValidator.BytesPerHourFraction = 0
	cfg.Caps.PerValidator.BytesPerDayFraction = 0
	cfg.Capacity.FloorValidatorBps = 0 // not read with both byte caps off
	cfg.Caps.PerValidator.RequestsPerMinute = 0
	cfg.Caps.PerValidator.MinRequestSpacing = 0 // keeps the test fast; spacing is covered elsewhere
	return cfg
}

// With sampling off every publication is admitted, however far over the
// budget the load is, and no draw is stamped on its rows.
func TestSamplingOffAdmitsEveryPublication(t *testing.T) {
	cfg := assumeNothing()
	cfg.Caps.Global.BytesPerHour = 1 // with sampling on, this would draw almost everything out
	p := newTest(t, cfg)
	now := time.Now()
	for i := 0; i < 50; i++ {
		pub := pubOf("h"+itoa(i), now, 128<<20, 4096, 148)
		if ok, why := p.Admit(pub, false); !ok {
			t.Fatalf("publication %d sampled out with sampling off: %s", i, why)
		}
		if _, _, commitment := p.SamplingFor(pub); commitment != "" {
			t.Fatalf("publication %d stamped with a day commitment %q although no draw was made", i, commitment)
		}
	}
}

// With the per-validator byte caps off, a validator is read however many
// bytes that costs; the observer's own global cap still binds.
func TestNoPerValidatorByteCap(t *testing.T) {
	now := time.Now()
	pub := pubOf("h", now, 128<<20, 4096)
	tgt := probe.Target{AddressHex: pub.Assignment.Validators[0].Address, RowCount: 4096}
	shard := ShardBytes(128<<20, 4096, 4096)
	read := func(p *Policy) (bool, string) {
		allow, why := p.BeforeProbe(pub, tgt, now)
		if allow {
			m := probe.Measurement{ValidatorAddress: tgt.AddressHex, StartedAt: now, AssignedRowCount: 4096}
			m.Download.Attempted = true
			p.AfterProbe(pub, m)
		}
		return allow, why
	}

	// Twenty full 128 MiB shards in a minute: far past what the old
	// per-validator caps allowed a 4096-row validator in a day.
	p := newTest(t, assumeNothing())
	for i := 0; i < 20; i++ {
		if allow, why := read(p); !allow {
			t.Fatalf("read %d denied with the per-validator byte caps off: %s", i, why)
		}
	}

	cfg := assumeNothing()
	cfg.Caps.Global.BytesPerHour = 3 * shard
	p = newTest(t, cfg)
	for i := 0; i < 3; i++ {
		if allow, why := read(p); !allow {
			t.Fatalf("read %d denied under the global cap: %s", i, why)
		}
	}
	if allow, why := read(p); allow || why != "budget:global_bytes_per_hour" {
		t.Fatalf("fourth read: allow=%v reason=%q, want denied by the global hourly cap", allow, why)
	}
}

// A negative per-validator fraction is a typo that would deny every probe,
// and is refused; a zero global cap would too, and is refused as before.
func TestNoCapValuesValidate(t *testing.T) {
	cfg := assumeNothing()
	cfg.Sampling.AllowEphemeralSecret = true
	if _, err := New(cfg); err != nil {
		t.Fatalf("policy with sampling and per-validator byte caps off rejected: %v", err)
	}
	neg := cfg
	neg.Caps.PerValidator.BytesPerDayFraction = -0.1
	if _, err := New(neg); err == nil || !strings.Contains(err.Error(), "must not be negative") {
		t.Fatalf("negative fraction: err = %v", err)
	}
	zero := cfg
	zero.Caps.Global.BytesPerDay = 0
	if _, err := New(zero); err == nil {
		t.Fatal("zero global daily cap accepted")
	}
	onNoModel := cfg
	onNoModel.Caps.PerValidator.BytesPerHourFraction = 0.01
	if _, err := New(onNoModel); err == nil || !strings.Contains(err.Error(), "capacity_model") {
		t.Fatalf("per-validator cap without a capacity model: err = %v", err)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
