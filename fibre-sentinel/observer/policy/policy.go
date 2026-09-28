// Package policy keeps what is left of the prober's load policy: the
// sampling master secret, so the day secrets of the draws made while
// publications were sampled can still be revealed (reveal.go) and the
// draws audited against the rows.
//
// Nothing is sampled any more, and no budget or cap decides what is read:
// every blob is read the way celestia-app's Fibre client reads it
// (internal/probe, blobread.go), paced only by this observer's own request,
// byte and per-validator connection limits. Once the last day that had a
// draw is revealed, the secret file can be deleted and this package with
// it.
package policy

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.yaml.in/yaml/v3"
)

// Config is the on-disk policy (YAML). Only the master secret's file is
// read; the budget, cap and sampling keys of earlier policies are ignored,
// so a policy file written for them still loads.
type Config struct {
	Sampling struct {
		MasterSecretFile string `yaml:"master_secret_file"`
		// AllowEphemeralSecret permits a process-local master secret, which
		// is only ever right in a test: the day secrets it reveals would not
		// be the ones the draws used. Not settable from YAML.
		AllowEphemeralSecret bool `yaml:"-"`
	} `yaml:"sampling"`
}

// Load reads a YAML file. An empty path returns the zero Config.
func Load(path string) (Config, error) {
	var c Config
	if path == "" {
		return c, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	if err := yaml.Unmarshal(b, &c); err != nil {
		return c, fmt.Errorf("parse %s: %w", path, err)
	}
	return c, nil
}

// Policy holds the master secret.
type Policy struct {
	master    []byte
	ephemeral bool
}

// EphemeralSecret reports whether the master secret is process-local.
func (p *Policy) EphemeralSecret() bool { return p.ephemeral }

// New reads the master secret from cfg.Sampling.MasterSecretFile, or
// creates it (0600) when the file is absent. With no path the secret is
// process-local, which is refused unless the caller says this is a test: the
// secrets it revealed would not be the ones the draws used.
func New(cfg Config) (*Policy, error) {
	if cfg.Sampling.MasterSecretFile == "" && !cfg.Sampling.AllowEphemeralSecret {
		return nil, errors.New("sampling: master_secret_file is not set, so the day secrets revealed would not be " +
			"the ones the draws used; set it (deploy/README.md) or allow an ephemeral secret for a test")
	}
	master, err := loadOrCreateSecret(cfg.Sampling.MasterSecretFile)
	if err != nil {
		return nil, err
	}
	return &Policy{master: master, ephemeral: cfg.Sampling.MasterSecretFile == ""}, nil
}

func loadOrCreateSecret(path string) ([]byte, error) {
	if path == "" {
		b := make([]byte, 32)
		if _, err := rand.Read(b); err != nil {
			return nil, err
		}
		return b, nil
	}
	b, err := os.ReadFile(path)
	if err == nil {
		if len(b) < 16 {
			return nil, fmt.Errorf("%s: master secret shorter than 16 bytes", path)
		}
		return b, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	b = make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create master secret directory: %w", err)
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		return nil, fmt.Errorf("write master secret: %w", err)
	}
	return b, nil
}

// DaySecret derives the per-day secret for the UTC day containing t.
func (p *Policy) DaySecret(t time.Time) []byte {
	mac := hmac.New(sha256.New, p.master)
	mac.Write([]byte(t.UTC().Format("2006-01-02")))
	return mac.Sum(nil)
}

// DayCommitment is SHA256(day secret), the commitment the rows of that day
// carried.
func (p *Policy) DayCommitment(t time.Time) string {
	s := sha256.Sum256(p.DaySecret(t))
	return hex.EncodeToString(s[:])
}
