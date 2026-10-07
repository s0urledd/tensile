package api

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/export"
)

// The pages of /v1/exports (handleExports): about two months of days when
// the caller does not ask for a number, and at most four years' worth.
const (
	exportsPageDefault = 60
	exportsPageMax     = 1500
)

// exportIndexCache keeps the exports index as last read, keyed by the
// index file's size and modification time: the collector rewrites it once
// a day, and the listing read and decoded the whole of it on every request.
type exportIndexCache struct {
	mu      sync.Mutex
	dir     string
	size    int64
	mod     time.Time
	entries []export.Entry
}

// read is export.ReadIndex(dir), from the cache while the file is the one
// it was read from. The entries are shared: the caller does not change
// them.
func (c *exportIndexCache) read(dir string) ([]export.Entry, error) {
	fi, err := os.Stat(filepath.Join(dir, "index.json"))
	if errors.Is(err, fs.ErrNotExist) {
		return []export.Entry{}, nil
	}
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries != nil && c.dir == dir && c.size == fi.Size() && c.mod.Equal(fi.ModTime()) {
		return c.entries, nil
	}
	entries, err := export.ReadIndex(dir)
	if err != nil {
		return nil, err
	}
	c.dir, c.size, c.mod, c.entries = dir, fi.Size(), fi.ModTime(), entries
	return entries, nil
}
