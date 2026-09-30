//go:build race

package api

// raceOn: the race detector slows every statement several times over, so
// the partials' longer runs are shortened or skipped under it.
const raceOn = true
