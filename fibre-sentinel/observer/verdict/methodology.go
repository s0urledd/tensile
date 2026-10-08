package verdict

// MethodologyVersion names the rules every published figure was computed
// under: the verdict taxonomy, what enters a rate and how it is counted. It
// is a date (with a .N suffix for a second change on the same day), bumped
// in the same change as any rule that can move a figure,
// and it travels with the figures (/v1/meta, every export's manifest) so a
// number can always be matched to the rules that made it. The history is
// the git log of the methodology page and of docs/verdicts.md.
const MethodologyVersion = "2026-10-08"
