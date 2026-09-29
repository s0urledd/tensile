# web

The observer dashboard: a static Next.js export that reads the observer API
in the browser. Three data views (network overview, validator detail, blob
detail) plus methodology, about and API pages.

```
npm ci
NEXT_PUBLIC_API_BASE=http://127.0.0.1:8080 npm run dev     # against a local observer-api
npm run build                                               # writes out/ for Caddy
```

`NEXT_PUBLIC_API_BASE` defaults to same-origin `/api`, which is what the
Caddyfile in `deploy/` proxies to `observer-api`. `NEXT_PUBLIC_API_URL` is
the public address the API page (`/developers/`) prints in its commands,
`https://tensile.huginn.tech/api/v1` unless set.

The pages share one small system, all in `src/app/globals.css`: white paper
and hairlines rather than cards, IBM Plex Sans for prose and figures with
Plex Mono only for addresses, hashes and heights, nothing heavier than 600,
and one accent. Green marks only "reachable", amber only "unreachable" and
observer-side notices, and red only a broken obligation, because that is the
only accusation the site makes. Every headline figure maps to one field of
the API; definitions stay on the methodology page, one disclosure away.
Verdict arithmetic follows `docs/verdicts.md`; `test/README.md` describes
the fixture and the audit that check the pages at scale.
