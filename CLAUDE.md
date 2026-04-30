# CLAUDE.md — `yl-hb-sp` (Spotify Artist Enrichment)

This file teaches Claude how this repo is laid out and what to be careful of
when editing it. Conventions shared across the `yl-hb-*` fleet live in
[`SCRAPER-CLAUDE-TEMPLATE.md`](../SCRAPER-CLAUDE-TEMPLATE.md) — read both.

## What this repo does

Pulls Spotify artist data via the `spotify-api25.p.rapidapi.com` RapidAPI host
and upserts it into the HypeBase Supabase project. Targets artists whose
Spotify socials are stale (`hb_socials.check_spotify_enrichment` older than 30
days) or never enriched (NULL). Also hydrates `hb_talent.image` and `biography`
when those fields are blank, and discovers cross-platform socials
(Instagram / Facebook / Twitter / Wikipedia) from the artist's Spotify
external links.

## Stack

**Standard enrichment** variant: TypeScript, Node 20, `ts-node --transpile-only`,
`@supabase/supabase-js`, `node-fetch`, `dotenv`. No browser needed.

## Repo layout

```
src/
  spotify-enrichment.ts    # entry point — invoked by the workflow
  supabase.ts              # service-role client (autoRefreshToken/persistSession off)
.github/workflows/
  spotify-unified-enrichment.yml
package.json
tsconfig.json
```

## Supabase auth

Standard fleet convention — `SUPABASE_URL` + `SUPABASE_SERVICE_KEY`, client in
`src/supabase.ts`. See the template for the canonical client setup.

## Workflow lifecycle convention

Standard: `log_workflow_run` start + result, `if: always()`, `WORKFLOW_ID_SPOTIFY_ENRICHMENT`
GitHub variable. See [`spotify-unified-enrichment.yml`](.github/workflows/spotify-unified-enrichment.yml).

The script *also* calls `log_workflow_run` directly via `supabase.rpc()` at
the end (`updateWorkflowSummary` in `spotify-enrichment.ts:81`) to attach a
JSON `p_summary` payload with `enriched`, `failed`, `talent_hydrated`,
`external_socials` counts. The CI start/end calls and the in-script summary
call all log to the same `workflows` table — by design.

Cron: `0 */6 * * *` (every 6h). `workflow_dispatch` accepts a `limit` input
(default 100).

## Tables this repo touches

| Table | Operation | Notes |
|---|---|---|
| `public.hb_socials` | SELECT (primary query — stale Spotify rows) and UPSERT | The main write target. Upsert via implicit primary key on `id`. External-social inserts use `onConflict: 'type,identifier'` with `ignoreDuplicates: true`. |
| `public.hb_talent` | SELECT (pre-load by `id`) and UPSERT | Only patches `image` and `biography` when they're currently blank — never overwrites curated talent fields. |

## Running locally

```bash
cp .env.example .env.local      # if .env.example exists; otherwise create .env
# Set: SUPABASE_URL, SUPABASE_SERVICE_KEY, RAPIDAPI_KEY
# Optional: LIMIT (default 100), STALE_DAYS (30), CONCURRENCY (3),
#           SLEEP_MS (1500), WORKFLOW_ID
npm install
npm start                        # → ts-node --transpile-only src/spotify-enrichment.ts
```

## Per-repo gotchas

- **A hardcoded fallback RapidAPI key was committed to git history**
  in the now-deleted `src/social-enrich-spotify.ts` (deleted as part of
  cleanup, but the value remains in git history at the parent of the
  deletion commit). Treat that key as compromised and rotate it in
  RapidAPI before relying on the new `RAPIDAPI_KEY` env var alone.
- **RapidAPI host is `spotify-api25.p.rapidapi.com`.** RapidAPI hosts come and
  go; if requests start 404'ing across the board, check the host name.
- **429 handling:** the script retries once after 10s on 429, then gives up
  for that artist and moves on. Don't tighten this without understanding
  RapidAPI's tier limits.
- **Cold-start retries on Supabase:** `withRetry()` wraps the initial
  `hb_socials` and `hb_talent` queries because the database can be slow to
  wake up (commit `261ed1b`). 3 attempts, 3s delay. Don't remove this.
- **Talent hydration is patch-only.** Only `image` and `biography` get filled,
  and only when blank. Never overwrite a curated value — the convention is
  that human-set fields win.
- **External-social discovery dedupes by `(linked_talent, type)`.** One
  Instagram per artist, one Twitter per artist, etc. Don't widen that.
- **No `.gitignore` exists in this repo.** That means `node_modules/`,
  `.env*`, `dist/` are technically eligible to be committed. Add one before
  the first stray commit slips through.
- **`detailed_array` column gets the full RapidAPI JSON response.** Schema
  changes upstream from RapidAPI will land in that column unchanged. If
  downstream code reads from `detailed_array`, it can break silently.

## Conventions Claude should follow when editing this repo

All the fleet-wide rules from [`SCRAPER-CLAUDE-TEMPLATE.md`](../SCRAPER-CLAUDE-TEMPLATE.md)
apply. The two most relevant here:

- **Don't introduce anon-key code paths.** Service-role only.
- **Keep `withRetry` on the initial hb_socials / hb_talent queries.**
  Supabase free-tier projects sleep; the retry is load-bearing.

## Related repos

- `yl-hb-am`, `yl-hb-dz`, `yl-hb-tmdb`, `yl-hb-imdb` — sibling enrichment
  workflows hitting different sources but writing to the same `hb_socials` /
  `hb_talent` tables.
- `yl-hb-dtp` — nightly cleanup/dedup that runs over what this repo writes.
- `hb_app_build` — the Next.js app that reads `hb_socials` / `hb_talent`.
