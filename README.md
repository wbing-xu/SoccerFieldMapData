# SoccerFieldMapData

This repository provides a resumable scraper for soccerfieldmap.com with Wikipedia enrichment and built-in retry/backoff logic.

## What it does
- Pulls paginated field listings from `https://www.soccerfieldmap.com/api/fields` (adjustable with `LISTING_URL`). Pagination starts at page **1** to match the live API, uses browser-like headers plus a homepage warmup to mimic real users, and gracefully stops if the server replies 404 for a page.
- If the API endpoint keeps returning 404 for the first page, the scraper automatically falls back to extracting the field list from the public Explore page (`https://www.soccerfieldmap.com/explore`) by parsing the embedded Next.js data blob, tolerating both script-tag and inline `__NEXT_DATA__` shapes. If that markup is unavailable (e.g., the Explore page only renders a block/403 shell), it will grab the Next.js `buildId` from any reachable page and hit the corresponding `/_next/data/{buildId}/explore.json` endpoint before giving up.
- For each field, attempts to enrich the record with area and pitch-count information from Wikipedia via the public API.
- Saves progress and results in a SQLite database so the job can restart after any interruption.

## Running the scraper
```bash
python scraper.py \
  --db data/fields.sqlite \
  --page-size 200 \
  --wiki-workers 5 \
  --backoff 1.0 \
  --backoff-cap 20 \
  --log-level INFO
```

Key options:
- `--wiki-workers`: concurrent Wikipedia lookups to speed up enrichment while staying polite.
- `--max-attempts`, `--backoff`, `--backoff-cap`, `--timeout`: tune anti-scraping resilience and retry behaviour.
- `--max-pages`: stop early for spot checks; progress is always saved in SQLite tables.
- HTTP requests include rotating User-Agents, Referer headers, and an automatic warmup visit to the homepage to pick up cookies; 404s on the listing endpoint are suppressed and treated as end-of-data rather than hard failures.

The database stores:
- `fields` table: name, address (required), coordinates, inferred area (square metres), pitch count, and Wikipedia source info.
- `progress` table: tracks the last listing page processed to enable resuming.

Outputs are written immediately to the database after each field; retries/backoff and graceful interrupt handling prevent unexpected crashes.
