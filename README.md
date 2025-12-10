# SoccerFieldMapData

This repository provides a resumable scraper for soccerfieldmap.com with Wikipedia enrichment and built-in retry/backoff logic.

## What it does
- Pulls paginated field listings from `https://www.soccerfieldmap.com/api/fields` (adjustable with `LISTING_URL`).
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

The database stores:
- `fields` table: name, address (required), coordinates, inferred area (square metres), pitch count, and Wikipedia source info.
- `progress` table: tracks the last listing page processed to enable resuming.

Outputs are written immediately to the database after each field; retries/backoff and graceful interrupt handling prevent unexpected crashes.
