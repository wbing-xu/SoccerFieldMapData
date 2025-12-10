# SoccerFieldMapData

This repository provides a resumable scraper for soccerfieldmap.com with Wikipedia enrichment and built-in retry/backoff logic.

## What it does
- Pulls paginated field listings from `https://www.soccerfieldmap.com/api/fields` (adjustable with `LISTING_URL`). Pagination starts at page **1** to match the live API, uses browser-like headers plus a homepage warmup to mimic real users, and gracefully stops if the server replies 404 for a page.
- If the API endpoint keeps returning 404 for the first page, the scraper automatically falls back to extracting the field list from the public Explore page (`https://www.soccerfieldmap.com/explore`) by parsing the embedded Next.js data blob, tolerating both script-tag and inline `__NEXT_DATA__` shapes. If that markup is unavailable (e.g., the Explore page only renders a block/403 shell), it will grab the Next.js `buildId` from any reachable page (including by scanning static asset URLs when the data blob is missing) and hit the corresponding `/_next/data/{buildId}/explore.json` endpoint before giving up. If every Next.js path is blocked, it will crawl the Explore page like a user to harvest all `/field/...` links and parse each detail page's `application/ld+json`. As a final escape hatch, it scans the public sitemap for `/field/` URLs and parses each page to recover names/addresses so the run can still complete.
- For each field, attempts to enrich the record with area information from Wikipedia via the public API and validates that the area falls in a plausible range.
- Saves progress and results directly to CSV so the job can restart after any interruption; each row is flushed immediately so you can inspect data live while the scraper runs.

## Running the scraper
```bash
python scraper.py \
  --csv data/fields.csv \
  --progress data/progress.json \
  --page-size 200 \
  --wiki-workers 5 \
  --backoff 1.0 \
  --backoff-cap 20 \
  --log-level INFO
```

Key options:
- `--wiki-workers`: concurrent Wikipedia lookups to speed up enrichment while staying polite.
- `--max-attempts`, `--backoff`, `--backoff-cap`, `--timeout`: tune anti-scraping resilience and retry behaviour.
- `--csv`: path to the incrementally written CSV output; existing rows are deduplicated so restarts only append new fields.
- `--progress`: JSON file that records the last completed listing page for quick resume without SQLite.
- `--max-pages`: stop early for spot checks; progress is always saved in the JSON checkpoint and the CSV is flushed on every row.
- HTTP requests include rotating User-Agents, Referer headers, and an automatic warmup visit to the homepage to pick up cookies; 404s on the listing endpoint are suppressed and treated as end-of-data rather than hard failures.

Each CSV row contains:
- 场地名
- 场地链接（scraped URL on soccerfieldmap.com）
- 场地面积（从 Wikipedia 解析到的面积，无法获取则留空）
- 场地面积 wiki 链接（如果找到面积对应的页面，否则留空）
