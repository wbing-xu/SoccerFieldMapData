"""Offline-friendly scraper for SoccerFieldMap.

This script downloads soccer field metadata from soccerfieldmap.com,
optionally enriches it with Wikipedia information (area and pitch count),
and stores everything in a SQLite database so work can resume after any
interruption.
"""
from __future__ import annotations

import argparse
import dataclasses
import logging
import random
import re
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests

LISTING_URL = "https://www.soccerfieldmap.com/api/fields"
WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
DEFAULT_USER_AGENTS = [
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
]


@dataclasses.dataclass
class Field:
    external_id: str
    name: str
    address: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    area_sqm: Optional[float] = None
    pitch_count: Optional[int] = None
    wiki_title: Optional[str] = None
    wiki_url: Optional[str] = None


class FieldStorage:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._conn = sqlite3.connect(self.db_path)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS fields (
                external_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                address TEXT NOT NULL,
                latitude REAL,
                longitude REAL,
                area_sqm REAL,
                pitch_count INTEGER,
                wiki_title TEXT,
                wiki_url TEXT,
                updated_at TEXT NOT NULL
            );
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS progress (
                name TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def last_page(self) -> int:
        cur = self._conn.execute(
            "SELECT value FROM progress WHERE name='listing_page' LIMIT 1;"
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0

    def mark_page(self, page: int) -> None:
        self._conn.execute(
            "REPLACE INTO progress (name, value) VALUES ('listing_page', ?);",
            (str(page),),
        )
        self._conn.commit()

    def processed_ids(self) -> set[str]:
        cur = self._conn.execute("SELECT external_id FROM fields;")
        return {row[0] for row in cur.fetchall()}

    def save_field(self, field: Field) -> None:
        self._conn.execute(
            """
            REPLACE INTO fields (
                external_id, name, address, latitude, longitude, area_sqm,
                pitch_count, wiki_title, wiki_url, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'));
            """,
            (
                field.external_id,
                field.name,
                field.address,
                field.latitude,
                field.longitude,
                field.area_sqm,
                field.pitch_count,
                field.wiki_title,
                field.wiki_url,
            ),
        )
        self._conn.commit()


class HttpClient:
    def __init__(
        self,
        user_agents: List[str],
        max_attempts: int = 5,
        backoff_seconds: float = 1.0,
        backoff_cap: float = 30.0,
        timeout: int = 20,
    ) -> None:
        self.user_agents = user_agents
        self.max_attempts = max_attempts
        self.backoff_seconds = backoff_seconds
        self.backoff_cap = backoff_cap
        self.timeout = timeout
        self._local = threading.local()

    def _headers(self) -> Dict[str, str]:
        return {"User-Agent": random.choice(self.user_agents)}

    def _session(self) -> requests.Session:
        if not hasattr(self._local, "session"):
            self._local.session = requests.Session()
        return self._local.session

    def get(self, url: str, *, params: Optional[Dict[str, object]] = None) -> requests.Response:
        delay = self.backoff_seconds
        for attempt in range(1, self.max_attempts + 1):
            try:
                response = self._session().get(
                    url,
                    params=params,
                    headers=self._headers(),
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                logging.warning("Request error (%s) attempt %s/%s", exc, attempt, self.max_attempts)
                if attempt == self.max_attempts:
                    raise
            else:
                if response.status_code in {429, 503, 502, 500}:
                    retry_after = response.headers.get("Retry-After")
                    wait = float(retry_after) if retry_after else delay
                    jitter = random.uniform(0, 0.5 * wait)
                    logging.warning(
                        "Server replied %s, backing off for %.1fs (attempt %s/%s)",
                        response.status_code,
                        wait + jitter,
                        attempt,
                        self.max_attempts,
                    )
                    time.sleep(wait + jitter)
                    delay = min(delay * 2, self.backoff_cap)
                    continue
                response.raise_for_status()
                return response

            jitter = random.uniform(0, 0.5 * delay)
            time.sleep(delay + jitter)
            delay = min(delay * 2, self.backoff_cap)
        raise RuntimeError("Exceeded retry attempts")


def fetch_listing(client: HttpClient, page: int, page_size: int) -> List[Dict[str, object]]:
    response = client.get(
        LISTING_URL,
        params={"page": page, "page_size": page_size},
    )
    payload = response.json()
    if isinstance(payload, dict) and "results" in payload:
        return payload["results"]
    if isinstance(payload, list):
        return payload
    raise ValueError("Unexpected listing payload structure")


def infer_field(record: Dict[str, object]) -> Field:
    external_id = str(record.get("id") or record.get("uuid") or record.get("_id"))
    name = str(record.get("name") or "").strip()
    address = str(record.get("address") or record.get("location") or "").strip()
    latitude = record.get("lat") or record.get("latitude")
    longitude = record.get("lng") or record.get("longitude")
    if not external_id or not name or not address:
        raise ValueError("Listing record missing required fields")
    return Field(
        external_id=external_id,
        name=name,
        address=address,
        latitude=float(latitude) if latitude is not None else None,
        longitude=float(longitude) if longitude is not None else None,
    )


def wikipedia_search(client: HttpClient, title: str) -> Optional[Dict[str, object]]:
    response = client.get(
        WIKIPEDIA_API,
        params={
            "action": "query",
            "list": "search",
            "srsearch": title,
            "format": "json",
            "srlimit": 1,
        },
    )
    data = response.json()
    hits = data.get("query", {}).get("search", [])
    return hits[0] if hits else None


def wikipedia_page(client: HttpClient, pageid: int) -> Optional[str]:
    response = client.get(
        WIKIPEDIA_API,
        params={
            "action": "parse",
            "pageid": pageid,
            "prop": "wikitext",
            "format": "json",
        },
    )
    data = response.json()
    wikitext = data.get("parse", {}).get("wikitext", {}).get("*")
    return wikitext


def parse_area(area_text: str) -> Optional[float]:
    cleaned = area_text.replace("\u00a0", " ").strip().lower()
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*(ha|hectare|m2|m\^2|square metres|sq m)", cleaned)
    if not match:
        return None
    value = float(match.group(1))
    unit = match.group(2)
    if unit.startswith("ha") or unit.startswith("hectare"):
        return value * 10000
    return value


def validate_area(area_sqm: Optional[float]) -> Optional[float]:
    if area_sqm is None:
        return None
    if 800 <= area_sqm <= 250_000:
        return area_sqm
    logging.info("Rejecting implausible area value: %s", area_sqm)
    return None


def parse_pitch_count(wikitext: str) -> Optional[int]:
    pitch_match = re.search(r"pitches?\s*=\s*([0-9]+)", wikitext, re.IGNORECASE)
    if pitch_match:
        return int(pitch_match.group(1))
    field_match = re.search(r"fields?\s*=\s*([0-9]+)", wikitext, re.IGNORECASE)
    if field_match:
        return int(field_match.group(1))
    return None


def extract_area_from_wikitext(wikitext: str) -> Optional[float]:
    for line in wikitext.splitlines():
        if "area" in line.lower():
            candidate = parse_area(line)
            if candidate is not None:
                return candidate
    return None


def enrich_with_wikipedia(client: HttpClient, field: Field) -> Field:
    try:
        search_hit = wikipedia_search(client, field.name)
    except Exception as exc:  # noqa: BLE001 - we want to continue processing
        logging.warning("Wikipedia search failed for '%s': %s", field.name, exc)
        return field

    if not search_hit:
        return field

    pageid = search_hit.get("pageid")
    title = search_hit.get("title")
    wikitext = None

    if pageid is not None:
        try:
            wikitext = wikipedia_page(client, pageid)
        except Exception as exc:  # noqa: BLE001 - continue without crash
            logging.warning("Wikipedia page fetch failed for '%s' (%s): %s", field.name, pageid, exc)

    area = None
    pitch_count = None
    if wikitext:
        area = validate_area(extract_area_from_wikitext(wikitext))
        pitch_count = parse_pitch_count(wikitext)
    wiki_url = f"https://en.wikipedia.org/?curid={pageid}" if pageid else None
    return Field(
        external_id=field.external_id,
        name=field.name,
        address=field.address,
        latitude=field.latitude,
        longitude=field.longitude,
        area_sqm=area,
        pitch_count=pitch_count,
        wiki_title=title,
        wiki_url=wiki_url,
    )


def iter_fields(client: HttpClient, start_page: int, page_size: int) -> Iterable[List[Dict[str, object]]]:
    page = start_page
    while True:
        try:
            batch = fetch_listing(client, page, page_size)
        except Exception as exc:  # noqa: BLE001 - fail fast after repeated retries
            logging.error("Failed to fetch listing page %s: %s", page, exc)
            raise
        if not batch:
            break
        yield batch
        page += 1


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape soccerfieldmap.com listings.")
    parser.add_argument("--db", type=Path, default=Path("data/fields.sqlite"))
    parser.add_argument("--page-size", type=int, default=200)
    parser.add_argument("--wiki-workers", type=int, default=5, help="Parallel workers for Wikipedia enrichment")
    parser.add_argument("--max-attempts", type=int, default=6, help="HTTP retry attempts per request")
    parser.add_argument("--backoff", type=float, default=1.0, help="Initial backoff (seconds) for retries")
    parser.add_argument("--backoff-cap", type=float, default=20.0, help="Max backoff (seconds) for retries")
    parser.add_argument("--timeout", type=int, default=20, help="HTTP timeout per request")
    parser.add_argument("--max-pages", type=int, default=None, help="Optional limit for listing pages")
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING)")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="[%(asctime)s] %(levelname)s %(message)s",
        stream=sys.stdout,
    )

    args.db.parent.mkdir(parents=True, exist_ok=True)
    storage = FieldStorage(args.db)
    listing_client = HttpClient(
        DEFAULT_USER_AGENTS,
        max_attempts=args.max_attempts,
        backoff_seconds=args.backoff,
        backoff_cap=args.backoff_cap,
        timeout=args.timeout,
    )
    wiki_client = HttpClient(
        DEFAULT_USER_AGENTS,
        max_attempts=args.max_attempts,
        backoff_seconds=args.backoff,
        backoff_cap=args.backoff_cap,
        timeout=args.timeout,
    )
    processed = storage.processed_ids()
    start_page = storage.last_page()

    try:
        with ThreadPoolExecutor(max_workers=args.wiki_workers) as executor:
            for page_index, batch in enumerate(
                iter_fields(listing_client, start_page, args.page_size), start=start_page
            ):
                pending: List[Field] = []
                for record in batch:
                    try:
                        field = infer_field(record)
                    except ValueError:
                        logging.warning("Skipping malformed listing: %s", record)
                        continue
                    if field.external_id in processed:
                        continue
                    pending.append(field)

                futures = {
                    executor.submit(enrich_with_wikipedia, wiki_client, field): field.external_id
                    for field in pending
                }

                for future in as_completed(futures):
                    external_id = futures[future]
                    try:
                        enriched = future.result()
                    except Exception as exc:  # noqa: BLE001 - continue on worker failure
                        logging.error("Worker failed for %s: %s", external_id, exc)
                        continue
                    storage.save_field(enriched)
                    processed.add(external_id)

                storage.mark_page(page_index + 1)

                if args.max_pages is not None and (page_index + 1) >= (start_page + args.max_pages):
                    logging.info("Reached max pages limit (%s), stopping.", args.max_pages)
                    break
    except KeyboardInterrupt:
        logging.warning("Interrupted by user, progress preserved up to page %s", storage.last_page())
    finally:
        storage.close()


if __name__ == "__main__":
    main()
