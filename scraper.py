"""Offline-friendly scraper for SoccerFieldMap.

This script downloads soccer field metadata from soccerfieldmap.com,
optionally enriches it with Wikipedia information (area),
and stores everything directly in a CSV for live inspection and resume.
"""
from __future__ import annotations

import argparse
import csv
import dataclasses
import json
import logging
import os
import random
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests

LISTING_URL = "https://www.soccerfieldmap.com/api/fields"
EXPLORE_URL = "https://www.soccerfieldmap.com/explore"
SITEMAP_URL = "https://www.soccerfieldmap.com/sitemap.xml"
WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
DEFAULT_USER_AGENTS = [
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
]
CSV_COLUMNS = ["场地名", "场地链接", "场地面积", "场地面积wiki链接"]


@dataclasses.dataclass
class Field:
    external_id: str
    name: str
    url: str
    address: str = ""
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    area_sqm: Optional[float] = None
    wiki_url: Optional[str] = None


class ProgressTracker:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def last_page(self) -> int:
        if not self.path.exists():
            return 1
        try:
            with self.path.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
            return int(data.get("last_page", 1))
        except (OSError, ValueError, json.JSONDecodeError):
            return 1

    def mark_page(self, page: int) -> None:
        with self._lock:
            try:
                with self.path.open("w", encoding="utf-8") as handle:
                    json.dump({"last_page": page}, handle)
                    handle.flush()
                    os.fsync(handle.fileno())
            except OSError as exc:
                logging.warning("Failed to persist progress to %s: %s", self.path, exc)


class CsvSink:
    def __init__(self, csv_path: Path) -> None:
        self.csv_path = csv_path
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._existing_links = self._load_existing_links()

    @property
    def existing_links(self) -> set[str]:
        return set(self._existing_links)

    def _load_existing_links(self) -> set[str]:
        if not self.csv_path.exists():
            return set()
        links: set[str] = set()
        try:
            with self.csv_path.open("r", newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                if CSV_COLUMNS[1] not in (reader.fieldnames or []):
                    return set()
                for row in reader:
                    link = row.get(CSV_COLUMNS[1])
                    if link:
                        links.add(link)
        except (OSError, csv.Error) as exc:
            logging.warning("Unable to read existing CSV %s: %s", self.csv_path, exc)
        return links

    def append(self, field: Field) -> None:
        with self._lock:
            if field.url in self._existing_links:
                return
            write_header = not self.csv_path.exists() or self.csv_path.stat().st_size == 0
            row = {
                CSV_COLUMNS[0]: field.name,
                CSV_COLUMNS[1]: field.url,
                CSV_COLUMNS[2]: field.area_sqm if field.area_sqm is not None else "",
                CSV_COLUMNS[3]: field.wiki_url or "",
            }
            try:
                with self.csv_path.open("a", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
                    if write_header:
                        writer.writeheader()
                    writer.writerow(row)
                    handle.flush()
                    os.fsync(handle.fileno())
                self._existing_links.add(field.url)
            except OSError as exc:
                logging.error("Failed to write CSV row for %s: %s", field.external_id, exc)


class HttpClient:
    def __init__(
        self,
        user_agents: List[str],
        max_attempts: int = 5,
        backoff_seconds: float = 1.0,
        backoff_cap: float = 30.0,
        timeout: int = 20,
        warmup_url: Optional[str] = None,
    ) -> None:
        self.user_agents = user_agents
        self.max_attempts = max_attempts
        self.backoff_seconds = backoff_seconds
        self.backoff_cap = backoff_cap
        self.timeout = timeout
        self.warmup_url = warmup_url
        self._local = threading.local()

    def _headers(self, referer: Optional[str] = None) -> Dict[str, str]:
        headers = {
            "User-Agent": random.choice(self.user_agents),
            "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.8",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Connection": "keep-alive",
        }
        if referer:
            headers["Referer"] = referer
        return headers

    def _session(self) -> requests.Session:
        if not hasattr(self._local, "session"):
            self._local.session = requests.Session()
            if self.warmup_url:
                try:
                    self._local.session.get(
                        self.warmup_url,
                        headers=self._headers(),
                        timeout=self.timeout,
                    )
                    logging.debug("Warmed session with %s", self.warmup_url)
                except requests.RequestException as exc:
                    logging.debug("Warmup request to %s failed: %s", self.warmup_url, exc)
        return self._local.session

    def get(
        self,
        url: str,
        *,
        params: Optional[Dict[str, object]] = None,
        referer: Optional[str] = None,
        allow_statuses: Optional[set[int]] = None,
    ) -> requests.Response:
        delay = self.backoff_seconds
        for attempt in range(1, self.max_attempts + 1):
            try:
                response = self._session().get(
                    url,
                    params=params,
                    headers=self._headers(referer=referer),
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                logging.warning("Request error (%s) attempt %s/%s", exc, attempt, self.max_attempts)
                if attempt == self.max_attempts:
                    raise
            else:
                if allow_statuses and response.status_code in allow_statuses:
                    return response
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
    try:
        response = client.get(
            LISTING_URL,
            params={"page": page, "page_size": page_size},
            referer=EXPLORE_URL,
            allow_statuses={404},
        )
    except requests.HTTPError as exc:  # noqa: BLE001 - controlled handling for 404
        status = exc.response.status_code if exc.response else None
        if status == 404:
            logging.warning("Listing page %s returned 404, treating as end of data", page)
            return []
        raise

    if response.status_code == 404:
        logging.warning("Listing page %s returned 404 after allowlist, treating as end of data", page)
        return []

    payload = response.json()
    if isinstance(payload, dict) and "results" in payload:
        return payload["results"]
    if isinstance(payload, list):
        return payload
    raise ValueError("Unexpected listing payload structure")


def _iter_matching_lists(obj: object) -> Iterable[List[object]]:
    if isinstance(obj, list):
        yield obj
        for item in obj:
            yield from _iter_matching_lists(item)
    elif isinstance(obj, dict):
        for value in obj.values():
            yield from _iter_matching_lists(value)


def _looks_like_field(record: object) -> bool:
    if not isinstance(record, dict):
        return False
    has_name = bool(record.get("name"))
    has_address = bool(record.get("address") or record.get("location"))
    has_id = any(record.get(key) is not None for key in ("id", "uuid", "_id"))
    return has_name and has_address and has_id


def _extract_next_data(html: str) -> Dict[str, object]:
    """Extract and decode the __NEXT_DATA__ blob from varied markup shapes."""

    decoder = json.JSONDecoder()

    def _try_decode(body: str) -> Optional[Dict[str, object]]:
        body = body.strip()
        if not body:
            return None
        try:
            payload, _ = decoder.raw_decode(body)
            return payload
        except json.JSONDecodeError:
            return None

    patterns = [
        r"<script[^>]*id=[\"']__NEXT_DATA__[\"'][^>]*>(?P<body>{.*?})</script>",
        r"__NEXT_DATA__\s*=\s*(?P<body>{.*})",
    ]
    for pattern in patterns:
        match = re.search(pattern, html, flags=re.IGNORECASE | re.DOTALL)
        if match:
            payload = _try_decode(match.group("body"))
            if payload is not None:
                return payload

    marker = "__NEXT_DATA__"
    marker_index = html.find(marker)
    if marker_index != -1:
        brace_index = html.find("{", marker_index)
        if brace_index != -1:
            payload = _try_decode(html[brace_index:])
            if payload is not None:
                return payload

    snippet = html[:500].replace("\n", " ")
    raise ValueError(f"Unable to locate NEXT_DATA payload on explore page (snippet: {snippet[:120]}...)")


def _extract_build_id(payload: Dict[str, object]) -> Optional[str]:
    """Pull the Next.js buildId out of a decoded __NEXT_DATA__ payload."""

    if not isinstance(payload, dict):
        return None
    build_id = payload.get("buildId")
    if isinstance(build_id, str) and build_id:
        return build_id
    return None


def _extract_build_id_from_html(html: str) -> Optional[str]:
    """Look for buildId in static asset paths when __NEXT_DATA__ is absent."""

    match = re.search(r"/_next/static/([A-Za-z0-9_-]+)/_buildManifest\.js", html)
    if match:
        return match.group(1)
    return None


def _fetch_next_data_json(client: HttpClient, path: str, build_id: str) -> Optional[Dict[str, object]]:
    url = f"https://www.soccerfieldmap.com/_next/data/{build_id}{path}.json"
    try:
        response = client.get(url, allow_statuses={403, 404})
    except Exception as exc:  # noqa: BLE001 - continue to fallbacks
        logging.warning("Failed to fetch _next data %s: %s", url, exc)
        return None
    if response.status_code >= 400:
        logging.warning("_next data endpoint returned %s for %s", response.status_code, url)
        return None
    try:
        return response.json()
    except Exception as exc:  # noqa: BLE001 - continue to fallbacks
        logging.warning("Failed to decode _next data %s: %s", url, exc)
        return None


def bootstrap_from_explore(client: HttpClient) -> List[Dict[str, object]]:
    logging.warning(
        "Primary listing endpoint returned nothing; attempting to bootstrap from %s",
        EXPLORE_URL,
    )
    response = client.get(EXPLORE_URL, allow_statuses={403})
    try:
        payload = _extract_next_data(response.text)
    except Exception as exc:  # noqa: BLE001 - try alternate pathways
        logging.warning("Explore page missing NEXT_DATA (%s); falling back to _next data", exc)
        payload = None

    build_id = _extract_build_id(payload) if payload else None
    if not build_id:
        build_id = _extract_build_id_from_html(response.text)
    if not build_id:
        try:
            home_payload = _extract_next_data(client.get("https://www.soccerfieldmap.com/", allow_statuses={403}).text)
            build_id = _extract_build_id(home_payload)
        except Exception as exc:  # noqa: BLE001 - keep trying without crash
            logging.warning("Unable to read buildId from homepage: %s", exc)
    if not build_id:
        try:
            home_html = client.get("https://www.soccerfieldmap.com/", allow_statuses={403}).text
            build_id = _extract_build_id_from_html(home_html)
        except Exception as exc:  # noqa: BLE001 - keep trying without crash
            logging.warning("Unable to infer buildId from homepage markup: %s", exc)

    if build_id:
        logging.info("Attempting _next data fetch using buildId %s", build_id)
        for path in ("/explore", "/explore/index"):
            next_payload = _fetch_next_data_json(client, path, build_id)
            if isinstance(next_payload, dict):
                payload = next_payload.get("pageProps") or next_payload
                break

    if not payload:
        logging.error("No explore payload available from HTML, inferred buildId, or _next data")
        return []

    for candidate_list in _iter_matching_lists(payload):
        if len(candidate_list) > 100 and all(_looks_like_field(item) for item in candidate_list):
            logging.info("Found %s candidate fields in explore bootstrap", len(candidate_list))
            return [item for item in candidate_list if isinstance(item, dict)]
    logging.error("No candidate field list discovered in explore bootstrap")
    return []





def _extract_ld_json(html: str) -> List[Dict[str, object]]:
    ld_blocks: List[Dict[str, object]] = []
    for match in re.finditer(r"<script[^>]*application/ld\+json[^>]*>(.*?)</script>", html, re.DOTALL | re.IGNORECASE):
        try:
            data = json.loads(match.group(1))
        except Exception:  # noqa: BLE001 - ignore malformed JSON
            continue
        if isinstance(data, list):
            ld_blocks.extend([item for item in data if isinstance(item, dict)])
        elif isinstance(data, dict):
            ld_blocks.append(data)
    return ld_blocks


def _extract_field_from_page(url: str, html: str) -> Optional[Dict[str, object]]:
    ld_blocks = _extract_ld_json(html)
    for block in ld_blocks:
        if block.get("@type") in {"Place", "LocalBusiness", "SportsActivityLocation"}:
            name = block.get("name") or block.get("headline") or ""
            address = block.get("address") or block.get("streetAddress") or ""
            if isinstance(address, dict):
                address = " ".join(str(v) for v in address.values() if v)
            name = str(name).strip()
            address = str(address).strip()
            if name and address:
                return {
                    "id": url.rstrip("/").split("/")[-1],
                    "url": url,
                    "name": name,
                    "address": address,
                    "lat": block.get("geo", {}).get("latitude") if isinstance(block.get("geo"), dict) else None,
                    "lng": block.get("geo", {}).get("longitude") if isinstance(block.get("geo"), dict) else None,
                }

    # Fallback: parse __NEXT_DATA__ and hunt for a dict that looks like a field record.
    try:
        payload = _extract_next_data(html)
    except Exception:  # noqa: BLE001 - treat as absence and continue
        payload = None

    def _iter_dicts(obj: object) -> Iterable[Dict[str, object]]:
        if isinstance(obj, dict):
            yield obj
            for value in obj.values():
                yield from _iter_dicts(value)
        elif isinstance(obj, list):
            for item in obj:
                yield from _iter_dicts(item)

    if payload:
        for candidate in _iter_dicts(payload):
            if _looks_like_field(candidate):
                name = str(candidate.get("name") or "").strip()
                address = str(candidate.get("address") or candidate.get("location") or "").strip()
                if name and address:
                    return {
                        "id": candidate.get("id") or candidate.get("uuid") or url.rstrip("/").split("/")[-1],
                        "url": url,
                        "name": name,
                        "address": address,
                        "lat": candidate.get("lat") or candidate.get("latitude"),
                        "lng": candidate.get("lng") or candidate.get("longitude"),
                    }

    return None


def _discover_field_urls_from_html(html: str) -> List[str]:
    urls = set()
    for match in re.finditer(r"href=\"([^\"]*?/field/[^\"#?']+)\"", html, re.IGNORECASE):
        urls.add(match.group(1))
    for match in re.finditer(r"href='([^']*?/field/[^'#?\"]+)'", html, re.IGNORECASE):
        urls.add(match.group(1))
    return sorted(urls)


def bootstrap_from_explore_links(client: HttpClient, limit: Optional[int] = None) -> List[Dict[str, object]]:
    try:
        response = client.get(EXPLORE_URL, allow_statuses={403, 404})
    except Exception as exc:  # noqa: BLE001 - fallback should never crash caller
        logging.warning("Explore-page link scrape failed: %s", exc)
        return []

    if response.status_code >= 400:
        logging.warning("Explore page unavailable (status %s)", response.status_code)
        return []

    field_urls = _discover_field_urls_from_html(response.text)
    if not field_urls:
        logging.error("No field links discovered on explore page")
        return []

    if limit:
        field_urls = field_urls[:limit]

    records: List[Dict[str, object]] = []
    for idx, url in enumerate(field_urls, start=1):
        full_url = url if url.startswith("http") else f"https://www.soccerfieldmap.com{url}"
        try:
            html = client.get(full_url, referer=EXPLORE_URL, allow_statuses={403}).text
            record = _extract_field_from_page(full_url, html)
            if record:
                records.append(record)
            else:
                logging.debug("No structured data found for %s", full_url)
        except Exception as exc:  # noqa: BLE001 - continue despite failures
            logging.warning("Failed to parse field page %s: %s", full_url, exc)
        if idx % 50 == 0:
            logging.info("Processed %s/%s field pages discovered from explore", idx, len(field_urls))

    logging.info("Collected %s fields from explore page links", len(records))
    return records


def bootstrap_from_sitemap(client: HttpClient, limit: Optional[int] = None) -> List[Dict[str, object]]:
    try:
        response = client.get(SITEMAP_URL, allow_statuses={403, 404})
    except Exception as exc:  # noqa: BLE001 - fallback should never crash caller
        logging.warning("Sitemap fetch failed: %s", exc)
        return []

    if response.status_code >= 400:
        logging.warning("Sitemap unavailable (status %s)", response.status_code)
        return []

    urls: List[str] = re.findall(r"<loc>(.*?)</loc>", response.text)
    field_urls = [u for u in urls if "/field/" in u]
    if not field_urls:
        logging.error("No field URLs discovered in sitemap")
        return []

    if limit:
        field_urls = field_urls[:limit]

    records: List[Dict[str, object]] = []
    for idx, url in enumerate(field_urls, start=1):
        try:
            html = client.get(url, referer=EXPLORE_URL, allow_statuses={403}).text
            record = _extract_field_from_page(url, html)
            if record:
                records.append(record)
            else:
                logging.debug("No structured data found for %s", url)
        except Exception as exc:  # noqa: BLE001 - continue despite failures
            logging.warning("Failed to parse field page %s: %s", url, exc)
        if idx % 50 == 0:
            logging.info("Processed %s/%s field detail pages from sitemap", idx, len(field_urls))

    logging.info("Collected %s fields from sitemap pages", len(records))
    return records

def infer_field(record: Dict[str, object]) -> Field:
    external_id = str(record.get("id") or record.get("uuid") or record.get("_id"))
    name = str(record.get("name") or "").strip()
    address = str(record.get("address") or record.get("location") or "").strip()
    url = str(
        record.get("url")
        or record.get("link")
        or record.get("path")
        or f"https://www.soccerfieldmap.com/field/{external_id}"
    )
    latitude = record.get("lat") or record.get("latitude")
    longitude = record.get("lng") or record.get("longitude")
    if not external_id or not name or not address:
        raise ValueError("Listing record missing required fields")
    return Field(
        external_id=external_id,
        name=name,
        url=url,
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
    wikitext = None

    if pageid is not None:
        try:
            wikitext = wikipedia_page(client, pageid)
        except Exception as exc:  # noqa: BLE001 - continue without crash
            logging.warning("Wikipedia page fetch failed for '%s' (%s): %s", field.name, pageid, exc)

    area = None
    if wikitext:
        area = validate_area(extract_area_from_wikitext(wikitext))
    wiki_url = f"https://en.wikipedia.org/?curid={pageid}" if pageid else None
    return Field(
        external_id=field.external_id,
        name=field.name,
        url=field.url,
        address=field.address,
        latitude=field.latitude,
        longitude=field.longitude,
        area_sqm=area,
        wiki_url=wiki_url,
    )


def iter_fields(client: HttpClient, start_page: int, page_size: int) -> Iterable[List[Dict[str, object]]]:
    page = start_page
    attempted_bootstrap = False
    while True:
        try:
            batch = fetch_listing(client, page, page_size)
        except Exception as exc:  # noqa: BLE001 - fail fast after repeated retries
            logging.error("Failed to fetch listing page %s: %s", page, exc)
            raise
        if not batch:
            if page == start_page and not attempted_bootstrap:
                attempted_bootstrap = True
                for source_name, bootstrap_fn in (
                    ("explore bootstrap", bootstrap_from_explore),
                    ("explore link crawl", lambda c: bootstrap_from_explore_links(c)),
                    ("sitemap bootstrap", lambda c: bootstrap_from_sitemap(c)),
                ):
                    try:
                        bootstrap = bootstrap_fn(client)
                    except Exception as exc:  # noqa: BLE001 - keep trying other fallbacks
                        logging.error("%s failed: %s", source_name.capitalize(), exc)
                        bootstrap = []
                    if bootstrap:
                        logging.info(
                            "Yielding %s data in %s-sized chunks",
                            source_name,
                            page_size,
                        )
                        for idx in range(0, len(bootstrap), page_size):
                            yield bootstrap[idx : idx + page_size]
                            page += 1
                        break
                else:
                    break
                break
            break
        yield batch
        page += 1


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape soccerfieldmap.com listings.")
    parser.add_argument("--csv", type=Path, default=Path("data/fields.csv"), help="CSV output written incrementally")
    parser.add_argument(
        "--progress",
        type=Path,
        default=Path("data/progress.json"),
        help="Progress checkpoint for resuming from the last completed page",
    )
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

    args.csv.parent.mkdir(parents=True, exist_ok=True)
    progress = ProgressTracker(args.progress)
    csv_sink = CsvSink(args.csv)
    listing_client = HttpClient(
        DEFAULT_USER_AGENTS,
        max_attempts=args.max_attempts,
        backoff_seconds=args.backoff,
        backoff_cap=args.backoff_cap,
        timeout=args.timeout,
        warmup_url="https://www.soccerfieldmap.com/",
    )
    wiki_client = HttpClient(
        DEFAULT_USER_AGENTS,
        max_attempts=args.max_attempts,
        backoff_seconds=args.backoff,
        backoff_cap=args.backoff_cap,
        timeout=args.timeout,
    )
    processed = csv_sink.existing_links
    start_page = progress.last_page()

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
                    if field.url in processed:
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
                    csv_sink.append(enriched)
                    processed.add(enriched.url)

                progress.mark_page(page_index + 1)

                if args.max_pages is not None and (page_index + 1) >= (start_page + args.max_pages):
                    logging.info("Reached max pages limit (%s), stopping.", args.max_pages)
                    break
    except KeyboardInterrupt:
        logging.warning("Interrupted by user, progress preserved up to page %s", progress.last_page())


if __name__ == "__main__":
    main()
