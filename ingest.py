#!/usr/bin/env python3
"""Fetch launch and NASA context data, normalize Kaggle CSVs, and optionally load DuckDB."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import requests

LOG = logging.getLogger("space_missions_ingest")
LAUNCHES_URL = "https://ll.thespacedevs.com/2.2.0/launch/"
NASA_NEO_URL = "https://api.nasa.gov/neo/rest/v1/feed"
MAX_AUTOMATIC_RETRY_DELAY_SECONDS = 60
FIELDNAMES = [
    "source_name",
    "source_id",
    "company",
    "location",
    "launch_datetime",
    "rocket_name",
    "mission_name",
    "rocket_status",
    "mission_status",
    "mission_type",
    "orbit",
    "agency",
    "payload_mass_kg",
]
HEADER_ALIASES = {
    "company": ("company name", "company", "provider", "agency", "launch service provider"),
    "location": ("location", "launch site", "launch location", "pad"),
    "launch_datetime": ("datum", "date", "launch date", "datetime", "launch datetime"),
    "rocket_name": ("rocket", "rocket name", "vehicle"),
    "mission_name": ("mission", "mission name", "payload name"),
    "rocket_status": ("status rocket", "rocketstatus", "rocket status"),
    "mission_status": ("status mission", "missionstatus", "mission status", "launch status"),
    "mission_type": ("mission type", "type"),
    "orbit": ("orbit", "target orbit"),
    "agency": ("agency", "operator", "customer"),
    "payload_mass_kg": ("payload mass kg", "payload mass", "mass kg", "payload"),
    "source_name": ("source name",),
    "source_id": ("source id", "mission id", "launch id", "id"),
}
DATE_FORMATS = (
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%d %H:%M:%S",
    "%a %b %d, %Y %H:%M %Z",
    "%a %b %d, %Y %H:%M",
    "%Y-%m-%d",
    "%b %d, %Y",
    "%B %d, %Y",
    "%d %b %Y",
    "%d %B %Y",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv-dir", type=Path, default=Path("data/kaggle"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/landing"))
    parser.add_argument("--since", type=date.fromisoformat, default=date.today() - timedelta(days=6))
    parser.add_argument("--until", type=date.fromisoformat, default=date.today())
    parser.add_argument("--skip-apis", action="store_true", help="Do not request either API.")
    parser.add_argument("--skip-csv", action="store_true", help="Do not read Kaggle CSV files.")
    parser.add_argument(
        "--load-warehouse",
        "--upload",
        dest="load_warehouse",
        action="store_true",
        help="Load this run into the local DuckDB warehouse (--upload is a compatibility alias).",
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=Path("data/space_missions.duckdb"),
        help="Local DuckDB database file path",
    )
    parser.add_argument("--max-launch-pages", type=int, default=10)
    return parser.parse_args()


def safe_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        return str(value.get("name") or value.get("full_name") or "")
    if isinstance(value, list):
        return ", ".join(filter(None, (safe_text(item) for item in value)))
    return str(value).strip()


def nested(value: Any, *keys: str) -> Any:
    for key in keys:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def normalized_launch(record: dict[str, Any]) -> dict[str, str]:
    location = nested(record, "pad", "location")
    provider = record.get("launch_service_provider") or record.get("provider")
    rocket = record.get("rocket")
    rocket_config = rocket.get("configuration") if isinstance(rocket, dict) else None
    mission = record.get("mission")
    orbit_value = nested(mission, "orbit", "name") or nested(record, "orbit", "name")
    launch_id = record.get("id") or record.get("slug") or ""
    return {
        "source_name": "launch_library",
        "source_id": safe_text(launch_id),
        "company": safe_text(provider),
        "location": " - ".join(filter(None, (safe_text(nested(record, "pad", "name")), safe_text(nested(location, "name"))))),
        "launch_datetime": parse_source_datetime(safe_text(record.get("net") or record.get("window_start"))),
        "rocket_name": safe_text(rocket_config),
        "mission_name": safe_text(mission.get("name") if isinstance(mission, dict) else record.get("name")),
        "rocket_status": safe_text(nested(rocket_config, "name") if isinstance(rocket_config, dict) else None),
        "mission_status": safe_text(nested(record, "status", "name")),
        "mission_type": safe_text(nested(mission, "type")),
        "orbit": safe_text(orbit_value),
        "agency": safe_text(provider),
        "payload_mass_kg": safe_text(nested(mission, "total_payload_mass")),
    }


def parse_source_datetime(value: str) -> str:
    candidate = value.strip()
    if not candidate:
        return ""
    try:
        parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            return parsed.astimezone(timezone.utc).replace(tzinfo=None).isoformat(sep=" ")
        return parsed.isoformat(sep=" ")
    except ValueError:
        pass
    for date_format in DATE_FORMATS:
        try:
            return datetime.strptime(candidate, date_format).isoformat(sep=" ")
        except ValueError:
            continue
    return candidate


def normalized_headers(headers: Iterable[str]) -> dict[str, str]:
    cleaned = {re.sub(r"[^a-z0-9 ]", "", header.lower()).strip(): header for header in headers}
    result: dict[str, str] = {}
    for field, aliases in HEADER_ALIASES.items():
        for alias in aliases:
            if alias in cleaned:
                result[field] = cleaned[alias]
                break
    return result


def csv_launches(csv_dir: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    if not csv_dir.exists():
        LOG.info("CSV directory does not exist; skipping CSV input: %s", csv_dir)
        return rows
    for path in sorted(csv_dir.glob("*.csv")):
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                LOG.warning("Skipping CSV without a header: %s", path)
                continue
            mapping = normalized_headers(reader.fieldnames)
            for index, row in enumerate(reader, start=1):
                result = {field: "" for field in FIELDNAMES}
                result["source_name"] = "kaggle"
                for field, header in mapping.items():
                    result[field] = (row.get(header) or "").strip()
                if not result["source_name"]:
                    result["source_name"] = "kaggle"
                if not result["source_id"]:
                    result["source_id"] = f"{path.stem}:{index}"
                result["launch_datetime"] = parse_source_datetime(result["launch_datetime"])
                rows.append(result)
    return rows


def request_json(session: requests.Session, url: str, *, params: dict[str, str] | None = None) -> dict[str, Any]:
    for attempt in range(5):
        response = session.get(url, params=params, timeout=(10, 45))
        if response.status_code != 429 or attempt == 4:
            response.raise_for_status()
            break
        retry_after = response.headers.get("Retry-After", "")
        delay = int(retry_after) if retry_after.isdigit() else min(60, 2**attempt)
        if delay > MAX_AUTOMATIC_RETRY_DELAY_SECONDS:
            raise RuntimeError(
                f"The API rate limit requires waiting {delay} seconds. "
                "The script stopped instead of waiting that long; rerun later or use --skip-apis."
            )
        LOG.warning("The API rate limited the request; retrying in %d seconds", delay)
        time.sleep(delay)
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError(f"Expected a JSON object from {response.url}")
    return payload


def fetch_launches(session: requests.Session, max_pages: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    url: str | None = LAUNCHES_URL
    params: dict[str, str] | None = {"limit": "100", "ordering": "-net"}
    for _ in range(max_pages):
        if not url:
            break
        payload = request_json(session, url, params=params)
        results = payload.get("results", [])
        if not isinstance(results, list):
            raise ValueError("Launch Library API response has no results list")
        rows.extend(item for item in results if isinstance(item, dict))
        url = payload.get("next")
        params = None
    return rows


def fetch_nasa_neo(session: requests.Session, start: date, end: date) -> dict[str, Any]:
    if end < start or (end - start).days > 6:
        raise ValueError("NASA NeoWs feed requires an inclusive date range of at most seven days")
    return request_json(
        session,
        NASA_NEO_URL,
        params={
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "api_key": os.getenv("NASA_API_KEY", "DEMO_KEY"),
        },
    )


def write_jsonl(path: Path, source: str, records: Iterable[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for record in records:
            row = {
                "source_name": source,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "payload": record,
            }
            handle.write(json.dumps(row, separators=(",", ":"), ensure_ascii=False) + "\n")


def write_launch_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def load_warehouse_run(run_dir: Path, run_id: str, database_path: Path) -> None:
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError("Install project dependencies with: python -m pip install -r requirements.txt") from exc
    from warehouse import load_run

    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(database_path))
    try:
        load_run(connection, run_dir, run_id)
    except Exception as exc:
        raise RuntimeError(f"Local DuckDB load failed: {exc}") from exc
    finally:
        connection.close()


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if args.max_launch_pages < 1:
        LOG.error("--max-launch-pages must be positive")
        return 2
    if args.until < args.since or (args.until - args.since).days > 6:
        LOG.error("Choose an inclusive --since/--until range no longer than seven days")
        return 2

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = args.output_dir / run_id
    api_dir, csv_dir = run_dir / "api", run_dir / "csv"
    api_dir.mkdir(parents=True, exist_ok=True)
    csv_dir.mkdir(parents=True, exist_ok=True)
    launches: list[dict[str, str]] = []
    session = requests.Session()
    session.headers.update({"User-Agent": "space-missions-warehouse/1.0"})
    try:
        if not args.skip_apis:
            ll_records = fetch_launches(session, args.max_launch_pages)
            write_jsonl(api_dir / "launch_library.jsonl", "launch_library", ll_records)
            launches.extend(normalized_launch(item) for item in ll_records)
            nasa_payload = fetch_nasa_neo(session, args.since, args.until)
            write_jsonl(api_dir / "nasa_neows.jsonl", "nasa_neows", [nasa_payload])
        if not args.skip_csv:
            launches.extend(csv_launches(args.csv_dir))
        write_launch_csv(csv_dir / "launches.csv", launches)
        LOG.info("Wrote %d canonical launch rows under %s", len(launches), run_dir)
        if args.load_warehouse:
            load_warehouse_run(run_dir, run_id, args.database)
            LOG.info("Loaded run %s into local DuckDB database %s", run_id, args.database)
    except (requests.RequestException, ValueError, OSError, RuntimeError) as exc:
        LOG.error("Ingestion failed: %s", exc)
        return 1
    finally:
        session.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
