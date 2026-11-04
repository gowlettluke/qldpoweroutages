"""Short-lived, validated backups of published provider event allocations.

Backups keep their original collection/publisher times. A failed poll never
extends their life, and current collection failures stay visible as warnings.
"""
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path

from write_provider_council_counts import ENERGEX_API, ERGON_TEXT, count, current_stamp, iso

MAX_BACKUP_AGE_SECONDS = 30 * 60


def timestamp(value):
    if not isinstance(value, str):
        raise ValueError("Missing provider snapshot time")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Provider snapshot time needs a timezone")
    return parsed.astimezone(timezone.utc)


def validate_events(events, metadata, now):
    updated = timestamp(metadata.get("source_updated_utc"))
    current_stamp(updated.timestamp(), now)
    if not isinstance(events, list):
        raise ValueError("Invalid provider snapshot event list")
    ids, total = set(), 0
    for event in events:
        event_id = event.get("event_id")
        if not isinstance(event_id, str) or not event_id or event_id in ids:
            raise ValueError("Missing or duplicate provider event ID")
        ids.add(event_id)
        allocations = event.get("allocations")
        if not isinstance(allocations, list) or not allocations:
            raise ValueError("Missing provider council allocations")
        councils, allocated = set(), 0
        for row in allocations:
            name = row.get("lga_name")
            if not isinstance(name, str) or not name.strip() or name.casefold() in councils:
                raise ValueError("Missing or duplicate provider council")
            councils.add(name.casefold())
            allocated += count(row.get("affected_customers"))
        if allocated != count(event.get("affected_customers")):
            raise ValueError("Provider snapshot council counts do not match its event")
        if not isinstance(event.get("details"), dict):
            raise ValueError("Provider event details are unavailable")
        total += allocated
    if total != count(metadata.get("published_customers")) or len(events) != count(metadata.get("event_count")):
        raise ValueError("Provider snapshot does not match its published total")


def read_cache(path):
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return data.get("providers", {}) if isinstance(data, dict) and data.get("schema_version") == 1 and isinstance(data.get("providers"), dict) else {}


def remember(cache, provider, events, metadata, now):
    if metadata.get("status") != "provider":
        raise ValueError("Only a newly verified provider snapshot may refresh the backup")
    validate_events(events, metadata, now)
    expected_source = {"Energex": ENERGEX_API, "Ergon": ERGON_TEXT.split("?")[0]}.get(provider)
    if not expected_source or metadata.get("source_url", "").rstrip("/") != expected_source.rstrip("/"):
        raise ValueError("Provider backup has an unexpected source")
    try:
        prior = timestamp(cache[provider]["metadata"]["source_updated_utc"])
    except (KeyError, TypeError, ValueError):
        prior = None
    if prior and timestamp(metadata["source_updated_utc"]) < prior:
        raise ValueError("Provider returned an older publisher snapshot")
    cache[provider] = {"fetched_utc": iso(now), "metadata": deepcopy(metadata), "events": deepcopy(events)}


def restore(cache, provider, now, error):
    saved = cache.get(provider)
    if not isinstance(saved, dict):
        raise ValueError("No verified provider backup is available")
    age = (now - timestamp(saved.get("fetched_utc"))).total_seconds()
    if not -300 <= age <= MAX_BACKUP_AGE_SECONDS:
        raise ValueError("Verified provider backup is older than 30 minutes")
    events, metadata = deepcopy(saved.get("events")), deepcopy(saved.get("metadata"))
    if not isinstance(metadata, dict) or metadata.get("status") != "provider":
        raise ValueError("Provider backup was not verified")
    expected_source = {"Energex": ENERGEX_API, "Ergon": ERGON_TEXT.split("?")[0]}.get(provider)
    if not expected_source or metadata.get("source_url", "").rstrip("/") != expected_source.rstrip("/"):
        raise ValueError("Provider backup has an unexpected source")
    validate_events(events, metadata, now)
    metadata.update(status="provider_cache", fetched_utc=saved["fetched_utc"], collection_error=str(error))
    return events, metadata


def collect_verified_snapshot(cache, provider, loader, now_fn, allow_backup=True):
    try:
        events, metadata = loader()
        collected = now_fn()
        remember(cache, provider, events, metadata, collected)
        return events, {**metadata, "fetched_utc": iso(collected)}
    except Exception as error:
        if not allow_backup:
            raise
        try:
            return restore(cache, provider, now_fn(), error)
        except Exception as backup_error:
            raise RuntimeError(f"{type(error).__name__}: {error}. {backup_error}") from error


def write_cache(path, cache):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps({"schema_version": 1, "providers": cache}, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(path)
