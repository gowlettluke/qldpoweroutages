import json
from datetime import datetime, timezone
from pathlib import Path
from provider_snapshot_cache import MAX_BACKUP_AGE_SECONDS, timestamp
from write_provider_council_counts import count, current_stamp

manifest_path = Path("data/run_manifest.json")
manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

error_count = int(manifest.get("provider_fetch_error_count") or 0)
errors = dict(manifest.get("provider_fetch_errors") or {})
counts = manifest.get("provider_council_counts") or {}
for provider in ("energex", "ergon"):
    data = counts.get(provider) or {}
    if data.get("status") != "ok":
        errors[provider + " council counts"] = data.get("error") or "Council counts unavailable"
    elif data.get("retained"):
        try:
            now = datetime.now(timezone.utc)
            age = (now - timestamp(data.get("fetched_utc"))).total_seconds()
            if not -300 <= age <= MAX_BACKUP_AGE_SECONDS:
                raise ValueError("Retained provider snapshot is too old")
            current_stamp(timestamp(data.get("source_updated_utc")).timestamp(), now)
            if sum(count(row["affected_customers"]) for row in data.get("rows", [])) != count(data.get("total_customers")):
                raise ValueError("Retained council totals are inconsistent")
        except Exception as error:
            errors[provider + " council counts"] = str(error)

if error_count > 0 or errors:
    print("One or more provider feeds failed:")
    for provider, error in errors.items():
        print(f"- {provider}: {error}")
    raise SystemExit(1)

warnings = manifest.get("provider_fetch_warnings") or {}
if warnings:
    print("Verified provider data published with delayed updates: " + ", ".join(sorted(warnings)))
else:
    print("All provider feeds OK.")

