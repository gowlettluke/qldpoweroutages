import csv
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

DATA_DIR = Path("data")
MANIFEST_PATH = DATA_DIR / "run_manifest.json"
HEALTH_PATH = DATA_DIR / "provider_health_48h.csv"

PROVIDERS = [
    ("Energex", "current_energex_records"),
    ("Ergon", "current_ergon_records"),
    ("Essential Energy", "current_essential_records_fetched"),
]

FIELDS = [
    "generated_utc",
    "generated_aest",
    "provider_name",
    "status",
    "record_count",
    "error_message",
]

def parse_utc(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None

def main():
    if not MANIFEST_PATH.exists():
        print("No run_manifest.json found.")
        return 1

    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    errors = manifest.get("provider_fetch_errors") or {}

    generated_utc = manifest.get("generated_utc", "")
    generated_aest = manifest.get("generated_aest", "")
    generated_dt = parse_utc(generated_utc)
    cutoff_base = generated_dt or datetime.now(timezone.utc)

    existing = []
    if HEALTH_PATH.exists():
        with HEALTH_PATH.open("r", encoding="utf-8", newline="") as f:
            existing = list(csv.DictReader(f))

    new_rows = []
    current_keys = set()
    for provider_name, count_field in PROVIDERS:
        error_message = errors.get(provider_name, "")
        current_keys.add((generated_utc, provider_name))
        new_rows.append({
            "generated_utc": generated_utc,
            "generated_aest": generated_aest,
            "provider_name": provider_name,
            "status": "FAIL" if error_message else "OK",
            "record_count": str(manifest.get(count_field, 0)),
            "error_message": error_message,
        })

    cutoff = cutoff_base - timedelta(hours=48)

    kept = []
    seen = set()
    for row in existing:
        row_time = parse_utc(row.get("generated_utc"))
        key = (row.get("generated_utc", ""), row.get("provider_name", ""))
        if row_time is None or row_time < cutoff or key in current_keys or key in seen:
            continue
        seen.add(key)
        kept.append({field: row.get(field, "") for field in FIELDS})

    all_rows = kept + new_rows

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with HEALTH_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"Wrote {HEALTH_PATH} with {len(all_rows)} rows.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
