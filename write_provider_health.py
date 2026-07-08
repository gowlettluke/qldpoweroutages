import csv
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

DATA_DIR = Path("data")
MANIFEST_PATH = DATA_DIR / "run_manifest.json"
HEALTH_PATH = DATA_DIR / "provider_health_48h.csv"

PROVIDERS = [
    ("Energex", ("Energex",), "current_energex_records_fetched", "current_energex_records"),
    ("Ergon Energy", ("Ergon", "Ergon Energy"), "current_ergon_records_fetched", "current_ergon_records"),
    ("Essential Energy", ("Essential Energy",), "current_essential_records_fetched", "current_essential_records_included_qld"),
]

FIELDS = [
    "generated_utc",
    "generated_aest",
    "provider_name",
    "status",
    "record_count",
    "error_message",
]

PROVIDER_NAME_ALIASES = {
    "Ergon": "Ergon Energy",
}

def normalise_provider_name(value):
    return PROVIDER_NAME_ALIASES.get(value, value)

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
    for provider_name, error_keys, fetched_count_field, fallback_count_field in PROVIDERS:
        error_message = next((errors.get(key, "") for key in error_keys if errors.get(key)), "")
        record_count = manifest.get(fetched_count_field, manifest.get(fallback_count_field, 0))
        current_keys.add((generated_utc, provider_name))
        new_rows.append({
            "generated_utc": generated_utc,
            "generated_aest": generated_aest,
            "provider_name": provider_name,
            "status": "FAIL" if error_message else "OK",
            "record_count": str(record_count),
            "error_message": error_message,
        })

    cutoff = cutoff_base - timedelta(hours=48)

    kept = []
    seen = set()
    for row in existing:
        row_time = parse_utc(row.get("generated_utc"))
        row_provider_name = normalise_provider_name(row.get("provider_name", ""))
        key = (row.get("generated_utc", ""), row_provider_name)
        if row_time is None or row_time < cutoff or key in current_keys or key in seen:
            continue
        seen.add(key)
        normalised_row = {field: row.get(field, "") for field in FIELDS}
        normalised_row["provider_name"] = row_provider_name
        kept.append(normalised_row)

    all_rows = kept + new_rows

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with HEALTH_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"Wrote {HEALTH_PATH} with {len(all_rows)} rows.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
