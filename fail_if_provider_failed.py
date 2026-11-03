import json
from pathlib import Path

manifest_path = Path("data/run_manifest.json")
manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

error_count = int(manifest.get("provider_fetch_error_count") or 0)
errors = dict(manifest.get("provider_fetch_errors") or {})
counts = manifest.get("provider_council_counts") or {}
for provider in ("energex", "ergon"):
    data = counts.get(provider) or {}
    if data.get("status") != "ok":
        errors[provider + " council counts"] = data.get("error") or "Council counts unavailable"

if error_count > 0 or errors:
    print("One or more provider feeds failed:")
    for provider, error in errors.items():
        print(f"- {provider}: {error}")
    raise SystemExit(1)

print("All provider feeds OK.")

