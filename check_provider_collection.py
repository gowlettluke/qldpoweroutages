"""Exercise repeated hosted collections, including the production recovery path."""
import argparse
from datetime import datetime, timezone
import json
import time

from provider_councils import fetch_energex_councils, fetch_ergon_councils
from provider_snapshot_cache import collect_verified_snapshot
from write_provider_council_counts import fetch_text


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rounds", type=int, default=3)
    args = parser.parse_args()
    if not 1 <= args.rounds <= 5:
        raise ValueError("Use one to five verification rounds")
    cache, direct = {}, {"Energex": 0, "Ergon": 0}
    failures = []
    for number in range(args.rounds):
        for provider, loader in (
            ("Energex", lambda: fetch_energex_councils(lambda url: json.loads(fetch_text(url)))),
            ("Ergon", lambda: fetch_ergon_councils(fetch_text)),
        ):
            try:
                events, meta = collect_verified_snapshot(cache, provider, loader, lambda: datetime.now(timezone.utc))
                assert sum(e["affected_customers"] for e in events) == meta["published_customers"]
                direct[provider] += meta["status"] == "provider"
                print(f"Round {number + 1}: {provider} {meta['status']}; {len(events)} events; "
                      f"{meta['published_customers']} customers; publisher {meta['source_updated_utc']}", flush=True)
            except Exception as error:
                failures.append(f"Round {number + 1}: {provider}: {error}")
                print(failures[-1], flush=True)
        if number + 1 < args.rounds:
            time.sleep(2)
    if failures or not all(direct.values()):
        raise SystemExit("Hosted collection verification failed: " + "; ".join(failures or ["No direct successful collection for every provider"]))
    print("Hosted verification passed. Direct successful collections: " + json.dumps(direct), flush=True)


if __name__ == "__main__":
    main()
