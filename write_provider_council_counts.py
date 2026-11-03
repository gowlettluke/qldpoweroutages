"""Add providers' current unplanned council counts to the map run manifest.

Council counts are independent of the centroid-assigned historical CSVs and
whole-outage map polygons. Never substitute those estimates for provider counts.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from html.parser import HTMLParser
import json
from pathlib import Path
import re
from urllib.error import HTTPError
from urllib.request import Request, urlopen


ENERGEX_API = "https://www.energex.com.au/api/outages-text/v1.0.ex/"
ERGON_TEXT = (
    "https://www.ergon.com.au/network/outages/outage-finder/"
    "outage-finder-text-view?SQ_SESSION_MODE=read_only_async"
)
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36"
MAX_AGE_SECONDS = 45 * 60


def iso(dt):
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def current_stamp(seconds, now):
    if isinstance(seconds, bool):
        raise ValueError("Missing or invalid provider update time")
    stamp = datetime.fromtimestamp(float(seconds), timezone.utc)
    if not -300 <= (now - stamp).total_seconds() <= MAX_AGE_SECONDS:
        raise ValueError("Provider council counts are stale or future-dated")
    return iso(stamp)


def count(value):
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError("Missing or invalid customer count")
    text = str(value).strip()
    if not re.fullmatch(r"(?:\d+|\d{1,3}(?:,\d{3})+)", text):
        raise ValueError("Missing or invalid customer count")
    number = int(text.replace(",", ""))
    if number > 2**53 - 1:
        raise ValueError("Customer count exceeds the supported range")
    return number


def council_name(value):
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Missing council name")
    return re.sub(r"\s+Council$", "", " ".join(value.split()), flags=re.I)


def fetch_text(url):
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
        "Cache-Control": "no-cache",
    }
    try:
        with urlopen(Request(url, headers=headers), timeout=20) as response:
            return response.read().decode("utf-8")
    except HTTPError as error:
        if error.code not in (403, 429, 503):
            raise
        # Same public request, using the dependency already installed by the poller.
        from curl_cffi import requests
        response = requests.get(url, headers=headers, timeout=20, impersonate="chrome")
        response.raise_for_status()
        return response.text


def energex_counts(fetch, now):
    def data(path):
        return json.loads(fetch(ENERGEX_API + path))["data"]

    before = data("summary")
    councils = data("council?council=")
    after = data("summary")
    if (before["secondsSinceEpoch"], before["totalCustomersAffected"]) != (
        after["secondsSinceEpoch"], after["totalCustomersAffected"]
    ):
        raise ValueError("Energex council counts changed during collection")
    updated = current_stamp(after["secondsSinceEpoch"], now)
    if not isinstance(councils, list):
        raise ValueError("Energex council list is unavailable")
    rows, seen = [], set()
    for item in councils:
        name = council_name(item["name"])
        if name.casefold() in seen:
            raise ValueError("Duplicate Energex council")
        seen.add(name.casefold())
        rows.append({"lga_name": name, "affected_customers": count(item["customersAffected"])})
    total = count(after["totalCustomersAffected"])
    if sum(row["affected_customers"] for row in rows) != total:
        raise ValueError("Energex council counts do not reconcile with its current total")
    return {"source_url": ENERGEX_API + "council?council=", "source_updated_utc": updated,
            "total_customers": total, "rows": sorted(rows, key=lambda row: row["lga_name"])}


class ErgonCouncils(HTMLParser):
    """Read the same council/event customer rows used by Ergon's text view.

    Its HTML omits some closing li tags, so rows end at the next outage/header.
    An event can legitimately appear in several councils with different counts.
    """

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.results = False
        self.council = None
        self.row = None
        self.rows = {}
        self.capture = None
        self.text = []
        self.updated = None

    def finish_row(self):
        if self.row is None:
            return
        row, self.row = self.row, None
        if row["kind"] == "PLN":
            return
        if not row["council"] or not row["id"] or len(row["counts"]) != 1:
            raise ValueError("Incomplete Ergon council outage row")
        key = (row["council"], row["id"])
        number = count(row["counts"][0])
        if key in self.rows and self.rows[key] != number:
            raise ValueError("Conflicting Ergon council outage rows")
        self.rows[key] = number

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        classes = set(attrs.get("class", "").split())
        if tag == "ul" and "results" in classes:
            self.results = True
        if attrs.get("id") == "hiddenDate":
            self.finish_row()
            self.capture, self.text = (tag, "updated"), []
        elif self.results and tag == "h2":
            self.finish_row()
            self.capture, self.text = (tag, "council"), []
        elif self.results and tag == "li" and "outage-container" in classes:
            self.finish_row()
            kinds = classes & {"UNPLN", "FORCD", "PLN"}
            if len(kinds) != 1:
                raise ValueError("Unknown Ergon outage type")
            self.row = {"kind": kinds.pop(), "council": self.council,
                        "id": attrs.get("data-outageid"), "counts": []}
        elif self.row is not None and tag == "span" and "customers-number" in classes:
            self.capture, self.text = (tag, "count"), []

    def handle_data(self, text):
        if self.capture:
            self.text.append(text)

    def handle_endtag(self, tag):
        if self.capture and self.capture[0] == tag:
            field, value = self.capture[1], "".join(self.text).strip()
            self.capture = None
            if field == "council":
                self.council = council_name(value)
            elif field == "updated":
                if self.updated is not None:
                    raise ValueError("Duplicate Ergon update time")
                self.updated = value
            else:
                self.row["counts"].append(value)


def ergon_counts(fetch, now):
    parser = ErgonCouncils()
    parser.feed(fetch(ERGON_TEXT))
    parser.close()
    parser.finish_row()
    if not parser.results or not parser.updated or parser.capture:
        raise ValueError("Ergon council results are unavailable or incomplete")
    updated = current_stamp(parser.updated, now)
    totals = {}
    for (council, _event), number in parser.rows.items():
        totals[council] = totals.get(council, 0) + number
    rows = [{"lga_name": name, "affected_customers": number} for name, number in sorted(totals.items())]
    return {"source_url": ERGON_TEXT, "source_updated_utc": updated,
            "total_customers": sum(totals.values()), "rows": rows}


def collect_counts(snapshot_utc, fetch=fetch_text, now=None):
    now = now or datetime.now(timezone.utc)
    snapshot = datetime.fromisoformat(snapshot_utc.replace("Z", "+00:00"))
    current_stamp(snapshot.timestamp(), now)
    result = {"schema_version": 1, "snapshot_utc": snapshot_utc}

    def collect(item):
        name, parser = item
        try:
            return name, {"status": "ok", "fetched_utc": iso(now), "outage_type": "unplanned",
                          "scope": "all_councils", **parser(fetch, now)}
        except Exception as error:
            return name, {"status": "error", "fetched_utc": iso(now),
                          "error": f"{type(error).__name__}: {error}"}

    with ThreadPoolExecutor(max_workers=2) as pool:
        result.update(pool.map(collect, [("energex", energex_counts), ("ergon", ergon_counts)]))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path("data/run_manifest.json"))
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    counts = collect_counts(manifest["generated_utc"])
    manifest["provider_council_counts"] = counts
    args.manifest.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    for code in ("energex", "ergon"):
        data = counts[code]
        print(f"{code} council counts: {data['status']}" +
              (f" ({data['total_customers']} customers)" if data["status"] == "ok" else f" ({data['error']})"))


if __name__ == "__main__":
    main()
