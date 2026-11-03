"""Read the customer allocations published by the two Queensland distributors.

Map polygons carry an event total, not a per-council count. Keep one event and
attach its published council allocations so statewide totals count it once.
"""
from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Any, Callable
from urllib.parse import urlencode

from write_provider_council_counts import count, current_stamp, ergon_counts

ENERGEX_TEXT_API = "https://www.energex.com.au/api/outages-text/v1.0.ex/"
ERGON_TEXT_URL = "https://www.ergon.com.au/network/outages/outage-finder/outage-finder-text-view"


def council_name(value: str) -> str:
    name = re.sub(r"\s+Council$", "", value.strip(), flags=re.I)
    aliases = {
        "noosa": "Noosa Shire",
        "sunshine coast": "Sunshine Coast Regional",
        "moreton bay regional": "Moreton Bay City",
        "mckinlay shire": "McKinlay Shire",
    }
    return aliases.get(name.casefold(), name)


def council_key(value: str) -> str:
    name = council_name(value).casefold()
    name = re.sub(r"\s+(?:aboriginal shire|island|regional|shire|city)$", "", name)
    return re.sub(r"[^a-z0-9]", "", name)


def customer_count(value: Any) -> int:
    return count(value)


def group_council_events(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    events: dict[str, dict[str, Any]] = {}
    for row in rows:
        event_id = str(row.get("event") or "").strip()
        name = council_name(str(row.get("council") or ""))
        if not event_id or not name:
            raise ValueError("Provider outage is missing its event ID or council")
        event = events.setdefault(event_id, {
            "event_id": event_id, "allocations": {}, "suburbs": set(),
            "streets": set(), "details": row,
        })
        allocation = event["allocations"].setdefault(name, {
            "lga_name": name, "lga_code": None, "affected_customers": 0,
        })
        allocation["affected_customers"] += customer_count(row["customersAffected"])
        suburbs = row.get("suburb") or ""
        if suburbs:
            event["suburbs"].update(s.strip().upper() for s in suburbs.split(",") if s.strip())
        event["streets"].update(row.get("streets") or [])
    result = []
    for event in events.values():
        event["allocations"] = sorted(
            event["allocations"].values(),
            key=lambda a: (-a["affected_customers"], a["lga_name"]),
        )
        event["affected_customers"] = sum(a["affected_customers"] for a in event["allocations"])
        event["suburb"] = ", ".join(sorted(event.pop("suburbs")))
        event["street_name"] = ", ".join(sorted(event.pop("streets")))
        result.append(event)
    return sorted(result, key=lambda e: e["event_id"])


def fetch_energex_councils(get_json: Callable[[str], Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Use the same council/search API as Energex's emergency-outages text view.

    Fetch councils concurrently and check their details against the published
    totals. A moving/incomplete response is retried as a whole, never rescaled.
    """
    last_error: Exception | None = None
    for attempt in range(2):
        def fetch(path: str, **params: str) -> Any:
            payload = get_json(ENERGEX_TEXT_API + path + "?" + urlencode({
                **params, "SQ_SESSION_MODE": "read_only_async",
            }))
            if not isinstance(payload, dict) or "data" not in payload:
                raise ValueError("Invalid Energex text API response")
            return payload["data"]

        try:
            summary = fetch("summary")
            councils = fetch("council", council="")
            if not isinstance(summary, dict) or not isinstance(councils, list):
                raise ValueError("Invalid Energex summary/council data")
            published = customer_count(summary.get("totalCustomersAffected"))
            expected = {}
            for item in councils:
                name = item.get("name")
                if not isinstance(name, str) or name in expected:
                    raise ValueError("Missing or duplicate Energex council")
                expected[name] = customer_count(item.get("customersAffected"))
            if sum(expected.values()) != published:
                raise ValueError("Energex council totals changed during polling")

            def fetch_council(name: str) -> list[dict[str, Any]]:
                details = fetch("search", council=name)
                if not isinstance(details, list):
                    raise ValueError("Invalid Energex council outage details")
                seen = set()
                unique = []
                for row in details:
                    if not isinstance(row, dict) or council_key(row.get("council", "")) != council_key(name):
                        raise ValueError("Energex returned details for an unexpected council")
                    identity = (row.get("event"), row.get("suburb"), row.get("postcode"))
                    if identity in seen:
                        raise ValueError("Duplicate Energex event/suburb allocation")
                    seen.add(identity)
                    unique.append(row)
                if sum(customer_count(r.get("customersAffected")) for r in unique) != expected[name]:
                    raise ValueError(f"Energex details changed during polling for {name}")
                return unique

            with ThreadPoolExecutor(max_workers=4) as pool:
                rows = [r for group in pool.map(fetch_council, expected) for r in group]
            after = fetch("summary")
            if not isinstance(after, dict) or (summary.get("secondsSinceEpoch"), published) != (
                after.get("secondsSinceEpoch"), customer_count(after.get("totalCustomersAffected"))
            ):
                raise ValueError("Energex publisher updated during council collection")
            updated = current_stamp(after["secondsSinceEpoch"], datetime.now(timezone.utc))
            events = group_council_events(rows)
            return events, {
                "status": "provider", "source_url": ENERGEX_TEXT_API,
                "source_updated": summary.get("lastUpdated", ""),
                "source_updated_utc": updated,
                "published_customers": published, "event_count": len(events),
            }
        except Exception as exc:
            last_error = exc
    raise ValueError(f"Energex council allocations unavailable: {last_error}") from last_error


class ErgonTextParser(HTMLParser):
    """Read Ergon's server-rendered council/event list, including optional </li> tags."""
    FIELDS = {
        "outage-planned": "type", "outage-start": "startTime",
        "outage-fix-time": "restoreTime", "reason": "cause",
        "outage-status": "status",
    }

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.council = ""
        self.heading: list[str] | None = None
        self.current: dict[str, Any] | None = None
        self.rows: list[dict[str, Any]] = []
        self.div_fields: list[str | None] = []
        self.capture: str | None = None
        self.capture_text: list[str] = []
        self.has_results = False

    def finish_event(self) -> None:
        if self.current is not None:
            if self.current.get("type", "").upper() == "UNPLANNED":
                customer_count(self.current.get("customersAffected"))
                self.rows.append(self.current)
            elif self.current.get("type", "").upper() != "PLANNED":
                raise ValueError("Ergon event has no recognised outage type")
        self.current = None
        self.capture = None
        self.div_fields = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = dict(attrs)
        classes = set((values.get("class") or "").split())
        if tag == "ul" and "results" in classes:
            self.has_results = True
        if tag == "h2":
            self.finish_event()
            self.heading = []
        if tag == "li" and "outage-container" in classes:
            self.finish_event()
            self.current = {
                "event": values.get("data-outageid"), "council": self.council,
                "suburb": (values.get("data-suburb") or "").replace(",", ", "),
            }
        if self.current is None:
            return
        if tag == "div":
            self.div_fields.append(next((v for k, v in self.FIELDS.items() if k in classes), None))
        if tag == "span":
            if "customers-number" in classes:
                self.capture = "customersAffected"
            elif "value" in classes:
                self.capture = next((f for f in reversed(self.div_fields) if f), None)
            else:
                self.capture = None
            self.capture_text = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "h2" and self.heading is not None:
            self.council = " ".join("".join(self.heading).split())
            self.heading = None
        if tag == "span" and self.capture and self.current is not None:
            self.current[self.capture] = " ".join("".join(self.capture_text).split())
            self.capture = None
        if tag == "div" and self.div_fields:
            self.div_fields.pop()

    def handle_data(self, data: str) -> None:
        if self.heading is not None:
            self.heading.append(data)
        if self.capture:
            self.capture_text.append(data)


def parse_ergon_councils(page: str) -> list[dict[str, Any]]:
    parser = ErgonTextParser()
    parser.feed(page)
    parser.close()
    parser.finish_event()
    if not parser.has_results:
        raise ValueError("Ergon response does not contain the outage results list")
    seen = {}
    unique = []
    for row in parser.rows:
        identity = (row.get("event"), row.get("council"))
        if identity in seen:
            if seen[identity] != customer_count(row["customersAffected"]):
                raise ValueError("Conflicting Ergon event/council allocation")
            continue
        seen[identity] = customer_count(row["customersAffected"])
        unique.append(row)
    return group_council_events(unique)


def fetch_ergon_councils(get_text: Callable[[str], str]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    # Use the provider's public read-only mode and verify its publisher time.
    url = ERGON_TEXT_URL + "?" + urlencode({"SQ_SESSION_MODE": "read_only_async"})
    page = get_text(url)
    verified = ergon_counts(lambda _: page, datetime.now(timezone.utc))
    events = parse_ergon_councils(page)
    if sum(e["affected_customers"] for e in events) != verified["total_customers"]:
        raise ValueError("Ergon event allocations do not reconcile with its published counts")
    return events, {
        "status": "provider", "source_url": ERGON_TEXT_URL,
        "source_updated_utc": verified["source_updated_utc"],
        "published_customers": sum(e["affected_customers"] for e in events),
        "event_count": len(events),
    }
