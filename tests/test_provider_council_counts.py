from datetime import datetime, timezone
import json
import unittest
from urllib.parse import urlsplit

from write_provider_council_counts import (
    ENERGEX_API, ERGON_TEXT, collect_counts, count, energex_counts, ergon_counts,
)

NOW = datetime(2026, 11, 3, 9, 15, tzinfo=timezone.utc)
EPOCH = str(int(NOW.timestamp()))


def page(rows, epoch=EPOCH):
    # Ergon's published HTML does not consistently close li tags.
    return "<ul class='results'>" + "".join(
        f"<h2>{council} Council</h2><ul><li class='outage-container {kind}' data-outageid='{event}'>"
        f"<span class='customers-number value'>{number}</span>"
        for council, event, number, kind in rows
    ) + f"</ul><div id='hiddenDate'>{epoch}</div>"


def energex_fetch(rows=None, total=1009, epoch=EPOCH):
    rows = rows if rows is not None else [{"name": "Ipswich City", "customersAffected": 1009}]
    def fetch(url):
        data = rows if "council?" in url else {"secondsSinceEpoch": epoch, "totalCustomersAffected": total}
        return json.dumps({"data": data})
    return fetch


class CouncilCountsTests(unittest.TestCase):
    def test_ergon_uses_provider_split_and_counts_forced_as_unplanned(self):
        raw = page([
            ("North Burnett Regional", "26WB10512", 1, "UNPLN"),
            ("South Burnett Regional", "26WB10512", "1,007", "UNPLN"),
            ("Western Downs Regional", "26WB10512", 1, "UNPLN"),
            ("Western Downs Regional", "forced", 10, "FORCD"),
            ("Western Downs Regional", "planned", 5000, "PLN"),
        ])
        result = ergon_counts(lambda _: raw, NOW)
        self.assertEqual(result["total_customers"], 1019)
        self.assertEqual(result["rows"], [
            {"lga_name": "North Burnett Regional", "affected_customers": 1},
            {"lga_name": "South Burnett Regional", "affected_customers": 1007},
            {"lga_name": "Western Downs Regional", "affected_customers": 11},
        ])

    def test_same_event_in_one_council_deduplicates_but_different_counts_fail(self):
        row = ("Alpha Regional", "same", 5, "UNPLN")
        self.assertEqual(ergon_counts(lambda _: page([row, row]), NOW)["total_customers"], 5)
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            ergon_counts(lambda _: page([row, ("Alpha Regional", "same", 6, "UNPLN")]), NOW)

    def test_missing_council_count_id_or_unknown_type_cannot_be_a_zero(self):
        good = page([("Alpha", "id", 1, "UNPLN")])
        variants = [good.replace("<h2>Alpha Council</h2>", ""),
                    good.replace("customers-number", "changed-layout"),
                    good.replace("data-outageid='id'", ""),
                    good.replace("UNPLN", "UNKNOWN"),
                    good[:good.index("<div id='hiddenDate'")], "<html>Unavailable</html>"]
        for raw in variants:
            with self.subTest(raw=raw), self.assertRaises((ValueError, TypeError)):
                ergon_counts(lambda _: raw, NOW)

    def test_only_verified_empty_current_results_are_zero(self):
        self.assertEqual(ergon_counts(lambda _: page([]), NOW)["total_customers"], 0)
        self.assertEqual(energex_counts(energex_fetch([], 0), NOW)["rows"], [])
        with self.assertRaisesRegex(ValueError, "reconcile"):
            energex_counts(energex_fetch([], 100), NOW)

    def test_energex_reconciles_council_totals_and_rejects_duplicates(self):
        rows = [{"name": "Ipswich City", "customersAffected": 37},
                {"name": "Scenic Rim Regional", "customersAffected": 26}]
        result = energex_counts(energex_fetch(rows, 63), NOW)
        self.assertEqual(result["total_customers"], 63)
        self.assertEqual(len(result["rows"]), 2)
        with self.assertRaisesRegex(ValueError, "reconcile"):
            energex_counts(energex_fetch(rows, 64), NOW)
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            energex_counts(energex_fetch(rows + rows, 126), NOW)

    def test_energex_rejects_an_update_during_collection(self):
        calls = []
        def fetch(url):
            calls.append(url)
            if urlsplit(url).path.endswith("/summary") and len(calls) == 3:
                return json.dumps({"data": {"secondsSinceEpoch": str(int(EPOCH) + 1), "totalCustomersAffected": 1009}})
            return energex_fetch()(url)
        with self.assertRaisesRegex(ValueError, "changed"):
            energex_counts(fetch, NOW)

    def test_both_publishers_need_a_fresh_update_time(self):
        for delta in (-2701, 301):
            epoch = str(int(EPOCH) + delta)
            with self.subTest(delta=delta):
                with self.assertRaisesRegex(ValueError, "stale|future"):
                    ergon_counts(lambda _: page([], epoch), NOW)
                with self.assertRaisesRegex(ValueError, "stale|future"):
                    energex_counts(energex_fetch([], 0, epoch), NOW)

    def test_invalid_counts_are_rejected_not_converted_to_zero(self):
        for value in (None, True, False, -1, "", "unknown", "1.5", 1.5, "1,2", "-5", 2**53):
            with self.subTest(value=value), self.assertRaises(ValueError):
                count(value)

    def test_provider_failure_keeps_other_provider_without_fabricating_counts(self):
        def fetch(url):
            if url.startswith(ENERGEX_API):
                raise RuntimeError("Unavailable")
            self.assertEqual(url, ERGON_TEXT)
            return page([("South Burnett Regional", "x", 1007, "UNPLN")])
        result = collect_counts(NOW.isoformat(), fetch=fetch, now=NOW)
        self.assertEqual(result["energex"]["status"], "error")
        self.assertNotIn("rows", result["energex"])
        self.assertEqual(result["ergon"]["total_customers"], 1007)
        self.assertEqual(result["schema_version"], 1)


if __name__ == "__main__":
    unittest.main()
