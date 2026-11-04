import json
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import qld_power_outage_pipeline as pipeline
import write_provider_council_counts as council_writer
from provider_councils import fetch_energex_councils, fetch_ergon_councils, parse_ergon_councils


def ergon_item(event, council, customers, outage_type="UNPLANNED"):
    # The live page omits </li> between some events/councils.
    return (f'<h2>{council} Council</h2><ul class="LGA"><li class="outage-container UNPLN" '
            f'data-outageid="{event}" data-suburb="EXAMPLE">'
            f'<div class="outage-planned"><span class="value">{outage_type}</span></div>'
            f'<span class="customers-number value">{customers}</span>')


def energex_fixture():
    return {
        "Ipswich City": [
            {"event": "INCD-992580-g", "suburb": "THAGOONA", "customersAffected": "12"},
            {"event": "INCD-992604-g", "suburb": "GOOLMAN", "customersAffected": "24"},
            {"event": "INCD-992604-g", "suburb": "PURGA", "customersAffected": "13"},
        ],
        "Scenic Rim Regional": [
            {"event": "INCD-992604-g", "suburb": "PEAK CROSSING", "customersAffected": "26"},
            {"event": "INCD-992683-g", "suburb": "MOOGERAH", "customersAffected": "1"},
        ],
    }


def fixture_fetch(url, fixture=None):
    fixture = fixture or energex_fixture()
    parsed = urlparse(url)
    if parsed.path.endswith("summary"):
        return {"data": {"totalCustomersAffected": 76, "lastUpdated": "3 November 2026 7:30 PM", "secondsSinceEpoch": str(int(time.time()))}}
    if parsed.path.endswith("council"):
        return {"data": [{"name": k, "customersAffected": sum(int(r["customersAffected"]) for r in v)} for k, v in fixture.items()]}
    council = parse_qs(parsed.query)["council"][0]
    return {"data": [{**r, "council": council} for r in fixture[council]]}


class CouncilAllocationTests(unittest.TestCase):
    def test_ergon_cross_council_event_is_one_outage(self):
        page = '<ul class="results">' + "".join([
            ergon_item("26WB10530", "Cherbourg Aboriginal Shire", "380"),
            ergon_item("26WB10530", "Gympie Regional", "1,007"),
            ergon_item("26WB10530", "Somerset Regional", "2"),
            ergon_item("26WB10530", "South Burnett Regional", "4,150"),
            ergon_item("PLANNED-1", "South Burnett Regional", "900", "PLANNED"),
        ])
        events = parse_ergon_councils(page)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["affected_customers"], 5539)
        self.assertEqual({a["lga_name"]: a["affected_customers"] for a in events[0]["allocations"]}, {
            "Cherbourg Aboriginal Shire": 380, "Gympie Regional": 1007,
            "Somerset Regional": 2, "South Burnett Regional": 4150,
        })

    def test_ergon_blocked_and_unknown_count_pages_cannot_mean_zero(self):
        for page in ['<h1>Access denied</h1>', '<ul class="results">' + ergon_item("X", "Gympie Regional", "Unknown")]:
            with self.assertRaises(ValueError):
                parse_ergon_councils(page)
        self.assertEqual(parse_ergon_councils('<ul class="results"></ul>'), [])

    def test_ergon_transient_fetch_retry_still_requires_a_fresh_verified_page(self):
        page = ('<ul class="results">' + ergon_item("26E1", "Gympie Regional", "5")
                + f'<div id="hiddenDate">{int(time.time())}</div>')
        with patch("provider_councils.time.sleep"), patch("builtins.print"):
            from unittest.mock import Mock
            fetch = Mock(side_effect=[RuntimeError("Temporary failure"), page])
            events, meta = fetch_ergon_councils(fetch)
            self.assertEqual(fetch.call_count, 2)
            self.assertEqual(events[0]["affected_customers"], 5)
            self.assertEqual(meta["published_customers"], 5)
            self.assertTrue(meta["source_updated_utc"])

    def test_energex_suburbs_are_summed_within_their_published_councils(self):
        events, meta = fetch_energex_councils(fixture_fetch)
        self.assertEqual(meta["published_customers"], 76)
        self.assertEqual(len(events), 3)
        event = next(e for e in events if e["event_id"] == "INCD-992604-g")
        self.assertEqual(event["affected_customers"], 63)
        self.assertEqual({a["lga_name"]: a["affected_customers"] for a in event["allocations"]}, {
            "Ipswich City": 37, "Scenic Rim Regional": 26,
        })

    def test_incomplete_energex_details_are_rejected_not_rescaled(self):
        def incomplete(url):
            result = fixture_fetch(url)
            if urlparse(url).path.endswith("search"):
                result["data"] = []
            return result
        with self.assertRaisesRegex(ValueError, "details changed"):
            fetch_energex_councils(incomplete)

    def test_energex_valid_zero_and_invalid_zero(self):
        def zero(url):
            return {"data": {"totalCustomersAffected": 0, "secondsSinceEpoch": str(int(time.time()))}} if urlparse(url).path.endswith("summary") else {"data": []}
        events, meta = fetch_energex_councils(zero)
        self.assertEqual(events, [])
        self.assertEqual(meta["published_customers"], 0)
        with self.assertRaises(ValueError):
            fetch_energex_councils(lambda url: {"data": []})

    def test_website_only_events_count_and_csv_councils_conserve_customers(self):
        events, meta = fetch_energex_councils(fixture_fetch)
        records = pipeline.apply_council_events([], "au.qld.energex", "Energex", events, meta["source_url"])
        self.assertEqual(len(records), 3)
        self.assertEqual(sum(r.affected_customers for r in records), 76)
        self.assertTrue(all(r.geometry is None for r in records))
        with tempfile.TemporaryDirectory() as temp, patch.object(pipeline, "lookup_lga_for_point", side_effect=AssertionError("must use provider allocations")):
            pipeline.assign_lgas(records, Path(temp) / "cache.json", 1, "test", 5, False)
        rows = [pipeline.snapshot_row(r, datetime(2026, 11, 3, 9, 31, tzinfo=timezone.utc)) for r in records]
        totals = pipeline.build_lga_totals(rows)
        provider = {r["lga_name"]: r["total_affected_customers"] for r in totals if r["provider_code"] == "au.qld.energex"}
        self.assertEqual(provider, {"Ipswich City": 49, "Scenic Rim Regional": 27})
        self.assertEqual(sum(r["total_affected_customers"] for r in totals if r["provider_code"] == "all"), 76)

    def test_ergon_history_row_ids_become_one_stable_event_without_backdating_allocations(self):
        rows = [{
            "provider_code": "au.qld.ergon", "outage_type": "unplanned", "outage_id": object_id,
            "outage_key": f"au.qld.ergon|unplanned|{object_id}", "snapshot_utc": timestamp,
            "raw_json": json.dumps({"OBJECTID": int(object_id), "EVENT_ID": "26FN10815"}),
            "affected_customers": customers, "lga_name": "Cairns Regional",
        } for object_id, timestamp, customers in [("915719", "2026-11-03T09:01:25Z", "131"), ("915729", "2026-11-03T09:15:58Z", "2")]]
        repaired = pipeline.migrate_snapshot_event_ids(rows)
        self.assertEqual(len(set(r["outage_key"] for r in repaired)), 1)
        self.assertTrue(all(r["outage_id"] == "26FN10815" for r in repaired))
        self.assertEqual([r["affected_customers"] for r in repaired], ["131", "2"])
        self.assertTrue(all(r["lga_assignment_source"] == "geometry_legacy" for r in repaired))
        self.assertTrue(all(not r.get("lga_allocations_json") for r in repaired))

    def test_corrupt_stored_allocations_do_not_silently_change_totals(self):
        row = {"affected_customers": "99", "lga_allocations_json": json.dumps([{"lga_name": "Ipswich City", "affected_customers": 12}])}
        with self.assertRaisesRegex(ValueError, "do not match"):
            pipeline.row_lga_allocations(row)

    def test_failed_council_source_keeps_map_counts_and_reports_the_fallback(self):
        events, meta = fetch_energex_councils(fixture_fetch)
        energex = pipeline.apply_council_events([], "au.qld.energex", "Energex", events, meta["source_url"])
        energex = [pipeline.replace(r, lga_allocations=None, lga_assignment_source="geometry") for r in energex]
        page = '<ul class="results">' + ergon_item("26E1", "Gympie Regional", "5")
        ergon_events = parse_ergon_councils(page)
        with tempfile.TemporaryDirectory() as temp:
            args = pipeline.build_arg_parser().parse_args(["--output-dir", temp, "--skip-essential"])
            with patch.object(pipeline, "fetch_energex", return_value=energex), \
                 patch.object(pipeline, "fetch_ergon", return_value=[]), \
                 patch.object(pipeline, "fetch_energex_councils", side_effect=ValueError("Unavailable")), \
                 patch.object(pipeline, "fetch_ergon_councils", return_value=(ergon_events, {
                     "source_url": "https://www.ergon.com.au/network/outages/outage-finder/outage-finder-text-view",
                     "status": "provider", "source_updated_utc": pipeline.to_utc_iso(datetime.now(timezone.utc)),
                     "published_customers": 5, "event_count": 1})), \
                 patch.object(pipeline, "assign_lgas"):
                pipeline.run_pipeline(args)
            manifest = json.loads((Path(temp) / "run_manifest.json").read_text())
            current = json.loads((Path(temp) / "current_outages.geojson").read_text())["features"]
            self.assertEqual(manifest["provider_fetch_error_count"], 1)
            self.assertEqual(manifest["council_allocation_status"]["Energex"]["status"], "geometry_fallback")
            self.assertEqual(sum(f["properties"]["affected_customers"] for f in current), 81)
            self.assertEqual(manifest["provider_council_counts"]["ergon"]["total_customers"], 5)
            self.assertEqual(manifest["provider_council_counts"]["energex"]["status"], "error")

    def test_zero_current_poll_does_not_reuse_old_lga_totals(self):
        with tempfile.TemporaryDirectory() as temp:
            old = {"snapshot_utc": "2026-11-03T09:00:00Z", "provider_code": "au.qld.energex",
                   "outage_type": "unplanned", "outage_key": "old", "affected_customers": "99",
                   "lga_name": "Ipswich City"}
            pipeline.write_csv_rows(Path(temp) / "outage_snapshots_48h.csv", [old], pipeline.SNAPSHOT_COLUMNS)
            args = pipeline.build_arg_parser().parse_args([
                "--output-dir", temp, "--skip-essential", "--skip-energex", "--skip-ergon",
                "--snapshot-utc", "2026-11-03T10:00:00Z",
            ])
            pipeline.run_pipeline(args)
            self.assertEqual(pipeline.read_csv_rows(Path(temp) / "lga_totals_latest.csv"), [])

    def test_manifest_writer_reuses_the_verified_event_snapshot(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "run_manifest.json"
            snapshot = "2026-11-03T10:00:00Z"
            counts = {"schema_version": 1, "snapshot_utc": snapshot,
                      "energex": {"status": "ok", "total_customers": 76},
                      "ergon": {"status": "ok", "total_customers": 5}}
            path.write_text(json.dumps({"generated_utc": snapshot, "provider_council_counts": counts,
                                        "council_allocation_status": {"Energex": {"status": "provider"}}}))
            with patch("sys.argv", ["write_provider_council_counts.py", "--manifest", str(path)]), \
                 patch.object(council_writer, "collect_counts", side_effect=AssertionError("must not refetch")), \
                 patch("builtins.print"):
                council_writer.main()
            self.assertEqual(json.loads(path.read_text())["provider_council_counts"], counts)


if __name__ == "__main__":
    unittest.main()
