from contextlib import chdir, redirect_stdout
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
import runpy
import tempfile
import unittest
from unittest.mock import Mock, patch

import provider_snapshot_cache as snapshots
import qld_power_outage_pipeline as pipeline
import write_provider_health as health

NOW = datetime.now(timezone.utc).replace(microsecond=0)


def fixture(customers=5):
    events = [{"event_id": "26TEST1", "affected_customers": customers,
               "allocations": [{"lga_name": "Gympie Regional", "affected_customers": customers, "lga_code": None}],
               "suburb": "GYMPIE", "street_name": "", "details": {}}] if customers else []
    meta = {"status": "provider", "source_updated_utc": snapshots.iso(NOW - timedelta(minutes=1)),
            "published_customers": customers, "event_count": len(events),
            "source_url": "https://www.ergon.com.au/network/outages/outage-finder/outage-finder-text-view"}
    return events, meta


class VerifiedSnapshotTests(unittest.TestCase):
    def saved(self, customers=5):
        cache = {}
        snapshots.remember(cache, "Ergon", *fixture(customers), NOW)
        return cache

    def test_temporary_failure_retains_exact_counts_and_original_times(self):
        cache = self.saved()
        original = deepcopy(cache)
        events, meta = snapshots.collect_verified_snapshot(
            cache, "Ergon", Mock(side_effect=RuntimeError("HTTP 403")),
            lambda: NOW + timedelta(minutes=15),
        )
        self.assertEqual(events, fixture()[0])
        self.assertEqual(meta["status"], "provider_cache")
        self.assertEqual(meta["fetched_utc"], snapshots.iso(NOW))
        self.assertEqual(meta["source_updated_utc"], fixture()[1]["source_updated_utc"])
        self.assertIn("403", meta["collection_error"])
        self.assertEqual(cache, original)

    def test_repeated_failures_never_extend_the_backup_lifetime(self):
        cache = self.saved()
        snapshots.restore(cache, "Ergon", NOW + timedelta(minutes=29), "Unavailable")
        with self.assertRaisesRegex(ValueError, "older than 30"):
            snapshots.restore(cache, "Ergon", NOW + timedelta(minutes=31), "Still unavailable")
        self.assertEqual(cache["Ergon"]["fetched_utc"], snapshots.iso(NOW))

    def test_publisher_age_limit_still_applies_to_a_recently_collected_backup(self):
        events, meta = fixture()
        meta["source_updated_utc"] = snapshots.iso(NOW - timedelta(minutes=44))
        cache = {}
        snapshots.remember(cache, "Ergon", events, meta, NOW)
        with self.assertRaisesRegex(ValueError, "stale"):
            snapshots.restore(cache, "Ergon", NOW + timedelta(minutes=2), "Unavailable")

    def test_corrupt_or_wrong_provider_backup_cannot_pass_as_verified_data(self):
        cache = self.saved()
        cache["Ergon"]["events"][0]["allocations"][0]["affected_customers"] = 900
        with self.assertRaisesRegex(ValueError, "do not match"):
            snapshots.restore(cache, "Ergon", NOW, "Unavailable")
        with self.assertRaisesRegex(ValueError, "No verified"):
            snapshots.restore(cache, "Energex", NOW, "Unavailable")

    def test_verified_zero_is_valid_but_expired_zero_is_rejected(self):
        cache = self.saved(0)
        events, meta = snapshots.restore(cache, "Ergon", NOW, "Unavailable")
        self.assertEqual(events, [])
        self.assertEqual(meta["published_customers"], 0)
        with self.assertRaises(ValueError):
            snapshots.restore(cache, "Ergon", NOW + timedelta(hours=1), "Unavailable")

    def test_a_fresh_collection_replaces_the_old_backup_and_strict_mode_does_not_use_it(self):
        cache = self.saved()
        snapshots.collect_verified_snapshot(cache, "Ergon", lambda: fixture(12), lambda: NOW + timedelta(minutes=5))
        self.assertEqual(cache["Ergon"]["metadata"]["published_customers"], 12)
        with self.assertRaisesRegex(RuntimeError, "HTTP 403"):
            snapshots.collect_verified_snapshot(cache, "Ergon", Mock(side_effect=RuntimeError("HTTP 403")), lambda: NOW, allow_backup=False)

    def test_cache_survives_workflow_processes_and_malformed_files_are_ignored(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "cache.json"
            snapshots.write_cache(path, self.saved())
            self.assertEqual(snapshots.restore(snapshots.read_cache(path), "Ergon", NOW, "Unavailable")[1]["published_customers"], 5)
            path.write_text("[]")
            self.assertEqual(snapshots.read_cache(path), {})

    def test_older_cdn_response_cannot_replace_a_newer_verified_snapshot(self):
        cache = self.saved()
        events, meta = fixture()
        meta["source_updated_utc"] = snapshots.iso(NOW - timedelta(minutes=2))
        restored, retained = snapshots.collect_verified_snapshot(cache, "Ergon", lambda: (events, meta), lambda: NOW)
        self.assertEqual(restored, fixture()[0])
        self.assertEqual(retained["status"], "provider_cache")
        self.assertIn("older publisher", retained["collection_error"])

    def test_snapshot_cannot_be_swapped_between_providers(self):
        cache = {"Energex": self.saved()["Ergon"]}
        with self.assertRaisesRegex(ValueError, "unexpected source"):
            snapshots.restore(cache, "Energex", NOW, "Unavailable")

    def run_poll(self, root, cached_age=15):
        data = root / "data"
        data.mkdir()
        snapshots.write_cache(data / "provider_council_cache.json", self.saved())
        now = NOW + timedelta(minutes=cached_age)
        energex_events, energex_meta = fixture(12)
        energex_meta["source_updated_utc"] = snapshots.iso(now)
        energex_meta["source_url"] = "https://www.energex.com.au/api/outages-text/v1.0.ex/"
        args = pipeline.build_arg_parser().parse_args(["--output-dir", str(data), "--skip-essential"])
        with patch.object(pipeline, "utc_now", return_value=now), \
             patch.object(pipeline, "fetch_energex", return_value=[]), \
             patch.object(pipeline, "fetch_ergon", return_value=[]), \
             patch.object(pipeline, "fetch_energex_councils", return_value=(energex_events, energex_meta)), \
             patch.object(pipeline, "fetch_ergon_councils", side_effect=RuntimeError("HTTP 403")), \
             patch.object(pipeline, "assign_lgas"), redirect_stdout(io.StringIO()):
            pipeline.run_pipeline(args)
        return data, json.loads((data / "run_manifest.json").read_text())

    def test_recovered_poll_publishes_warning_and_fatal_guard_accepts_only_verified_data(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            data, manifest = self.run_poll(root)
            self.assertEqual(manifest["provider_fetch_error_count"], 0)
            self.assertIn("Ergon", manifest["provider_fetch_warnings"])
            self.assertTrue(manifest["provider_council_counts"]["ergon"]["retained"])
            self.assertEqual(manifest["provider_council_counts"]["ergon"]["fetched_utc"], snapshots.iso(NOW))
            events = json.loads((data / "current_outages.geojson").read_text())["features"]
            self.assertEqual(sum(f["properties"]["affected_customers"] for f in events), 17)
            ergon = next(f["properties"] for f in events if f["properties"]["provider_code"] == "au.qld.ergon")
            self.assertEqual(ergon["lga_assignment_source"], "provider_cache")
            self.assertEqual(ergon["provider_data_as_of_utc"], fixture()[1]["source_updated_utc"])
            with patch.object(health, "DATA_DIR", data), patch.object(health, "MANIFEST_PATH", data / "run_manifest.json"), \
                 patch.object(health, "HEALTH_PATH", data / "provider_health_48h.csv"), redirect_stdout(io.StringIO()):
                health.main()
            rows = pipeline.read_csv_rows(data / "provider_health_48h.csv")
            self.assertEqual(next(r["status"] for r in rows if r["provider_name"] == "Ergon Energy"), "WARNING")
            with chdir(root), redirect_stdout(io.StringIO()):
                runpy.run_path(str(Path(pipeline.__file__).with_name("fail_if_provider_failed.py")))

    def test_an_expired_backup_still_fails_the_production_guard(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            _data, manifest = self.run_poll(root, cached_age=31)
            self.assertEqual(manifest["provider_fetch_error_count"], 1)
            self.assertEqual(manifest["provider_council_counts"]["ergon"]["status"], "error")
            with chdir(root), redirect_stdout(io.StringIO()), self.assertRaises(SystemExit):
                runpy.run_path(str(Path(pipeline.__file__).with_name("fail_if_provider_failed.py")))

    def test_final_guard_rejects_expired_or_inconsistent_retained_manifest(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            data, manifest = self.run_poll(root)
            for field, invalid in (
                ("fetched_utc", snapshots.iso(NOW - timedelta(minutes=31))),
                ("source_updated_utc", snapshots.iso(NOW - timedelta(minutes=46))),
                ("total_customers", 999),
            ):
                with self.subTest(field=field):
                    changed = deepcopy(manifest)
                    changed["provider_council_counts"]["ergon"][field] = invalid
                    (data / "run_manifest.json").write_text(json.dumps(changed))
                    with chdir(root), redirect_stdout(io.StringIO()), self.assertRaises(SystemExit):
                        runpy.run_path(str(Path(pipeline.__file__).with_name("fail_if_provider_failed.py")))


if __name__ == "__main__":
    unittest.main()
