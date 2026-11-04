from contextlib import chdir, redirect_stdout
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import call, patch

import publish_outage_data as publisher


class SnapshotPublicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.remote = self.root / "remote.git"
        self.work = self.root / "work"
        self.work.mkdir()
        self.run_git(self.root, "init", "--bare", str(self.remote))
        self.run_git(self.work, "init", "--initial-branch=main")
        self.run_git(self.work, "config", "user.name", "Snapshot test")
        self.run_git(self.work, "config", "user.email", "snapshot@example.invalid")
        self.run_git(self.work, "remote", "add", "origin", str(self.remote))
        (self.work / "data").mkdir()
        (self.work / "README.md").write_text("Source documentation\n")
        self.write_snapshot(5)
        self.run_git(self.work, "add", ".")
        self.run_git(self.work, "commit", "-m", "Initial snapshot")
        self.run_git(self.work, "push", "origin", "main")

    @staticmethod
    def run_git(cwd, *args):
        return subprocess.run(["git", *args], cwd=cwd, text=True,
                              capture_output=True, check=True).stdout.strip()

    def write_snapshot(self, total):
        (self.work / "data/run_manifest.json").write_text(json.dumps({"total": total}))
        (self.work / "data/lga_totals_latest.csv").write_text(f"lga_name,total\nGympie,{total}\n")

    def reject_pushes(self, failures, message="Internal Server Error"):
        # A real Git remote rejects receives with the error seen on GitHub.
        # Its counter survives individual pushes, exercising the full publisher.
        script = f'''#!/usr/bin/env python3
from pathlib import Path
import sys
path = Path({str(self.remote / "attempts")!r})
attempt = int(path.read_text()) + 1 if path.exists() else 1
path.write_text(str(attempt))
if attempt <= {failures}:
    print({message!r}, file=sys.stderr)
    sys.exit(1)
'''
        hook = self.remote / "hooks/pre-receive"
        hook.write_text(script)
        hook.chmod(0o755)

    def publish(self):
        with chdir(self.work), redirect_stdout(io.StringIO()):
            publisher.publish_data("main")

    def test_github_server_errors_retry_the_same_complete_snapshot(self):
        self.write_snapshot(17)
        (self.work / "README.md").write_text("Unpublished source edit\n")
        self.reject_pushes(2)
        with patch.object(publisher.time, "sleep") as sleep:
            self.publish()
        self.assertEqual((self.remote / "attempts").read_text(), "3")
        self.assertEqual(sleep.call_args_list, [call(5), call(15)])
        remote_head = self.run_git(self.root, "--git-dir", str(self.remote), "rev-parse", "main")
        self.assertEqual(remote_head, self.run_git(self.work, "rev-parse", "HEAD"))
        self.assertEqual(json.loads(self.run_git(self.root, "--git-dir", str(self.remote), "show", "main:data/run_manifest.json")), {"total": 17})
        self.assertEqual(self.run_git(self.root, "--git-dir", str(self.remote), "show", "main:data/lga_totals_latest.csv"), "lga_name,total\nGympie,17")
        self.assertEqual(self.run_git(self.root, "--git-dir", str(self.remote), "show", "main:README.md"), "Source documentation")
        self.assertEqual(self.run_git(self.work, "rev-list", "--count", "HEAD"), "2")

    def test_no_change_does_not_create_or_push_another_commit(self):
        self.reject_pushes(99)
        with patch.object(publisher.time, "sleep") as sleep:
            self.publish()
        self.assertFalse((self.remote / "attempts").exists())
        self.assertEqual(self.run_git(self.work, "rev-list", "--count", "HEAD"), "1")
        sleep.assert_not_called()

    def test_retry_budget_expires_without_losing_the_generated_snapshot(self):
        self.write_snapshot(17)
        self.reject_pushes(99)
        with patch.object(publisher.time, "sleep") as sleep, self.assertRaisesRegex(RuntimeError, "four attempts"):
            self.publish()
        self.assertEqual((self.remote / "attempts").read_text(), "4")
        self.assertEqual(sleep.call_args_list, [call(5), call(15), call(30)])
        self.assertEqual(json.loads((self.work / "data/run_manifest.json").read_text()), {"total": 17})
        self.assertEqual(json.loads(self.run_git(self.root, "--git-dir", str(self.remote), "show", "main:data/run_manifest.json")), {"total": 5})

    def test_permanent_rejection_is_not_retried_or_reported_as_success(self):
        self.write_snapshot(17)
        self.reject_pushes(99, "Permission denied")
        with patch.object(publisher.time, "sleep") as sleep, self.assertRaisesRegex(RuntimeError, "rejected"):
            self.publish()
        self.assertEqual((self.remote / "attempts").read_text(), "1")
        sleep.assert_not_called()

    def test_a_concurrent_repository_edit_cannot_be_overwritten(self):
        other = self.root / "other"
        self.run_git(self.root, "clone", "--branch", "main", str(self.remote), str(other))
        self.run_git(other, "config", "user.name", "Other editor")
        self.run_git(other, "config", "user.email", "editor@example.invalid")
        (other / "README.md").write_text("A concurrent repository edit\n")
        self.run_git(other, "add", "README.md")
        self.run_git(other, "commit", "-m", "Concurrent edit")
        self.run_git(other, "push", "origin", "main")
        self.write_snapshot(17)
        with patch.object(publisher.time, "sleep") as sleep, self.assertRaisesRegex(RuntimeError, "rejected"):
            self.publish()
        sleep.assert_not_called()
        self.assertEqual(self.run_git(self.root, "--git-dir", str(self.remote), "show", "main:README.md"), "A concurrent repository edit")
        self.assertEqual(json.loads((self.work / "data/run_manifest.json").read_text()), {"total": 17})


if __name__ == "__main__":
    unittest.main()
