"""Commit one outage snapshot and retry transient GitHub publication failures."""
import os
from pathlib import Path
import re
import subprocess
import time


RETRY_DELAYS = (5, 15, 30)
TRANSIENT_ERROR = re.compile(
    r"internal server error|bad gateway|service unavailable|gateway timeout|"
    r"(?:http[^\n]*|returned error: )[ ]*(?:429|500|502|503|504)\b|"
    r"connection (?:reset|timed out)|could not resolve host|failed to connect|"
    r"remote end hung up|unexpected disconnect|too many requests",
    re.IGNORECASE,
)


def git(*args, check=True):
    result = subprocess.run(["git", *args], capture_output=True, text=True)
    if check and result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "Git command failed")
    return result


def push_snapshot(commit, branch):
    # Every attempt pushes the same complete snapshot. Git's normal fast-forward
    # protection stays enabled; never force or regenerate data between retries.
    for attempt in range(len(RETRY_DELAYS) + 1):
        result = git("push", "origin", f"{commit}:refs/heads/{branch}", check=False)
        output = result.stdout + result.stderr
        if output.strip():
            print(output.strip(), flush=True)
        if not result.returncode:
            return
        if not TRANSIENT_ERROR.search(output):
            raise RuntimeError("Snapshot publication was rejected; automatic retries apply only to temporary server/network errors")
        if attempt == len(RETRY_DELAYS):
            raise RuntimeError("Snapshot publication failed after four attempts; the generated snapshot is preserved for recovery")
        delay = RETRY_DELAYS[attempt]
        print(f"::warning::Temporary publication failure. Retrying the same snapshot in {delay} seconds.", flush=True)
        time.sleep(delay)


def publish_data(branch):
    files = sorted(str(path) for pattern in ("*.csv", "*.geojson", "*.json")
                   for path in Path("data").glob(pattern) if path.is_file())
    if not files:
        raise RuntimeError("No generated outage data is available to publish")
    git("config", "user.name", "github-actions")
    git("config", "user.email", "github-actions@github.com")
    git("add", "--", *files)
    pending = git("diff", "--cached", "--quiet", check=False)
    if pending.returncode == 0:
        print("No data changes to commit.")
        return
    if pending.returncode != 1:
        raise RuntimeError("Unable to check generated outage data")
    # Refuse unrelated staged edits rather than accidentally publishing code.
    staged = git("diff", "--cached", "--name-only").stdout.splitlines()
    if any(path not in files for path in staged):
        raise RuntimeError("Unrelated staged files must not be included in an outage data commit")
    print(git("commit", "-m", "Update outage data").stdout.strip(), flush=True)
    commit = git("rev-parse", "HEAD").stdout.strip()
    push_snapshot(commit, branch)
    print(f"Published outage snapshot {commit}.", flush=True)


if __name__ == "__main__":
    publish_data(os.environ["GITHUB_REF_NAME"])
