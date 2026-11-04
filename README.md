# Queensland outage data

The poll workflow fetches current outage geometries, provider council allocations,
and publishes the GeoJSON, historical CSVs and `provider_council_counts` in the
manifest for the Mapping report. `write_provider_council_counts.py` reuses the
pipeline's verified snapshot; it also supports older manifests independently.

## Provider council counts

- Energex: the official emergency text view's `v1.0.ex/council` and `summary`
  endpoints. Council counts must sum to the provider total, and the update time
  must stay consistent during collection. Requests explicitly select the public
  read-only Squiz session mode.
- Ergon: current text-view council/event rows, including `UNPLN` and `FORCD`,
  excluding `PLN`. Its `hiddenDate` supplies the publisher update time. Events
  split across councils retain the provider's individual council counts.

Both provider websites can return old CDN-cached responses despite
`Cache-Control: no-cache`. Every Energex API and Ergon text-page request includes
a distinct query value and the read-only session mode to avoid reusing those
responses. The event
allocation pipeline and standalone collector share this transport. The original
publisher timestamp still determines freshness; a new fetch does not make old
data current.

Both sources require a publisher timestamp within 45 minutes (at most five
minutes ahead). Energex retries the whole collection three times with short
backoffs when its summary and details disagree. Counts are never rescaled.

The pipeline saves each fully verified event/council snapshot in
`data/provider_council_cache.json`. If a later collection fails, it may retain
that exact snapshot for at most 30 minutes after its original collection, while
also enforcing the 45-minute publisher age limit. Repeated failures never
extend either limit, and an older publisher response cannot replace a newer
snapshot. A retained snapshot keeps its original timestamps, sets `retained:
true` in the Mapping manifest and `provider_cache` in allocation metadata, and
publishes a `provider_fetch_warnings` message. The dashboard shows the provider
update as delayed and labels the original publisher time. Health history records
`WARNING` instead of `OK`. `--strict-provider-failures` disables this recovery.

Without a valid recent backup, an invalid or incomplete response produces an
explicit provider error, never an empty successful result. The workflow commits
failure metadata before reporting the run as failed. The final check also
revalidates retained snapshot age and council totals. Collection, backups and
publishing run entirely in this repository and GitHub Actions; the site remains
on GitHub Pages.

`provider_council_counts` has `schema_version: 1`, a `snapshot_utc` matching the
manifest, and `energex` / `ergon` entries. Successful entries contain `status:
"ok"`, `scope: "all_councils"`, `outage_type: "unplanned"`, `fetched_utc`,
`source_updated_utc`, `source_url`, `total_customers` and `rows` of
`{lga_name, affected_customers}`. Failed entries have `status: "error"` and an
`error` description, without customer rows.

The dashboard pipeline also reads the providers' event/council allocations using
`provider_councils.py`. Energex suburb entries are summed within the council the
provider supplies, then checked against its council and network totals. Ergon's
per-event council rows are retained. Both use the publisher freshness checks.

Each outage remains one event in `current_outages.geojson` and the snapshot CSV.
Its `affected_customers` is the sum of its council allocations. The GeoJSON's
`lga_allocations` array is stored as `lga_allocations_json` in the CSV. Each entry
contains `lga_name`, `lga_code` when available, and `affected_customers`.
`lga_name`/`lga_code` on the event identify its primary (largest) council for
compatibility; use the allocations array or LGA totals CSV for council totals.
The dashboard uses all allocations for council counts, map shading and shares,
while statewide/provider totals and active-outage counts count the event once.
Website-only events are counted even if a map polygon has not arrived yet.
The Mapping manifest, event counts and CSV totals share this same collection;
they are not fetched again after the snapshot is written.

`lga_assignment_source` is `provider` for fresh published allocations,
`provider_cache` for retained verified allocations, and
`geometry_fallback` when a provider council source fails validation. Fallbacks
retain available map data and publish a provider error instead of silently
claiming verified counts. Essential Energy continues to use geographical lookup.

Ergon event identity uses `EVENT_ID`, not ArcGIS `OBJECTID`. Every poll repairs
retained 48-hour snapshot keys from their original `raw_json`, joining records
whose database row IDs changed. Historical customer counts and timestamps are
preserved. Old council snapshots have `lga_assignment_source: geometry_legacy`;
their original estimates cannot be reconstructed from today's allocations. The
dashboard labels that historical limitation until those rows leave the window.

Install `requirements.txt`, then run offline checks with
`python -m unittest discover -s tests -v` and `node tests/dashboard.test.cjs`.
CI also runs three live collection rounds from the hosted runner, where website
access can differ from a local environment. Each provider must have at least one
direct successful collection, and every round must either collect verified data
or recover a valid recent snapshot. Run it locally with
`python check_provider_collection.py --rounds 3`.

