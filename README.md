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

Ergon's website can return an old CDN-cached page despite `Cache-Control:
no-cache`. Each public text-page request includes a distinct query value and
the read-only session mode to avoid reusing that cached response. The event
allocation pipeline and standalone collector share this transport. The original
publisher timestamp still determines freshness; a new fetch does not make old
data current.

Both sources require a publisher timestamp within 45 minutes (at most five
minutes ahead). Invalid or incomplete responses produce an explicit error for
that provider, never an empty successful result. No previous council counts are
carried into a new run. The workflow commits failure metadata before reporting
the run as failed, matching existing provider-failure handling.

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

`lga_assignment_source` is `provider` for published allocations and
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
CI also checks live retrieval from the hosted runner, where website access can
differ from a local environment.

