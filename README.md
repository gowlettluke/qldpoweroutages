# Queensland outage data

The poll workflow fetches current outage geometries and publishes the existing
GeoJSON and historical CSVs. It also runs `write_provider_council_counts.py` to
add `provider_council_counts` to `data/run_manifest.json` for the Mapping report.

## Provider council counts

- Energex: the official emergency text view's `v1.0.ex/council` and `summary`
  endpoints. Council counts must sum to the provider total, and the update time
  must stay consistent during collection. Requests explicitly select the public
  read-only Squiz session mode.
- Ergon: current text-view council/event rows, including `UNPLN` and `FORCD`,
  excluding `PLN`. Its `hiddenDate` supplies the publisher update time. Events
  split across councils retain the provider's individual council counts.

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

These are provider-published counts. Existing centroid-derived `lga_name`
properties and historical LGA CSVs are separate geographical estimates and
must not be presented as provider council allocations.

Run offline checks with `python -m unittest discover -s tests -v`.
CI also checks live retrieval from the hosted runner, where website access can
differ from a local environment.
