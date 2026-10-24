# Deploy

Check that you have cloned [signoz/signoz](https://github.com/signoz/signoz)
and currently are in `signoz/deploy` folder.

## Installation

> **Note:** The `install.sh` script and the `docker-compose` manifests have been deprecated.

SigNoz now installs and runs through [Foundry](https://signoz.io/docs/install/docker/).

> **Already running SigNoz via Docker Compose?** See the [Migration Guide](./MIGRATION.md) to transition your existing deployment to Foundry.

Please follow the latest installation instructions at [signoz.io/docs/install/docker](https://signoz.io/docs/install/docker/).
Foundry has support for **different platforms and architectures**, please review the project documentation for more details.

## Uninstall/Troubleshoot?

Go to our official documentation site [signoz.io/docs](https://signoz.io/docs) for more.


## AI Vision Session Token query switch

`SIGNOZ_AIVISION_SESSION_TOKEN_QUERY_OPTIMIZATION_ENABLED=true` enables typed
`JSON_TABLE` extraction for canonical Session requests with a Token threshold,
including zero. The default is off; only the literal `true` enables it. This
switch is independent of the Trace read-model and tracking switches and does
not enable the Session API by itself. Requests without a Token threshold keep
the original query. Supported responses report the actual path in
`meta.sessionTokenQueryStrategy` (`original` or `json_table`).

Set the switch to `false` (or unset it) and restart the query service to restore
the original Token query. No schema migration or backfill is required by this
switch; the existing read-model coverage requirements still apply.

## Optional Session page and stored Token acceleration

Two additional independent switches default off and accept only the literal
`true`. They require the same canonical read-model coverage and authoritative
ingestion as the existing Session endpoint:

- `SIGNOZ_AIVISION_SESSION_PAGE_BATCH_ENABLED=true` fetches the selected page's
  summaries in two queries instead of two queries per Session. It retains the
  same snapshot, complete turns, match IDs, counts, facets and page order.
  `meta.sessionPageSummaryStrategy` reports `individual` or `batch`.
- `SIGNOZ_AIVISION_SESSION_TOKEN_GENERATED_COLUMNS_ENABLED=true` checks for the
  optional versioned STORED generated columns and uses them for Token thresholds,
  including zero. `meta.sessionTokenQueryStrategy` reports `generated_columns`
  only when the complete stored expressions and types match. Missing or
  mismatched columns retain `json_table` or `original`, according to the first
  Token switch; `meta.sessionTokenGeneratedColumns` explains the capability
  result. Inspection/permission/database errors remain errors.

No request, startup hook or publisher executes a schema migration. Validate
[the forward SQL](oceanbase/session-token-generated-v1.sql) on a representative
clone first, keep both new switches off, and schedule an explicit maintenance
window before applying it to a populated projection table. The SQL uses the
default `signoz` prefix; review the table name for your installation. Measure
DDL time, write behavior and storage headroom separately from read latency.

The two generated values depend directly on `row_json`, so older publishers
continue to update them automatically. Existing revision/context/semantics
coverage checks still run in the same read snapshot. The expressions preserve
the original SQL CASE, unknown usage and numeric conversion behavior; they do
not repair the existing JSON/DECIMAL behavior for extremely large numbers.
Zero is a real threshold and excludes sessions without complete known usage.

Roll out page batching and stored Token reads separately: pilot, 1k, 10k, long
sessions, large IO and representative framework data. Compare complete result
oracles, stable order, totals, facets and match IDs before measuring paired
latencies. The batch path preflights the whole page before fetching summaries;
it may return the existing payload-limit fallback earlier near the 16 MiB
boundary. The 50-session, 5,000-Trace and final encoded-size limits remain.

To roll back queries, unset either new switch (or set it to `false`) and restart
query workers; verify the reported strategies. This retains the stored columns
and does not reverse their write/storage effect. To reverse that storage change,
first disable stored Token reads on every worker, verify the JSON strategy,
then schedule [the separate rollback SQL](oceanbase/session-token-generated-v1-rollback.sql).
It drops only the two optional derived columns and retains `row_json`, source
tracking and raw spans. Keep the existing Python `legacy`/`shadow`/`prefer`
controls available for a broader functional rollback.
