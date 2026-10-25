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


## AI Vision native query maintenance

Native query acceleration is standard in matching source builds. Session pages use
bounded summary batches; Token thresholds use verified STORED generated values,
with an internal JSON_TABLE capability fallback. There are no per-strategy flags.

`SIGNOZ_AIVISION_PROJECTION_MAINTENANCE_ENABLED` defaults to `true`. Startup applies
serialized, versioned additive schema migration, including the standard Token
columns. First activation rebuilds historical derived state; ordinary restarts
preserve valid coverage/cursors. A failed migration leaves derived serving
ineligible and retries without changing ordinary query/global SQL deadlines.
Raw Spans remain authoritative and are never deleted by projection recovery.

Set the maintenance control to `false` and restart writers to durably suspend
maintenance fleet-wide before untracked raw writes. Default-on replicas cannot
clear this latch. After repair, set all writers back to `true`, then explicitly
resume using the AI Vision operator command. The admin-only resume endpoint also
requires the existing deployment-internal ingest key. Rebuilding removes orphan
projections and increments the shared generation. Direct SQL import, retention
or restore requires this suspend/rebuild workflow; external mutation detection
is not claimed.

AI Vision has one reader control, `AIVISION_QUERY_MODE=auto|raw`, default `auto`.
It covers Trace lists, canonical Session lists and facets together. The standard
backend entrypoint supervises automatic projection discovery and maintenance.
A complete hybrid/raw snapshot supports 50,000 Traces, 500,000 Spans and 256 MiB;
resource excess is an explicit error, never partial success. See the product
operator reference and same-data validation report for recovery instructions,
semantics and prepared-environment testing boundaries.

The SQL files in `oceanbase/session-token-generated-v1*.sql` are manual repair
artifacts for older experimental schemas. They are not required startup steps
or a functional query rollback. Do not drop standard columns to recover queries.
