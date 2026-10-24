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
