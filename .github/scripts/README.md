# Downstream Go CI and source image receipts

## PR validation boundary

`goci.yaml` identifies an internal PR by `head.repo.full_name == github.repository`.
The fact that `ob-labs/signoz` is itself a fork does not make its own branches
external. Internal non-Dependabot PRs run on open, reopen, update, and label
events. Merge queue checks remain enabled.

External PRs run the broad Go jobs only when a maintainer applies `safe-to-test`
to the reviewed head. This uses the ordinary `pull_request: labeled` event;
there is no `pull_request_target` execution path. After an external PR head
changes, remove and reapply the label to approve the new head. Other label events
and a pre-existing label on a synchronize event do not authorize another run.
The `pull_request` branch filter applies to `main` for both internal and external
PRs.

Jobs check out the immutable head SHA, grant only `contents: read`, do not
persist checkout credentials, and do not inherit repository secrets or use the
upstream Primus app. Go and linter shared caches are disabled. Test, formatting,
lint, and dependency checks run directly from this public source tree. The Go
version comes from `go.mod`; lint uses the version pinned in the workflow.
These changes do not establish that the full Go checks or image build pass.

Run the focused controlled checks without Go or Docker:

```sh
python3 -m unittest discover -s .github/scripts -p 'test_*.py' -v
```

The tests evaluate the actual repeated job expressions against controlled
internal-fork, external-fork, label, bot, merge-queue, and unsupported-event
fixtures. They also check the no-secret checkout contract and image receipt
validation. They are not a local GitHub Actions executor.

For this downstream headless fork, `goci.yaml` and the path-filtered
`aivision-query-ci.yaml` are the maintained source checks. Primus-backed
frontend jobs in `jsci.yaml` and the Primus label-removal job in `postci.yaml`
are limited to `SigNoz/signoz`; applying a downstream review label cannot start
those credential-bearing jobs. Their upstream behavior is preserved.

Other inherited frontend, commit, integration, and e2e suites are retained.
Their `pull_request_target` local steps check out the base by default, so their
results do not demonstrate validation of the current PR head. They are not
acceptance evidence for the headless image, OceanBase runtime, or AI Vision
product combination. This change does not migrate those suites or upstream
release workflows to downstream ownership.

## Source-owned signoz-ob image

`release-ob-image.yaml` is the owner of `ghcr.io/<owner>/signoz-ob`, including its
release, `latest`, and manually selected aliases such as `dev` and `daily`.
Product publishers consume its digest; they do not write aliases into this
package. Stable published releases continue to advance `latest`; prereleases
do not. The existing `linux/amd64` and `linux/arm64` image platforms remain.

The workflow checks out the event's exact commit and verifies `git rev-parse
HEAD == GITHUB_SHA`. A release uses its source tag as the binary version; a
manual build uses `sha-<full source SHA>`. The build timestamp is the UTC time
recorded before the image build. These values populate the existing
`pkg/version` linker fields and the OCI source/revision/version/created labels.
Plain local Docker builds remain supported with `dev`/`unknown`/`local` defaults;
those defaults do not represent a workflow provenance record.

After a successful push, the workflow inspects the exact returned digest and
writes `signoz-ob-build-receipt.json`. It refuses an absent, malformed, or zero
digest, a mismatched source SHA, missing run metadata, or an image index without
exactly the two requested runnable platforms. BuildKit attestation manifests
are excluded from the runnable platform list. The receipt is uploaded as the
Actions artifact
`signoz-ob-build-receipt-<run_id>-<run_attempt>`.

The version 1 receipt has these typed fields:

| Object | Fields |
| --- | --- |
| Root | `schema_version` (integer), `artifact_type` (`signoz-ob-image-build`) |
| `source` | `repository`, full `sha`, source `ref` |
| `build` | binary `version`, `variant` (`community`), UTC `timestamp`, `workflow_ref`, full `workflow_sha`, integer `run_id`/`run_attempt`, attempt-specific `run_url` |
| `image` | package `name`, manifest `digest`, digest-pinned `reference`, published `tags` list, observed runnable `platforms` list |

This is a build record from the workflow, not a signed attestation, vulnerability
scan, deployment result, or product-combination acceptance. No receipt for an
actual image exists until that workflow build and upload succeed. A downstream
product release must bind the selected SigNoz digest and receipt separately
from its own source and four owned images.
