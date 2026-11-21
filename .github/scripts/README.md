# Downstream Go CI and source image receipts

## PR validation boundary

`goci.yaml` identifies an internal PR by `head.repo.full_name == github.repository`.
The fact that `ob-labs/signoz` is itself a fork does not make its own branches
external. Internal non-Dependabot PRs run on open, reopen, update, and label
events. Merge queue checks remain enabled.

External PRs run the broad Go jobs only when a maintainer applies `safe-to-test`
to the reviewed head. This uses the ordinary `pull_request: labeled` event;
there is no `pull_request_target` execution path for Go jobs. Neither a
pre-existing `safe-to-test` label on a synchronize event nor applying a
different label authorizes another run. Follow the runbook below.
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
`aivision-query-ci.yaml` are the maintained source checks. The four inherited
Primus frontend calls have been removed from `jsci.yaml`: GitHub validates their
nested OIDC permission demand before evaluating a false job condition, so an
upstream-only `if` cannot make those calls usable here. No OIDC grant is added.
The retained local frontend suites are explicitly upstream-only and do not
establish downstream frontend validation.

`postci.yaml` uses a repository-scoped `pull-requests: write` token only to clear
an external/bot PR's review label after its head changes. It reads trusted event
metadata without checkout or execution of PR-controlled code, inherited app
credentials, or OIDC. PR-code jobs remain read-only.

### Maintainer runbook: review and label cleanup

On December 2, 2026, GitHub will enforce its default block on
`pull_request_target` in public repositories without an applicable event
policy. If the event is blocked, `postci.yaml` does not clear stale labels.
The label is not approval for the current commit unless it was freshly
applied to that reviewed head. The Go workflow's fresh-label requirement is
independent of cleanup and remains in effect.

**Keep automatic cleanup.** A repository administrator can configure an
Actions workflow execution policy scoped to `ob-labs/signoz` and the exact
workflow path `.github/workflows/postci.yaml`. Allow `pull_request_target`
only for this workflow, subject to existing organization and enterprise
restrictions. The workflow itself limits dispatch to `synchronize` on PRs
targeting `main`. It must stay metadata-only: do not give it checkout, caches,
inherited credentials, or any permission beyond the pull-request write
token used to remove the label.

Use the [GitHub Actions policy guide](https://docs.github.com/en/actions/how-tos/administer/control-workflow-execution)
and check Policy insights and the effective inherited rules before and after
enabling it. Check that a controlled external PR update can run this workflow
and that its stale label is removed, without adding permissions or permitting
`pull_request_target` for any other workflow. A scoped custom policy does not
by itself prove other paths stay blocked: verify effective behavior on
non-`postci.yaml` paths; if necessary, explicitly block the event elsewhere
while retaining permitted events. Do not apply a repository-wide allow
exception. A documentation or source merge does not configure this setting.

**If cleanup is blocked or unavailable.** Check the PR's full current head SHA
and compare it with the commit you reviewed. If they differ, review the new
head first. Remove any old `safe-to-test` label, confirm the reviewed SHA is
still the current head, then apply `safe-to-test` again to emit a fresh event.
The label operation does not atomically bind a SHA. Verify the resulting Go
run checks out the reviewed SHA before approving any queued fork run or using
its result; if the head moved during labeling, cancel the mismatched run,
clear the label, and repeat review. Do not trust an unchanged label or
reuse results from a previous head.

Other inherited frontend, commit, integration, and e2e suites are retained.
Their `pull_request_target` local steps check out the base by default, so their
results do not demonstrate validation of the current PR head. They are not
acceptance evidence for the headless image, OceanBase runtime, or AI Vision
product combination. This change does not migrate those suites or upstream
release workflows to downstream ownership.

## Historical lint baseline

The pinned linter scans the whole repository with the checked-in configuration
and uncapped findings. The accepted bootstrap policy is **no new lint debt**,
not zero historical findings. `.github/lint-baseline.json` records the approved
existing findings from a specific Linux CI source/run/attempt, tool version and
configuration hash. It is not a directory exclusion or changed-lines filter.

CI rejects new or changed findings, additional occurrences of an existing
finding, invalid reports, type-check/infrastructure errors, and tool/configuration
drift. Source anchors are hashed; line relocation alone does not introduce a new
finding. Resolved/stale entries are reported. A missing or pending baseline fails
the gate, and CI never records or refreshes the baseline automatically.

After verifying an actual CI report and obtaining maintainer authorization,
record the frozen report locally with its exact source SHA and workflow identity:

```sh
python3 .github/scripts/lint_baseline.py record \
  --report /absolute/path/to/lint-static-findings.json \
  --baseline .github/lint-baseline.json \
  --source-sha ACTUAL_SOURCE_SHA --run-id ACTUAL_RUN_ID \
  --run-attempt ACTUAL_ATTEMPT --acknowledge-existing-findings
```

Review and commit the resulting baseline as evidence of accepted historical debt.
Do not refresh it merely to make a failing PR green. The command is forbidden in
GitHub Actions. The `lint-static-findings-<run_id>-<attempt>` artifact contains only
normalized static findings and a typed gate summary, without raw source lines,
compiler/runtime logs or environment dumps. Generated OpenAPI drift separately
retains the actual spec only after its generator succeeds.

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
