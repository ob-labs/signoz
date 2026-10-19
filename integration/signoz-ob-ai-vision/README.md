# AI Vision integration tests

Run commands from the repository root against a test environment.

## Minimal AI Vision image

```bash
make docker-build-ob DOCKER_IMAGE_OB=local/signoz-ob:slim
# Cross-compile and load a single arm64 image without QEMU:
make docker-build-ob DOCKER_PLATFORM_OB=linux/arm64 DOCKER_IMAGE_OB=local/signoz-ob:slim-arm64
```

The release workflow `.github/workflows/release-ob-image.yaml` and this target
both use `cmd/community/Dockerfile.ob`. The `docker-build-community` targets
still build the full SigNoz frontend distribution.

AI Vision uses port 8080 for OTLP traces/logs/metrics ingestion, query APIs,
registration and service-account authentication. It selects the OceanBase
telemetry store, disables the SigNoz web UI, and persists the SQLite metastore
in `/var/lib/signoz`. These contracts are defined in the consuming repository's
`aivision-cli/blueprints/aivision.yaml` and
`ai-vision/aivision-py-be/apps/core/services/signoz_integration.py`.

The final image contains the static community server, CA certificates,
notification/email templates and the license. Time-zone data is compiled into
the binary. Its `scratch` filesystem includes `/tmp` and `/var/lib/signoz`;
`/root/templates` points to `/app/templates` for the server's default template
configuration. There is no shell, package manager, frontend, source tree or
build cache in the runtime image. Health checks use HTTP `/api/v1/health` from
the host or orchestrator, as in AI Vision's deployment configuration.

The Dockerfile-specific `.dockerignore` allows only Go build inputs, embedded
assets, templates and the license into the build context. This also prevents
frontend or local-artifact changes from invalidating the backend build cache.
The community backend and its APIs remain intact: ClickHouse-related Go types
are shared with the OceanBase implementation, so this change does not claim
to remove every unused feature from the binary.

Compare unpacked local image sizes with:

```bash
docker image inspect --format '{{.RepoTags}} {{.Size}} bytes' \
  ghcr.io/ob-labs/signoz-ob:dev local/signoz-ob:slim
```

## OceanBase query tests

Requires Node.js, Go, and a running OceanBase instance.

```bash
GOFLAGS=-mod=readonly node integration/signoz-ob-ai-vision/test-main.mjs
```

Set `SIGNOZ_TEST_OCEANBASE_DSN` to the test database DSN. If unset, the script
uses Docker to read the DSN from `SIGNOZ_E2E_CONTAINER` (default: `signoz-ob-e2e`)
and replaces its address with `SIGNOZ_TEST_OCEANBASE_ADDRESS`
(default: `127.0.0.1:2881`). `GO_BINARY` optionally selects the Go executable.

Tests create and drop their own `sn_test_*` temporary tables.

## HTTP smoke tests

The Python standard-library script writes synthetic traces, logs, and metrics
through a test gateway and verifies SigNoz queries. Set `SIGNOZ_API_KEY`,
`SIGNOZ_ORG_ID`, `AIVISION_SPACE_ID`, and `AIVISION_USER_ID` for the same test
identity. Set `AIVISION_INGEST_AUTHORIZATION` to the gateway's complete
Authorization header value, including the `Bearer ` prefix when required.
Adjust the URLs for your environment.

```bash
python3 integration/signoz-ob-ai-vision/smoke.py \
  --collector-url http://127.0.0.1:4318 \
  --query-url http://127.0.0.1:18091 \
  --query-api-key-env SIGNOZ_API_KEY \
  --collector-header-env Authorization=AIVISION_INGEST_AUTHORIZATION \
  --org-id "$SIGNOZ_ORG_ID" \
  --space-id "$AIVISION_SPACE_ID" \
  --user-id "$AIVISION_USER_ID"
```

To also verify the AI Vision backend, set `AI_VISION_JWT` to the user's login
token and append these arguments. `AIVISION_USER_ID` must match that user's
`work_id`:

```bash
  --ai-vision-url http://127.0.0.1:8001 \
  --ai-vision-space-id "$AIVISION_SPACE_ID" \
  --ai-vision-token-env AI_VISION_JWT
```

The gateway token must match the organization, space, and user used for queries.
