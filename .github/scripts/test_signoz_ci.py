"""Controlled workflow-condition and source-image receipt contract checks.

Run with: python3 -m unittest discover -s .github/scripts -p 'test_*.py' -v
These checks do not run Go, Docker, or GitHub Actions.
"""

import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[2]
GOCI = ROOT / ".github/workflows/goci.yaml"
PROVENANCE = Path(__file__).with_name("signoz_ob_provenance.py")
JOBS = {
    "test", "fmt", "lint", "deps", "semconv-generated", "build",
    "openapi", "authz", "web-settings", "transaction-groups",
}


def conditions():
    """Read the actual job expressions, without a YAML dependency."""
    text = GOCI.read_text()
    return dict(re.findall(r"^  ([a-z-]+):\n    if: \|\n((?:      .+\n)+)", text, re.M))


def allows(expression, event, *, same_repository=False, fork=True, labels=(),
           actor="contributor", action="labeled", label="safe-to-test", repository="ob-labs/signoz"):
    """Evaluate the small expression subset used by goci against PR fixtures."""
    head_repo = SimpleNamespace(full_name=repository if same_repository else "contributor/signoz", fork=fork)
    github = SimpleNamespace(
        event_name=event, repository=repository,
        event=SimpleNamespace(
            action=action, label=SimpleNamespace(name=label),
            pull_request=SimpleNamespace(
                head=SimpleNamespace(repo=head_repo),
                user=SimpleNamespace(login=actor),
                labels=[SimpleNamespace(name=name) for name in labels],
            ),
        ),
    )
    expression = expression.replace("github.event.pull_request.labels.*.name", "label_names")
    expression = expression.replace("&&", " and ").replace("||", " or ")
    expression = re.sub(r"!(?!=)", " not ", expression)
    expression = " ".join(expression.split())
    return bool(eval(expression, {"__builtins__": {}}, {
        "github": github, "label_names": list(labels),
        "contains": lambda values, value: value in values,
    }))


class GoCIConditionsTest(unittest.TestCase):
    def test_all_go_jobs_share_the_condition(self):
        expressions = conditions()
        self.assertEqual(JOBS, set(expressions))
        self.assertEqual(1, len(set(expressions.values())))

    def test_internal_pr_in_downstream_fork_runs(self):
        for job, expression in conditions().items():
            with self.subTest(job=job):
                self.assertTrue(allows(expression, "pull_request", same_repository=True, fork=True))

    def test_internal_pr_in_nonfork_runs(self):
        for expression in conditions().values():
            self.assertTrue(allows(expression, "pull_request", same_repository=True, fork=False))

    def test_internal_pr_keeps_running_after_safe_to_test_label(self):
        for expression in conditions().values():
            self.assertTrue(allows(expression, "pull_request", same_repository=True, labels=("safe-to-test",)))

    def test_external_pr_requires_reviewed_label_event(self):
        for job, expression in conditions().items():
            for event, labels, expected in (
                ("pull_request", (), False),
                ("pull_request", ("safe-to-test",), True),
                ("pull_request_target", (), False),
                ("pull_request_target", ("safe-to-test",), False),
            ):
                with self.subTest(job=job, event=event, labels=labels):
                    self.assertEqual(expected, allows(expression, event, labels=labels))
            self.assertFalse(allows(expression, "pull_request", labels=("safe-to-test",), label="documentation"))
            self.assertFalse(allows(expression, "pull_request", labels=("safe-to-test",), action="synchronize"))

    def test_supported_events_and_bot_policy(self):
        for expression in conditions().values():
            self.assertTrue(allows(expression, "merge_group"))
            self.assertFalse(allows(expression, "push", same_repository=True))
            self.assertFalse(allows(expression, "workflow_dispatch", same_repository=True))
            self.assertFalse(allows(expression, "pull_request", same_repository=True, actor="dependabot[bot]"))
            self.assertFalse(allows(expression, "pull_request_target", same_repository=True, labels=("safe-to-test",)))

    def test_downstream_label_cannot_start_inherited_credential_jobs(self):
        guarded = []
        for filename in ("jsci.yaml", "postci.yaml"):
            text = (ROOT / ".github/workflows" / filename).read_text()
            blocks = re.findall(r"^  ([a-z-]+):\n(.*?)(?=^  [a-z-]+:\n|\Z)", text, re.M | re.S)
            for job, block in blocks:
                if "secrets: inherit" not in block:
                    continue
                guarded.append((filename, job))
                expression = re.search(r"^    if: (?:\|\n((?:      .+\n)+)|([^\n]+))", block, re.M)
                self.assertIsNotNone(expression, (filename, job))
                value = expression.group(1) or expression.group(2)
                with self.subTest(filename=filename, job=job):
                    self.assertFalse(allows(value, "pull_request_target", labels=("safe-to-test",)))
                    self.assertFalse(allows(value, "pull_request", same_repository=True, fork=False))
                    self.assertTrue(allows(value, "pull_request_target", labels=("safe-to-test",), repository="SigNoz/signoz"))
        self.assertEqual(5, len(guarded))

    def test_pr_code_never_receives_privileged_credentials_or_shared_cache(self):
        text = GOCI.read_text()
        self.assertIn("permissions:\n  contents: read\n", text)
        self.assertNotIn("secrets:", text)
        self.assertNotIn("secrets.", text)
        self.assertNotIn("id-token:", text)
        self.assertNotIn("signoz/primus", text)
        self.assertNotIn("actions/cache", text)
        self.assertNotIn("  pull_request_target:", text)
        self.assertEqual(10, text.count("persist-credentials: false"))
        self.assertEqual(10, text.count("ref: ${{ github.event.pull_request.head.sha || github.sha }}"))
        self.assertEqual(10, len(re.findall(r"^          cache: false$", text, re.M)))


class ImageReceiptTest(unittest.TestCase):
    def module(self):
        self.assertTrue(PROVENANCE.is_file(), "source-owned image needs a typed receipt writer")
        spec = importlib.util.spec_from_file_location("signoz_ob_provenance", PROVENANCE)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def environment(self):
        return {
            "GITHUB_SERVER_URL": "https://github.com", "GITHUB_REPOSITORY": "ob-labs/signoz",
            "GITHUB_SHA": "a" * 40, "GITHUB_REF": "refs/heads/main", "GITHUB_REF_NAME": "main",
            "GITHUB_EVENT_NAME": "workflow_dispatch", "GITHUB_RUN_ID": "123", "GITHUB_RUN_ATTEMPT": "2",
            "GITHUB_WORKFLOW_REF": "ob-labs/signoz/.github/workflows/release-ob-image.yaml@refs/heads/main",
            "GITHUB_WORKFLOW_SHA": "b" * 40, "SOURCE_SHA": "a" * 40,
            "BUILD_VERSION": "sha-" + "a" * 40, "BUILD_TIMESTAMP": "2026-11-03T07:00:00Z",
            "IMAGE_NAME": "ghcr.io/ob-labs/signoz-ob", "IMAGE_DIGEST": "sha256:" + "c" * 64,
            "IMAGE_TAGS": "ghcr.io/ob-labs/signoz-ob:dev\nghcr.io/ob-labs/signoz-ob:sha-aaaaaaa",
            "IMAGE_PLATFORMS": "linux/amd64,linux/arm64",
        }

    def image_index(self):
        return {"manifests": [
            {"platform": {"os": "linux", "architecture": "amd64"}},
            {"platform": {"os": "linux", "architecture": "arm64"}},
            {"platform": {"os": "unknown", "architecture": "unknown"},
             "annotations": {"vnd.docker.reference.type": "attestation-manifest"}},
        ]}

    def test_platforms_must_match_the_pushed_image_index(self):
        module = self.module()
        for index in ({}, {"manifests": []}, {"manifests": self.image_index()["manifests"][:1]}):
            with self.subTest(index=index):
                with self.assertRaises(ValueError):
                    module.build_receipt(self.environment(), index)

    def test_receipt_binds_exact_source_build_and_image(self):
        receipt = self.module().build_receipt(self.environment(), self.image_index())
        self.assertEqual(1, receipt["schema_version"])
        self.assertEqual("signoz-ob-image-build", receipt["artifact_type"])
        self.assertEqual("a" * 40, receipt["source"]["sha"])
        self.assertEqual("refs/heads/main", receipt["source"]["ref"])
        self.assertEqual(123, receipt["build"]["run_id"])
        self.assertEqual(2, receipt["build"]["run_attempt"])
        self.assertEqual("https://github.com/ob-labs/signoz/actions/runs/123/attempts/2", receipt["build"]["run_url"])
        self.assertEqual("b" * 40, receipt["build"]["workflow_sha"])
        self.assertEqual("community", receipt["build"]["variant"])
        self.assertEqual("2026-11-03T07:00:00Z", receipt["build"]["timestamp"])
        self.assertEqual(["linux/amd64", "linux/arm64"], receipt["image"]["platforms"])
        self.assertEqual("ghcr.io/ob-labs/signoz-ob@sha256:" + "c" * 64, receipt["image"]["reference"])
        self.assertEqual(2, len(receipt["image"]["tags"]))
        json.dumps(receipt)

    def test_cli_writes_the_validated_receipt_from_workflow_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "index.json"
            output = Path(directory) / "receipt.json"
            manifest.write_text(json.dumps(self.image_index()))
            result = subprocess.run(
                [sys.executable, str(PROVENANCE), "--manifest", str(manifest), "--output", str(output)],
                env=self.environment(), capture_output=True, text=True,
            )
            self.assertEqual(0, result.returncode, result.stderr)
            self.assertEqual("a" * 40, json.loads(output.read_text())["source"]["sha"])

    def test_invalid_or_incomplete_build_evidence_is_rejected(self):
        module = self.module()
        for key, value in (
            ("IMAGE_DIGEST", ""), ("IMAGE_DIGEST", "latest"), ("IMAGE_DIGEST", "sha256:" + "0" * 64),
            ("SOURCE_SHA", "d" * 40), ("GITHUB_WORKFLOW_SHA", "0" * 40),
            ("GITHUB_RUN_ID", "0"), ("GITHUB_RUN_ATTEMPT", "unknown"),
            ("GITHUB_WORKFLOW_SHA", "main"), ("BUILD_TIMESTAMP", "today"),
            ("IMAGE_PLATFORMS", "linux/amd64"), ("IMAGE_TAGS", "another/image:dev"),
        ):
            with self.subTest(key=key, value=value):
                env = self.environment()
                env[key] = value
                with self.assertRaises(ValueError):
                    module.build_receipt(env, self.image_index())

    def test_workflow_links_provenance_to_the_actual_build(self):
        workflow = (ROOT / ".github/workflows/release-ob-image.yaml").read_text()
        dockerfile = (ROOT / "cmd/community/Dockerfile.ob").read_text()
        for value in ("SOURCE_SHA", "BUILD_VERSION", "BUILD_TIMESTAMP"):
            self.assertIn(value, workflow)
        for field in ("version", "variant", "hash", "time", "branch"):
            self.assertIn("-X github.com/SigNoz/signoz/pkg/version." + field + "=", dockerfile)
        self.assertIn("IMAGE_DIGEST: ${{ steps.build.outputs.digest }}", workflow)
        self.assertIn("org.opencontainers.image.revision=${{ steps.source.outputs.sha }}", workflow)
        self.assertIn("org.opencontainers.image.source=${{ github.server_url }}/${{ github.repository }}", workflow)
        self.assertIn("signoz-ob-build-receipt.json", workflow)
        self.assertIn("ref: ${{ github.sha }}", workflow)


if __name__ == "__main__":
    unittest.main()
