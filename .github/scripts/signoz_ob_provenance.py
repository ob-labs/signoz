"""Write a source-owned image build receipt from successful workflow outputs.

This is a build record, not an attestation or product-combination acceptance.
"""

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import re
from typing import Mapping, TypedDict


class Source(TypedDict):
    repository: str
    sha: str
    ref: str


class Build(TypedDict):
    version: str
    variant: str
    timestamp: str
    workflow_ref: str
    workflow_sha: str
    run_id: int
    run_attempt: int
    run_url: str


class Image(TypedDict):
    name: str
    digest: str
    reference: str
    tags: list[str]
    platforms: list[str]


class Receipt(TypedDict):
    schema_version: int
    artifact_type: str
    source: Source
    build: Build
    image: Image


def build_receipt(env: Mapping[str, str], image_index: dict) -> Receipt:
    def required(name: str) -> str:
        value = env.get(name, "").strip()
        if not value:
            raise ValueError(f"missing {name}")
        return value

    def sha(name: str) -> str:
        value = required(name)
        if not re.fullmatch(r"[0-9a-f]{40}", value) or value == "0" * 40:
            raise ValueError(f"invalid {name}: expected full source SHA")
        return value

    def positive_int(name: str) -> int:
        value = required(name)
        if not value.isdecimal() or int(value) < 1:
            raise ValueError(f"invalid {name}: expected positive integer")
        return int(value)

    source_sha = sha("SOURCE_SHA")
    if source_sha != sha("GITHUB_SHA"):
        raise ValueError("checked-out source differs from the triggering commit")
    repository = required("GITHUB_REPOSITORY")
    run_id = positive_int("GITHUB_RUN_ID")
    run_attempt = positive_int("GITHUB_RUN_ATTEMPT")
    timestamp = required("BUILD_TIMESTAMP")
    try:
        datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as error:
        raise ValueError("BUILD_TIMESTAMP must be a UTC RFC3339 timestamp") from error
    image_name = required("IMAGE_NAME")
    digest = required("IMAGE_DIGEST")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest) or digest == "sha256:" + "0" * 64:
        raise ValueError("invalid image manifest digest")
    requested_platforms = required("IMAGE_PLATFORMS").split(",")
    if sorted(requested_platforms) != ["linux/amd64", "linux/arm64"]:
        raise ValueError("expected the complete amd64/arm64 image build")
    platforms = []
    for manifest in image_index.get("manifests", []):
        # BuildKit provenance/SBOM manifests are not runnable image platforms.
        if manifest.get("annotations", {}).get("vnd.docker.reference.type") == "attestation-manifest":
            continue
        platform = manifest.get("platform", {})
        platforms.append(f"{platform.get('os')}/{platform.get('architecture')}")
    if sorted(platforms) != sorted(requested_platforms):
        raise ValueError("pushed image index does not contain exactly the requested platforms")
    platforms.sort()
    tags = required("IMAGE_TAGS").splitlines()
    if any(not tag.startswith(image_name + ":") for tag in tags):
        raise ValueError("image tags must belong to the built package")
    server_url = required("GITHUB_SERVER_URL").rstrip("/")
    return {
        "schema_version": 1,
        "artifact_type": "signoz-ob-image-build",
        "source": {"repository": repository, "sha": source_sha, "ref": required("GITHUB_REF")},
        "build": {
            "version": required("BUILD_VERSION"), "variant": "community", "timestamp": timestamp,
            "workflow_ref": required("GITHUB_WORKFLOW_REF"), "workflow_sha": sha("GITHUB_WORKFLOW_SHA"),
            "run_id": run_id, "run_attempt": run_attempt,
            "run_url": f"{server_url}/{repository}/actions/runs/{run_id}/attempts/{run_attempt}",
        },
        "image": {
            "name": image_name, "digest": digest, "reference": f"{image_name}@{digest}",
            "tags": tags, "platforms": platforms,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    args = parser.parse_args()
    receipt = build_receipt(os.environ, json.loads(args.manifest.read_text()))
    args.output.write_text(json.dumps(receipt, indent=2) + "\n")


if __name__ == "__main__":
    main()
