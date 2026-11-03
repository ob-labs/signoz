"""Run full lint with an explicitly reviewed historical finding baseline.

CI can normalize and check reports, never accept or update a baseline. Reports
contain static messages and source hashes, not source lines or compiler logs.
"""

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys

REPOSITORY = "ob-labs/signoz"
ROOT = Path(__file__).resolve().parents[2]
VERSION = "2.13.2"
SHA = re.compile(r"[a-f0-9]{40}")
HASH = re.compile(r"[a-f0-9]{64}")
POSITIVE = re.compile(r"[1-9][0-9]*")
FINDING_KEYS = {"path", "line", "column", "linter", "message", "source_anchor_sha256", "file_sha256", "fingerprint"}
REPORT_KEYS = {"schema_version", "status", "source_repository", "source_sha", "producer", "tool", "go", "config_sha256", "findings"}


class InvalidReport(ValueError):
    """A fixed error code safe to retain in a public static-analysis receipt."""


def require(condition, code):
    if not condition:
        raise InvalidReport(code)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate_json_key")
        result[key] = value
    return result


def read_json(path):
    data = Path(path).read_bytes()
    require(len(data) <= 32 * 1024 * 1024, "report_too_large")
    return json.loads(data, object_pairs_hook=unique_object)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def valid_text(value, limit=4096):
    return isinstance(value, str) and 0 < len(value) <= limit and all(ord(c) >= 32 and ord(c) != 127 for c in value)


def canonical_path(value):
    require(valid_text(value, 1024) and "\\" not in value and ":" not in value, "noncanonical_path")
    path = PurePosixPath(value)
    require(not path.is_absolute() and path.suffix == ".go" and str(path) == value
            and all(part not in {"", ".", ".."} for part in path.parts), "noncanonical_path")
    return value


def fingerprint(finding):
    # Positions and whole-file hashes are audit data. Moving unchanged code does
    # not erase its debt; changing the reported source line creates a new issue.
    identity = [finding[k] for k in ("path", "linter", "message", "source_anchor_sha256")]
    return digest(json.dumps(identity, ensure_ascii=False, separators=(",", ":")).encode())


def validate_identity(report):
    require(report.get("source_repository") == REPOSITORY, "foreign_repository")
    require(isinstance(report.get("source_sha"), str) and SHA.fullmatch(report["source_sha"])
            and report["source_sha"] != "0" * 40, "invalid_source")
    producer = report.get("producer")
    require(isinstance(producer, dict) and set(producer) == {"run_id", "run_attempt", "event", "workflow_ref", "workflow_sha", "run_url"}, "invalid_producer")
    require(all(isinstance(producer.get(k), str) and POSITIVE.fullmatch(producer[k]) for k in ("run_id", "run_attempt")), "invalid_producer")
    require(producer["event"] in {"pull_request", "merge_group", "workflow_dispatch"}, "invalid_producer")
    require(valid_text(producer["workflow_ref"]) and producer["workflow_ref"].startswith(REPOSITORY + "/.github/workflows/goci.yaml@refs/"), "invalid_workflow")
    require(isinstance(producer["workflow_sha"], str) and SHA.fullmatch(producer["workflow_sha"])
            and producer["workflow_sha"] != "0" * 40, "invalid_workflow")
    require(producer["run_url"] == f"https://github.com/{REPOSITORY}/actions/runs/{producer['run_id']}/attempts/{producer['run_attempt']}", "invalid_run_url")
    tool = report.get("tool")
    require(isinstance(tool, dict) and set(tool) == {"version", "commit", "compiler_version"}
            and tool["version"] == VERSION and valid_text(tool["commit"], 80)
            and re.fullmatch(r"[a-f0-9]{7,40}", tool["commit"])
            and valid_text(tool["compiler_version"], 80), "invalid_tool")
    go = report.get("go")
    require(isinstance(go, dict) and set(go) == {"version", "platform"}
            and isinstance(go["version"], str) and re.fullmatch(r"go[0-9]+\.[0-9]+(?:\.[0-9]+)?", go["version"])
            and go["platform"] == "linux/amd64", "invalid_go_identity")
    require(isinstance(report.get("config_sha256"), str) and HASH.fullmatch(report["config_sha256"]), "invalid_config")


def validate_findings(findings):
    require(isinstance(findings, list), "invalid_findings")
    positions = set()
    for finding in findings:
        require(isinstance(finding, dict) and set(finding) == FINDING_KEYS, "invalid_finding")
        canonical_path(finding["path"])
        require(isinstance(finding["linter"], str) and re.fullmatch(r"[a-z][a-z0-9_-]*", finding["linter"])
                and finding["linter"] != "typecheck", "incomplete_analysis")
        require(valid_text(finding["message"]), "invalid_message")
        require(all(type(finding[k]) is int and finding[k] > 0 for k in ("line", "column")), "invalid_position")
        require(all(isinstance(finding[k], str) and HASH.fullmatch(finding[k]) for k in ("source_anchor_sha256", "file_sha256", "fingerprint")), "invalid_hash")
        require(finding["fingerprint"] == fingerprint(finding), "invalid_fingerprint")
        position = tuple(finding[k] for k in ("path", "line", "column", "linter", "message"))
        require(position not in positions, "duplicate_finding")
        positions.add(position)


def validate_report(report):
    require(isinstance(report, dict) and set(report) == REPORT_KEYS
            and type(report.get("schema_version")) is int and report["schema_version"] == 1
            and report.get("status") == "complete", "invalid_report")
    validate_identity(report)
    validate_findings(report["findings"])
    return report


def normalize(raw, *, root, identity, config, tool_info, go_info, exit_code, tracked):
    require(exit_code == 0, "lint_execution_failed")
    require(isinstance(raw, dict) and "Issues" in raw and isinstance(raw.get("Report"), dict), "unsupported_lint_report")
    require(not raw["Report"].get("Error") and not raw["Report"].get("Errors"), "incomplete_analysis")
    issues = raw["Issues"] if raw["Issues"] is not None else []
    require(isinstance(issues, list), "invalid_findings")
    report = {"schema_version": 1, "status": "complete", **identity,
              "tool": {"version": tool_info.get("version"), "commit": tool_info.get("commit"), "compiler_version": tool_info.get("goVersion")},
              "go": {"version": go_info.get("GOVERSION"), "platform": f"{go_info.get('GOOS')}/{go_info.get('GOARCH')}"},
              "config_sha256": digest(config), "findings": []}
    validate_identity(report)
    root = Path(root).resolve()
    contents = {}
    for issue in issues:
        require(isinstance(issue, dict) and isinstance(issue.get("Pos"), dict), "invalid_finding")
        name = canonical_path(issue["Pos"].get("Filename"))
        require(name in tracked, "untracked_source")
        path = root / name
        require(path.resolve().is_relative_to(root) and path.is_file()
                and not any(parent.is_symlink() for parent in [path, *path.parents] if parent != root and root in parent.parents), "unsafe_source")
        if name not in contents:
            data = path.read_bytes()
            require(len(data) <= 8 * 1024 * 1024, "source_too_large")
            contents[name] = (data.decode("utf-8").splitlines(), digest(data))
        lines, file_hash = contents[name]
        line, column = issue["Pos"].get("Line"), issue["Pos"].get("Column")
        require(type(line) is int and 1 <= line <= len(lines) and type(column) is int
                and 1 <= column <= len(lines[line - 1].encode("utf-8")) + 1, "invalid_position")
        finding = {"path": name, "line": line, "column": column,
                   "linter": issue.get("FromLinter"), "message": issue.get("Text"),
                   "source_anchor_sha256": digest(lines[line - 1].encode("utf-8")), "file_sha256": file_hash}
        finding["fingerprint"] = fingerprint(finding)
        report["findings"].append(finding)
    report["findings"].sort(key=lambda f: (f["path"], f["line"], f["column"], f["linter"] or "", f["message"] or ""))
    return validate_report(report)


def record(report, *, source_sha, run_id, run_attempt, acknowledge, environment):
    require(not any(environment.get(k, "").lower() == "true" for k in ("CI", "GITHUB_ACTIONS")), "record_forbidden_in_ci")
    require(acknowledge, "explicit_acceptance_required")
    validate_report(report)
    require(report["source_sha"] == source_sha and report["producer"]["run_id"] == run_id
            and report["producer"]["run_attempt"] == run_attempt, "acceptance_identity_mismatch")
    return {"schema_version": 1, "status": "accepted", "source_repository": report["source_repository"],
            "accepted_source_sha": report["source_sha"], "accepted_producer": report["producer"],
            "tool": report["tool"], "go": report["go"], "config_sha256": report["config_sha256"],
            "recorded_at_utc": datetime.now(timezone.utc).isoformat(), "findings": report["findings"]}


def check(report, baseline):
    validate_report(report)
    require(isinstance(baseline, dict) and baseline.get("status") == "accepted", "missing_or_pending_baseline")
    keys = {"schema_version", "status", "source_repository", "accepted_source_sha", "accepted_producer", "tool", "go", "config_sha256", "recorded_at_utc", "findings"}
    require(set(baseline) == keys and type(baseline["schema_version"]) is int and baseline["schema_version"] == 1, "invalid_baseline")
    accepted = {"schema_version": 1, "status": "complete", "source_repository": baseline["source_repository"],
                "source_sha": baseline["accepted_source_sha"], "producer": baseline["accepted_producer"],
                **{k: baseline[k] for k in ("tool", "go", "config_sha256", "findings")}}
    validate_report(accepted)
    require(valid_text(baseline["recorded_at_utc"], 100), "invalid_baseline")
    require(all(report[k] == accepted[k] for k in ("source_repository", "tool", "go", "config_sha256")), "analysis_identity_drift")
    previous = Counter(f["fingerprint"] for f in accepted["findings"])
    current = Counter(f["fingerprint"] for f in report["findings"])
    new = current - previous
    resolved = previous - current
    return {"schema_version": 1, "status": "failed" if new else "passed",
            "source_repository": report["source_repository"], "source_sha": report["source_sha"],
            "baseline_source_sha": baseline["accepted_source_sha"],
            "current_findings": sum(current.values()), "baseline_findings": sum(previous.values()),
            "known_findings": sum((previous & current).values()), "new_findings": sum(new.values()),
            "resolved_baseline_findings": sum(resolved.values()), "new_fingerprints": sorted(new)}


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    norm = sub.add_parser("normalize")
    for name in ("raw", "exit-code", "tool-info", "go-info", "config", "source-sha", "output"):
        norm.add_argument("--" + name, required=True)
    gate = sub.add_parser("check")
    gate.add_argument("--report", required=True)
    gate.add_argument("--baseline", required=True)
    gate.add_argument("--summary", required=True)
    accept = sub.add_parser("record")
    for name in ("report", "baseline", "source-sha", "run-id", "run-attempt"):
        accept.add_argument("--" + name, required=True)
    accept.add_argument("--acknowledge-existing-findings", action="store_true")
    args = parser.parse_args()
    try:
        if args.command == "normalize":
            require(subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip() == args.source_sha, "source_checkout_mismatch")
            env = os.environ
            run_id, attempt = env.get("GITHUB_RUN_ID", ""), env.get("GITHUB_RUN_ATTEMPT", "")
            identity = {"source_repository": env.get("GITHUB_REPOSITORY"), "source_sha": args.source_sha,
                        "producer": {"run_id": run_id, "run_attempt": attempt, "event": env.get("GITHUB_EVENT_NAME"),
                                     "workflow_ref": env.get("GITHUB_WORKFLOW_REF"), "workflow_sha": env.get("GITHUB_WORKFLOW_SHA"),
                                     "run_url": f"https://github.com/{REPOSITORY}/actions/runs/{run_id}/attempts/{attempt}"}}
            tracked = set(subprocess.check_output(["git", "ls-files", "-z", "--", "*.go"], cwd=ROOT, text=True).split("\0"))
            value = normalize(read_json(args.raw), root=ROOT, identity=identity, config=Path(args.config).read_bytes(),
                              tool_info=read_json(args.tool_info), go_info=read_json(args.go_info),
                              exit_code=int(Path(args.exit_code).read_text().strip()), tracked=tracked)
            write_json(args.output, value)
        elif args.command == "record":
            value = record(read_json(args.report), source_sha=args.source_sha, run_id=args.run_id,
                           run_attempt=args.run_attempt, acknowledge=args.acknowledge_existing_findings, environment=os.environ)
            write_json(args.baseline, value)
        else:
            value = check(read_json(args.report), read_json(args.baseline))
            write_json(args.summary, value)
            if value["status"] != "passed":
                return 1
        print("lint baseline " + args.command + ": " + value["status"])
        return 0
    except (ValueError, OSError, KeyError, TypeError, RecursionError, subprocess.CalledProcessError) as error:
        reason = str(error) if isinstance(error, InvalidReport) else "normalization_or_input_failed"
        value = {"schema_version": 1, "status": "invalid", "reason": reason}
        output = args.output if args.command == "normalize" else args.summary if args.command == "check" else None
        if output:
            write_json(output, value)
        print("lint baseline " + args.command + ": " + reason, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
