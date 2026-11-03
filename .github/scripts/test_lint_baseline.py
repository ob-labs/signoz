"""Adversarial tests for a reviewed historical lint baseline, using stdlib only."""

import copy
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location("lint_baseline", Path(__file__).with_name("lint_baseline.py"))
lint = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(lint)


class LintBaselineTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / "sample.go").write_text("package sample\nfunc one() { bad() }\nfunc two() { bad() }\n")
        self.identity = {"source_repository": "ob-labs/signoz", "source_sha": "a" * 40,
                         "producer": {"run_id": "123", "run_attempt": "1", "event": "pull_request",
                                      "workflow_ref": "ob-labs/signoz/.github/workflows/goci.yaml@refs/pull/19/merge",
                                      "workflow_sha": "b" * 40,
                                      "run_url": "https://github.com/ob-labs/signoz/actions/runs/123/attempts/1"}}
        self.tool = {"version": "2.13.2", "commit": "27774aaf", "goVersion": "go1.27.0"}
        self.go = {"GOVERSION": "go1.25.7", "GOOS": "linux", "GOARCH": "amd64"}

    def raw(self, *, line=2, text="unchecked operation", rule="errcheck", path="sample.go"):
        return {"Issues": [{"FromLinter": rule, "Text": text,
                            "SourceLines": ["private raw source must never be copied"],
                            "Pos": {"Filename": path, "Line": line, "Column": 1}}], "Report": {"Linters": []}}

    def normalize(self, raw=None, **changes):
        args = {"root": self.root, "identity": self.identity, "config": b"lint-config",
                "tool_info": self.tool, "go_info": self.go, "exit_code": 0, "tracked": {"sample.go"}}
        args.update(changes)
        return lint.normalize(self.raw() if raw is None else raw, **args)

    def baseline(self, report=None):
        return lint.record(self.normalize() if report is None else report, source_sha="a" * 40,
                           run_id="123", run_attempt="1", acknowledge=True, environment={})

    def test_normalization_only_exports_allowlisted_hashes_and_identity(self):
        report = self.normalize()
        self.assertEqual("a" * 40, report["source_sha"])
        self.assertEqual("b" * 40, report["producer"]["workflow_sha"])
        self.assertEqual("pull_request", report["producer"]["event"])
        self.assertNotIn("private raw source", json.dumps(report))
        self.assertNotIn("SourceLines", json.dumps(report))
        self.assertEqual(lint.digest((self.root / "sample.go").read_bytes()), report["findings"][0]["file_sha256"])

    def test_known_findings_pass_but_new_findings_block(self):
        baseline = self.baseline()
        self.assertEqual("passed", lint.check(self.normalize(), baseline)["status"])
        report = self.normalize(self.raw(text="a new failure"))
        summary = lint.check(report, baseline)
        self.assertEqual("failed", summary["status"])
        self.assertEqual(1, summary["new_findings"])

    def test_line_relocation_preserves_debt_but_changed_source_does_not(self):
        baseline = self.baseline()
        path = self.root / "sample.go"
        path.write_text("// inserted comment\n" + path.read_text())
        self.assertEqual("passed", lint.check(self.normalize(self.raw(line=3)), baseline)["status"])
        path.write_text(path.read_text().replace("func one() { bad() }", "func one() { worse() }"))
        self.assertEqual("failed", lint.check(self.normalize(self.raw(line=3)), baseline)["status"])

    def test_an_additional_identical_fingerprint_cannot_hide_in_a_set(self):
        # Identical source lines at different positions have identical anchors;
        # occurrence counts, rather than a set, must enforce the accepted debt.
        path = self.root / "sample.go"
        path.write_text("package sample\nfunc one() {\n bad()\n}\nfunc two() {\n bad()\n}\n")
        baseline = self.baseline(self.normalize(self.raw(line=3)))
        raw = self.raw(line=3)
        raw["Issues"].extend(self.raw(line=6)["Issues"])
        summary = lint.check(self.normalize(raw), baseline)
        self.assertEqual("failed", summary["status"])
        self.assertEqual(1, summary["known_findings"])
        self.assertEqual(1, summary["new_findings"])

    def test_resolved_entries_are_reported_and_not_automatically_removed(self):
        baseline = self.baseline()
        before = copy.deepcopy(baseline)
        summary = lint.check(self.normalize({"Issues": None, "Report": {}}), baseline)
        self.assertEqual("passed", summary["status"])
        self.assertEqual(1, summary["resolved_baseline_findings"])
        self.assertEqual(before, baseline)

    def test_missing_pending_or_empty_baseline_never_accepts_findings(self):
        for baseline in (None, {}, {"schema_version": 1, "status": "pending"}):
            with self.subTest(baseline=baseline), self.assertRaises(lint.InvalidReport):
                lint.check(self.normalize(), baseline)

    def test_record_requires_explicit_local_acceptance_of_exact_run(self):
        report = self.normalize()
        args = {"source_sha": "a" * 40, "run_id": "123", "run_attempt": "1", "acknowledge": True, "environment": {}}
        for change in ({"acknowledge": False}, {"source_sha": "c" * 40}, {"run_id": "124"},
                       {"run_attempt": "2"}, {"environment": {"GITHUB_ACTIONS": "true"}}, {"environment": {"CI": "true"}}):
            with self.subTest(change=change), self.assertRaises(lint.InvalidReport):
                lint.record(report, **{**args, **change})
        self.assertEqual("accepted", lint.record(report, **args)["status"])

    def test_tool_config_go_or_platform_drift_is_not_a_new_baseline(self):
        baseline = self.baseline()
        for key, value in (("config_sha256", "d" * 64), ("tool", {**baseline["tool"], "commit": "1234567"}),
                           ("go", {**baseline["go"], "version": "go1.25.8"})):
            report = self.normalize()
            report[key] = value
            with self.subTest(key=key), self.assertRaises(lint.InvalidReport):
                lint.check(report, baseline)
        with self.assertRaises(lint.InvalidReport):
            self.normalize(tool_info={**self.tool, "version": "2.14.0"})
        with self.assertRaises(lint.InvalidReport):
            self.normalize(go_info={**self.go, "GOOS": "darwin"})

    def test_raw_typecheck_or_infrastructure_failures_cannot_be_recorded(self):
        for raw, exit_code in ((self.raw(rule="typecheck"), 0), (self.raw(), 3),
                               ({"Issues": [], "Report": {"Error": "compiler failure"}}, 0), ({}, 0)):
            with self.subTest(raw=raw, exit_code=exit_code), self.assertRaises(lint.InvalidReport):
                self.normalize(raw, exit_code=exit_code)

    def test_paths_must_be_canonical_tracked_go_files_inside_checkout(self):
        for path in ("../sample.go", "./sample.go", "/tmp/sample.go", "sub/../sample.go", "sample\\file.go", "C:/sample.go", "sample.go\n", "missing.go", "sample.txt"):
            with self.subTest(path=path), self.assertRaises(lint.InvalidReport):
                self.normalize(self.raw(path=path))
        linked = self.root / "link.go"
        linked.symlink_to(self.root / "sample.go")
        with self.assertRaises(lint.InvalidReport):
            self.normalize(self.raw(path="link.go"), tracked={"link.go"})

    def test_duplicate_findings_keys_positions_and_forged_hashes_reject(self):
        raw = self.raw()
        raw["Issues"] *= 2
        with self.assertRaises(lint.InvalidReport):
            self.normalize(raw)
        with self.assertRaises(lint.InvalidReport):
            json.loads('{"Issues":[],"Issues":[]}', object_pairs_hook=lint.unique_object)
        report = self.normalize()
        report["findings"][0]["fingerprint"] = "f" * 64
        with self.assertRaises(lint.InvalidReport):
            lint.check(report, self.baseline())

    def test_invalid_positions_and_terminal_control_messages_reject(self):
        for raw in (self.raw(line=0), self.raw(line=99), self.raw(text="failure\x1b[31m")):
            with self.subTest(raw=raw), self.assertRaises(lint.InvalidReport):
                self.normalize(raw)
        raw = self.raw()
        raw["Issues"][0]["Pos"]["Column"] = 999
        with self.assertRaises(lint.InvalidReport):
            self.normalize(raw)

    def test_record_cli_is_local_only_with_a_controlled_environment(self):
        report = self.root / "report.json"
        report.write_text(json.dumps(self.normalize()))
        baseline = self.root / "baseline.json"
        command = [sys.executable, str(Path(lint.__file__)), "record", "--report", str(report),
                   "--baseline", str(baseline), "--source-sha", "a" * 40, "--run-id", "123",
                   "--run-attempt", "1", "--acknowledge-existing-findings"]
        # Never inherit the test runner's hosted GITHUB_* or CI environment.
        result = subprocess.run(command, env={"PATH": os.defpath}, capture_output=True, text=True)
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("accepted", json.loads(baseline.read_text())["status"])
        baseline.unlink()
        result = subprocess.run(command, env={"PATH": os.defpath, "GITHUB_ACTIONS": "true"}, capture_output=True, text=True)
        self.assertNotEqual(0, result.returncode)
        self.assertIn("record_forbidden_in_ci", result.stderr)
        self.assertFalse(baseline.exists())

    def test_normalize_cli_uses_checkout_head_separately_from_merge_event_sha(self):
        script = self.root / ".github/scripts/lint_baseline.py"
        script.parent.mkdir(parents=True)
        script.write_bytes(Path(lint.__file__).read_bytes())
        tools = self.root / "tools"
        tools.mkdir()
        git = tools / "git"
        git.write_text("#!/bin/sh\ncase \"$1\" in\nrev-parse) printf '%s\\n' '" + "a" * 40
                       + "';;\nls-files) printf 'sample.go\\000';;\n*) exit 1;;\nesac\n")
        git.chmod(0o755)
        values = {"raw": self.raw(), "tool-info": self.tool, "go-info": self.go}
        for name, value in values.items():
            (self.root / name).write_text(json.dumps(value))
        (self.root / "exit-code").write_text("0\n")
        (self.root / "config").write_text("lint-config")
        output = self.root / "normalized.json"
        command = [sys.executable, str(script), "normalize", "--source-sha", "a" * 40, "--output", str(output)]
        for name in ("raw", "tool-info", "go-info", "exit-code", "config"):
            command.extend(["--" + name, str(self.root / name)])
        producer = self.identity["producer"]
        env = {"PATH": str(tools) + os.pathsep + os.defpath, "GITHUB_ACTIONS": "true",
               "GITHUB_REPOSITORY": "ob-labs/signoz", "GITHUB_SHA": "c" * 40,
               "GITHUB_RUN_ID": "123", "GITHUB_RUN_ATTEMPT": "1", "GITHUB_EVENT_NAME": "pull_request",
               "GITHUB_WORKFLOW_REF": producer["workflow_ref"], "GITHUB_WORKFLOW_SHA": producer["workflow_sha"]}
        result = subprocess.run(command, env=env, capture_output=True, text=True)
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("a" * 40, json.loads(output.read_text())["source_sha"])
        self.assertEqual("b" * 40, json.loads(output.read_text())["producer"]["workflow_sha"])


if __name__ == "__main__":
    unittest.main()
