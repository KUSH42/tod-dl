"""Local tests for the acquisition-engine evaluation harness."""

from __future__ import annotations

import hashlib
import http.client
import json
import tempfile
import unittest
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from acquisition_evaluation import (DeterministicBytes, EvaluationError, Fixture,
                                    FixtureServer, ResponseScript, ScenarioResult,
                                    build_manifest, build_report, manifest_hash,
                                    write_manifest)


class DeterministicFixtureTests(unittest.TestCase):
    def test_ranges_join_to_the_full_deterministic_representation(self):
        generator = DeterministicBytes("fixture-seed", 409)
        full = generator.read(0, 409)
        joined = generator.read(0, 63) + generator.read(63, 191) + generator.read(254, 155)
        self.assertEqual(joined, full)
        self.assertEqual(generator.sha256(31), hashlib.sha256(full).hexdigest())

    def test_invalid_ranges_are_rejected(self):
        generator = DeterministicBytes("fixture-seed", 10)
        with self.assertRaises(EvaluationError):
            generator.read(9, 2)
        with self.assertRaises(EvaluationError):
            DeterministicBytes("", 10)

    def test_manifest_is_stable_and_includes_expected_hashes(self):
        alpha = Fixture.create("alpha.bin", "alpha", 31)
        beta = Fixture.create("beta.bin", "beta", 29)
        first = build_manifest([beta, alpha])
        second = build_manifest([alpha, beta])
        self.assertEqual(first, second)
        self.assertEqual(manifest_hash(first), manifest_hash(second))
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "fixture-manifest.json"
            digest = write_manifest(path, [alpha, beta])
            saved = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(saved["manifest_sha256"], digest)
            self.assertEqual(saved["fixtures"][0]["sha256"], alpha.sha256)
            with self.assertRaises(EvaluationError):
                write_manifest(path, [alpha, beta])


class FixtureServerTests(unittest.TestCase):
    def test_range_response_and_event_log(self):
        fixture = Fixture.create("range.bin", "range-seed", 1024)
        with tempfile.TemporaryDirectory() as temporary:
            log = Path(temporary) / "events.json"
            with FixtureServer([fixture], log) as server:
                request = Request(server.url("range.bin"), headers={"Range": "bytes=100-299"})
                with urlopen(request, timeout=5) as response:
                    self.assertEqual(response.status, 206)
                    self.assertEqual(response.headers["Content-Range"], "bytes 100-299/1024")
                    self.assertEqual(response.read(), fixture.generator.read(100, 200))
            events = json.loads(log.read_text(encoding="utf-8"))["events"]
            self.assertEqual(events[0]["range"], "bytes=100-299")
            self.assertEqual(events[0]["status"], 206)
            self.assertEqual(events[0]["transmitted_bytes"], 200)

    def test_ignored_range_and_unsatisfiable_range_are_scripted(self):
        fixture = Fixture.create("script.bin", "script-seed", 64)
        with tempfile.TemporaryDirectory() as temporary:
            log = Path(temporary) / "events.json"
            scripts = {"script.bin": [ResponseScript(ignore_range=True)]}
            with FixtureServer([fixture], log, scripts) as server:
                request = Request(server.url("script.bin"), headers={"Range": "bytes=2-3"})
                with urlopen(request, timeout=5) as response:
                    self.assertEqual(response.status, 200)
                    self.assertEqual(response.read(), fixture.generator.read(0, 64))
                request = Request(server.url("script.bin"), headers={"Range": "bytes=70-"})
                with self.assertRaises(HTTPError) as caught:
                    urlopen(request, timeout=5)
                self.assertEqual(caught.exception.code, 416)

    def test_short_body_keeps_the_advertised_length_for_engine_validation(self):
        fixture = Fixture.create("short.bin", "short-seed", 64)
        with tempfile.TemporaryDirectory() as temporary:
            log = Path(temporary) / "events.json"
            scripts = {"short.bin": [ResponseScript(short_body_bytes=9)]}
            with FixtureServer([fixture], log, scripts) as server:
                with urlopen(server.url("short.bin"), timeout=5) as response:
                    self.assertEqual(response.headers["Content-Length"], "64")
                    with self.assertRaises(http.client.IncompleteRead) as caught:
                        response.read()
                    self.assertEqual(len(caught.exception.partial), 9)
            event = json.loads(log.read_text(encoding="utf-8"))["events"][0]
            self.assertEqual(event["transmitted_bytes"], 9)


class EvaluationReportTests(unittest.TestCase):
    def test_only_all_pass_results_are_selection_eligible(self):
        passing = ScenarioResult("E01", "pass", "bytes", "matched")
        blocked = ScenarioResult("E02", "blocked", "resume", "engine unavailable")
        report = build_report("aria2-native", ["aria2c"], "a" * 64,
                              [passing, blocked], "revision")
        self.assertFalse(report["selection_eligible"])
        all_passing = [ScenarioResult(f"E{number:02d}", "pass", "fixture", "matched")
                       for number in range(1, 15)]
        passing_report = build_report("aria2-native", ["aria2c"], "a" * 64,
                                      all_passing, "revision")
        self.assertTrue(passing_report["selection_eligible"])


if __name__ == "__main__":
    unittest.main()
