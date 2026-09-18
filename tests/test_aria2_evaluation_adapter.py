"""Local tests for the aria2 evaluation adapter's non-network helpers."""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from acquisition_evaluation import Fixture
from aria2_evaluation_adapter import (FIXTURE_PREFIX, fixture_server, isolated_dirs,
                                      read_downloads, write_queue_file)


class FixtureRoutingTests(unittest.TestCase):
    def test_fixture_urls_match_tod_dl_relative_path_shape(self):
        fixture = Fixture.create("sample.bin", "adapter-seed", 16)
        with tempfile.TemporaryDirectory() as temporary:
            with fixture_server([fixture], Path(temporary) / "events.json") as server:
                url = server.url(fixture.name)
        self.assertIn(f"/{FIXTURE_PREFIX}/{fixture.name}", url)
        # The fixture prefix still uses the older <source>/data/<tail> shape, which relative_path() accepts.
        source, data, name = FIXTURE_PREFIX.split("/") + [fixture.name]
        self.assertEqual(data, "data")


class QueueFileTests(unittest.TestCase):
    def test_write_queue_file_writes_one_url_per_line(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = write_queue_file(Path(temporary) / "queue.txt",
                                    ["http://a/x/data/1", "http://a/x/data/2"])
            self.assertEqual(path.read_text(encoding="utf-8").splitlines(),
                             ["http://a/x/data/1", "http://a/x/data/2"])


class IsolatedDirsTests(unittest.TestCase):
    def test_isolated_dirs_creates_fresh_destination_and_state(self):
        with tempfile.TemporaryDirectory() as temporary:
            destination, state = isolated_dirs(Path(temporary) / "scenario")
            self.assertTrue(destination.is_dir())
            self.assertTrue(state.is_dir())
            self.assertNotEqual(destination, state)


class ReadDownloadsTests(unittest.TestCase):
    def test_read_downloads_returns_empty_mapping_without_a_database(self):
        with tempfile.TemporaryDirectory() as temporary:
            self.assertEqual(read_downloads(Path(temporary)), {})

    def test_read_downloads_reads_the_real_schema_columns(self):
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            with sqlite3.connect(state / "manifest.sqlite") as db:
                db.execute(
                    "CREATE TABLE downloads (url TEXT PRIMARY KEY, relative_path TEXT, "
                    "storage_path TEXT, staging_path TEXT, status TEXT, attempts INTEGER, "
                    "bytes INTEGER, sha256 TEXT, last_error TEXT, review_code TEXT)"
                )
                db.execute(
                    "INSERT INTO downloads VALUES ('u', 'r', 's', 'st', 'complete', 1, "
                    "10, 'abc', NULL, NULL)"
                )
                db.commit()
            rows = read_downloads(state)
        self.assertEqual(rows["u"]["status"], "complete")


if __name__ == "__main__":
    unittest.main()
