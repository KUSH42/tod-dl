"""Tests for local inventory parsing, reproducible manifests, and safe queue export."""

from __future__ import annotations

import contextlib
import copy
import hashlib
import importlib.util
import io
import json
import os
import shutil
import sys
import tempfile
import unittest
import unicodedata
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import inventory  # noqa: E402

FIXTURES = ROOT / "tests" / "fixtures" / "inventory"
LISTING = FIXTURES / "listing-basic.txt"
POLICY = FIXTURES / "policy-basic.json"
BASE_URL = "https://source.example/case1/data"

# Load a private copy under its own name. Registering it as "tod_dl" would replace the
# module that tests/test_tod_dl.py patches by name when both run in one process.
_spec = importlib.util.spec_from_file_location("tod_dl_inventory_check", ROOT / "src" / "tod-dl.py")
if _spec is None or _spec.loader is None:
    raise RuntimeError("cannot load tod-dl.py for tests")
tod_dl = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tod_dl)


def parse_bytes(data: bytes):
    parser = inventory.ListingParser()
    entries = list(parser.parse(io.BytesIO(data)))
    return parser, entries


class InventoryTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="inventory-test-"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.store = self.tmp / "store"

    def run_cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = inventory.main([str(a) for a in argv])
        return code, out.getvalue(), err.getvalue()

    def write_listing(self, data: bytes, name="listing.txt") -> Path:
        path = self.tmp / name
        path.write_bytes(data)
        return path

    def import_snapshot(self, source=LISTING, *extra) -> Path:
        code, out, err = self.run_cli("snapshot", "--input", source, "--store", self.store, *extra)
        self.assertEqual(code, 0, out + err)
        return next((self.store / "snapshots").iterdir())

    def make_manifest(self, snapshot: Path, name="m1", policy=POLICY) -> Path:
        output = self.tmp / name
        code, out, err = self.run_cli("manifest", "--snapshot", snapshot, "--policy", policy,
                                      "--base-url", BASE_URL, "--output", output)
        self.assertEqual(code, 0, out + err)
        return output

    @staticmethod
    def items(manifest: Path):
        lines = (manifest / "manifest.jsonl").read_text(encoding="utf-8").splitlines()
        return json.loads(lines[0]), [json.loads(line) for line in lines[1:]]


class ParserTests(InventoryTestCase):
    def test_every_line_has_exactly_one_class(self):
        parser, entries = parse_bytes(LISTING.read_bytes())
        report = parser.report()
        self.assertEqual(sum(report["classes"].values()), report["lines"])
        self.assertEqual(report["classes"]["file"], len(entries))
        self.assertEqual(report["classes"]["header"], 4)
        self.assertEqual(report["issue_total"], 0)

    def test_names_keep_their_exact_text(self):
        # Names must survive byte for byte: the legacy diff.py strip() lost edge spaces.
        _, entries = parse_bytes(LISTING.read_bytes())
        names = {e.name for e in entries}
        self.assertIn(" leading space.txt", names)
        self.assertIn("100% done.txt", names)
        self.assertIn("100%25 done.txt", names)
        self.assertIn("café.txt", names)

    def test_file_with_trailing_colon_is_not_a_header(self):
        # The legacy diff.py tested "ends with ':'" first and turned this file into a directory.
        for data in (b".:\n\n- 5 note:\n\nDocs:\n\n- 1 a\n", b".:\n- 5 note:\n\nDocs:\n- 1 a\n"):
            parser, entries = parse_bytes(data)
            self.assertEqual([e.name for e in entries], ["note:", "a"])
            self.assertEqual(parser.report()["classes"]["header"], 2)
            self.assertEqual(parser.report()["issue_total"], 0)

    def test_invalid_utf8_is_reported_and_never_dropped_silently(self):
        parser, entries = parse_bytes(b".:\n- 5 bad\xff.txt\n- 6 good.txt\n")
        report = parser.report()
        self.assertEqual([e.name for e in entries], ["good.txt"])
        self.assertEqual(report["classes"]["decode_error"], 1)
        self.assertEqual(report["issues"][0]["line"], 2)
        self.assertIn("ff", report["issues"][0]["detail"])

    def test_overlong_line_is_bounded_and_parsing_resumes(self):
        long_name = b"x" * (inventory.MAX_LINE_BYTES * 3)
        parser, entries = parse_bytes(b".:\n- 1 " + long_name + b"\n- 2 after.txt\n")
        self.assertEqual([e.name for e in entries], ["after.txt"])
        self.assertEqual(parser.report()["issue_counts"], {"line_too_long": 1})
        self.assertEqual(parser.lines, 3)

    def test_malformed_lines_get_specific_codes(self):
        parser, _ = parse_bytes(b".:\n- 5\n- 5 \n- big name\nl 5 link\n\nnot a header\n")
        self.assertEqual(parser.report()["issue_counts"],
                         {"bad_entry": 2, "bad_size_token": 1, "empty_name": 1, "not_a_header": 1})

    def test_entry_after_bad_header_is_not_attributed_to_previous_section(self):
        parser, entries = parse_bytes(b"Docs:\n- 1 a\n\nnot a header\n- 2 b\n")
        self.assertEqual([e.directory for e in entries], ["Docs"])
        self.assertEqual(parser.report()["issue_counts"], {"bad_entry": 1, "not_a_header": 1})


class SnapshotTests(InventoryTestCase):
    def test_accepted_snapshot_keeps_exact_bytes_and_is_read_only(self):
        before = hashlib.sha256(LISTING.read_bytes()).hexdigest()
        snap = self.import_snapshot()
        meta = json.loads((snap / "snapshot.json").read_text())
        self.assertEqual(meta["status"], "accepted")
        self.assertEqual(meta["sha256"], before)
        self.assertEqual(inventory.sha256_file(snap / "raw.txt"), before)
        self.assertFalse(os.access(snap / "raw.txt", os.W_OK) and os.geteuid() != 0)
        self.assertEqual(hashlib.sha256(LISTING.read_bytes()).hexdigest(), before)

    def test_import_never_replaces_an_existing_snapshot(self):
        snap = self.import_snapshot()
        before = inventory.sha256_file(snap / "snapshot.json")
        code, _, err = self.run_cli("snapshot", "--input", LISTING, "--store", self.store)
        self.assertEqual(code, 2)
        self.assertIn("already exists", err)
        self.assertEqual(inventory.sha256_file(snap / "snapshot.json"), before)

    def test_http_200_error_page_cannot_become_a_baseline(self):
        page = self.write_listing(b"<html><body><h1>503 Service Unavailable</h1></body></html>\n")
        code, out, _ = self.run_cli("snapshot", "--input", page, "--store", self.store)
        self.assertEqual(code, 1)
        self.assertIn("rejected", out)
        self.assertFalse((self.store / "snapshots").exists())
        rejected = next((self.store / "rejected").iterdir())
        code, _, err = self.run_cli("activate", "--store", self.store,
                                    "--snapshot", json.loads((rejected / "snapshot.json").read_text())["sha256"])
        self.assertEqual(code, 2)
        self.assertFalse((self.store / "activations.jsonl").exists())
        code, _, err = self.run_cli("manifest", "--snapshot", rejected, "--policy", POLICY,
                                    "--base-url", BASE_URL, "--output", self.tmp / "m")
        self.assertEqual(code, 2)
        self.assertIn("not accepted", err)

    def test_partial_input_is_rejected_unless_operator_allows_the_issue_count(self):
        data = self.write_listing(b".:\n- 1 a\n- broken\n")
        code, _, _ = self.run_cli("snapshot", "--input", data, "--store", self.store)
        self.assertEqual(code, 1)
        code, _, _ = self.run_cli("snapshot", "--input", data, "--store", self.tmp / "other", "--max-issues", 1)
        self.assertEqual(code, 0)

    def test_activation_records_hash_and_previous_baseline(self):
        snap = self.import_snapshot()
        sha = json.loads((snap / "snapshot.json").read_text())["sha256"]
        self.assertEqual(self.run_cli("activate", "--store", self.store, "--snapshot", sha[:12])[0], 0)
        self.assertEqual(self.run_cli("activate", "--store", self.store, "--snapshot", sha)[0], 0)
        records = [json.loads(l) for l in (self.store / "activations.jsonl").read_text().splitlines()]
        self.assertEqual([r["snapshot_sha256"] for r in records], [sha, sha])
        self.assertEqual([r["previous_sha256"] for r in records], [None, sha])

    def test_tampered_snapshot_is_refused_by_manifest(self):
        snap = self.import_snapshot()
        os.chmod(snap / "raw.txt", 0o644)
        (snap / "raw.txt").write_bytes(b".:\n- 1 forged\n")
        code, _, err = self.run_cli("manifest", "--snapshot", snap, "--policy", POLICY,
                                    "--base-url", BASE_URL, "--output", self.tmp / "m")
        self.assertEqual(code, 2)
        self.assertIn("does not match", err)


class ManifestTests(InventoryTestCase):
    def test_same_inputs_give_identical_manifest_bytes_and_hash(self):
        snap = self.import_snapshot()
        first, second = self.make_manifest(snap, "m1"), self.make_manifest(snap, "m2")
        self.assertEqual((first / "manifest.jsonl").read_bytes(), (second / "manifest.jsonl").read_bytes())
        self.assertEqual((first / "manifest.sha256").read_text(), (second / "manifest.sha256").read_text())
        text = (first / "manifest.jsonl").read_text()
        self.assertNotIn("generated_utc", text)  # volatile values live only in the sidecar
        sidecar = json.loads((first / "manifest.meta.json").read_text())
        self.assertIn("generated_utc", sidecar)
        self.assertEqual(sidecar["manifest_sha256"], inventory.sha256_file(first / "manifest.jsonl"))

    def test_every_file_is_priority_deferred_or_rejected_and_totals_reconcile(self):
        header, items = self.items(self.make_manifest(self.import_snapshot()))
        totals = header["totals"]
        self.assertEqual((totals["files"], totals["priority"], totals["deferred"], totals["rejected"]),
                         (14, 7, 4, 3))
        self.assertEqual(len(items), totals["files"])
        self.assertTrue(all(i["disposition"] in inventory.DISPOSITIONS and i["reason"] for i in items))
        self.assertEqual(sum(totals["by_rule"].values()), totals["files"])
        self.assertEqual(header["snapshot_sha256"], inventory.sha256_file(LISTING))
        self.assertEqual(header["policy_sha256"], inventory.sha256_file(POLICY))

    def test_items_are_ordered_by_disposition_then_rule_rank_then_source_line(self):
        _, items = self.items(self.make_manifest(self.import_snapshot()))
        keys = [(inventory.DISPOSITIONS.index(i["disposition"]), i["rank"], i["line"]) for i in items]
        self.assertEqual(keys, sorted(keys))
        deferred = [i["path"] for i in items if i["disposition"] == "deferred"]
        self.assertEqual(deferred, ["Mail/box.pst", "notes.txt", "note:", "Mail/dup.txt"])  # rule rank first

    def test_literal_percent_and_encoded_name_stay_distinct(self):
        _, items = self.items(self.make_manifest(self.import_snapshot()))
        urls = {i["path"]: i["source_url"] for i in items}
        self.assertEqual(urls["Docs/100% done.txt"], f"{BASE_URL}/Docs/100%25%20done.txt")
        self.assertEqual(urls["Docs/100%25 done.txt"], f"{BASE_URL}/Docs/100%2525%20done.txt")
        self.assertEqual(urls["Docs/café.txt"], f"{BASE_URL}/Docs/caf%C3%A9.txt")

    def test_destination_equals_the_path_the_downloader_computes(self):
        _, items = self.items(self.make_manifest(self.import_snapshot()))
        mapped = [i for i in items if i["source_url"]]
        self.assertGreater(len(mapped), 10)
        for item in mapped:
            self.assertEqual(tod_dl.relative_path(item["source_url"]), PurePosixPath(item["destination"]), item)

    def test_unsafe_duplicate_and_listing_entries_are_rejected_not_lost(self):
        data = (b".:\n\n- 0 ALL_FILES\n- 1 a\n- 2 a\n- 3 ../up\n- 4 ctl\x01.txt\n- 5 tab\tname\n\n"
                b"Docs/../etc:\n\n- 6 evil\n\nDocs/:\n\n- 7 slash\n")
        snap = self.import_snapshot(self.write_listing(data))
        header, items = self.items(self.make_manifest(snap))
        by_line = {i["line"]: i for i in items}
        self.assertEqual(len(items), header["totals"]["files"])
        self.assertEqual(by_line[3]["rule"], "builtin:inventory_listing")
        self.assertEqual(by_line[5]["rule"], "builtin:duplicate_path")
        self.assertIn("line 4", by_line[5]["reason"])
        for line in (6, 7, 8, 12, 16):
            self.assertEqual(by_line[line]["rule"], "builtin:unsafe_path", by_line[line])
            self.assertIsNone(by_line[line]["source_url"])
        self.assertEqual(by_line[4]["disposition"], "deferred")

    def test_nfc_collision_keeps_both_names_and_flags_the_later_one(self):
        composed, decomposed = "café.txt", unicodedata.normalize("NFD", "café.txt")
        data = f".:\n- 1 {composed}\n- 1 {decomposed}\n".encode("utf-8")
        header, items = self.items(self.make_manifest(self.import_snapshot(self.write_listing(data))))
        self.assertEqual([i["flags"] for i in items], [[], ["nfc_collision_with_line:2"]])
        self.assertNotEqual(items[0]["source_url"], items[1]["source_url"])
        self.assertEqual(header["totals"]["nfc_collisions"], 1)

    def test_manifest_holds_size_tokens_only_never_checksums(self):
        _, items = self.items(self.make_manifest(self.import_snapshot()))
        self.assertTrue(all("sha256" not in i and "expected_checksum" not in i for i in items))
        self.assertIn("1.8M", {i["size_token"] for i in items})

    def test_existing_output_is_never_replaced(self):
        snap = self.import_snapshot()
        out = self.make_manifest(snap)
        before = inventory.sha256_file(out / "manifest.jsonl")
        code, _, err = self.run_cli("manifest", "--snapshot", snap, "--policy", POLICY,
                                    "--base-url", BASE_URL, "--output", out)
        self.assertEqual(code, 2)
        self.assertEqual(inventory.sha256_file(out / "manifest.jsonl"), before)
        self.assertEqual([p.name for p in self.tmp.iterdir() if p.name.startswith(".manifest-")], [])

    def test_failed_run_leaves_no_partial_output(self):
        snap = self.import_snapshot()
        code, _, _ = self.run_cli("manifest", "--snapshot", snap, "--policy", self.tmp / "missing.json",
                                  "--base-url", BASE_URL, "--output", self.tmp / "m")
        self.assertEqual(code, 2)
        self.assertFalse((self.tmp / "m").exists())

    def test_unsafe_base_urls_are_rejected(self):
        for bad in ("https://user:pw@h.example/c/data", "https://h.example/c/data?token=1",
                    "https://h.example/c/other", "ftp://h.example/c/data", "https://h.example/data",
                    "https://h.example/c/data#frag", "https://h.example/../data", "https://h.example/c d/data"):
            with self.assertRaises(inventory.InventoryError, msg=bad):
                inventory.parse_base_url(bad)


class PolicyTests(InventoryTestCase):
    def check_bad(self, mutate):
        policy = json.loads(POLICY.read_text())
        mutate(policy)
        path = self.tmp / "bad.json"
        path.write_text(json.dumps(policy))
        with self.assertRaises(inventory.InventoryError):
            inventory.load_policy(path)

    def test_invalid_policies_are_rejected_with_an_error(self):
        cases = [
            lambda p: p.update(unknown=1),
            lambda p: p["rules"][0].update(disposition="skip"),
            lambda p: p["rules"][0].update(reason=" "),
            lambda p: p["rules"][1].update(id="skip-tmp"),
            lambda p: p["rules"][0].update(match={}),
            lambda p: p["rules"][0]["match"].update(extensions=[".TMP"]),
            lambda p: p["rules"][0]["match"].update(extensions="tmp"),
            lambda p: p["rules"][2]["match"].update(path_prefix="Docs/../x"),
            lambda p: p["rules"][2]["match"].update(bogus=["x"]),
            lambda p: p.pop("default"),
            lambda p: p.pop("policy_version"),
            lambda p: p.update(rules=p["rules"] * 200),
        ]
        for mutate in cases:
            self.check_bad(mutate)

    def test_path_prefix_matches_whole_segments_only(self):
        policy = inventory.load_policy(POLICY)
        self.assertEqual(inventory.classify(policy, "Docs/a.txt", "a.txt")[1].id, "docs")
        self.assertEqual(inventory.classify(policy, "Docs2/a.txt", "a.txt")[1].id, "default")

    def test_first_matching_rule_wins(self):
        policy = inventory.load_policy(POLICY)
        self.assertEqual(inventory.classify(policy, "Docs/x.TMP", "x.TMP")[1].id, "skip-tmp")


class QueueExportTests(InventoryTestCase):
    def export(self, manifest: Path, name="urls.txt", *extra):
        return self.run_cli("queue", "--manifest", manifest, "--output", self.tmp / name, *extra)

    def test_every_exported_line_is_accepted_by_the_downloader_queue_reader(self):
        manifest = self.make_manifest(self.import_snapshot())
        code, _, err = self.export(manifest)
        self.assertEqual(code, 0, err)
        rejects = []
        rows = list(tod_dl.read_queues([self.tmp / "urls.txt"], on_reject=lambda u, r: rejects.append((u, r))))
        self.assertEqual(rejects, [])
        self.assertEqual(len(rows), 7)
        self.assertIn("1.8M", {row[2] for row in rows})
        self.assertTrue(all(row[4] is None for row in rows))  # no generation unless the operator asks

    def test_queue_matches_manifest_priority_order_and_is_reproducible(self):
        snap = self.import_snapshot()
        m1, m2 = self.make_manifest(snap, "m1"), self.make_manifest(snap, "m2")
        self.export(m1, "a.txt")
        self.export(m2, "b.txt")
        self.assertEqual((self.tmp / "a.txt").read_bytes(), (self.tmp / "b.txt").read_bytes())
        _, items = self.items(m1)
        expected = [i["source_url"] for i in items if i["disposition"] == "priority"]
        urls = [l.split()[0] for l in (self.tmp / "a.txt").read_text().splitlines() if not l.startswith("#")]
        self.assertEqual(urls, expected)

    def test_generation_token_only_when_requested_and_validated(self):
        manifest = self.make_manifest(self.import_snapshot())
        self.assertEqual(self.export(manifest, "g.txt", "--generation", "snap-1")[0], 0)
        rows = list(tod_dl.read_queues([self.tmp / "g.txt"]))
        self.assertEqual({row[4] for row in rows}, {"snap-1"})
        code, _, err = self.export(manifest, "bad.txt", "--generation", "has space")
        self.assertEqual(code, 2)
        self.assertFalse((self.tmp / "bad.txt").exists())

    def test_deferred_export_and_provenance_sidecar(self):
        manifest = self.make_manifest(self.import_snapshot())
        self.assertEqual(self.export(manifest, "d.txt", "--disposition", "deferred")[0], 0)
        prov = json.loads((self.tmp / "d.txt.provenance.json").read_text())
        self.assertEqual(prov["items"], 4)
        self.assertEqual(prov["queue_sha256"], inventory.sha256_file(self.tmp / "d.txt"))
        self.assertEqual(prov["manifest_sha256"], inventory.sha256_file(manifest / "manifest.jsonl"))

    def test_existing_queue_is_never_replaced_or_updated_in_place(self):
        manifest = self.make_manifest(self.import_snapshot())
        existing = self.tmp / "urls.txt"
        existing.write_text("https://x.example/c/data/keep.txt\n")
        code, _, err = self.export(manifest)
        self.assertEqual(code, 2)
        self.assertEqual(existing.read_text(), "https://x.example/c/data/keep.txt\n")
        self.assertEqual([p.name for p in self.tmp.iterdir() if p.name.startswith(".queue-")], [])

    def test_tampered_manifest_blocks_export(self):
        manifest = self.make_manifest(self.import_snapshot())
        os.chmod(manifest / "manifest.jsonl", 0o644)
        with (manifest / "manifest.jsonl").open("a") as handle:
            handle.write('{"type":"item"}\n')
        code, _, err = self.export(manifest)
        self.assertEqual(code, 2)
        self.assertIn("does not match", err)
        self.assertFalse((self.tmp / "urls.txt").exists())

    def test_unsafe_manifest_url_is_refused_before_any_queue_exists(self):
        for bad in ("https://u:p@h.example/c/data/a", "https://h.example/c/data/a?x=1",
                    "https://h.example/c/data/a b", "https://h.example/c/data/ALL_FILES",
                    "https://h.example/c/other/a"):
            with self.assertRaises(inventory.InventoryError, msg=bad):
                inventory.check_queue_url(bad)

    def test_inputs_are_unchanged_by_the_whole_workflow(self):
        snap = self.import_snapshot()
        hashes = {p: inventory.sha256_file(p) for p in (LISTING, POLICY, snap / "raw.txt", snap / "snapshot.json")}
        self.export(self.make_manifest(snap))
        self.assertEqual({p: inventory.sha256_file(p) for p in hashes}, hashes)


class DiffTests(InventoryTestCase):
    OLD = (b".:\n\n- 1 gone.txt\n- 1.8M same.bin\n- 1.8M size.bin\n- 5 dup.txt\n- 5 dup.txt\n"
           b"- 0 ALL_FILES\n- 3 caf\xc3\xa9.txt\n- 9 stays.txt\n")
    NEW = (b".:\n\n- 1 new.txt\n- 1.8M same.bin\n- 1.9M size.bin\n- 5 dup.txt\n- 2 ALL_FILES\n"
           b"- 3 cafe\xcc\x81.txt\n- 9 stays.txt\n\nDocs/../x:\n\n- 1 evil\n")

    def snapshots(self, old=None, new=None):
        one = self.import_snapshot(self.write_listing(old or self.OLD, "old.txt"))
        shutil.move(str(self.store), str(self.tmp / "store-old"))
        one = next((self.tmp / "store-old" / "snapshots").iterdir())
        two = self.import_snapshot(self.write_listing(new or self.NEW, "new.txt"))
        return one, two

    def run_diff(self, old, new, name="d1"):
        code, out, err = self.run_cli("diff", "--old", old, "--new", new, "--output", self.tmp / name)
        self.assertEqual(code, 0, out + err)
        lines = (self.tmp / name / "diff.jsonl").read_text().splitlines()
        return json.loads(lines[0]), {r["path"]: r for r in map(json.loads, lines[1:])}

    def test_categories_and_reconciliation(self):
        old, new = self.snapshots()
        header, records = self.run_diff(old, new)
        cats = {p: r["category"] for p, r in records.items()}
        self.assertEqual(cats["gone.txt"], "removed")
        self.assertEqual(cats["new.txt"], "added")
        self.assertEqual(cats["size.bin"], "metadata_changed")
        self.assertEqual((records["size.bin"]["old_size_token"], records["size.bin"]["new_size_token"]), ("1.8M", "1.9M"))
        self.assertEqual(cats["dup.txt"], "ambiguous")  # listed twice in the old snapshot
        self.assertEqual(records["dup.txt"]["reasons"], ["duplicate_path_in_old"])
        self.assertEqual(cats["Docs/../x/evil"], "ambiguous")
        self.assertNotIn("same.bin", records)  # unchanged paths are counted, not listed
        self.assertNotIn("ALL_FILES", records)  # the listing file is excluded, not a change
        totals = header["totals"]
        self.assertEqual(totals["categories"]["unchanged"], 2)  # same.bin, stays.txt
        self.assertEqual(totals["old_files"], 8)
        self.assertEqual(totals["old_inventory_listing_entries"], 1)
        self.assertEqual(totals["old_duplicate_lines"], 1)

    def test_unicode_normalization_change_is_ambiguous_not_add_plus_remove(self):
        old, new = self.snapshots()
        _, records = self.run_diff(old, new)
        composed, decomposed = "caf\u00e9.txt", unicodedata.normalize("NFD", "caf\u00e9.txt")
        for name in (composed, decomposed):
            self.assertEqual(records[name]["category"], "ambiguous")
            self.assertEqual(records[name]["reasons"], ["nfc_equivalent_add_remove"])

    def test_equal_rounded_size_is_reported_as_unchanged_metadata_only(self):
        old, new = self.snapshots()
        self.run_diff(old, new)
        report = (self.tmp / "d1" / "report.md").read_text()
        self.assertIn("unchanged inventory metadata", report)
        self.assertIn("does not authorize local deletion", report)
        self.assertIn(inventory.sha256_file(old / "raw.txt"), report)

    def test_diff_is_reproducible_and_leaves_inputs_unchanged(self):
        old, new = self.snapshots()
        before = [inventory.sha256_file(p) for p in (old / "raw.txt", new / "raw.txt")]
        self.run_diff(old, new, "d1")
        self.run_diff(old, new, "d2")
        self.assertEqual((self.tmp / "d1" / "diff.jsonl").read_bytes(), (self.tmp / "d2" / "diff.jsonl").read_bytes())
        self.assertEqual([inventory.sha256_file(p) for p in (old / "raw.txt", new / "raw.txt")], before)
        self.assertNotIn("generated_utc", (self.tmp / "d1" / "diff.jsonl").read_text())

    def test_identical_snapshots_have_no_changes(self):
        old, _ = self.snapshots()
        header, records = self.run_diff(old, old)
        # Only the path that the listing repeats stays ambiguous; nothing is added, removed, or changed.
        self.assertEqual(set(records), {"dup.txt"})
        self.assertEqual(header["totals"]["categories"],
                         {"added": 0, "removed": 0, "metadata_changed": 0, "ambiguous": 1, "unchanged": 5})

    def test_rejected_snapshot_and_existing_output_are_refused(self):
        old, new = self.snapshots()
        self.run_diff(old, new)
        code, _, err = self.run_cli("diff", "--old", old, "--new", new, "--output", self.tmp / "d1")
        self.assertEqual(code, 2)
        page = self.write_listing(b"<html>error</html>\n", "bad.txt")
        self.run_cli("snapshot", "--input", page, "--store", self.tmp / "bad-store")
        rejected = next((self.tmp / "bad-store" / "rejected").iterdir())
        code, _, err = self.run_cli("diff", "--old", old, "--new", rejected, "--output", self.tmp / "d3")
        self.assertEqual(code, 2)
        self.assertFalse((self.tmp / "d3").exists())


if __name__ == "__main__":
    unittest.main()
