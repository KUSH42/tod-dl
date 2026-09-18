#!/usr/bin/env python3
"""Local inventory snapshot parsing, reproducible manifests, and safe queue export.

See specs/SPEC-inventory-snapshot-manifest.md. This module reads local files
only. It never opens the acquisition database and never replaces a file.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
import unicodedata
from pathlib import Path
from typing import Iterator, NamedTuple
from urllib.parse import urlsplit

TOOL_VERSION = "inventory-1"
PARSER_VERSION = "1"
MANIFEST_FORMAT = 1
MAX_LINE_BYTES = 8192
MAX_SNAPSHOT_BYTES = 1 << 30
MAX_LISTED_ISSUES = 1000
MAX_RULES = 256
LINE_CLASSES = ("blank", "header", "directory", "file", "unparsed", "decode_error")
STRUCTURAL_CODES = frozenset({
    "decode_error", "line_too_long", "bad_entry", "bad_size_token", "empty_name", "not_a_header",
})
DISPOSITIONS = ("priority", "deferred", "rejected")
ENTRY_RE = re.compile(r"^([d-]) (\S+) (.*)$", re.S)
SIZE_TOKEN_RE = re.compile(r"^[0-9]+(?:\.[0-9]+)?[KMGTPE]?$")
GENERATION_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
SEGMENT_SAFE = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~")


class InventoryError(Exception):
    """A setup or validation failure that the operator must fix."""


class Entry(NamedTuple):
    line: int
    size_token: str
    directory: str
    name: str


def utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def canonical_json(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def pretty_json(value) -> str:
    return json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True) + "\n"


def write_new(path: Path, data: str, mode: int = 0o444) -> None:
    """Create a file that must not exist yet."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(data)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


# ---------------------------------------------------------------- parsing

def read_lines(handle) -> Iterator[tuple[int, bytes, bool]]:
    """Yield (line number, line bytes, too_long) with bounded memory."""
    number = 0
    while True:
        chunk = handle.readline(MAX_LINE_BYTES + 1)
        if not chunk:
            return
        number += 1
        too_long = len(chunk) > MAX_LINE_BYTES and not chunk.endswith(b"\n")
        if too_long:
            chunk = chunk[:MAX_LINE_BYTES]
            while True:  # discard the rest of the line
                rest = handle.readline(MAX_LINE_BYTES)
                if not rest or rest.endswith(b"\n"):
                    break
        elif chunk.endswith(b"\n"):
            chunk = chunk[:-1]
        yield number, chunk, too_long


class ListingParser:
    """Classify every line of an ls -R style listing into one class."""

    def __init__(self) -> None:
        self.lines = 0
        self.classes = dict.fromkeys(LINE_CLASSES, 0)
        self.issue_counts: dict[str, int] = {}
        self.issues: list[dict] = []
        self._headers: set[str] = set()

    def _issue(self, line: int, code: str, detail: str) -> None:
        self.issue_counts[code] = self.issue_counts.get(code, 0) + 1
        if len(self.issues) < MAX_LISTED_ISSUES:
            self.issues.append({"line": line, "code": code, "detail": detail[:120]})

    def parse(self, handle) -> Iterator[Entry]:
        directory: str | None = None
        expect_header = True
        after_header = False  # a header is followed by one blank separator line
        for number, raw, too_long in read_lines(handle):
            self.lines += 1
            if too_long:
                self.classes["unparsed"] += 1
                self._issue(number, "line_too_long", f"line exceeds {MAX_LINE_BYTES} bytes")
                if expect_header:
                    directory, expect_header = None, False
                continue
            if not raw:
                self.classes["blank"] += 1
                if after_header:
                    after_header = False
                else:
                    expect_header = True
                continue
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError as exc:
                self.classes["decode_error"] += 1
                self._issue(number, "decode_error", f"{exc.reason}; bytes {raw[:64].hex()}")
                if expect_header:
                    directory, expect_header = None, False
                continue
            if expect_header:
                expect_header, after_header = False, False
                name = text[:-1] if text.endswith(":") else None
                if name is not None and name.startswith("./"):
                    name = name[2:]
                if not name:
                    self.classes["unparsed"] += 1
                    self._issue(number, "not_a_header", text)
                    directory = None
                    continue
                self.classes["header"] += 1
                if name in self._headers:
                    self._issue(number, "duplicate_header", name)
                self._headers.add(name)
                directory = "" if name == "." else name
                after_header = True
                continue
            after_header = False
            entry = self._entry(number, text, directory)
            if entry is not None:
                yield entry

    def _entry(self, number: int, text: str, directory: str | None) -> Entry | None:
        match = ENTRY_RE.match(text)
        if match is None:
            code, detail = "bad_entry", text
        elif not match.group(3):
            code, detail = "empty_name", text
        elif not SIZE_TOKEN_RE.match(match.group(2)):
            code, detail = "bad_size_token", match.group(2)
        elif directory is None:
            code, detail = "bad_entry", "entry outside a valid section"
        else:
            if match.group(1) == "d":
                self.classes["directory"] += 1
                return None
            self.classes["file"] += 1
            return Entry(number, match.group(2), directory, match.group(3))
        self.classes["unparsed"] += 1
        self._issue(number, code, detail)
        return None

    def report(self) -> dict:
        if sum(self.classes.values()) != self.lines:
            raise RuntimeError("parse accounting error: class counts do not add up to the line total")
        return {
            "parser_version": PARSER_VERSION,
            "lines": self.lines,
            "classes": dict(self.classes),
            "issue_counts": dict(sorted(self.issue_counts.items())),
            "issue_total": sum(self.issue_counts.values()),
            "issues": self.issues,
            "issues_truncated": sum(self.issue_counts.values()) > len(self.issues),
        }


def structural_issue_total(report: dict) -> int:
    return sum(n for code, n in report["issue_counts"].items() if code in STRUCTURAL_CODES)


# ---------------------------------------------------------------- identity

def encode_segment(segment: str) -> str:
    """Percent-encode UTF-8 bytes; keep only RFC 3986 unreserved characters."""
    return "".join(
        ch if ch in SEGMENT_SAFE else "".join(f"%{b:02X}" for b in ch.encode("utf-8"))
        for ch in segment
    )


def unsafe_path_reason(path: str, name: str) -> str | None:
    if "/" in name:
        return "name contains a slash"
    for segment in path.split("/"):
        if segment in ("", ".", ".."):
            return f"unsafe segment {segment!r}"
        if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in segment):
            return "control character in path"
    return None


def parse_base_url(base_url: str) -> tuple[str, str]:
    """Return (normalized base URL, destination prefix) or raise InventoryError.

    The prefix is COLLECTION, or COLLECTION/data if the base URL ends in /data.
    """
    try:
        parts = urlsplit(base_url)
        port_ok = parts.port is None or parts.port > 0
    except ValueError as exc:
        raise InventoryError(f"invalid base URL: {exc}") from exc
    segments = parts.path.strip("/").split("/")
    if (parts.scheme not in ("http", "https") or not parts.hostname or not port_ok
            or parts.username or parts.password or parts.query or parts.fragment
            or len(segments) not in (1, 2) or not segments[0]
            or (len(segments) == 2 and segments[1] != "data")
            or segments[0] in (".", "..") or not base_url.isascii()
            or any(ch.isspace() for ch in base_url)):
        raise InventoryError("base URL must look like http(s)://HOST/COLLECTION or http(s)://HOST/COLLECTION/data, "
                             "without user information, query, or fragment")
    return f"{parts.scheme}://{parts.netloc}/{'/'.join(segments)}", "/".join(segments)


# ---------------------------------------------------------------- policy

class Rule(NamedTuple):
    id: str
    prefix: tuple[str, ...]
    extensions: tuple[str, ...]
    names: frozenset[str]
    disposition: str
    reason: str


class Policy(NamedTuple):
    version: str
    sha256: str
    rules: tuple[Rule, ...]
    default: Rule


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise InventoryError(f"policy: {message}")


def _check_keys(obj, allowed: set[str], where: str) -> None:
    _require(isinstance(obj, dict), f"{where} must be an object")
    unknown = sorted(set(obj) - allowed)
    _require(not unknown, f"{where} has unknown key(s): {', '.join(unknown)}")


def _outcome(obj: dict, where: str) -> tuple[str, str]:
    _require(obj.get("disposition") in DISPOSITIONS,
             f"{where}.disposition must be one of {', '.join(DISPOSITIONS)}")
    reason = obj.get("reason")
    _require(isinstance(reason, str) and reason.strip() != "", f"{where}.reason must be a non-empty string")
    return obj["disposition"], reason


def load_policy(path: Path) -> Policy:
    try:
        raw = path.read_bytes()
        data = json.loads(raw.decode("utf-8"))
    except (OSError, ValueError) as exc:  # ValueError covers bad UTF-8 and bad JSON
        raise InventoryError(f"policy: cannot read {path}: {exc}") from exc
    _check_keys(data, {"policy_version", "rules", "default"}, "policy")
    version = data.get("policy_version")
    _require(isinstance(version, str) and version != "", "policy_version must be a non-empty string")
    raw_rules = data.get("rules")
    _require(isinstance(raw_rules, list) and len(raw_rules) <= MAX_RULES,
             f"rules must be a list of at most {MAX_RULES} rules")
    rules, ids = [], set()
    for index, item in enumerate(raw_rules):
        where = f"rules[{index}]"
        _check_keys(item, {"id", "match", "disposition", "reason"}, where)
        rule_id = item.get("id")
        _require(isinstance(rule_id, str) and rule_id != "" and rule_id not in ids,
                 f"{where}.id must be a unique non-empty string")
        ids.add(rule_id)
        match = item.get("match")
        _check_keys(match, {"path_prefix", "extensions", "names"}, f"{where}.match")
        _require(bool(match), f"{where}.match must not be empty")
        prefix: tuple[str, ...] = ()
        if "path_prefix" in match:
            value = match["path_prefix"]
            _require(isinstance(value, str) and value != "", f"{where}.match.path_prefix must be a string")
            prefix = tuple(value.split("/"))
            _require(all(seg not in ("", ".", "..") for seg in prefix),
                     f"{where}.match.path_prefix has an empty, '.' or '..' segment")
        extensions = _string_list(match, "extensions", where)
        _require(all(e.startswith(".") and len(e) > 1 and e == e.lower() for e in extensions),
                 f"{where}.match.extensions must be lowercase and start with '.'")
        names = frozenset(n.lower() for n in _string_list(match, "names", where))
        disposition, reason = _outcome(item, where)
        rules.append(Rule(rule_id, prefix, tuple(extensions), names, disposition, reason))
    default = data.get("default")
    _check_keys(default, {"disposition", "reason"}, "default")
    disposition, reason = _outcome(default, "default")
    return Policy(version, hashlib.sha256(raw).hexdigest(), tuple(rules),
                  Rule("default", (), (), frozenset(), disposition, reason))


def _string_list(match: dict, key: str, where: str) -> list[str]:
    if key not in match:
        return []
    value = match[key]
    _require(isinstance(value, list) and value != [] and all(isinstance(v, str) and v for v in value),
             f"{where}.match.{key} must be a non-empty list of strings")
    return value


def classify(policy: Policy, path: str, name: str) -> tuple[int, Rule]:
    segments = path.split("/")
    lowered = name.lower()
    for rank, rule in enumerate(policy.rules):
        if rule.prefix and tuple(segments[:len(rule.prefix)]) != rule.prefix:
            continue
        if rule.extensions and not lowered.endswith(rule.extensions):
            continue
        if rule.names and lowered not in rule.names:
            continue
        return rank, rule
    return len(policy.rules), policy.default


# ---------------------------------------------------------------- snapshot store

def command_snapshot(args) -> int:
    source, store = Path(args.input), Path(args.store)
    if not source.is_file():
        raise InventoryError(f"input is not a regular file: {source}")
    if source.stat().st_size > MAX_SNAPSHOT_BYTES:
        raise InventoryError(f"input is larger than {MAX_SNAPSHOT_BYTES} bytes")
    now = utc_now()
    staging = Path(tempfile.mkdtemp(prefix=".import-", dir=_make_store(store)))
    try:
        raw_path = staging / "raw.txt"
        digest, size = hashlib.sha256(), 0
        with source.open("rb") as src, raw_path.open("wb") as dst:  # copy first: parse the copy, not the live file
            for block in iter(lambda: src.read(1 << 20), b""):
                size += len(block)
                if size > MAX_SNAPSHOT_BYTES:
                    raise InventoryError(f"input grew beyond {MAX_SNAPSHOT_BYTES} bytes while copying")
                digest.update(block)
                dst.write(block)
        sha = digest.hexdigest()
        for found in (store / "snapshots").glob(f"*-{sha[:16]}"), (store / "rejected").glob(f"*-{sha[:16]}"):
            if next(found, None) is not None:
                raise InventoryError(f"a snapshot with SHA-256 {sha} already exists; not replacing it")
        parser = ListingParser()
        with raw_path.open("rb") as handle:
            for _ in parser.parse(handle):
                pass
        report = parser.report()
        accepted = structural_issue_total(report) <= args.max_issues and report["classes"]["file"] >= 1
        meta = {
            "status": "accepted" if accepted else "rejected",
            "sha256": sha, "size_bytes": size, "source_path": str(source),
            "imported_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "tool_version": TOOL_VERSION, "parser_version": PARSER_VERSION,
            "max_issues": args.max_issues,
        }
        write_new(staging / "parse-report.json", pretty_json(report))
        write_new(staging / "snapshot.json", pretty_json(meta))
        os.chmod(raw_path, 0o444)
        final = store / ("snapshots" if accepted else "rejected") / f"{now:%Y-%m-%d}-{sha[:16]}"
        final.parent.mkdir(exist_ok=True)
        if final.exists():
            raise InventoryError(f"snapshot directory exists: {final}")
        os.rename(staging, final)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)  # staging holds only our own partial copy
        raise
    print(f"{meta['status']}: {final}")
    print(f"lines={report['lines']} files={report['classes']['file']} issues={report['issue_total']}")
    return 0 if accepted else 1


def _make_store(store: Path) -> Path:
    store.mkdir(parents=True, exist_ok=True)
    return store


def load_snapshot(directory: Path) -> dict:
    """Load an accepted snapshot and re-verify its raw bytes."""
    try:
        meta = json.loads((directory / "snapshot.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise InventoryError(f"not a snapshot directory: {directory}: {exc}") from exc
    if meta.get("status") != "accepted":
        raise InventoryError(f"snapshot {directory.name} is not accepted; it cannot be a baseline")
    if sha256_file(directory / "raw.txt") != meta["sha256"]:
        raise InventoryError(f"snapshot {directory.name} raw.txt does not match its recorded SHA-256")
    meta["report"] = json.loads((directory / "parse-report.json").read_text(encoding="utf-8"))
    return meta


def command_activate(args) -> int:
    store = Path(args.store)
    prefix = args.snapshot.lower()
    if not re.fullmatch(r"[0-9a-f]{8,64}", prefix):
        raise InventoryError("snapshot must be 8 to 64 hex characters of the SHA-256")
    matches = sorted((store / "snapshots").glob("*/snapshot.json"))
    found = [p.parent for p in matches if json.loads(p.read_text(encoding="utf-8"))["sha256"].startswith(prefix)]
    if not found:
        raise InventoryError("no accepted snapshot matches; rejected snapshots cannot be activated")
    if len(found) > 1:
        raise InventoryError("prefix matches more than one snapshot; give more characters")
    meta = load_snapshot(found[0])
    log = store / "activations.jsonl"
    previous = None
    if log.exists():
        lines = log.read_text(encoding="utf-8").splitlines()
        previous = json.loads(lines[-1])["snapshot_sha256"] if lines else None
    record = {"snapshot_sha256": meta["sha256"], "previous_sha256": previous,
              "activated_utc": utc_now().strftime("%Y-%m-%dT%H:%M:%SZ"), "snapshot_dir": found[0].name}
    fd = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    with os.fdopen(fd, "a", encoding="utf-8", newline="\n") as handle:
        handle.write(canonical_json(record) + "\n")
    print(f"activated {meta['sha256']}")
    return 0


# ---------------------------------------------------------------- manifest

def command_manifest(args) -> int:
    snapshot_dir, output = Path(args.snapshot), Path(args.output)
    meta = load_snapshot(snapshot_dir)
    policy = load_policy(Path(args.policy))
    base, prefix = parse_base_url(args.base_url)
    if output.exists():
        raise InventoryError(f"output exists; not replacing it: {output}")
    if not output.parent.is_dir():
        raise InventoryError(f"output parent directory is missing: {output.parent}")
    staging = Path(tempfile.mkdtemp(prefix=".manifest-", dir=output.parent))
    try:
        totals = _write_manifest(staging, snapshot_dir, meta, policy, base, prefix)
        expected = meta["report"]["classes"]["file"]
        if totals["files"] != expected:
            raise InventoryError(f"totals do not reconcile: manifest {totals['files']} files, "
                                 f"parse report {expected}")
        sidecar = {
            "generated_utc": utc_now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "tool_version": TOOL_VERSION, "python": sys.version.split()[0],
            "snapshot_path": str(snapshot_dir), "policy_path": str(args.policy),
            "manifest_sha256": totals.pop("_sha256"),
        }
        write_new(staging / "manifest.meta.json", pretty_json(sidecar))
        os.chmod(staging, 0o755)
        os.rename(staging, output)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)  # staging holds only our own partial output
        raise
    print(f"manifest: {output}")
    print(" ".join(f"{k}={totals[k]}" for k in ("files", "priority", "deferred", "rejected")))
    return 0


def _write_manifest(staging: Path, snapshot_dir: Path, meta: dict, policy: Policy,
                    base: str, prefix: str) -> dict:
    totals = {"files": 0, "priority": 0, "deferred": 0, "rejected": 0,
              "nfc_collisions": 0, "by_rule": {}}
    buckets: dict[tuple[int, int], object] = {}
    seen: dict[str, int] = {}
    nfc_seen: dict[str, int] = {}
    last_rank = len(policy.rules) + 1
    try:
        with (snapshot_dir / "raw.txt").open("rb") as handle:
            for entry in ListingParser().parse(handle):
                path = f"{entry.directory}/{entry.name}" if entry.directory else entry.name
                item = {"line": entry.line, "path": path, "size_token": entry.size_token,
                        "source_url": None, "destination": None, "flags": []}
                unsafe = unsafe_path_reason(path, entry.name)
                if unsafe:
                    rank, rule_id, disposition, reason = last_rank, "builtin:unsafe_path", "rejected", unsafe
                elif path == "ALL_FILES":
                    rank, rule_id, disposition, reason = last_rank, "builtin:inventory_listing", "rejected", \
                        "root ALL_FILES is the inventory listing itself"
                elif path in seen:
                    rank, rule_id, disposition = last_rank, "builtin:duplicate_path", "rejected"
                    reason = f"same path as line {seen[path]}"
                else:
                    rank, rule = classify(policy, path, entry.name)
                    rule_id, disposition, reason = rule.id, rule.disposition, rule.reason
                    item["source_url"] = f"{base}/" + "/".join(encode_segment(s) for s in path.split("/"))
                    item["destination"] = f"{prefix}/{path}"
                    key = unicodedata.normalize("NFC", path)
                    if key in nfc_seen:
                        item["flags"].append(f"nfc_collision_with_line:{nfc_seen[key]}")
                        totals["nfc_collisions"] += 1
                    else:
                        nfc_seen[key] = entry.line
                seen.setdefault(path, entry.line)
                item.update(disposition=disposition, rule=rule_id, reason=reason, rank=rank, type="item")
                order = DISPOSITIONS.index(disposition)
                if (order, rank) not in buckets:
                    buckets[(order, rank)] = open(staging / f"b-{order}-{rank:04d}.tmp", "w",
                                                  encoding="utf-8", newline="\n")
                buckets[(order, rank)].write(canonical_json(item) + "\n")
                totals["files"] += 1
                totals[disposition] += 1
                totals["by_rule"][rule_id] = totals["by_rule"].get(rule_id, 0) + 1
    finally:
        for handle in buckets.values():
            handle.close()
    header = {"type": "header", "manifest_format": MANIFEST_FORMAT, "snapshot_sha256": meta["sha256"],
              "parser_version": PARSER_VERSION, "policy_version": policy.version,
              "policy_sha256": policy.sha256, "base_url": base, "totals": dict(totals)}
    digest = hashlib.sha256()
    manifest = staging / "manifest.jsonl"
    with manifest.open("wb") as out:
        line = (canonical_json(header) + "\n").encode("ascii")
        out.write(line)
        digest.update(line)
        for key in sorted(buckets):
            part = staging / f"b-{key[0]}-{key[1]:04d}.tmp"
            with part.open("rb") as src:
                for block in iter(lambda: src.read(1 << 20), b""):
                    out.write(block)
                    digest.update(block)
            part.unlink()
    write_new(staging / "manifest.sha256", f"{digest.hexdigest()}  manifest.jsonl\n")
    os.chmod(manifest, 0o444)
    totals["_sha256"] = digest.hexdigest()
    return totals


# ---------------------------------------------------------------- queue export

def verify_manifest(directory: Path) -> str:
    try:
        recorded = (directory / "manifest.sha256").read_text(encoding="utf-8").split()[0]
    except (OSError, IndexError) as exc:
        raise InventoryError(f"not a manifest directory: {directory}: {exc}") from exc
    actual = sha256_file(directory / "manifest.jsonl")
    if actual != recorded:
        raise InventoryError(f"manifest.jsonl does not match manifest.sha256 (recorded {recorded}, actual {actual})")
    return actual


def check_queue_url(url: str) -> None:
    parts = urlsplit(url)
    segments = parts.path.lstrip("/").split("/", 1)
    if (parts.scheme not in ("http", "https") or not parts.hostname or parts.username or parts.password
            or parts.query or parts.fragment or not url.isascii() or any(ch.isspace() for ch in url)
            or len(segments) != 2 or segments[1] in ("", "ALL_FILES", "data/ALL_FILES")):
        raise InventoryError(f"refusing to export an unsafe queue URL: {url!r}")


def command_queue(args) -> int:
    directory, output = Path(args.manifest), Path(args.output)
    sidecar = Path(f"{output}.provenance.json")
    manifest_sha = verify_manifest(directory)
    if args.generation is not None and not GENERATION_RE.match(args.generation):
        raise InventoryError("generation must match [A-Za-z0-9._:-]{1,128}")
    for target in (output, sidecar):
        if target.exists():
            raise InventoryError(f"output exists; not replacing it: {target}")
    if not output.parent.is_dir():
        raise InventoryError(f"output parent directory is missing: {output.parent}")
    fd, tmp_name = tempfile.mkstemp(prefix=".queue-", dir=output.parent)
    tmp, digest, count, seen = Path(tmp_name), hashlib.sha256(), 0, set()
    try:
        with os.fdopen(fd, "wb") as out, (directory / "manifest.jsonl").open(encoding="utf-8") as src:
            def emit(text: str) -> None:
                data = text.encode("ascii")
                out.write(data)
                digest.update(data)
            emit(f"# manifest_sha256={manifest_sha}\n# disposition={args.disposition}\n")
            next(src)  # header line
            for line in src:
                item = json.loads(line)
                if item["disposition"] != args.disposition:
                    continue
                url = item["source_url"]
                check_queue_url(url)
                if url in seen:
                    raise InventoryError(f"manifest lists a URL twice: {url}")
                seen.add(url)
                extra = f" generation={args.generation}" if args.generation else ""
                emit(f"{url} size={item['size_token']}{extra}\n")
                count += 1
        os.chmod(tmp, 0o444)
        os.link(tmp, output)  # fails if the output appeared meanwhile, so it never replaces a queue
    finally:
        tmp.unlink(missing_ok=True)
    provenance = {"manifest_sha256": manifest_sha, "queue_sha256": digest.hexdigest(),
                  "disposition": args.disposition, "items": count, "generation": args.generation,
                  "generated_utc": utc_now().strftime("%Y-%m-%dT%H:%M:%SZ"), "tool_version": TOOL_VERSION}
    write_new(sidecar, pretty_json(provenance))
    print(f"queue: {output} ({count} URLs)")
    return 0


# ---------------------------------------------------------------- snapshot diff

DIFF_CATEGORIES = ("added", "removed", "metadata_changed", "ambiguous", "unchanged")


def _load_sizes(snapshot_dir: Path) -> dict:
    """Read one snapshot into path -> size token, keeping duplicates and unsafe paths apart."""
    side = {"sizes": {}, "dups": set(), "unsafe": set(), "files": 0, "listing": 0, "dup_lines": 0}
    with (snapshot_dir / "raw.txt").open("rb") as handle:
        for entry in ListingParser().parse(handle):
            path = f"{entry.directory}/{entry.name}" if entry.directory else entry.name
            side["files"] += 1
            if path == "ALL_FILES":  # the listing file itself changes with every refresh
                side["listing"] += 1
            elif path in side["sizes"]:
                side["dups"].add(path)
                side["dup_lines"] += 1
            else:
                side["sizes"][path] = entry.size_token
                if unsafe_path_reason(path, entry.name):
                    side["unsafe"].add(path)
    return side


def diff_snapshots(old: dict, new: dict) -> tuple[list[dict], dict]:
    """Classify every path once. Return (records without unchanged, totals)."""
    counts = dict.fromkeys(DIFF_CATEGORIES, 0)
    records: list[dict] = []
    for path in sorted(old["sizes"].keys() | new["sizes"].keys()):
        before, after = old["sizes"].get(path), new["sizes"].get(path)
        reasons = []
        if path in old["dups"]:
            reasons.append("duplicate_path_in_old")
        if path in new["dups"]:
            reasons.append("duplicate_path_in_new")
        if path in old["unsafe"] or path in new["unsafe"]:
            reasons.append("unsafe_path")
        if reasons:
            category = "ambiguous"
        elif before is None:
            category = "added"
        elif after is None:
            category = "removed"
        elif before != after:
            category = "metadata_changed"
        else:
            counts["unchanged"] += 1
            continue
        records.append({"type": "item", "path": path, "category": category,
                        "old_size_token": before, "new_size_token": after, "reasons": reasons})
    # A name that only changed Unicode normalization is not a real add plus remove.
    removed = {}
    for record in records:
        if record["category"] == "removed":
            removed.setdefault(unicodedata.normalize("NFC", record["path"]), []).append(record)
    for record in records:
        twins = removed.get(unicodedata.normalize("NFC", record["path"])) if record["category"] == "added" else None
        if twins:
            for other in (record, *twins):
                other["category"], other["reasons"] = "ambiguous", ["nfc_equivalent_add_remove"]
    for record in records:
        counts[record["category"]] += 1
    totals = {"old_files": old["files"], "new_files": new["files"],
              "old_unique_paths": len(old["sizes"]), "new_unique_paths": len(new["sizes"]),
              "old_duplicate_lines": old["dup_lines"], "new_duplicate_lines": new["dup_lines"],
              "old_inventory_listing_entries": old["listing"], "new_inventory_listing_entries": new["listing"],
              "categories": counts}
    for side in ("old", "new"):
        if totals[f"{side}_files"] != (totals[f"{side}_unique_paths"] + totals[f"{side}_duplicate_lines"]
                                       + totals[f"{side}_inventory_listing_entries"]):
            raise InventoryError(f"diff does not reconcile with the {side} snapshot file count")
    if sum(counts.values()) != len(old["sizes"].keys() | new["sizes"].keys()):
        raise InventoryError("diff categories do not cover every path exactly once")
    return records, totals


def _report_markdown(old_meta: dict, new_meta: dict, totals: dict, records: list[dict], sidecar: dict) -> str:
    def summary(meta):
        report = meta["report"]
        return (f"`{meta['sha256']}` ({meta['imported_utc']}): {report['lines']} lines, "
                f"{report['classes']['file']} files, {report['issue_total']} parse issues")
    rows = ["| Category | Paths |", "| --- | ---: |"]
    rows += [f"| {name} | {totals['categories'][name]} |" for name in DIFF_CATEGORIES]
    by_top: dict[str, dict[str, int]] = {}
    for record in records:
        top = record["path"].split("/", 1)[0] if "/" in record["path"] else "(root)"
        by_top.setdefault(top, {}).setdefault(record["category"], 0)
        by_top[top][record["category"]] += 1
    top_rows = sorted(by_top.items(), key=lambda kv: (-sum(kv[1].values()), kv[0]))[:25]
    lines = [f"# Inventory diff report, {sidecar['generated_utc'][:10]}", "",
             f"- Old snapshot: {summary(old_meta)}", f"- New snapshot: {summary(new_meta)}",
             f"- Parser version: {PARSER_VERSION}; tool version: {TOOL_VERSION}", "",
             "## Totals", "", *rows, "",
             "## Reading this report", "",
             "- A removed path does not authorize local deletion.",
             "- `unchanged` means unchanged inventory metadata. Size tokens are rounded, so an equal token "
             "does not prove equal bytes.",
             "- `ambiguous` paths appear more than once in a listing, cannot map safely, or differ only in "
             "Unicode normalization. Review them by hand.", ""]
    if top_rows:
        lines += [f"## Changes by top-level directory (first {len(top_rows)})", "",
                  "| Directory | Added | Removed | Metadata changed | Ambiguous |", "| --- | ---: | ---: | ---: | ---: |"]
        for top, cats in top_rows:
            lines.append(f"| `{top}` | {cats.get('added', 0)} | {cats.get('removed', 0)} | "
                         f"{cats.get('metadata_changed', 0)} | {cats.get('ambiguous', 0)} |")
        lines.append("")
    return "\n".join(lines)


def command_diff(args) -> int:
    old_dir, new_dir, output = Path(args.old), Path(args.new), Path(args.output)
    old_meta, new_meta = load_snapshot(old_dir), load_snapshot(new_dir)
    if output.exists():
        raise InventoryError(f"output exists; not replacing it: {output}")
    if not output.parent.is_dir():
        raise InventoryError(f"output parent directory is missing: {output.parent}")
    records, totals = diff_snapshots(_load_sizes(old_dir), _load_sizes(new_dir))
    header = {"type": "header", "diff_format": 1, "old_snapshot_sha256": old_meta["sha256"],
              "new_snapshot_sha256": new_meta["sha256"], "parser_version": PARSER_VERSION, "totals": totals}
    body = "".join(canonical_json(x) + "\n" for x in (header, *records))
    sha = hashlib.sha256(body.encode("ascii")).hexdigest()
    sidecar = {"generated_utc": utc_now().strftime("%Y-%m-%dT%H:%M:%SZ"), "tool_version": TOOL_VERSION,
               "python": sys.version.split()[0], "old_snapshot_path": str(old_dir),
               "new_snapshot_path": str(new_dir), "diff_sha256": sha}
    staging = Path(tempfile.mkdtemp(prefix=".diff-", dir=output.parent))
    try:
        write_new(staging / "diff.jsonl", body)
        write_new(staging / "diff.sha256", f"{sha}  diff.jsonl\n")
        write_new(staging / "diff.meta.json", pretty_json(sidecar))
        write_new(staging / "report.md", _report_markdown(old_meta, new_meta, totals, records, sidecar))
        os.chmod(staging, 0o755)
        os.rename(staging, output)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)  # staging holds only our own partial output
        raise
    print(f"diff: {output}")
    print(" ".join(f"{k}={v}" for k, v in totals["categories"].items()))
    return 0


# ---------------------------------------------------------------- command line

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inventory snapshot, manifest, and queue export.")
    sub = parser.add_subparsers(dest="command", required=True)
    snap = sub.add_parser("snapshot", help="import and validate a local listing")
    snap.add_argument("--input", required=True)
    snap.add_argument("--store", required=True)
    snap.add_argument("--max-issues", type=int, default=0)
    snap.set_defaults(func=command_snapshot)
    act = sub.add_parser("activate", help="record an accepted snapshot as the baseline")
    act.add_argument("--store", required=True)
    act.add_argument("--snapshot", required=True, help="SHA-256 or unique prefix")
    act.set_defaults(func=command_activate)
    man = sub.add_parser("manifest", help="write a reproducible manifest")
    man.add_argument("--snapshot", required=True, help="accepted snapshot directory")
    man.add_argument("--policy", required=True)
    man.add_argument("--base-url", required=True)
    man.add_argument("--output", required=True)
    man.set_defaults(func=command_manifest)
    dif = sub.add_parser("diff", help="compare two accepted snapshots")
    dif.add_argument("--old", required=True, help="older accepted snapshot directory")
    dif.add_argument("--new", required=True, help="newer accepted snapshot directory")
    dif.add_argument("--output", required=True)
    dif.set_defaults(func=command_diff)
    que = sub.add_parser("queue", help="export a plain URL queue from a manifest")
    que.add_argument("--manifest", required=True)
    que.add_argument("--output", required=True)
    que.add_argument("--disposition", choices=("priority", "deferred"), default="priority")
    que.add_argument("--generation")
    que.set_defaults(func=command_queue)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except InventoryError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
