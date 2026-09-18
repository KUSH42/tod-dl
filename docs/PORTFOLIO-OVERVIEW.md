# TOD-DL at a glance

Status: 2026-09-18. TOD-DL is a portfolio project. It is neither
production-ready nor fully specification-complete. Use it only for material
that you are authorized to acquire and retain.

## One sentence

TOD-DL is a resumable download manager for a fixed list of `.onion` URLs. It
never overwrites an evidence file, and it writes a signed, tamper-evident
record for each file that it completes.

## The problem

Investigators sometimes must copy a large collection from an unreliable
source that is reachable only through Tor. Transfers stop. Disks fill. A
second copy of a file can silently replace the first. Later, someone must show
that the local file is the one the tool wrote, and that no one changed the
record.

## What TOD-DL does

| Need | What TOD-DL does |
| --- | --- |
| Survive interruptions | Records state in SQLite. Resumes with HTTP Range requests. Recovers from a crash at each step of promotion. |
| Protect evidence | Creates the final file with an exclusive hard link. An existing final is never replaced. A collision goes to a review folder. |
| Prove integrity | Hashes each file with SHA-256. Checks an optional expected digest and the announced size before promotion. |
| Show what happened | Writes hash-chained events and an Ed25519-signed summary. A read-only verifier checks them. |
| Stay in scope | Fixes the selected set. A retry cannot add a later queue item. |
| Protect the host | Stops admission below a 10 GiB free-space reserve. Hashes one file at a time. |
| Protect the operator | Checks Tor stream isolation. Pauses an origin on HTTP 401 or 403 and does not try to get around it. |
| Keep a human in charge | Uncertain cases enter `review_required`. Every control action needs a confirmation. |

## Numbers

| Item | Value | Source |
| --- | --- | --- |
| Source lines (Python) | About 9,900 in `src/`, 6,200 in `tests/` | `wc -l` |
| Automated tests | 322 | `def test_` count in `tests/`; `specs/OPEN-WORK.md` |
| Acquisition evaluation scenarios | 14 of 14 pass (E01 to E14) | `specs/reports/acquisition-tool-evaluation-2026-09-18d.md` |
| Specifications | 18 `SPEC-*.md` files, plus `OPEN-WORK.md` and 6 reports | `ls specs` |
| Source pilot | 5 of 5 items complete; three resume tests byte-identical | `specs/reports/source-pilot-2026-09-18.md` |
| Repository age | 3 days, 135 commits (2026-09-16 to 2026-09-18) | `git log` |
| CI | Tests, syntax checks, gitleaks secret scan | `.github/workflows/ci.yml` |

## For software engineers

- **Crash-safe promotion.** SQLite records the intent before the file
  appears. The file is durable before the provenance event. On restart,
  `reconcile_promotions()` finishes or escalates each interrupted promotion.
  Three named failpoints test the windows.
- **Spec-first process.** Each behavior has a specification. Reviews found
  real defects, for example a URL that produced an absolute path
  (`/%2Fetc/passwd`), and a verifier that skipped the signature check when it
  had a fingerprint only.
- **Engine evaluation.** A local harness with synthetic fixtures (E01 to E14)
  decided the transfer engine. It runs without Tor and without a source
  request.
- **Terminal UI.** A Textual monitor shows a read-only telemetry snapshot. A
  separate same-user control socket, with a capability token, runs confirmed
  actions.
- **Small dependency set.** One crypto library for the writer, one UI
  library for the monitor. Every direct and transitive dependency is pinned
  to an exact version.

## For a recruiter

- The project shows systems design for failure: durable state, recovery, and
  honest limits.
- The project shows security thinking: exclusive file creation, path checks,
  secret redaction, signed records, and a trust anchor outside the record.
- The documentation is unusually candid. `specs/OPEN-WORK.md` and the gap
  table in [AUDIT-RAIL.md](AUDIT-RAIL.md) list what does not work yet, and
  reviews of the code found and fixed real bugs in earlier sessions.
- The repository holds no case data. The commit history was cleaned of real
  names before publication.
- The work is recent and fast: the whole project spans three days of commits.
  Judge it by the specs, tests, and reports, not by age.

## For a customer or reviewer

- **You get:** a record for each completed file with its source URL, HTTP
  status, redirect chain, byte count, SHA-256, and time, plus a verifier that
  you can run yourself.
- **You do not get:** proof that the source was authentic or complete. The
  record proves what the local file was when the tool finalized it.
- **You must supply:** the trust anchor. Keep the public key or its
  fingerprint outside the state directory.
- **Known limits today:** the signing key sits in the state directory by
  default; a hard kill leaves an unsigned session; operator identity is not
  recorded. [AUDIT-RAIL.md](AUDIT-RAIL.md#known-gaps) lists all gaps.
- **Not yet proven:** queues B and C and a bounded production run have not
  run. Do not treat the tool as validated for unattended production use.
- **License:** non-commercial. Personal, educational, and portfolio-review use
  is allowed. Commercial use needs written permission.

## Where to read next

| Goal | File |
| --- | --- |
| Install and run | [README.md](../README.md) |
| How records work, and what they cannot prove | [AUDIT-RAIL.md](AUDIT-RAIL.md) |
| How a file moves from queue to verified final | [CHAIN-OF-CUSTODY.md](CHAIN-OF-CUSTODY.md) |
| What is unfinished | [specs/OPEN-WORK.md](../specs/OPEN-WORK.md) |
| Field evidence | [specs/reports/source-pilot-2026-09-18.md](../specs/reports/source-pilot-2026-09-18.md) |
