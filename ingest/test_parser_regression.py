#!/usr/bin/env python3
"""
Regression test for the parser, against the fixture hour.

The numbers this asserts are not a snapshot of our own output. Task 0 measured
2026-09-10 17:00 UTC against the Wikimedia REST API and got 9,991,180 views for
en + en.m -- 4,024,554 desktop + 5,742,163 mobile-web + 224,463 mobile-app, all
agent=user. So this test checks two spec-critical things that a self-comparison
never could:

  * the en + en.m union is complete (miss en.m and you lose about 60%)
  * hour_start = filename hour minus 1 lines up with the hour the REST API
    calls 17:00

It also asserts the hostile-input handling stays honest: malformed rows, bad
integers and undecodable titles are counted, not silently absorbed.

    python3 test_parser_regression.py                       # fetch the fixture from S3
    python3 test_parser_regression.py --fixture local.gz    # use a local copy

Exit 0 means the parser still agrees with the external reference.
"""

import argparse
import gzip
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from ingest_hour import (  # noqa: E402
    PAGE_HOUR_MIN_VIEWS,
    REGRESSION_EXPECTED,
    REGRESSION_HOUR,
    build_tiers,
    hour_start_of,
    parse,
    parse_source_hour,
)

DEFAULT_BUCKET = "hype-decay-curated-820697996849"
FIXTURE_KEY = ("fixtures/raw_48h/dt=2026-09-10/hour=17/"
               "pageviews-20260910-180000.gz")

# Rows the real file does not contain, appended to a copy to prove the hostile
# paths are exercised rather than merely present.
HOSTILE_ROWS = [
    b"en Too_Few_Fields\n",                      # 2 columns, invalid row
    b"en Bad_Count notanumber 0\n",              # non-numeric view count
    b"en Bad_UTF8_\xff\xfe_Title 42 0\n",        # title is not valid UTF-8
    b"en.m Bad_UTF8_\xff\xfe_Title 8 0\n",       # ... and its mobile twin
]


def check(label, got, expected):
    ok = got == expected
    print(f"  [{'ok' if ok else 'FAIL'}]  {label}: {got:,}"
          + ("" if ok else f"  expected {expected:,}"))
    return ok


def fetch_fixture(bucket, dest):
    import boto3
    session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE"))
    print(f"  fetching s3://{bucket}/{FIXTURE_KEY}")
    session.client("s3").download_file(bucket, FIXTURE_KEY, str(dest))
    return dest


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--fixture", default="", help="local .gz instead of the S3 fixture")
    ap.add_argument("--bucket", default=DEFAULT_BUCKET)
    args = ap.parse_args()

    print("=" * 70)
    print(f"PARSER REGRESSION against {REGRESSION_HOUR} (fixture hour)")
    print("=" * 70)

    workdir = Path(tempfile.mkdtemp(prefix="hype-decay-test-"))
    try:
        fixture = Path(args.fixture) if args.fixture else fetch_fixture(
            args.bucket, workdir / "fixture.gz")

        source_hour = parse_source_hour(REGRESSION_HOUR)
        hour_start = hour_start_of(source_hour)
        print(f"  hour_start derived: {hour_start:%Y-%m-%d %H}:00 UTC")

        views, stats = parse(fixture)
        page_hour, _page_daily = build_tiers(views, hour_start)

        print()
        print("  against the Wikimedia REST API figures from Task 0:")
        passed = [
            check("views before exclusions", stats["views_before_exclusions"],
                  REGRESSION_EXPECTED["views_before_exclusions"]),
            check("views kept after exclusions", stats["views_kept"],
                  REGRESSION_EXPECTED["views_kept"]),
            check("distinct titles", len(views),
                  REGRESSION_EXPECTED["distinct_titles"]),
            check(f"page_hour rows (>= {PAGE_HOUR_MIN_VIEWS} views)",
                  len(page_hour["page_title"]),
                  REGRESSION_EXPECTED["rows_page_hour"]),
        ]

        # The parts of the REST comparison that pin the union specifically.
        print()
        print("  desktop/mobile split, pre-exclusion, straight off the REST API:")
        print("    en   == REST desktop/user                       4,024,554")
        print("    en.m == REST mobile-web/user + mobile-app/user   5,742,163 + 224,463")
        passed.append(check("en desktop views", stats["views_en"], 4_024_554))
        passed.append(check("en.m mobile views", stats["views_en_m"], 5_966_626))

        print()
        print("  hostile input handling, on a copy with 4 bad rows appended:")
        hostile = workdir / "hostile.gz"
        with gzip.open(fixture, "rb") as src, gzip.open(hostile, "wb") as out:
            out.write(src.read())
            for row in HOSTILE_ROWS:
                out.write(row)
        _hviews, hstats = parse(hostile)
        passed.append(check("invalid rows skipped", hstats["rows_skipped_invalid"], 1))
        passed.append(check("bad integers skipped", hstats["rows_skipped_bad_int"], 1))
        passed.append(check("undecodable titles skipped",
                            hstats["rows_skipped_bad_utf8"], 1))
        passed.append(check("views behind undecodable titles",
                            hstats["views_skipped_bad_utf8"], 50))
        passed.append(check("good rows unaffected by the bad ones",
                            hstats["views_kept"], stats["views_kept"]))

        print()
        if all(passed):
            print("  all checks passed: the parser still agrees with the external")
            print("  reference, and bad input is counted rather than absorbed.")
            return 0
        print("  REGRESSION: the parser no longer agrees with Task 0 measurements.")
        print("  Do not ship this. Either the parse changed meaning, or the")
        print("  fixture is not the hour it claims to be.")
        return 1
    finally:
        for leftover in workdir.glob("*"):
            leftover.unlink(missing_ok=True)
        workdir.rmdir()


if __name__ == "__main__":
    sys.exit(main())
