#!/usr/bin/env python3
"""
Spot-check page_daily (Iceberg) against the Wikimedia Pageviews REST API.

The REST per-article endpoint at all-access / user is the same source our
pipeline reads -- desktop (en) plus mobile-web and mobile-app (en.m), user agents
only -- so on any day a page's two numbers should be EQUAL, not close. A
mismatch would mean a broken en/en.m union, a shifted hour_start, or a lost
hour.

One REST request per page (politely, with the project User-Agent), one Athena
query for all pages and dates, filtered on dt.

    python3 spot_check_rest.py
"""

import json
import sys
from pathlib import Path
import time
import urllib.parse
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingest"))
from athena import Athena, mib  # noqa: E402

USER_AGENT = "hype-decay-verify/0.1 (MhBaig971@gmail.com)"
REST = ("https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
        "en.wikipedia/all-access/user/{title}/daily/{start}/{end}")

PAGES = ["Taylor_Swift", "Donald_Trump", "ChatGPT", "Cristiano_Ronaldo", "Kamala_Harris"]
DATES = ["2024-11-06", "2025-06-15", "2026-09-10"]


def rest_daily(title):
    url = REST.format(title=urllib.parse.quote(title, safe=""),
                      start=min(DATES).replace("-", ""), end=max(DATES).replace("-", ""))
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as r:
        items = json.load(r)["items"]
    time.sleep(1.0)
    return {f"{i['timestamp'][:4]}-{i['timestamp'][4:6]}-{i['timestamp'][6:8]}": i["views"]
            for i in items}


def main():
    a = Athena()
    titles = ", ".join(f"'{p}'" for p in PAGES)
    days = ", ".join(f"DATE '{d}'" for d in DATES)
    r = a.run(f"SELECT page_title, CAST(dt AS varchar), views FROM page_daily "
              f"WHERE dt IN ({days}) AND page_title IN ({titles})", fetch=True)
    ours = {(t, d): int(v) for t, d, v in r["rows"]}
    print(f"Athena scanned {mib(r['scanned'])} for {len(PAGES)} pages x {len(DATES)} dates")

    bad = 0
    print(f"{'page':<20} {'date':<11} {'ours':>11} {'REST API':>11}")
    for page in PAGES:
        theirs = rest_daily(page)
        for d in DATES:
            o, t = ours.get((page, d)), theirs.get(d)
            ok = o is not None and o == t
            bad += not ok
            print(f"{page:<20} {d:<11} {o if o is not None else '-':>11,} "
                  f"{t if t is not None else '-':>11,}  {'match' if ok else 'MISMATCH'}")
    print(f"{len(PAGES) * len(DATES) - bad} of {len(PAGES) * len(DATES)} match exactly")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
