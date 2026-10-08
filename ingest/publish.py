"""
Publishing compacted days into the Iceberg tables, and retiring their staging copy.

Since Task 4, hype_decay.page_hour and hype_decay.page_daily (Iceberg) are the
curated zone. The plain Parquet the ingester and compactor write is staging:
it exists so a day can be published, and is removed once it has been.

Three steps, each recorded on the manifest day row so the row is always true:

    replace_in_iceberg  DELETE that dt from both tables, INSERT it from staging,
                        check the result against the manifest. iceberg_state goes
                        replacing -> published. Two Iceberg commits per table, so
                        between them a reader sees the day EMPTY, never twice --
                        the same "a gap, never a double count" rule compaction
                        follows. A MERGE would be one commit, but Athena's MERGE
                        has no WHEN NOT MATCHED BY SOURCE, so it cannot remove a
                        row the new data no longer has; it is an upsert, not a
                        replacement.
    mark_published      manifest only: for the bulk build, which loaded every
                        day in one pass and verified it before marking.
    retire_staging      delete the day's 25 staging objects and drop every
                        manifest field that names them. Refuses unless the day
                        row says published.
"""

import datetime as dt

from botocore.exceptions import ClientError

from compact_day import source_hours_for
from ingest_hour import hour_start_of, parse_source_hour


class NotPublished(Exception):
    pass


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def staging_keys(day):
    """day.parquet first, then the 24 page_hour files of the day, in hour order."""
    keys = [f"curated/page_daily/dt={day}/day.parquet"]
    for h in source_hours_for(day):
        start = hour_start_of(parse_source_hour(h))
        keys.append(f"curated/page_hour/dt={start:%Y-%m-%d}/hour={start:%H}/part-{h}.parquet")
    return keys


def _set_state(table, day, state, condition_status="compacted"):
    try:
        table.update_item(
            Key={"source_hour": f"day#{day}"},
            UpdateExpression="SET iceberg_state = :st, iceberg_state_at = :now"
                             + (", iceberg_published_at = :now" if state == "published" else ""),
            ConditionExpression="#s = :compacted",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":st": state, ":now": now(),
                                       ":compacted": condition_status})
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            raise NotPublished(f"day#{day} is not compacted; cannot mark it {state}") from e
        raise


def mark_published(table, day):
    _set_state(table, day, "published")


def retire_staging(s3, table, bucket, day):
    """Deletes the day's staging objects, then makes the manifest stop naming them."""
    row = table.get_item(Key={"source_hour": f"day#{day}"}).get("Item") or {}
    if row.get("iceberg_state") != "published":
        raise NotPublished(f"day#{day} iceberg_state={row.get('iceberg_state')}: "
                           f"refusing to delete its only other copy")

    keys = staging_keys(day)
    present = []
    for k in keys:
        try:
            s3.head_object(Bucket=bucket, Key=k)
            present.append(k)
        except ClientError as e:
            if e.response["Error"]["Code"] not in ("404", "NoSuchKey", "NotFound"):
                raise
    if present:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in present],
                                                 "Quiet": True})

    # Files first, then the claims about them: a crash in between leaves the
    # manifest naming files that are gone, and a re-run (idempotent) fixes it.
    # The other order would leave files the manifest no longer knows about.
    when = now()
    table.update_item(Key={"source_hour": f"day#{day}"},
                      UpdateExpression="SET staging_removed_at = :now REMOVE key_compacted",
                      ExpressionAttributeValues={":now": when})
    for h in source_hours_for(day):
        table.update_item(Key={"source_hour": h},
                          UpdateExpression=("SET staging_removed_at = :now "
                                            "REMOVE key_page_hour, key_page_daily"),
                          ExpressionAttributeValues={":now": when})
    return len(present)


# --- Athena side --------------------------------------------------------------

def iceberg_day_problems(athena, table, day):
    """The day in Iceberg against the manifest: page_daily rows and views against
    the day row, page_hour rows per hour against each hour row."""
    problems = []
    row = table.get_item(Key={"source_hour": f"day#{day}"}).get("Item") or {}
    got = athena.run(f"SELECT count(*), sum(views) FROM page_daily WHERE dt = DATE '{day}'",
                     fetch=True)["rows"][0]
    want = [str(int(row.get("rows", -1))), str(int(row.get("views", -1)))]
    if got != want:
        problems.append(f"page_daily {day}: iceberg {got}, manifest {want}")
    hourly = {r[0]: int(r[1]) for r in athena.run(
        f"SELECT date_format(hour_start + INTERVAL '1' HOUR, '%Y-%m-%dT%H'), count(*) "
        f"FROM page_hour WHERE dt = DATE '{day}' GROUP BY 1", fetch=True)["rows"]}
    for h in source_hours_for(day):
        item = table.get_item(Key={"source_hour": h}).get("Item") or {}
        if hourly.get(h, 0) != int(item.get("rows_page_hour", -1)):
            problems.append(f"page_hour {h}: iceberg {hourly.get(h, 0)}, "
                            f"manifest {item.get('rows_page_hour')}")
    return problems


def replace_in_iceberg(athena, table, day):
    """DELETE + INSERT the day in both tables from staging, then verify."""
    _set_state(table, day, "replacing")
    d = f"DATE '{day}'"
    for sql in (
        f"DELETE FROM page_daily WHERE dt = {d}",
        f"INSERT INTO page_daily SELECT project, page_title, dt, views, hours_present "
        f"FROM page_daily_parquet WHERE dt = {d} ORDER BY page_title",
        f"DELETE FROM page_hour WHERE dt = {d}",
        f"INSERT INTO page_hour SELECT project, page_title, hour_start, views, dt "
        f"FROM page_hour_parquet WHERE dt = {d} ORDER BY page_title, hour_start",
    ):
        athena.run(sql)
    problems = iceberg_day_problems(athena, table, day)
    if problems:
        raise RuntimeError(f"day {day} does not match the manifest after replace: {problems}")
    _set_state(table, day, "published")
