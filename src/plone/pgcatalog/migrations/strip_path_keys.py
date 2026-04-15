"""One-shot migration: remove path/path_parent/path_depth from idx JSONB.

After issue #132, these three keys live exclusively in typed columns
(path, parent_path, path_depth).  This script removes them from existing
rows that were written before the cleanup.

Idempotent and batched.  Safe to re-run; safe to interrupt.

Usage (from a shell with a configured psycopg connection):

    from plone.pgcatalog.migrations.strip_path_keys import run
    result = run(conn, batch_size=5000)
    print(result)  # {"batches": N, "rows_updated": M}
"""

import logging


log = logging.getLogger(__name__)

# Rows that still have any of the three keys present in idx
_DIRTY_PREDICATE = "idx ?| ARRAY['path', 'path_parent', 'path_depth']"

_BATCH_SQL = f"""
    WITH batch AS (
        SELECT zoid
        FROM object_state
        WHERE zoid > %(after_zoid)s
          AND idx IS NOT NULL
          AND {_DIRTY_PREDICATE}
        ORDER BY zoid
        LIMIT %(batch_size)s
    )
    UPDATE object_state os
       SET idx = idx - 'path' - 'path_parent' - 'path_depth'
      FROM batch
     WHERE os.zoid = batch.zoid
    RETURNING os.zoid
"""


def run(conn, batch_size: int = 5000) -> dict:
    """Strip path keys from idx in batches.

    The connection is switched to autocommit for the duration of the
    migration so each batch commits independently; the prior autocommit
    state is restored on return (even if an exception escapes).  If the
    caller had an open transaction, ``conn.commit()`` is called first to
    flush pending work before flipping autocommit on.

    Args:
        conn: psycopg connection.  Each batch commits independently --
              the caller's transaction state (if any) is committed first
              before switching to autocommit, and the original autocommit
              flag is restored on exit.
        batch_size: rows per batch.  Default 5000 keeps each UPDATE under
                    ~10 MB WAL and ~1 s on a typical pod.

    Returns: {"batches": int, "rows_updated": int}
    """
    original_autocommit = conn.autocommit
    if not original_autocommit:
        conn.commit()  # flush any pending work; migration needs per-batch commits
        conn.autocommit = True

    after_zoid = -1
    batches = 0
    total = 0

    try:
        while True:
            with conn.cursor() as cur:
                cur.execute(
                    _BATCH_SQL,
                    {
                        "after_zoid": after_zoid,
                        "batch_size": batch_size,
                    },
                )
                zoids = [
                    row[0] if isinstance(row, tuple) else row["zoid"]
                    for row in cur.fetchall()
                ]

            if not zoids:
                break

            batches += 1
            total += len(zoids)
            after_zoid = max(zoids)
            log.info(
                "strip_path_keys: batch %d, %d rows, last zoid=%d, total=%d",
                batches,
                len(zoids),
                after_zoid,
                total,
            )

        log.info("strip_path_keys: done. %d batches, %d rows updated.", batches, total)
        return {"batches": batches, "rows_updated": total}
    finally:
        conn.autocommit = original_autocommit
