"""Runtime-mediated, WAL-safe Kanban board snapshots and restore checks.

The dispatcher and dashboard must snapshot boards through the same board resolver and
SQLite connection lifecycle as normal runtime reads.  Copying ``kanban.db`` bytes is
not safe while WAL contains committed pages, so this module always uses SQLite's
online backup API and never touches a live board's files except for opening them.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import sqlite3
import tempfile
from pathlib import Path
from typing import Any, Iterable, Optional

from hermes_cli import kanban_db as kb
from hermes_cli import kanban_db_connect as kbc


def _row_digest(conn: sqlite3.Connection, table: str = "tasks") -> str:
    """Hash deterministic row values, including WAL-visible rows."""
    columns = [row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    if not columns:
        return hashlib.sha256(b"<missing-table>").hexdigest()
    digest = hashlib.sha256()
    query = f"SELECT * FROM {table} ORDER BY rowid"
    for row in conn.execute(query):
        digest.update(json.dumps(list(row), sort_keys=False, default=str, ensure_ascii=False).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _task_stats(conn: sqlite3.Connection) -> dict[str, Any]:
    """Return count and content digest without mutating the connection."""
    try:
        count = int(conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0])
    except sqlite3.Error:
        count = 0
    return {"tasks": count, "tasks_sha256": _row_digest(conn)}


def snapshot_board(board: str, destination: str | Path) -> dict[str, Any]:
    """Back up one resolved board into *destination* with SQLite's backup API.

    The source is opened through ``kanban_db_connect.connect_closing`` so WAL,
    busy-timeout, text handling, and runtime safety hooks are identical to normal
    board operations.  The destination must be a disposable/new file.
    """
    slug = kb._normalize_board_slug(board)
    if not slug or not kb.board_exists(slug):
        raise ValueError(f"board {board!r} does not exist")
    source_path = kb.kanban_db_path(slug)
    destination = Path(destination).expanduser()
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        raise FileExistsError(destination)
    before = source_path.stat()
    with kbc.connect_readonly(board=slug) as source, contextlib.closing(sqlite3.connect(str(destination))) as target:
        source_stats = _task_stats(source)
        source_journal_mode = str(source.execute("PRAGMA journal_mode").fetchone()[0]).lower()
        source.backup(target)
        target.commit()
        backup_stats = _task_stats(target)
    after = source_path.stat()
    return {
        "board": slug,
        "source": str(source_path),
        "destination": str(destination),
        "source_stats": source_stats,
        "source_journal_mode": source_journal_mode,
        "backup_stats": backup_stats,
        "integrity": integrity_check(destination),
        "source_mtime_unchanged": (before.st_mtime_ns, before.st_size) == (after.st_mtime_ns, after.st_size),
    }


def integrity_check(database: str | Path) -> str:
    """Run SQLite's built-in integrity check on a disposable snapshot."""
    with contextlib.closing(sqlite3.connect(str(Path(database)))) as conn:
        result = conn.execute("PRAGMA integrity_check").fetchone()
    return str(result[0]) if result else ""


def restore_snapshot(snapshot: str | Path, destination: str | Path) -> dict[str, Any]:
    """Restore a snapshot into a new disposable database using the backup API."""
    snapshot = Path(snapshot).expanduser()
    destination = Path(destination).expanduser()
    if not snapshot.is_file():
        raise FileNotFoundError(snapshot)
    if destination.exists():
        raise FileExistsError(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with contextlib.closing(sqlite3.connect(str(snapshot))) as source, contextlib.closing(sqlite3.connect(str(destination))) as target:
        source.backup(target)
        target.commit()
        stats = _task_stats(target)
    return {"destination": str(destination), "integrity": integrity_check(destination), **stats}


def doctor(boards: Optional[Iterable[str]] = None) -> dict[str, Any]:
    """Snapshot, validate, restore, and compare every selected runtime board.

    This function creates only temporary disposable files.  It is intentionally
    read-only with respect to live board databases and reports one result per
    board, making it suitable for a scheduled/runtime health gate.
    """
    selected = list(boards) if boards is not None else [item["slug"] for item in kb.list_boards(include_archived=False)]
    results: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="hermes-kanban-doctor-") as temp:
        root = Path(temp)
        for slug in selected:
            source = kb.kanban_db_path(slug)
            if not source.exists():
                results.append({"board": slug, "status": "missing"})
                continue
            snapshot = root / f"{slug}.snapshot.db"
            restored = root / f"{slug}.restored.db"
            report = snapshot_board(slug, snapshot)
            restore = restore_snapshot(snapshot, restored)
            ok = (
                report["integrity"] == "ok"
                and report["source_stats"] == report["backup_stats"]
                and restore["integrity"] == "ok"
                and restore["tasks"] == report["source_stats"]["tasks"]
                and restore["tasks_sha256"] == report["source_stats"]["tasks_sha256"]
                and report["source_mtime_unchanged"]
            )
            results.append({**report, "restore": restore, "status": "pass" if ok else "fail"})
    return {"boards": results, "count": len(results), "passed": sum(r["status"] == "pass" for r in results), "ok": all(r["status"] == "pass" for r in results)}
