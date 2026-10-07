"""Runnable WAL/restore gate for hermes_cli.kanban_snapshot."""
from __future__ import annotations

import contextlib
import sqlite3
import tempfile
from pathlib import Path
from unittest import mock

from hermes_cli import kanban_db as kb
from hermes_cli import kanban_db_connect as kbc
from hermes_cli import kanban_snapshot as snapshot


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="kanban-snapshot-selfcheck-") as raw:
        root = Path(raw)
        paths: dict[str, Path] = {}
        writers: list[sqlite3.Connection] = []
        for index in range(10):
            slug = f"board-{index + 1}"
            path = root / f"{slug}.db"
            paths[slug] = path
            conn = sqlite3.connect(path)
            writers.append(conn)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("CREATE TABLE tasks (id TEXT PRIMARY KEY, status TEXT NOT NULL, body TEXT)")
            conn.execute("INSERT INTO tasks VALUES (?, 'ready', ?)", (f"task-{index + 1}", "committed in WAL"))
            conn.commit()
            assert path.with_name(path.name + "-wal").exists(), "fixture row was not left in WAL"

        def board_path(slug: str) -> Path:
            return paths[slug]

        @contextlib.contextmanager
        def connect(*, board=None, **_kwargs):
            with contextlib.closing(sqlite3.connect(paths[str(board)])) as conn:
                yield conn

        before = {slug: (path.stat().st_mtime_ns, path.read_bytes()) for slug, path in paths.items()}
        with mock.patch.object(kb, "list_boards", return_value=[{"slug": slug} for slug in paths]), \
             mock.patch.object(kb, "board_exists", side_effect=lambda slug: slug in paths), \
             mock.patch.object(kb, "kanban_db_path", side_effect=board_path), \
             mock.patch.object(kbc, "connect_readonly", side_effect=connect):
            result = snapshot.doctor()

        assert result["count"] == 10, result
        assert result["passed"] == 10, result
        assert result["ok"] is True, result
        assert all(item["source_stats"]["tasks"] == 1 for item in result["boards"])
        assert all(item["source_stats"] == item["backup_stats"] for item in result["boards"])
        assert all(item["restore"]["tasks"] == 1 for item in result["boards"])
        after = {slug: (path.stat().st_mtime_ns, path.read_bytes()) for slug, path in paths.items()}
        changed = [slug for slug in paths if before[slug] != after[slug]]
        assert not changed, f"doctor mutated live boards: {changed}"
        for conn in writers:
            conn.close()
    print("PASS: ten WAL-backed boards snapshot, integrity-check, restore, and remain unchanged")


if __name__ == "__main__":
    main()
