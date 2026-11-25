"""
Tests for the schema migration logic in app.py.

These are UNIT tests: they test one small, isolated piece of logic
(`_migrate_system_logs_schema`) directly, using a throwaway in-memory
SQLite database -- no Redis, no Kafka, no real app.py "enterprise_telemetry.db"
file involved. That's deliberate: this logic doesn't NEED those things to
run, so the test shouldn't need them either. Fast, isolated, repeatable.
"""
import sqlite3
import pytest

# Import the real functions from your actual app.py -- we're testing the
# real code, not a copy of it.
from app import _get_existing_columns, _migrate_system_logs_schema


def test_migrate_renames_old_column_to_new():
    """
    ARRANGE: build a fake database that has the OLD schema (processed_by),
             mimicking exactly the real bug you hit earlier in this project.
    ACT:     run the migration function against it.
    ASSERT:  the column got renamed, and nothing else broke.
    """
    # ":memory:" means SQLite creates the database purely in RAM, gone the
    # instant this test function ends -- perfect for tests, since we want
    # a clean slate every time, not to touch any real file on disk.
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()

    # Recreate the OLD schema on purpose, exactly like your real bug
    cursor.execute('''CREATE TABLE system_logs
                      (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT,
                       service TEXT, level TEXT, message TEXT,
                       ai_analysis TEXT, processed_by TEXT)''')

    # ACT
    _migrate_system_logs_schema(cursor)

    # ASSERT: the real check -- ask SQLite what columns exist NOW
    columns_after = _get_existing_columns(cursor, "system_logs")
    assert "routed_by" in columns_after
    assert "processed_by" not in columns_after

    conn.close()


def test_migrate_does_nothing_if_already_correct():
    """
    If the schema is ALREADY correct (routed_by exists), the migration
    should be a safe no-op -- running it again should never break anything.
    This is the "idempotent" property we talked about earlier.
    """
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE system_logs
                      (id INTEGER PRIMARY KEY AUTOINCREMENT, routed_by TEXT)''')

    # This should NOT raise an error, even though there's nothing to migrate
    _migrate_system_logs_schema(cursor)

    columns_after = _get_existing_columns(cursor, "system_logs")
    assert "routed_by" in columns_after

    conn.close()


def test_migrate_does_nothing_if_table_missing():
    """
    On a completely fresh install, system_logs doesn't exist yet at all.
    The migration function should just quietly do nothing and let
    CREATE TABLE IF NOT EXISTS (elsewhere in init_db) handle it --
    NOT crash because it can't find a table to migrate.
    """
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()

    # No CREATE TABLE at all -- table genuinely doesn't exist
    _migrate_system_logs_schema(cursor)  # should not raise

    conn.close()


def test_migrate_raises_on_unexpected_schema():
    """
    If the table exists but has NEITHER the old nor the new column name,
    something unexpected happened. We WANT this to raise loudly rather
    than silently doing nothing -- see the earlier discussion on why
    silent failure is worse than a visible crash during migrations.
    """
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE system_logs (id INTEGER, some_other_column TEXT)")

    with pytest.raises(RuntimeError):
        _migrate_system_logs_schema(cursor)

    conn.close()
