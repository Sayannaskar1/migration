

import json
import sqlite3
import threading
import time
from pathlib import Path

_DB_PATH = Path.home() / ".migration_agent_1" / "migration.db"
_conn_local = threading.local()


def _conn() -> sqlite3.Connection:
    """Thread-local SQLite connection."""
    conn = getattr(_conn_local, "conn", None)
    if conn is None:
        _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(_DB_PATH), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        _conn_local.conn = conn
    return conn


def init_db():
    """Create tables if they don't exist."""
    c = _conn()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS projects (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT DEFAULT '',
            sf_account TEXT DEFAULT '',
            sf_user TEXT DEFAULT '',
            sf_password TEXT DEFAULT '',
            sf_warehouse TEXT DEFAULT '',
            sf_role TEXT DEFAULT '',
            sf_database TEXT DEFAULT '',
            sf_schema TEXT DEFAULT '',
            db_hostname TEXT DEFAULT '',
            db_http_path TEXT DEFAULT '',
            db_token TEXT DEFAULT '',
            db_catalog TEXT DEFAULT '',
            db_schema TEXT DEFAULT '',
            created_at REAL,
            updated_at REAL
        );

        CREATE TABLE IF NOT EXISTS runs (
            id TEXT PRIMARY KEY,
            project_id TEXT,
            sf_account TEXT DEFAULT '',
            sf_user TEXT DEFAULT '',
            done INTEGER DEFAULT 0,
            error TEXT,
            progress TEXT DEFAULT '{}',
            cancel INTEGER DEFAULT 0,
            completed_steps TEXT DEFAULT '[]',
            conversions TEXT DEFAULT '[]',
            summary TEXT,
            report TEXT DEFAULT '',
            deploy_allowed INTEGER DEFAULT 0,
            refresh INTEGER DEFAULT 0,
            plan TEXT,
            confidence_scores TEXT,
            storage_report TEXT,
            catalog_ddl TEXT DEFAULT '[]',
            schema_ddl TEXT DEFAULT '[]',
            deploy_results TEXT DEFAULT '[]',
            data_migration_results TEXT DEFAULT '[]',
            sf_password TEXT DEFAULT '',
            sf_warehouse TEXT DEFAULT '',
            sf_role TEXT DEFAULT '',
            sf_database TEXT DEFAULT '',
            sf_schema TEXT DEFAULT '',
            db_hostname TEXT DEFAULT '',
            db_http_path TEXT DEFAULT '',
            db_token TEXT DEFAULT '',
            db_catalog TEXT DEFAULT '',
            db_schema TEXT DEFAULT '',
            s3_bucket TEXT DEFAULT '',
            s3_region TEXT DEFAULT '',
            s3_access_key TEXT DEFAULT '',
            s3_secret_key TEXT DEFAULT '',
            s3_iam_role TEXT DEFAULT '',
            s3_storage_integration TEXT DEFAULT '',
            tmp_dir TEXT,
            created_at REAL,
            updated_at REAL
        );

        CREATE TABLE IF NOT EXISTS ddl_cache (
            creds_hash TEXT PRIMARY KEY,
            project_tree TEXT NOT NULL,
            cached_at REAL
        );

        CREATE INDEX IF NOT EXISTS idx_runs_project ON runs(project_id);
        CREATE INDEX IF NOT EXISTS idx_runs_created ON runs(created_at DESC);
    """)
    c.commit()

    for col, typ, default in [
        ("sf_user", "TEXT", "''"),
        ("s3_bucket", "TEXT", "''"),
        ("s3_region", "TEXT", "''"),
        ("s3_access_key", "TEXT", "''"),
        ("s3_secret_key", "TEXT", "''"),
        ("s3_iam_role", "TEXT", "''"),
        ("s3_storage_integration", "TEXT", "''"),
    ]:
        try:
            c.execute(f"ALTER TABLE runs ADD COLUMN {col} {typ} DEFAULT {default}")
            c.commit()
        except sqlite3.OperationalError:
            pass

    for col in ("sf_user", "sf_password", "sf_warehouse", "sf_role", "sf_database", "sf_schema"):
        c.execute(f"""
            UPDATE runs SET {col} = (
                SELECT p.{col} FROM projects p WHERE p.id = runs.project_id
            ) WHERE ({col} = '' OR {col} IS NULL) AND project_id IS NOT NULL
        """)
    c.commit()


# ── Projects ──

def load_projects() -> list[dict]:
    rows = _conn().execute("SELECT * FROM projects ORDER BY CAST(id AS INTEGER)").fetchall()
    return [_row_to_dict(r) for r in rows]


def save_project(project: dict):
    c = _conn()
    now = time.time()
    project.setdefault("created_at", now)
    project["updated_at"] = now
    cols = list(project.keys())
    placeholders = ",".join(["?"] * len(cols))
    col_names = ",".join(cols)
    c.execute(
        f"INSERT OR REPLACE INTO projects ({col_names}) VALUES ({placeholders})",
        [project[k] for k in cols],
    )
    c.commit()


def delete_project(project_id: str):
    _conn().execute("DELETE FROM projects WHERE id = ?", (project_id,))
    _conn().commit()


def get_project(project_id: str) -> dict | None:
    row = _conn().execute("SELECT * FROM projects WHERE id = ?", (project_id,)).fetchone()
    return _row_to_dict(row) if row else None


def next_project_id() -> str:
    row = _conn().execute("SELECT MAX(CAST(id AS INTEGER)) FROM projects").fetchone()
    return str((row[0] or 0) + 1)


# ── Runs ──

def store_run(run_id: str, data: dict):
    """Store a run — data dict is the full run state."""
    c = _conn()
    now = time.time()
    data["updated_at"] = now
    if "created_at" not in data:
        data["created_at"] = now

    # Flatten nested dicts/lists to JSON strings for storage
    row = {
        "id": run_id,
        "project_id": data.get("project_id", ""),
        "sf_account": data.get("sf_account", ""),
        "sf_user": data.get("sf_user", ""),
        "done": int(data.get("done", False)),
        "error": data.get("error"),
        "progress": json.dumps(data.get("progress", {}), default=str),
        "cancel": int(data.get("cancel", False)),
        "completed_steps": json.dumps(data.get("completed_steps", []), default=str),
        "conversions": json.dumps(data.get("conversions", []), default=str),
        "summary": json.dumps(data.get("summary"), default=str) if data.get("summary") else None,
        "report": data.get("report", ""),
        "deploy_allowed": int(data.get("deploy_allowed", False)),
        "refresh": int(data.get("refresh", False)),
        "plan": json.dumps(data.get("plan"), default=str) if data.get("plan") else None,
        "confidence_scores": json.dumps(data.get("confidence_scores"), default=str) if data.get("confidence_scores") else None,
        "storage_report": json.dumps(data.get("storage_report"), default=str) if data.get("storage_report") else None,
        "catalog_ddl": json.dumps(data.get("catalog_ddl", []), default=str),
        "schema_ddl": json.dumps(data.get("schema_ddl", []), default=str),
        "deploy_results": json.dumps(data.get("deploy_results", []), default=str),
        "data_migration_results": json.dumps(data.get("data_migration_results", []), default=str),
        "sf_password": data.get("sf_password", ""),
        "sf_warehouse": data.get("sf_warehouse", ""),
        "sf_role": data.get("sf_role", ""),
        "sf_database": data.get("sf_database", ""),
        "sf_schema": data.get("sf_schema", ""),
        "db_hostname": data.get("db_hostname", ""),
        "db_http_path": data.get("db_http_path", ""),
        "db_token": data.get("db_token", ""),
        "db_catalog": data.get("db_catalog", ""),
        "db_schema": data.get("db_schema", ""),
        "s3_bucket": data.get("s3_bucket", ""),
        "s3_region": data.get("s3_region", ""),
        "s3_access_key": data.get("s3_access_key", ""),
        "s3_secret_key": data.get("s3_secret_key", ""),
        "s3_iam_role": data.get("s3_iam_role", ""),
        "s3_storage_integration": data.get("s3_storage_integration", ""),
        "tmp_dir": data.get("tmp_dir", ""),
        "created_at": data.get("created_at", now),
        "updated_at": now,
    }

    cols = list(row.keys())
    placeholders = ",".join(["?"] * len(cols))
    col_names = ",".join(cols)
    c.execute(
        f"INSERT OR REPLACE INTO runs ({col_names}) VALUES ({placeholders})",
        [row[k] for k in cols],
    )
    c.commit()


def get_run(run_id: str) -> dict | None:
    row = _conn().execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
    if row is None:
        return None
    return _row_to_run(row)


def list_runs(limit: int = 200) -> list[dict]:
    rows = _conn().execute(
        "SELECT * FROM runs ORDER BY created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    return [_row_to_run(r) for r in rows]


def list_runs_for_project(project_id: str) -> list[dict]:
    rows = _conn().execute(
        "SELECT * FROM runs WHERE project_id = ? ORDER BY created_at DESC",
        (project_id,),
    ).fetchall()
    return [_row_to_run(r) for r in rows]


def delete_run(run_id: str):
    _conn().execute("DELETE FROM runs WHERE id = ?", (run_id,))
    _conn().commit()


def run_exists(run_id: str) -> bool:
    row = _conn().execute("SELECT 1 FROM runs WHERE id = ?", (run_id,)).fetchone()
    return row is not None


# ── DDL Cache ──

def cache_ddl(creds_hash: str, project_tree: dict):
    """Store extracted DDL tree as JSON in SQLite."""
    _conn().execute(
        "INSERT OR REPLACE INTO ddl_cache (creds_hash, project_tree, cached_at) VALUES (?, ?, ?)",
        (creds_hash, json.dumps(project_tree, default=str), time.time()),
    )
    _conn().commit()


def get_cached_ddl(creds_hash: str) -> dict | None:
    row = _conn().execute(
        "SELECT project_tree FROM ddl_cache WHERE creds_hash = ?", (creds_hash,)
    ).fetchone()
    if row:
        return json.loads(row[0])
    return None


def clear_ddl_cache():
    _conn().execute("DELETE FROM ddl_cache")
    _conn().commit()


# ── Helpers ──

_JSON_FIELDS = {
    "progress", "completed_steps", "conversions", "summary", "plan",
    "confidence_scores", "storage_report", "catalog_ddl", "schema_ddl",
    "deploy_results", "data_migration_results",
}


def _row_to_dict(row: sqlite3.Row) -> dict:
    return dict(row)


def _row_to_run(row: sqlite3.Row) -> dict:
    d = dict(row)
    d["done"] = bool(d.get("done"))
    d["cancel"] = bool(d.get("cancel"))
    d["deploy_allowed"] = bool(d.get("deploy_allowed"))
    d["refresh"] = bool(d.get("refresh"))
    for key in _JSON_FIELDS:
        if d.get(key) and isinstance(d[key], str):
            try:
                d[key] = json.loads(d[key])
            except (json.JSONDecodeError, TypeError):
                pass
    return d


# Auto-initialize on import
init_db()
