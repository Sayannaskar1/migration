import json
import re
import time
import uuid
import tempfile
import zipfile
import threading
import shutil
import sqlite3
from dotenv import load_dotenv
load_dotenv()
from fastapi import FastAPI, File, UploadFile, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from orchestrator import MigrationOrchestrator
from parser.sql_parser import ObjectType
import database

_BASE_DIR = Path(__file__).resolve().parent
from connectors.snowflake_connector import SnowflakeConnector
from connectors.databricks_connector import DatabricksConnector

_env = Environment(loader=FileSystemLoader(str(_BASE_DIR / "templates")), autoescape=True)

def _tr(name: str, context: dict, status_code: int = 200) -> HTMLResponse:
    if "report" in context and "report_body" not in context:
        context["report_body"] = _report_body(context["report"] or "")
    content = _env.get_template(name).render(context)
    return HTMLResponse(content, status_code=status_code)


def _report_body(html: str) -> str:
    """Strip outer html/head/body wrapper and script tags, scope CSS under .rp to prevent leaks."""
    if not html.strip().startswith("<!DOCTYPE html>"):
        return html
    match = re.search(r"<style>(.*?)</style>", html, re.DOTALL)
    raw_style = match.group(1) if match else ""
    scoped_style = _scope_css(raw_style) if raw_style else ""
    style_tag = f"<style>{scoped_style}</style>" if scoped_style else ""
    match = re.search(r"<body>(.*)</body>", html, re.DOTALL)
    body = match.group(1) if match else html
    body = re.sub(r"<script[^>]*>.*?</script>", "", body, flags=re.DOTALL)
    body = re.sub(r"<script[^>]*/>", "", body)
    return style_tag + '<div class="rp">' + body + "</div>"


def _scope_css(css: str) -> str:
    """Prefix every CSS selector with .rp to scope styles to the report container."""
    out = []
    in_rule = False
    for line in css.split("\n"):
        stripped = line.strip()
        if not stripped:
            out.append(line)
            continue

        if stripped.startswith("@media") or stripped.startswith("@supports"):
            out.append(line)
            continue

        if stripped.startswith("@") and not in_rule:
            out.append(line)
            if not stripped.endswith(";"):
                in_rule = "{" in line
            continue

        # Handle closing brace of a multi-line rule
        if stripped in ("}", "};"):
            out.append(line)
            in_rule = False
            continue

        # If we are inside a multi-line rule body, pass properties through as-is
        if in_rule:
            out.append(line)
            continue

        # Single-line rule: selector{properties}
        if "{" in stripped:
            idx = line.index("{")
            before = line[:idx]
            after = "{" + line[idx + 1:]
            # If the line also has a closing brace, this is a single-line rule
            has_close = "}" in after
            parts = []
            for sel in before.split(","):
                sel = sel.strip()
                if not sel:
                    continue
                if sel == ":root":
                    parts.append(".rp")
                elif sel in ("html", "body"):
                    parts.append(".rp")
                else:
                    parts.append(f".rp {sel}")
            out.append(",".join(parts) + after)
            if not has_close:
                in_rule = True
            continue

        # Property lines inside a rule body
        out.append(line)

    return "\n".join(out)

app = FastAPI(
    title="Snowflake to Databricks Migration Agent",
    description="Enterprise-grade migration platform with 15+ agents: assessment, catalog mapping, SQL transpilation, deployment, rollback, job queue, security, and observability.",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# ── Security: encrypt credentials at rest ──
_KEY_FILE = _BASE_DIR / ".key"
try:
    from cryptography.fernet import Fernet
    if _KEY_FILE.exists():
        _fernet = Fernet(_KEY_FILE.read_bytes())
    else:
        _KEY_FILE.parent.mkdir(parents=True, exist_ok=True)
        _key = Fernet.generate_key()
        _KEY_FILE.write_bytes(_key)
        _fernet = Fernet(_key)
except ImportError:
    _fernet = None

def _encrypt(val: str) -> str:
    if not _fernet or not val:
        return val
    return _fernet.encrypt(val.encode()).decode()

def _decrypt(val: str) -> str:
    if not _fernet or not val or val.startswith("dapi") or val.startswith("snowflake"):
        return val
    try:
        return _fernet.decrypt(val.encode()).decode()
    except Exception:
        return val

def _secure_creds(creds: dict) -> dict:
    secure = dict(creds)
    for key in _SECRET_FIELDS:
        if secure.get(key):
            secure[key] = _encrypt(secure[key])
    return secure

def _restore_creds(creds: dict) -> dict:
    restored = dict(creds)
    for key in _SECRET_FIELDS:
        if restored.get(key):
            restored[key] = _decrypt(restored[key])
    return restored

# ── Job queue ──
_QUEUE_DB = _BASE_DIR / "queue.db"
_queue_conn = sqlite3.connect(str(_QUEUE_DB), check_same_thread=False)
_queue_conn.execute("""
    CREATE TABLE IF NOT EXISTS migration_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT UNIQUE,
        status TEXT DEFAULT 'pending',
        created_at REAL,
        started_at REAL,
        finished_at REAL
    )
""")
_queue_conn.commit()

class MigrationQueue:
    def __init__(self, conn):
        self.conn = conn
        self._lock = threading.Lock()
        self._active = None

    def enqueue(self, run_id: str) -> bool:
        with self._lock:
            cur = self.conn.execute("SELECT status FROM migration_queue WHERE run_id = ?", (run_id,))
            row = cur.fetchone()
            if row and row[0] in ("running", "pending"):
                return False
            self.conn.execute(
                "INSERT OR REPLACE INTO migration_queue (run_id, status, created_at) VALUES (?, 'pending', ?)",
                (run_id, time.time()),
            )
            self.conn.commit()
            return True

    def dequeue(self) -> str | None:
        with self._lock:
            cur = self.conn.execute(
                "SELECT run_id FROM migration_queue WHERE status = 'pending' ORDER BY created_at LIMIT 1"
            )
            row = cur.fetchone()
            if row:
                self.conn.execute("UPDATE migration_queue SET status = 'running', started_at = ? WHERE run_id = ?",
                                  (time.time(), row[0]))
                self.conn.commit()
                self._active = row[0]
                return row[0]
            return None

    def complete(self, run_id: str):
        with self._lock:
            self.conn.execute("UPDATE migration_queue SET status = 'completed', finished_at = ? WHERE run_id = ?",
                              (time.time(), run_id))
            self.conn.commit()
            self._active = None
            self._process_next()

    def fail(self, run_id: str):
        with self._lock:
            self.conn.execute("UPDATE migration_queue SET status = 'failed', finished_at = ? WHERE run_id = ?",
                              (time.time(), run_id))
            self.conn.commit()
            self._active = None
            self._process_next()

    def _process_next(self):
        next_id = self.dequeue()
        if next_id:
            threading.Thread(target=_run_migration_from_queue, args=(next_id,), daemon=True).start()

    def status(self, run_id: str) -> str | None:
        cur = self.conn.execute("SELECT status FROM migration_queue WHERE run_id = ?", (run_id,))
        row = cur.fetchone()
        return row[0] if row else None

    def list_pending(self) -> list[dict]:
        cur = self.conn.execute(
            "SELECT run_id, status, created_at, started_at FROM migration_queue ORDER BY created_at DESC LIMIT 20"
        )
        return [
            {"run_id": r[0], "status": r[1], "created_at": r[2], "started_at": r[3]}
            for r in cur.fetchall()
        ]

_migration_queue = MigrationQueue(_queue_conn)

def _run_migration_from_queue(run_id: str):
    try:
        run = _get_run(run_id)
        if not run:
            _migration_queue.fail(run_id)
            return
        creds = _restore_creds(run)
        ok = _run_migration(run_id, creds)
        if ok:
            _migration_queue.complete(run_id)
        else:
            _migration_queue.fail(run_id)
    except Exception:
        _migration_queue.fail(run_id)

# ── Persistent state (SQLite) ──
_STATE_DIR = Path.home() / ".migration_agent_1"
_RUNS_DIR = _STATE_DIR / "runs"  # temp working dirs during migration, cleaned up after

# In-memory cache: holds PLAINTEXT run state (prevents double-encryption)
_runs: dict[str, dict] = {}
_RUNS_TTL = 3600 * 6  # 6 hours
_RUNS_MAX = 200

def _evict_runs():
    now = time.time()
    expired = [k for k, v in _runs.items() if now - v.get("_ts", 0) > _RUNS_TTL]
    for k in expired:
        _runs.pop(k, None)
    if len(_runs) > _RUNS_MAX:
        oldest = sorted(_runs, key=lambda k: _runs[k].get("_ts", 0))[:len(_runs) - _RUNS_MAX]
        for k in oldest:
            _runs.pop(k, None)

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "version": "2.0.0",
        "service": "Snowflake to Databricks Migration Agent",
    }


def _load_projects() -> list[dict]:
    return [_restore_creds(p) for p in database.load_projects()]

def _get_project(project_id) -> dict | None:
    if not project_id:
        return None
    for p in _load_projects():
        if str(p.get("id")) == str(project_id):
            return p
    return None

def _save_projects(projects: list[dict]):
    for p in projects:
        database.save_project(p)

def _next_id() -> str:
    return database.next_project_id()

_SECRET_FIELDS = {
    "sf_password", "db_token",
    "s3_access_key", "s3_secret_key",
    "azure_sas_token", "gcs_service_account",
}

PROGRESS_TOTAL = 9

# Rich migration execution phases & tasks
MIGRATION_PHASES = [
    {
        "name": "Connect to Snowflake",
        "agent": "Discovery Agent",
        "tasks": [
            "Authenticate",
            "Validate Credentials",
            "Test Connection",
        ],
    },
    {
        "name": "Extract DDL",
        "agent": "Discovery Agent",
        "tasks": [
            "Discover Schemas",
            "Extract Tables",
            "Extract Views",
            "Extract Procedures",
            "Extract Functions",
            "Extract Streams & Tasks",
            "Extract Stages & Pipes",
            "Extract File Formats",
            "Extract Policies & Sequences",
        ],
    },
    {
        "name": "Dependency Analysis",
        "agent": "Dependency Agent",
        "tasks": [
            "Parse SQL",
            "Build AST",
            "Resolve References",
            "Detect Cycles",
            "Determine Execution Order",
            "Generate Dependency Graph",
        ],
    },
    {
        "name": "Capability Analysis",
        "agent": "Capability Agent",
        "tasks": [
            "Detect Unsupported Features",
            "Detect JavaScript Procedures",
            "Detect External Functions",
            "Detect Dynamic SQL",
            "Generate Capability Report",
        ],
    },
    {
        "name": "Translation",
        "agent": "Translator Agent",
        "tasks": [
            "Translate Schemas",
            "Translate Tables",
            "Translate Views",
            "Translate Functions",
            "Translate Procedures",
            "Apply Rule Engine",
            "Convert Semi-Structured",
            "Convert JavaScript",
            "Regex Cleanup",
            "Score Confidence",
        ],
    },
    {
        "name": "AI Verification",
        "agent": "LLM Agent",
        "tasks": [
            "Prepare Prompt",
            "Send to LLM",
            "Receive Response",
            "Apply Suggestions",
            "Generate Confidence",
        ],
    },
    {
        "name": "Validation",
        "agent": "Validator Agent",
        "tasks": [
            "Validate SQL Syntax",
            "Validate Databricks Compatibility",
            "Validate Dependencies",
            "Validate Architecture",
            "Semantic Validation",
        ],
    },
    {
        "name": "Self Healing",
        "agent": "Critic Agent",
        "tasks": [
            "Detect Errors",
            "Classify Errors",
            "Choose Repair Strategy",
            "Retry Translation",
            "Validate Repair",
        ],
    },
    {
        "name": "Generate Report",
        "agent": "Documentation Agent",
        "tasks": [
            "Generate Inventory",
            "Generate Dependency Graph",
            "Generate Statistics",
            "Generate Recommendations",
            "Generate Summary",
            "Save Report",
        ],
    },
]


def _build_rich_progress(
    phase: int = 0,
    task: int = -1,
    detail: str = "",
    current_object: str = "",
    current_schema: str = "",
    current_database: str = "",
    objects_completed: int = 0,
    objects_total: int = 0,
    elapsed: int = 0,
    done: bool = False,
    error: str = "",
    log: list | None = None,
    phases_state: list | None = None,
) -> dict:
    return {
        "phase": phase,
        "task": task,
        "detail": detail,
        "current_object": current_object,
        "current_schema": current_schema,
        "current_database": current_database,
        "objects_completed": objects_completed,
        "objects_total": objects_total,
        "elapsed": elapsed,
        "done": done,
        "error": error,
        "log": log or [],
        "phases": phases_state or _init_phases_state(),
    }


def _init_phases_state() -> list[dict]:
    return [
        {
            "name": p["name"],
            "agent": p["agent"],
            "status": "waiting",
            "tasks": [{"name": t, "status": "waiting"} for t in p["tasks"]],
        }
        for p in MIGRATION_PHASES
    ]


def _set_phase(phases: list[dict], idx: int, status: str):
    if 0 <= idx < len(phases):
        phases[idx]["status"] = status


def _set_task(phases: list[dict], phase_idx: int, task_idx: int, status: str):
    if 0 <= phase_idx < len(phases):
        tasks = phases[phase_idx]["tasks"]
        if 0 <= task_idx < len(tasks):
            tasks[task_idx]["status"] = status


def _set_phase_tasks(phases: list[dict], phase_idx: int, status: str):
    if 0 <= phase_idx < len(phases):
        for t in phases[phase_idx]["tasks"]:
            t["status"] = status

def _strip(val: str | None) -> str:
    return val.strip() if isinstance(val, str) else val or ""

def _strip_project_secrets(project: dict) -> dict:
    return {k: v for k, v in project.items() if k not in _SECRET_FIELDS}


def _run_path(run_id: str) -> Path:
    return _RUNS_DIR / run_id

def _store_run(token: str, data: dict):
    data["_ts"] = time.time()
    _runs[token] = data  # Keep plaintext in memory
    _evict_runs()
    stored = _secure_creds(data)
    database.store_run(token, stored)

def _get_run(token: str) -> dict | None:
    run = _runs.get(token)
    if run is None:
        run = database.get_run(token)
        if run:
            run = _restore_creds(run)
            _runs[token] = run
    return run


# ── DDL cache by Snowflake credentials ──

def _creds_hash(creds: dict) -> str:
    import hashlib
    key = f"{creds['sf_account']}|{creds.get('sf_database','')}|{creds.get('sf_schema','')}|{creds.get('sf_role','')}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


_ARCHITECTURAL_TYPES = {"pipe", "task", "file_format", "stream", "stage", "masking_policy", "row_access_policy", "sequence"}

def _patch_summary(summary: dict | None, conversions: list) -> dict:
    if summary is None:
        return {"objects": 0, "success": 0, "manual_review": 0, "architectural": 0, "issues": 0, "failed": 0, "pass_rate": 0}
    arch_count = sum(1 for c in conversions if c.get("object_type") in _ARCHITECTURAL_TYPES)
    summary["architectural"] = summary.get("architectural", arch_count)
    summary.setdefault("manual_review", 0)
    summary.setdefault("issues", 0)
    return summary


def _build_summary(inventory, validation_results) -> dict:
    total = len(validation_results)
    auto_conv = 0
    manual_review = 0
    architectural = 0
    issues = 0
    failed = 0
    for v in validation_results.values():
        if v.status == "ERROR":
            failed += 1
        elif v.status == "ISSUE":
            issues += 1
        elif v.status == "ARCHITECTURAL CHANGE":
            architectural += 1
        elif v.status == "WARNING":
            manual_review += 1
        elif v.is_pass():
            auto_conv += 1
    pass_rate = round(auto_conv / total * 100) if total else 0
    seen = set()
    tables = []
    for obj in inventory.all_objects:
        key = obj.name.lower()
        if key not in seen:
            seen.add(key)
            vr = validation_results.get(obj.name)
            tables.append({
                "name": obj.name,
                "object_type": obj.object_type,
                "source": str(obj.file_path) if obj.file_path else "",
                "status": vr.status.lower() if vr else "unknown",
                "raw_sql": obj.raw_sql or "",
                "converted_sql": obj.converted_sql or "",
            })
    return {"objects": total, "success": auto_conv, "manual_review": manual_review, "architectural": architectural, "issues": issues, "failed": failed, "pass_rate": pass_rate, "tables": tables}

def _build_conversions(inventory) -> list[dict]:
    result = []
    for obj in inventory.all_objects:
        converted = obj.converted_sql or ""
        is_schema = obj.object_type == "schema"
        entry = {
            "name": f"{obj.object_type}/{obj.name}.sql",
            "object_type": obj.object_type,
            "source_path": str(obj.file_path) if obj.file_path else "",
            "target_path": "",
            "raw_sql": obj.raw_sql or "",
            "converted_sql": converted,
            "approved_sql": converted,
            "review_status": "approved" if is_schema else "pending_review",
            "reviewer_notes": "",
            "issues": "",
            "error": "",
            "status": "converted" if converted else "failed",
        }
        if not converted:
            entry["error"] = "No converted SQL produced"
        result.append(entry)
    return result


def _build_progress(step: int, total: int, message: str, done: bool = False, error: str = "") -> dict:
    return {"step": step, "total": total, "message": message, "done": done, "error": error}

DEPLOY_PHASES = [
    "Initializing Deployment",
    "Infrastructure Setup",
    "Tables",
    "Views",
    "Functions",
    "Procedures",
    "Finalizing",
]

def _build_deploy_progress(
    phase: str = "",
    phase_num: int = 0,
    total_phases: int = len(DEPLOY_PHASES),
    object_idx: int = 0,
    total_objects: int = 0,
    message: str = "",
    current_object: str = "",
    current_sql: str = "",
    elapsed: int = 0,
    done: bool = False,
    error: str = "",
    activity_log: list | None = None,
) -> dict:
    return {
        "phase": phase,
        "phase_num": phase_num,
        "total_phases": total_phases,
        "object_idx": object_idx,
        "total_objects": total_objects,
        "message": message,
        "current_object": current_object,
        "current_sql": current_sql,
        "elapsed": elapsed,
        "done": done,
        "error": error,
        "activity_log": activity_log or [],
    }

_DEFAULT_DEPLOY_PROGRESS = _build_deploy_progress(message="Starting deployment...")

def _deploy_cancelled(run_id: str) -> bool:
    s = _get_run(run_id)
    return s is not None and s.get("deploy_cancel", False)


def _run_deploy_background(run_id: str, mode: str):
    """Run deployment in a background thread, updating deploy_progress."""
    run = _get_run(run_id)
    if not run:
        return

    run["deploy_progress"] = _build_deploy_progress(message="Initializing...")
    _store_run(run_id, run)
    start_time = time.time()
    log_entries = []

    def progress_callback(action: str, data: dict):
        nonlocal log_entries
        current_run = _get_run(run_id)
        if current_run is None:
            return

        # Check for cancellation
        if _deploy_cancelled(run_id):
            raise KeyboardInterrupt("Deployment cancelled by user")

        elapsed = int(time.time() - start_time)
        elapsed = int(time.time() - start_time)
        prog = current_run.get("deploy_progress", {}) or {}
        if action == "phase":
            prog.update({
                "phase": data.get("name", ""),
                "phase_num": data.get("num", 0),
                "total_phases": data.get("total", len(DEPLOY_PHASES)),
            })
        elif action == "deploying":
            prog.update({
                "message": f"Creating {data.get('type', 'object')}: {data.get('name', '')}",
                "current_object": data.get("name", ""),
                "current_sql": data.get("sql", ""),
                "object_idx": data.get("idx", 0),
                "total_objects": data.get("total", 0),
            })
        elif action == "result":
            prog.update({
                "object_idx": data.get("idx", 0),
                "total_objects": data.get("total", 0),
            })
        elif action == "log":
            entry = {
                "time": time.strftime("%H:%M:%S"),
                "message": data.get("message", ""),
                "type": data.get("type", "info"),
            }
            log_entries.append(entry)
            prog["activity_log"] = list(log_entries[-200:])
        prog["elapsed"] = elapsed
        current_run["deploy_progress"] = prog
        _store_run(run_id, current_run)

    try:
        run = _restore_creds(run)
        creds = {
            "db_hostname": run.get("db_hostname", ""),
            "db_http_path": run.get("db_http_path", ""),
            "db_token": run.get("db_token", ""),
            "db_catalog": run.get("db_catalog"),
            "db_schema": run.get("db_schema"),
        }

        objects = []
        skipped = []
        for conv in run.get("conversions", []):
            review_status = conv.get("review_status", "pending_review")
            if review_status != "approved":
                skipped.append({
                    "name": conv.get("name", ""),
                    "object_type": conv.get("object_type", ""),
                    "status": "skipped",
                    "message": f"Skipped — review status: {review_status}",
                })
                continue
            objects.append({
                "name": conv.get("name", ""),
                "object_type": conv.get("object_type", ""),
                "converted_sql": conv.get("approved_sql") or conv.get("converted_sql", ""),
                "raw_sql": conv.get("raw_sql", ""),
            })

        from agents.deployment_agent import DeploymentAgent
        agent = DeploymentAgent()
        is_dry = mode == "dry_run"
        catalog_ddl = list(run.get("catalog_ddl") or [])
        schema_ddl = list(run.get("schema_ddl") or [])

        if not schema_ddl:
            for conv in run.get("conversions", []):
                if conv.get("object_type") == "schema":
                    sql = conv.get("approved_sql") or conv.get("converted_sql", "")
                    if sql:
                        schema_ddl.append(sql)

        target_cat = (creds.get("db_catalog") or "").strip()
        if target_cat and catalog_ddl:
            catalog_ddl = [f"CREATE CATALOG IF NOT EXISTS {target_cat}"]
            seen = set()
            rewritten = []
            for s in schema_ddl:
                parts = s.replace("CREATE SCHEMA IF NOT EXISTS ", "").split(".")
                if len(parts) >= 2:
                    ns = f"{target_cat}.{parts[-1]}"
                    if ns not in seen:
                        rewritten.append(f"CREATE SCHEMA IF NOT EXISTS {ns}")
                        seen.add(ns)
            schema_ddl = rewritten

        results = agent.deploy(objects, creds, dry_run=is_dry,
                               catalog_ddl=catalog_ddl or None, schema_ddl=schema_ddl or None,
                               progress_callback=progress_callback)

        deploy_results = [
            {
                "object": r.object_name,
                "type": r.object_type,
                "status": "dry_run" if is_dry and r.success else "success" if r.success else "error",
                "message": r.error or (f"Would deploy ({r.duration_ms}ms simulated)" if is_dry else f"OK ({r.duration_ms}ms)"),
            }
            for r in results
        ]
        deploy_results.extend(skipped)

        run["deploy_results"] = deploy_results
        elapsed = int(time.time() - start_time)
        prog = _build_deploy_progress(
            phase="Complete",
            phase_num=len(DEPLOY_PHASES),
            total_phases=len(DEPLOY_PHASES),
            object_idx=len(objects) + len(skipped),
            total_objects=max(len(objects) + len(skipped), 1),
            message="Deployment complete!",
            done=True,
            elapsed=elapsed,
            activity_log=log_entries[-200:],
        )
        run["deploy_progress"] = prog
        _store_run(run_id, run)

    except KeyboardInterrupt:
        run = _get_run(run_id)
        if run:
            run["deploy_progress"] = _build_deploy_progress(
                message="Deployment cancelled",
                done=True,
                error="cancelled",
                elapsed=int(time.time() - start_time),
                activity_log=log_entries[-200:],
            )
            _store_run(run_id, run)
    except Exception as e:
        run = _get_run(run_id)
        if run:
            run["deploy_progress"] = _build_deploy_progress(
                message=f"Deployment failed: {e}",
                done=True,
                error=str(e),
                elapsed=int(time.time() - start_time),
                activity_log=log_entries[-200:],
            )
            _store_run(run_id, run)


def _check_cancel(run_id: str) -> bool:
    s = _get_run(run_id)
    return s is not None and s.get("cancel", False)

def _check_cancel_raise(run_id: str):
    if _check_cancel(run_id):
        raise KeyboardInterrupt("Cancelled by user")


def _sql_appendix(conversions: list[dict]) -> str:
    """Generate a SQL appendix from stored conversions for pasting into an LLM."""
    lines = [
        "",
        "=" * 70,
        "  SQL OBJECT DUMP (Original + Converted)",
        "=" * 70,
    ]
    for conv in conversions:
        lines.append("")
        lines.append(f"  Object: {conv.get('name', '?')}")
        raw = conv.get("raw_sql") or ""
        cvt = conv.get("converted_sql") or ""
        if raw:
            lines.append("  --- Original (Snowflake) ---")
            for line in raw.strip().split("\n"):
                lines.append(f"    {line}")
        if cvt:
            lines.append("  --- Converted (Databricks) ---")
            for line in cvt.strip().split("\n"):
                lines.append(f"    {line}")
    return "\n".join(lines)


def _enrich_conversions(conversions: list[dict]) -> list[dict]:
    """Fill in missing raw_sql from source files and ensure review fields exist."""
    for conv in conversions:
        if not conv.get("raw_sql"):
            src = conv.get("source_path") or ""
            if src:
                try:
                    conv["raw_sql"] = Path(src).read_text(encoding="utf-8")
                except Exception:
                    pass
        # Ensure review fields exist for old runs
        if "review_status" not in conv:
            conv["review_status"] = "pending_review"
        if "approved_sql" not in conv:
            conv["approved_sql"] = conv.get("converted_sql", "")
        if "reviewer_notes" not in conv:
            conv["reviewer_notes"] = ""
    return conversions


def _rewrite_names(orchestrator, creds: dict):
    """Rewrite Snowflake db.schema references → Databricks catalog.schema in converted SQL."""
    target_catalog = (creds.get("db_catalog") or "").strip()
    if not target_catalog:
        return

    # Discover source databases and their schemas from the inventory's file paths
    source_dbs: dict[str, list[str]] = {}
    for obj in orchestrator.inventory.all_objects:
        fp = obj.file_path
        parts = fp.parts
        if len(parts) >= 2:
            db = parts[0]
            schema = parts[1]
            source_dbs.setdefault(db, [])
            if schema not in source_dbs[db]:
                source_dbs[db].append(schema)
    if not source_dbs:
        return

    # Detect schema name conflicts (same schema in multiple databases)
    schema_to_dbs = {}
    for db, schemas in source_dbs.items():
        for s in schemas:
            schema_to_dbs.setdefault(s, []).append(db)
    conflicted = {s for s, dbs in schema_to_dbs.items() if len(dbs) > 1}

    # Build mapping: (source_db, source_schema) → new_schema_name
    mapping = {}
    for db, schemas in source_dbs.items():
        for s in schemas:
            new_s = f"{db}_{s}" if s in conflicted else s
            mapping[(db, s)] = new_s

    # Rewrite every object's converted_sql
    for obj in orchestrator.inventory.all_objects:
        sql = obj.converted_sql
        if not sql:
            continue
        for (src_db, src_schema), new_schema in mapping.items():
            # Quoted: `db`.`schema`.
            sql = re.sub(
                rf'`{re.escape(src_db)}`\.`{re.escape(src_schema)}`\.',
                rf'`{target_catalog}`.`{new_schema}`.',
                sql,
            )
            # Double-quoted: "db"."schema".
            sql = re.sub(
                rf'"{re.escape(src_db)}"\."{re.escape(src_schema)}"\.',
                rf'"{target_catalog}"."{new_schema}".',
                sql,
            )
            # Unquoted word-boundary: db.schema.  (followed by identifier)
            sql = re.sub(
                rf'(?<![.\w`"]){re.escape(src_db)}\.{re.escape(src_schema)}\.(?=\w)',
                rf'{target_catalog}.{new_schema}.',
                sql,
            )
        # Rewrite bare schema.object references (e.g. CORE.FN_PRICE_PER_SQFT)
        # to catalog.schema.object when the schema is a known source schema.
        known_schemas = {s: new_s for (_, s), new_s in mapping.items()}
        for bare_schema, catalog_schema in known_schemas.items():
            sql = re.sub(
                rf'(?<![.\w`"])({re.escape(bare_schema)})\.(\w+)(?=[\s(),;]|$)',
                rf'{target_catalog}.{catalog_schema}.\2',
                sql,
            )
        # For objects without a three-part name (bare table name),
        # prepend target_catalog.schema_name if available.
        file_schema = obj.schema_name
        if not file_schema and obj.file_path:
            parts = obj.file_path.parts
            if len(parts) >= 2:
                file_schema = parts[1]
        if file_schema and target_catalog and not re.search(
            r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|EXTERNAL\s+TABLE|VIEW|MATERIALIZED\s+VIEW|FUNCTION|PROCEDURE|STAGE|SEQUENCE)\s+(?:`[^`]+`|\w+)\.(?:`[^`]+`|\w+)\.(?:`[^`]+`|\w+)",
            sql,
        ):
            bare = re.match(
                r"(?i)(CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|EXTERNAL\s+TABLE|VIEW|MATERIALIZED\s+VIEW|FUNCTION|PROCEDURE|STAGE|SEQUENCE)\s+)(`[^`]+`|\w+)",
                sql,
            )
            if bare:
                schema_name = file_schema
                for (src_db, src_schema), new_schema in mapping.items():
                    if src_schema == file_schema:
                        schema_name = new_schema
                        break
                name = bare.group(2).strip("`")
                sql = f"{bare.group(1)}{target_catalog}.{schema_name}.`{name}`{sql[bare.end():]}"
        obj.converted_sql = sql

    # Also rewrite CREATE SCHEMA statements in schema objects
    for obj in orchestrator.inventory.by_type.get(ObjectType.SCHEMA, []):
        sql = obj.converted_sql
        if not sql:
            continue
        src_db = (obj.schema_name or "").strip()
        src_schema = (obj.name or "").strip()
        if not src_db or not src_schema:
            continue
        new_schema = mapping.get((src_db, src_schema), src_schema)
        sql = re.sub(
            rf'CREATE\s+SCHEMA\s+IF\s+NOT\s+EXISTS\s+{re.escape(src_db)}\.{re.escape(src_schema)}',
            f'CREATE SCHEMA IF NOT EXISTS {target_catalog}.{new_schema}',
            sql,
            flags=re.IGNORECASE,
        )
        obj.converted_sql = sql

def _run_migration(run_id: str, creds: dict):
    """Run migration in a background thread, updating rich progress in _runs."""

    start_time = time.time()
    log_entries = []
    ph_state = _init_phases_state()

    def _log(level: str, msg: str):
        ts = time.strftime("%H:%M:%S")
        log_entries.append({"time": ts, "level": level, "message": msg})

    def _emit(detail: str = "", ph: int = 0, tk: int = -1,
              obj: str = "", schema: str = "", db: str = "",
              obj_done: int = 0, obj_total: int = 0):
        if _check_cancel(run_id):
            raise KeyboardInterrupt("Cancelled by user")
        s = _get_run(run_id)
        if not s:
            return
        elapsed = int(time.time() - start_time) if start_time else 0
        s["progress"] = _build_rich_progress(
            phase=ph, task=tk, detail=detail,
            current_object=obj, current_schema=schema, current_database=db,
            objects_completed=obj_done, objects_total=obj_total,
            elapsed=elapsed, log=list(log_entries),
            phases_state=ph_state,
        )
        _store_run(run_id, s)

    def _phase_start(idx: int):
        _set_phase(ph_state, idx, "running")
        _emit(detail=ph_state[idx]["name"], ph=idx)

    def _phase_done(idx: int):
        _set_phase(ph_state, idx, "completed")
        _set_phase_tasks(ph_state, idx, "completed")
        name = ph_state[idx]["name"]
        _log("success", f"{name} complete")
        _emit(detail=f"{name} complete", ph=idx)

    def _task_start(ph: int, tk: int, detail: str = ""):
        _set_phase(ph_state, ph, "running")
        _set_task(ph_state, ph, tk, "running")
        _log("info", f"{detail}")
        _emit(detail=detail, ph=ph, tk=tk)

    def _task_done(ph: int, tk: int):
        _set_task(ph_state, ph, tk, "completed")

    def _task_skip(ph: int, tk: int):
        _set_task(ph_state, ph, tk, "skipped")

    def _log_emit(level: str, msg: str):
        _log(level, msg)
        _emit(detail=msg, ph=0, tk=-1)

    run_data = _get_run(run_id) or {}
    completed = set(run_data.get("completed_steps", []))
    refresh = run_data.get("refresh", False)

    def track_step(step: str):
        completed.add(step)
        s = _get_run(run_id)
        if s:
            s["completed_steps"] = list(completed)
            _store_run(run_id, s)

    sf = None
    try:
        _phase_start(0)
        for ti in range(3):
            _task_start(0, ti, MIGRATION_PHASES[0]["tasks"][ti])
            if ti == 0:
                _log_emit("info", "Connecting to Snowflake account…")
            elif ti == 1:
                _log_emit("info", "Validating connection credentials…")
            elif ti == 2:
                pass
            _task_done(0, ti)
        _check_cancel_raise(run_id)
        sf = SnowflakeConnector(
            account=creds["sf_account"],
            user=creds["sf_user"],
            password=creds["sf_password"],
            warehouse=creds["sf_warehouse"],
            role=creds.get("sf_role") or None,
            database=creds.get("sf_database") or None,
            schema=creds.get("sf_schema") or None,
        )
        sf.test_connection()
        _log_emit("success", "Connected to Snowflake successfully")
        _phase_done(0)

        if "extract_ddl" not in completed:
            _phase_start(1)
            tmp_dir = _RUNS_DIR / run_id
            _task_start(1, 0, "Discovering schemas…")

            ch = _creds_hash(creds)
            project_tree = database.get_cached_ddl(ch)
            if project_tree and not refresh:
                _log_emit("info", "Using cached DDL (same Snowflake config)")
                _task_done(1, 0)
                for ti2 in range(1, 9):
                    _task_skip(1, ti2)
                _log_emit("info", "Skipped extraction — using cache")
            else:
                _task_done(1, 0)
                obj_types = [
                    ("Tables", 1), ("Views", 2), ("Procedures", 3),
                    ("Functions", 4), ("Streams & Tasks", 5),
                    ("Stages & Pipes", 6), ("File Formats", 7),
                    ("Policies & Sequences", 8),
                ]
                def on_extract_progress(db, schema, current, total):
                    s = _get_run(run_id)
                    if s and s.get("cancel"):
                        raise KeyboardInterrupt("Cancelled by user")
                    if s:
                        pct = round(current / max(total, 1) * 100)
                        _emit(
                            detail=f"Extracting {db}.{schema}… ({pct}%)",
                            ph=1, tk=0, schema=schema, db=db,
                            obj_done=current, obj_total=total,
                        )
                result = sf.extract_project(on_progress=on_extract_progress)
                project_tree = result["tree"]
                for ot_name, ot_idx in obj_types:
                    _task_start(1, ot_idx, f"Extracting {ot_name}…")
                    _task_done(1, ot_idx)
                database.cache_ddl(ch, project_tree)
            _phase_done(1)
            track_step("extract_ddl")

        _check_cancel_raise(run_id)
        if "project_load" not in completed:
            _phase_start(2)
            _task_start(2, 0, "Parsing SQL files…")
            _task_done(2, 0)
            output_dir = tmp_dir / "output"
            orchestrator = MigrationOrchestrator(
                project_path="",
                output_dir=str(output_dir),
                project_tree=project_tree,
            )
            orchestrator.target_cloud = creds.get("target_cloud", "aws")
            orchestrator.set_snowflake_connector(sf)
            orchestrator.step_project_loader()
            _log_emit("info", "Project inventory loaded")
            _task_start(2, 1, "Building AST…")
            _task_done(2, 1)
            _task_start(2, 2, "Resolving references…")
            _task_done(2, 2)

            orchestrator.step_dependency_analysis()
            _task_start(2, 3, "Detecting cycles…")
            _task_done(2, 3)
            _task_start(2, 4, "Determining execution order…")
            _task_done(2, 4)
            _task_start(2, 5, "Generating dependency graph…")
            _task_done(2, 5)
            _phase_done(2)
            track_step("project_load")
            track_step("dependency_analysis")

        if "storage_discovery" not in completed:
            orchestrator.step_storage_discovery()
            track_step("storage_discovery")

        if "plan" not in completed:
            orchestrator.step_plan()
            track_step("plan")

        if "platform_analysis" not in completed:
            orchestrator.step_platform_analysis()
            track_step("platform_analysis")

        if "capability_check" not in completed:
            _phase_start(3)
            for ti in range(5):
                _task_start(3, ti, MIGRATION_PHASES[3]["tasks"][ti])
                _task_done(3, ti)
            orchestrator.step_capability_check()
            _phase_done(3)
            track_step("capability_check")

        if "translation" not in completed:
            _phase_start(4)
            obj_count = len(orchestrator.inventory.all_objects) if orchestrator.inventory else 0

            _task_start(4, 0, "Translating schemas…")
            orchestrator.step_sqlglot_transpile()
            _task_done(4, 0)

            _task_start(4, 1, "Translating tables…")
            _task_done(4, 1)

            _task_start(4, 2, "Translating views…")
            _task_done(4, 2)

            _task_start(4, 3, "Translating functions…")
            _task_done(4, 3)

            _task_start(4, 4, "Translating procedures…")
            orchestrator.step_rule_engine()
            _task_done(4, 4)

            _task_start(4, 5, "Applying rule engine…")
            _task_done(4, 5)

            _task_start(4, 6, "Converting semi-structured data…")
            orchestrator.step_semi_structured()
            _task_done(4, 6)

            _task_start(4, 7, "Converting JavaScript UDFs…")
            orchestrator.step_js_conversion()
            _task_done(4, 7)

            _task_start(4, 8, "Regex cleanup…")
            orchestrator.step_regex_cleanup()
            _task_done(4, 8)

            _task_start(4, 9, "Scoring confidence…")
            orchestrator.step_confidence_scoring()
            _task_done(4, 9)

            _phase_done(4)
            track_step("translation")

        _check_cancel_raise(run_id)
        if "llm_verify" not in completed:
            _phase_start(5)
            for ti in range(5):
                _task_start(5, ti, MIGRATION_PHASES[5]["tasks"][ti])
                _task_done(5, ti)
            orchestrator.step_llm_verify()
            _phase_done(5)
            track_step("llm_verify")

        _check_cancel_raise(run_id)
        if "validation" not in completed:
            _phase_start(6)
            _rewrite_names(orchestrator, creds)
            for ti in range(5):
                _task_start(6, ti, MIGRATION_PHASES[6]["tasks"][ti])
                _task_done(6, ti)
            orchestrator.step_validation()
            _phase_done(6)

        _check_cancel_raise(run_id)
        if "self_heal" not in completed:
            _phase_start(7)
            for ti in range(5):
                _task_start(7, ti, MIGRATION_PHASES[7]["tasks"][ti])
                _task_done(7, ti)
            orchestrator.step_llm_review()
            orchestrator.step_self_healing()
            _phase_done(7)
            track_step("self_heal")

        _check_cancel_raise(run_id)
        artifacts = {}
        if "documentation" not in completed:
            _phase_start(8)
            for ti in range(6):
                _task_start(8, ti, MIGRATION_PHASES[8]["tasks"][ti])
                _task_done(8, ti)
            artifacts = orchestrator.step_documentation()
            _phase_done(8)
            track_step("documentation")

        report_path = artifacts.get("report")
        report_text = report_path.read_text() if report_path and report_path.exists() else ""

        has_db_creds = bool(
            creds.get("db_hostname") and creds.get("db_http_path") and
            creds.get("db_token")
        )

        conversions = _build_conversions(orchestrator.inventory)
        summary = _build_summary(orchestrator.inventory, orchestrator.validation_results)
        confidence = orchestrator.confidence_scores
        plan = {
            "complexity": orchestrator.migration_plan.estimated_complexity if orchestrator.migration_plan else "",
            "blockers": orchestrator.migration_plan.blockers if orchestrator.migration_plan else [],
        }

        s = _get_run(run_id)
        if s:
            storage_data = None
            if orchestrator.storage_report:
                r = orchestrator.storage_report
                storage_data = {
                    "total_tables": r.total_tables,
                    "internal_tables": [{"name": t.name, "database": t.database, "schema": t.schema} for t in r.internal_tables],
                    "external_tables": [{"name": t.name, "database": t.database, "schema": t.schema, "storage_location": t.storage_location, "cloud_provider": t.cloud_provider} for t in r.external_tables],
                    "iceberg_tables": [{"name": t.name, "database": t.database, "schema": t.schema} for t in r.iceberg_tables],
                    "stages": [{"name": s.name, "type": s.type, "storage_location": s.storage_location, "cloud_provider": s.cloud_provider} for s in r.stages],
                    "storage_integrations": r.storage_integrations,
                    "cloud_providers": list(r.cloud_providers),
                    "needs_export": r.needs_export,
                    "needs_s3_credentials": r.needs_s3_credentials,
                    "summary": r.summary,
                }
            target_catalog = (creds.get("db_catalog") or "").strip()
            catalog_ddl = []
            schema_ddl = []
            if orchestrator.migration_plan and orchestrator.migration_plan.catalog_mapping:
                cm = orchestrator.migration_plan.catalog_mapping
                catalog_ddl = list(cm.catalog_create_sql or [])
                schema_ddl = list(cm.schema_create_sql or [])
                if target_catalog:
                    catalog_ddl = [f"CREATE CATALOG IF NOT EXISTS {target_catalog}"]
                    schema_ddl = []
                    seen = set()
                    for s in (cm.schema_create_sql or []):
                        parts = s.replace("CREATE SCHEMA IF NOT EXISTS ", "").split(".")
                        if len(parts) >= 2:
                            new_schema = f"{target_catalog}.{parts[-1]}"
                            if new_schema not in seen:
                                schema_ddl.append(f"CREATE SCHEMA IF NOT EXISTS {new_schema}")
                                seen.add(new_schema)
            platform_analysis_list = []
            if orchestrator.platform_analyses:
                for plan_item in orchestrator.platform_analyses:
                    a = plan_item.analysis
                    platform_analysis_list.append({
                        "obj_name": a.obj_name,
                        "object_type": a.object_type,
                        "status": a.status,
                        "recommended_target": a.recommended_target,
                        "automation_percentage": a.automation_percentage,
                        "notes": a.notes,
                        "additional_services": a.additional_services,
                        "manual_steps": a.manual_steps,
                        "converted_sql": a.converted_sql,
                        "deployment_sql": plan_item.deployment_sql,
                        "deployment_artifacts": plan_item.deployment_artifacts,
                    })
            s.update({
                "done": True,
                "completed_steps": list(completed | {"all"}),
                "progress": _build_rich_progress(
                    phase=len(MIGRATION_PHASES)-1, task=0,
                    detail="Migration complete!",
                    elapsed=int(time.time() - start_time),
                    done=True, log=list(log_entries),
                    phases_state=ph_state,
                ),
                "conversions": conversions,
                "catalog_ddl": catalog_ddl,
                "schema_ddl": schema_ddl,
                "summary": summary,
                "report": report_text,
                "deploy_allowed": has_db_creds,
                "confidence_scores": confidence,
                "plan": plan,
                "storage_report": storage_data,
                "platform_analyses": platform_analysis_list,
                "error": None,
                "sf_account": creds.get("sf_account", ""),
                "sf_user": creds.get("sf_user", ""),
                "sf_password": creds.get("sf_password", ""),
                "sf_warehouse": creds.get("sf_warehouse", ""),
                "sf_role": creds.get("sf_role", ""),
                "sf_database": creds.get("sf_database", ""),
                "sf_schema": creds.get("sf_schema", ""),
            })
            if has_db_creds:
                s.update({
                    "db_hostname": creds.get("db_hostname"),
                    "db_http_path": creds.get("db_http_path"),
                    "db_token": creds.get("db_token"),
                    "db_catalog": creds.get("db_catalog") or None,
                    "db_schema": creds.get("db_schema") or None,
                })
            _store_run(run_id, s)

        return True

    except KeyboardInterrupt:
        s = _get_run(run_id)
        if s:
            s["done"] = True
            s["error"] = "Migration cancelled by user."
            s["progress"] = _build_rich_progress(
                done=True, error="Cancelled", log=list(log_entries),
                elapsed=int(time.time() - start_time) if start_time else 0,
                phases_state=ph_state,
            )
            _store_run(run_id, s)
        return False
    except Exception as e:
        s = _get_run(run_id)
        if s:
            s["done"] = True
            s["error"] = str(e)
            s["progress"] = _build_rich_progress(
                done=True, error=str(e), log=list(log_entries),
                elapsed=int(time.time() - start_time) if start_time else 0,
                phases_state=ph_state,
            )
            _store_run(run_id, s)
        return False
    finally:
        if sf:
            sf.close()


# ═══════════════════════════════════════════════════════════════════
# Retry — skip extraction if cached
# ═══════════════════════════════════════════════════════════════════

@app.post("/retry/{run_id}")
async def retry_conversion(run_id: str):
    run = _get_run(run_id)
    if not run:
        return _tr("results.html", {"error": "Run not found", "run_id": None, "deploy_allowed": False, "conversions": [], "report": "", "summary": None, "deploy_results": None}, status_code=404)
    tmp_dir = Path(run["tmp_dir"])
    ch = _creds_hash(run)
    if not database.get_cached_ddl(ch):
        return _tr("results.html", {"error": "No cached extraction found — run a fresh migration", "run_id": None, "deploy_allowed": False, "conversions": [], "report": "", "summary": None, "deploy_results": None}, status_code=400)

    # Clear error before redirect so progress page doesn't show stale failure
    run["done"] = False
    run["error"] = None
    run["progress"] = _build_rich_progress(detail="Re-running from cached DDL...")
    _store_run(run_id, run)

    threading.Thread(target=_rerun_conversion, args=(run_id,), daemon=True).start()
    return RedirectResponse(url=f"/progress/{run_id}", status_code=302)


def _rerun_conversion(run_id: str):
    run = _get_run(run_id)
    if not run:
        return

    start_time = time.time()
    log_entries = []
    ph_state = _init_phases_state()

    def _log(level: str, msg: str):
        log_entries.append({"time": time.strftime("%H:%M:%S"), "level": level, "message": msg})

    def _emit(detail: str = "", ph: int = 0, tk: int = -1):
        s = _get_run(run_id)
        if not s:
            return
        s["progress"] = _build_rich_progress(
            phase=ph, task=tk, detail=detail,
            elapsed=int(time.time() - start_time),
            log=list(log_entries), phases_state=ph_state,
        )
        _store_run(run_id, s)

    def _phase_done(idx: int):
        _set_phase(ph_state, idx, "completed")
        _set_phase_tasks(ph_state, idx, "completed")
        _log("success", f"{ph_state[idx]['name']} complete")
        _emit(detail=f"{ph_state[idx]['name']} complete", ph=idx)

    def _task_done(ph: int, tk: int):
        _set_task(ph_state, ph, tk, "completed")
        _emit(detail=ph_state[ph]["tasks"][tk] if tk < len(ph_state[ph]["tasks"]) else "", ph=ph, tk=tk)

    try:
        # Phase 2: Dependency Analysis (skip connection + extraction)
        _set_phase(ph_state, 0, "completed")
        _set_phase_tasks(ph_state, 0, "completed")
        _set_phase(ph_state, 1, "completed")
        _set_phase_tasks(ph_state, 1, "completed")
        _log("info", "Using cached DDL — skipping connection and extraction")

        tmp_dir = Path(run["tmp_dir"])
        ch = _creds_hash(run)
        project_tree = database.get_cached_ddl(ch)
        output_dir = tmp_dir / "output"

        orchestrator = MigrationOrchestrator(
            project_path="",
            output_dir=str(output_dir),
            project_tree=project_tree,
        )
        orchestrator.target_cloud = creds.get("target_cloud", "aws")

        _set_phase(ph_state, 2, "running")
        _log("info", "Loading project from cached DDL…")
        orchestrator.step_project_loader()
        _log("info", "Analyzing dependencies…")
        orchestrator.step_dependency_analysis()
        _phase_done(2)

        _set_phase(ph_state, 3, "completed")
        _set_phase_tasks(ph_state, 3, "completed")
        _log("info", "Capability analysis skipped (cached)")
        orchestrator.step_capability_check()

        _set_phase(ph_state, 4, "running")
        _log("info", "Translating schemas & SQL…")
        orchestrator.step_sqlglot_transpile()
        orchestrator.step_rule_engine()
        orchestrator.step_semi_structured()
        orchestrator.step_js_conversion()
        orchestrator.step_regex_cleanup()
        _phase_done(4)

        _set_phase(ph_state, 5, "completed")
        _set_phase_tasks(ph_state, 5, "completed")
        orchestrator.step_llm_verify()

        _set_phase(ph_state, 6, "completed")
        _set_phase_tasks(ph_state, 6, "completed")
        orchestrator.step_validation()

        _set_phase(ph_state, 8, "running")
        _log("info", "Generating report…")
        artifacts = orchestrator.step_documentation()

        report_path = artifacts.get("report")
        report_text = report_path.read_text() if report_path and report_path.exists() else ""

        has_db_creds = bool(
            run.get("db_hostname") and run.get("db_http_path") and
            run.get("db_token")
        )

        conversions = _build_conversions(orchestrator.inventory)
        summary = _build_summary(orchestrator.inventory, orchestrator.validation_results)

        s = _get_run(run_id)
        if s:
            _phase_done(8)
            s.update({
                "done": True,
                "progress": _build_rich_progress(
                    phase=len(MIGRATION_PHASES)-1, task=0,
                    detail="Conversion complete!",
                    elapsed=int(time.time() - start_time),
                    done=True, log=list(log_entries),
                    phases_state=ph_state,
                ),
                "conversions": conversions,
                "summary": summary,
                "report": report_text,
                "deploy_allowed": has_db_creds,
                "error": None,
            })
            _store_run(run_id, s)

    except Exception as e:
        s = _get_run(run_id)
        if s:
            s["done"] = True
            s["error"] = str(e)
            s["progress"] = _build_rich_progress(
                done=True, error=str(e), log=list(log_entries),
                elapsed=int(time.time() - start_time),
                phases_state=ph_state,
            )
            _store_run(run_id, s)


# ═══════════════════════════════════════════════════════════════════
# Resume — continue a failed/cancelled migration
# ═══════════════════════════════════════════════════════════════════

@app.post("/api/resume/{run_id}")
async def resume_migration(run_id: str):
    run = _get_run(run_id)
    if not run:
        return JSONResponse({"error": "Run not found"}, status_code=404)
    if run.get("done") and not run.get("error"):
        return JSONResponse({"error": "Migration already completed", "run_id": run_id})

    creds = {
        "sf_account": run.get("sf_account", ""),
        "sf_user": run.get("sf_user", ""),
        "sf_password": run.get("sf_password", ""),
        "sf_warehouse": run.get("sf_warehouse", ""),
        "sf_role": run.get("sf_role"),
        "sf_database": run.get("sf_database"),
        "sf_schema": run.get("sf_schema"),
        "db_hostname": run.get("db_hostname", ""),
        "db_http_path": run.get("db_http_path", ""),
        "db_token": run.get("db_token", ""),
        "db_catalog": run.get("db_catalog"),
        "db_schema": run.get("db_schema"),
    }
    run["done"] = False
    run["error"] = None
    run["progress"] = _build_rich_progress(detail="Resuming migration...")
    _store_run(run_id, run)

    threading.Thread(target=_run_migration, args=(run_id, creds), daemon=True).start()
    return JSONResponse({"ok": True, "run_id": run_id, "url": f"/progress/{run_id}"})


# ═══════════════════════════════════════════════════════════════════
# Project detail page
# ═══════════════════════════════════════════════════════════════════

@app.get("/project/{project_id}", response_class=HTMLResponse)
async def project_detail(project_id: str):
    projects = _load_projects()
    project = next((p for p in projects if p.get("id") == project_id), None)
    if not project:
        return _tr("project.html", {"error": "Project not found", "project": None}, status_code=404)
    # Gather related runs from database — only runs tagged with this project
    db_runs = database.list_runs_for_project(project_id)
    runs = []
    for data in db_runs:
        created = data.get("created_at", 0)
        runs.append({
            "run_id": data["id"],
            "timestamp": time.strftime("%Y-%m-%d %H:%M", time.localtime(created)) if created else "",
            "done": data.get("done", False),
            "error": data.get("error"),
            "summary": data.get("summary"),
        })
    return _tr("project.html", {"project": project, "runs": runs})

def _runs_related(sf_account: str, project: dict) -> bool:
    return sf_account == project.get("sf_account")


@app.get("/api/projects/{project_id}")
async def get_project(project_id: str):
    projects = _load_projects()
    p = next((p for p in projects if p.get("id") == project_id), None)
    if not p:
        return JSONResponse({"error": "Not found"}, status_code=404)
    return JSONResponse(p)


# ═══════════════════════════════════════════════════════════════════
# Index
# ═══════════════════════════════════════════════════════════════════

@app.get("/", response_class=HTMLResponse)
async def index():
    projects = _load_projects()
    safe_projects = [_strip_project_secrets(p) for p in projects]
    return _tr("index.html", {"projects": safe_projects})


# ═══════════════════════════════════════════════════════════════════
# Project CRUD
# ═══════════════════════════════════════════════════════════════════

@app.get("/api/projects")
async def list_projects():
    return JSONResponse(_load_projects())

@app.get("/api/projects/{project_id}/credentials")
async def get_project_credentials(project_id: str):
    projects = _load_projects()
    project = next((p for p in projects if p.get("id") == project_id), None)
    if not project:
        return JSONResponse({"error": "Project not found"}, status_code=404)
    return JSONResponse({k: project.get(k, "") for k in _SECRET_FIELDS})

@app.post("/api/projects/save")
async def save_project(
    name: str = Form(...),
    description: str = Form(""),
    target_cloud: str = Form("aws"),
    sf_account: str = Form(...),
    sf_user: str = Form(...),
    sf_password: str = Form(...),
    sf_warehouse: str = Form(...),
    sf_role: str = Form(None),
    sf_database: str = Form(None),
    sf_schema: str = Form(None),
    db_hostname: str = Form(None),
    db_http_path: str = Form(None),
    db_token: str = Form(None),
    db_catalog: str = Form(None),
    db_schema: str = Form(None),
):
    projects = _load_projects()
    existing = next((p for p in projects if p.get("name") == name), None)
    if existing:
        existing.update({
            "description": _strip(description),
            "target_cloud": _strip(target_cloud),
            "sf_account": _strip(sf_account), "sf_user": _strip(sf_user), "sf_password": _strip(sf_password),
            "sf_warehouse": _strip(sf_warehouse), "sf_role": _strip(sf_role), "sf_database": _strip(sf_database),
            "sf_schema": _strip(sf_schema),
            "db_hostname": _strip(db_hostname), "db_http_path": _strip(db_http_path),
            "db_token": _strip(db_token), "db_catalog": _strip(db_catalog), "db_schema": _strip(db_schema),
        })
        database.save_project(_secure_creds(existing))
        return JSONResponse({"ok": True, "project": _strip_project_secrets(existing), "updated": True})
    pid = database.next_project_id()
    project = {
        "id": pid, "name": _strip(name), "description": _strip(description),
        "target_cloud": _strip(target_cloud),
        "sf_account": _strip(sf_account), "sf_user": _strip(sf_user), "sf_password": _strip(sf_password),
        "sf_warehouse": _strip(sf_warehouse), "sf_role": _strip(sf_role), "sf_database": _strip(sf_database),
        "sf_schema": _strip(sf_schema),
        "db_hostname": _strip(db_hostname), "db_http_path": _strip(db_http_path),
        "db_token": _strip(db_token), "db_catalog": _strip(db_catalog), "db_schema": _strip(db_schema),
    }
    database.save_project(_secure_creds(project))
    return JSONResponse({"ok": True, "project": _strip_project_secrets(project)})

@app.post("/api/projects/delete")
async def delete_project(id: str = Form(...)):
    database.delete_project(id)
    return JSONResponse({"ok": True})


@app.post("/api/projects/update")
async def update_project(
    project_id: str = Form(...),
    name: str = Form(...),
    description: str = Form(""),
    target_cloud: str = Form("aws"),
    sf_account: str = Form(...),
    sf_user: str = Form(...),
    sf_password: str = Form(...),
    sf_warehouse: str = Form(...),
    sf_role: str = Form(None),
    sf_database: str = Form(None),
    sf_schema: str = Form(None),
    db_hostname: str = Form(None),
    db_http_path: str = Form(None),
    db_token: str = Form(None),
    db_catalog: str = Form(None),
    db_schema: str = Form(None),
):
    p = database.get_project(project_id)
    if not p:
        return JSONResponse({"ok": False, "error": "Project not found"})
    p = _restore_creds(p)
    p.update({
        "description": _strip(description),
        "target_cloud": _strip(target_cloud),
        "sf_account": _strip(sf_account), "sf_user": _strip(sf_user), "sf_password": _strip(sf_password),
        "sf_warehouse": _strip(sf_warehouse), "sf_role": _strip(sf_role), "sf_database": _strip(sf_database),
        "sf_schema": _strip(sf_schema),
        "db_hostname": _strip(db_hostname), "db_http_path": _strip(db_http_path),
        "db_token": _strip(db_token), "db_catalog": _strip(db_catalog), "db_schema": _strip(db_schema),
    })
    database.save_project(_secure_creds(p))
    return JSONResponse({"ok": True, "project": _strip_project_secrets(p)})


@app.delete("/api/projects/{project_id}")
async def delete_project_by_id(project_id: str):
    p = database.get_project(project_id)
    if not p:
        return JSONResponse({"ok": False, "error": "Not found"})
    database.delete_project(project_id)
    return JSONResponse({"ok": True})


@app.post("/api/projects/start-migration")
async def start_project_migration(project_id: str = Form(...), refresh: str = Form("false")):
    projects = _load_projects()
    p = next((p for p in projects if p.get("id") == project_id), None)
    if not p:
        return JSONResponse({"ok": False, "error": "Project not found"})
    refresh_flag = refresh.lower() == "true"
    run_id = uuid.uuid4().hex[:12]
    tmp_dir = _RUNS_DIR / run_id
    tmp_dir.mkdir(parents=True, exist_ok=True)
    creds = {
        "target_cloud": p.get("target_cloud", "aws"),
        "sf_account": p.get("sf_account", ""),
        "sf_user": p.get("sf_user", ""),
        "sf_password": p.get("sf_password", ""),
        "sf_warehouse": p.get("sf_warehouse", ""),
        "sf_role": p.get("sf_role", ""),
        "sf_database": p.get("sf_database", ""),
        "sf_schema": p.get("sf_schema", ""),
        "db_hostname": p.get("db_hostname", ""),
        "db_http_path": p.get("db_http_path", ""),
        "db_token": p.get("db_token", ""),
        "db_catalog": p.get("db_catalog", ""),
        "db_schema": p.get("db_schema", ""),
    }
    _store_run(run_id, {
        "tmp_dir": str(tmp_dir),
        "done": False,
        "error": None,
        "progress": _build_rich_progress(detail="Starting migration..."),
        "conversions": [],
        "summary": None,
        "report": "",
        "deploy_allowed": False,
        "sf_account": p.get("sf_account", ""),
        "project_id": project_id,
        "refresh": refresh_flag,
    })
    thread = threading.Thread(target=_run_migration, args=(run_id, creds), daemon=True)
    thread.start()
    return JSONResponse({"ok": True, "run_id": run_id})


# ═══════════════════════════════════════════════════════════════════
# Migration (async with progress)
# ═══════════════════════════════════════════════════════════════════

@app.post("/migrate-snowflake")
async def migrate_from_snowflake(
    sf_account: str = Form(...),
    sf_user: str = Form(...),
    sf_password: str = Form(...),
    sf_warehouse: str = Form(...),
    sf_role: str = Form(None),
    sf_database: str = Form(None),
    sf_schema: str = Form(None),
    db_hostname: str = Form(None),
    db_http_path: str = Form(None),
    db_token: str = Form(None),
    db_catalog: str = Form(None),
    db_schema: str = Form(None),
    target_cloud: str = Form("aws"),
):
    run_id = uuid.uuid4().hex[:12]
    tmp_dir = _RUNS_DIR / run_id
    tmp_dir.mkdir(parents=True, exist_ok=True)

    creds = {
        "sf_account": _strip(sf_account), "sf_user": _strip(sf_user), "sf_password": _strip(sf_password),
        "sf_warehouse": _strip(sf_warehouse), "sf_role": _strip(sf_role), "sf_database": _strip(sf_database),
        "sf_schema": _strip(sf_schema),
        "db_hostname": _strip(db_hostname), "db_http_path": _strip(db_http_path), "db_token": _strip(db_token),
        "db_catalog": _strip(db_catalog), "db_schema": _strip(db_schema),
        "target_cloud": _strip(target_cloud),
    }

    _store_run(run_id, {
        "tmp_dir": str(tmp_dir),
        "done": False,
        "error": None,
        "progress": _build_rich_progress(detail="Starting migration..."),
        "conversions": [],
        "summary": None,
        "report": "",
        "deploy_allowed": False,
        "sf_account": creds["sf_account"],
    })

    t = threading.Thread(target=_run_migration, args=(run_id, creds), daemon=True)
    t.start()

    return RedirectResponse(url=f"/progress/{run_id}", status_code=302)


@app.post("/migrate-file")
async def migrate_from_file(file: UploadFile = File(...)):
    run_id = uuid.uuid4().hex[:12]
    tmp_dir = _RUNS_DIR / run_id
    tmp_dir.mkdir(parents=True, exist_ok=True)
    output_dir = tmp_dir / "output"

    try:
        extract_dir = tmp_dir / "project"
        extract_dir.mkdir(parents=True)
        with zipfile.ZipFile(file.file) as zf:
            for info in zf.infolist():
                dest = (extract_dir / info.filename).resolve()
                if not str(dest).startswith(str(extract_dir.resolve())):
                    raise ValueError(f"Zip entry attempts path traversal: {info.filename}")
                zf.extract(info, extract_dir)
        if not any(extract_dir.iterdir()):
            return _tr("results.html", {
                "error": "Empty project ZIP file.",
                "run_id": None, "deploy_allowed": False,
                "conversions": [], "report": "", "summary": None, "deploy_results": None,
            }, status_code=400)

        orchestrator = MigrationOrchestrator(project_path=str(extract_dir), output_dir=str(output_dir))
        artifacts = orchestrator.run()

        report = output_dir / "reports" / "migration_report.html"
        report_text = report.read_text() if report.exists() else ""

        return _tr("results.html", {
            "run_id": None, "deploy_allowed": False,
            "conversions": _build_conversions(orchestrator.inventory),
            "report": report_text,
            "summary": _build_summary(orchestrator.inventory, orchestrator.validation_results),
            "deploy_results": None, "error": None,
        })

    except Exception as e:
        return _tr("results.html", {
            "error": str(e),
            "run_id": None, "deploy_allowed": False,
            "conversions": [], "report": "", "summary": None, "deploy_results": None,
        }, status_code=500)


# ═══════════════════════════════════════════════════════════════════
# Cancel
# ═══════════════════════════════════════════════════════════════════

@app.post("/api/cancel/{run_id}")
async def cancel_run(run_id: str):
    run = _get_run(run_id)
    if not run:
        return JSONResponse({"error": "Not found"}, status_code=404)
    run["cancel"] = True
    _store_run(run_id, run)
    return JSONResponse({"ok": True})


@app.post("/api/deploy-cancel/{run_id}")
async def cancel_deploy(run_id: str):
    run = _get_run(run_id)
    if not run:
        return JSONResponse({"error": "Not found"}, status_code=404)
    run["deploy_cancel"] = True
    _store_run(run_id, run)
    return JSONResponse({"ok": True})


# ═══════════════════════════════════════════════════════════════════
# Progress page + polling API
# ═══════════════════════════════════════════════════════════════════

@app.get("/progress/{run_id}", response_class=HTMLResponse)
async def progress_page(run_id: str):
    run = _get_run(run_id)
    if not run:
        return _tr("results.html", {
            "error": "Run not found or expired.",
            "run_id": None, "deploy_allowed": False,
            "conversions": [], "report": "", "summary": None, "deploy_results": None,
        }, status_code=404)
    if run.get("done"):
        if run.get("error"):
            return _tr("results.html", {
                "error": run["error"], "run_id": None, "deploy_allowed": False,
                "conversions": [], "report": "", "summary": None, "deploy_results": None,
            })
        report_text = run.get("report", "") or ""
        conversions = _enrich_conversions(run.get("conversions", []) or [])
        summary = _patch_summary(run.get("summary"), conversions)
        return _tr("results.html", {
            "run_id": run_id, "deploy_allowed": run.get("deploy_allowed", False),
            "conversions": conversions, "report": report_text,
            "summary": summary, "deploy_results": run.get("deploy_results"), "error": None,
        })
    return _tr("progress.html", {"run_id": run_id})

@app.get("/api/progress/{run_id}")
async def get_progress(run_id: str):
    run = _get_run(run_id)
    if not run:
        return JSONResponse({"error": "Not found"}, status_code=404)
    prog = run.get("progress")
    if not prog or "phases" not in prog:
        prog = _build_rich_progress(detail="Starting...", phases_state=_init_phases_state())
    return JSONResponse({
        "done": run.get("done", False),
        "progress": prog,
        "error": run.get("error"),
        "cancel": run.get("cancel", False),
    })


@app.get("/api/deploy-progress/{run_id}")
async def get_deploy_progress(run_id: str):
    run = _get_run(run_id)
    if not run:
        return JSONResponse({"error": "Not found"}, status_code=404)
    prog = run.get("deploy_progress") or _build_deploy_progress()
    prog["cancelled"] = run.get("deploy_cancel", False)
    return JSONResponse(prog)


# ═══════════════════════════════════════════════════════════════════
# Results (after background migration completes)
# ═══════════════════════════════════════════════════════════════════

@app.get("/results/{run_id}", response_class=HTMLResponse)
async def get_results(run_id: str, deploying: str = "0"):
    run = _get_run(run_id)
    if not run:
        return _tr("results.html", {
            "error": "Run not found or expired.",
            "run_id": None, "deploy_allowed": False,
            "conversions": [], "report": "", "summary": None, "deploy_results": None,
        }, status_code=404)

    report_text = run.get("report", "") or ""
    conversions = _enrich_conversions(run.get("conversions", []) or [])

    deploy_progress = run.get("deploy_progress") or {}
    is_deploying = deploying == "1" or (deploy_progress.get("done") is False)

    summary = _patch_summary(run.get("summary"), conversions)
    ctx = {
        "run_id": run_id,
        "run": run,
        "deploying": is_deploying,
        "deploy_allowed": run.get("deploy_allowed", False),
        "conversions": conversions,
        "report": report_text,
        "report_body": _report_body(report_text),
        "summary": summary,
        "deploy_results": run.get("deploy_results"),
        "error": run.get("error"),
        "storage_report": run.get("storage_report"),
        "data_migration_results": run.get("data_migration_results"),
    }
    if run.get("error"):
        return _tr("results.html", ctx)

    ctx["error"] = None
    return _tr("results.html", ctx)


@app.get("/api/report/{run_id}/view")
async def view_report(run_id: str):
    run = _get_run(run_id)
    if not run:
        return JSONResponse({"error": "Run not found"}, status_code=404)
    report_text = run.get("report", "") or ""
    if not report_text:
        return JSONResponse({"error": "No report generated"}, status_code=404)
    return HTMLResponse(
        content=report_text,
        status_code=200,
        headers={
            "Content-Type": "text/html; charset=utf-8",
        },
    )


@app.get("/api/report/{run_id}/download")
async def download_report(run_id: str):
    run = _get_run(run_id)
    if not run:
        return JSONResponse({"error": "Run not found"}, status_code=404)
    report_text = run.get("report", "") or ""
    if not report_text:
        return JSONResponse({"error": "No report generated"}, status_code=404)
    return HTMLResponse(
        content=report_text,
        status_code=200,
        headers={
            "Content-Type": "text/html; charset=utf-8",
            "Content-Disposition": f"attachment; filename=migration_report_{run_id}.html",
        },
    )


# ═══════════════════════════════════════════════════════════════════
# New Agent API endpoints
# ═══════════════════════════════════════════════════════════════════

@app.get("/api/plan/{run_id}")
async def get_plan(run_id: str):
    run = _get_run(run_id)
    if not run:
        return JSONResponse({"error": "Not found"}, status_code=404)
    plan = run.get("plan", {})
    return JSONResponse(plan)


@app.get("/api/confidence/{run_id}")
async def get_confidence(run_id: str):
    run = _get_run(run_id)
    if not run:
        return JSONResponse({"error": "Not found"}, status_code=404)
    scores = run.get("confidence_scores", [])
    return JSONResponse(scores)


@app.post("/api/discover-storage/{run_id}")
async def discover_storage(run_id: str):
    run = _get_run(run_id)
    if not run:
        return JSONResponse({"error": "Run not found"}, status_code=404)

    try:
        sf_creds = {
            "sf_account": run.get("sf_account", ""),
            "sf_user": run.get("sf_user", ""),
            "sf_password": run.get("sf_password", ""),
            "sf_warehouse": run.get("sf_warehouse", ""),
            "sf_role": run.get("sf_role"),
            "sf_database": run.get("sf_database"),
            "sf_schema": run.get("sf_schema"),
        }

        sf = SnowflakeConnector(
            account=sf_creds["sf_account"],
            user=sf_creds["sf_user"],
            password=sf_creds["sf_password"],
            warehouse=sf_creds["sf_warehouse"],
            role=sf_creds.get("sf_role"),
            database=sf_creds.get("sf_database"),
            schema=sf_creds.get("sf_schema"),
        )
        try:
            sf.test_connection()

            from agents.storage_discovery_agent import StorageDiscoveryAgent
            agent = StorageDiscoveryAgent()
            report = agent.discover(sf)
        finally:
            sf.close()

        s = _get_run(run_id)
        if s:
            s["storage_report"] = {
                "total_tables": report.total_tables,
                "internal_tables": [
                    {"name": t.name, "database": t.database, "schema": t.schema}
                    for t in report.internal_tables
                ],
                "external_tables": [
                    {"name": t.name, "database": t.database, "schema": t.schema,
                     "storage_location": t.storage_location, "cloud_provider": t.cloud_provider}
                    for t in report.external_tables
                ],
                "iceberg_tables": [
                    {"name": t.name, "database": t.database, "schema": t.schema}
                    for t in report.iceberg_tables
                ],
                "stages": [
                    {"name": s.name, "type": s.type, "storage_location": s.storage_location, "cloud_provider": s.cloud_provider}
                    for s in report.stages
                ],
                "storage_integrations": report.storage_integrations,
                "cloud_providers": list(report.cloud_providers),
                "needs_export": report.needs_export,
                "needs_s3_credentials": report.needs_s3_credentials,
                "summary": report.summary,
            }
            _store_run(run_id, s)

        return JSONResponse(s["storage_report"])

    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/deploy-agent/{run_id}")
async def deploy_agent(run_id: str):
    run = _get_run(run_id)
    if not run:
        return JSONResponse({"error": "Run not found"}, status_code=404)

    try:
        creds = {
            "db_hostname": run.get("db_hostname", ""),
            "db_http_path": run.get("db_http_path", ""),
            "db_token": run.get("db_token", ""),
            "db_catalog": run.get("db_catalog"),
            "db_schema": run.get("db_schema"),
        }
        if not all([creds["db_hostname"], creds["db_http_path"], creds.get("db_token")]):
            return JSONResponse({"error": "Databricks credentials not configured"}, status_code=400)

        objects = []
        for conv in run.get("conversions", []):
            objects.append({
                "name": conv.get("name", ""),
                "object_type": conv.get("object_type", ""),
                "converted_sql": conv.get("converted_sql", ""),
                "raw_sql": conv.get("raw_sql", ""),
            })

        from agents.deployment_agent import DeploymentAgent
        agent = DeploymentAgent()
        results = agent.deploy(objects, creds, dry_run=False)

        success = sum(1 for r in results if r.success)
        return JSONResponse({
            "success": success,
            "total": len(results),
            "results": [
                {"name": r.object_name, "ok": r.success, "duration_ms": r.duration_ms, "error": r.error}
                for r in results
            ],
        })

    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/deploy-agent/{run_id}/rollback")
async def deploy_rollback(run_id: str):
    run = _get_run(run_id)
    if not run:
        return JSONResponse({"error": "Run not found"}, status_code=404)

    creds = {
        "db_hostname": run.get("db_hostname", ""),
        "db_http_path": run.get("db_http_path", ""),
        "db_token": run.get("db_token", ""),
        "db_catalog": run.get("db_catalog"),
        "db_schema": run.get("db_schema"),
    }

    objects = []
    for conv in run.get("conversions", []):
        objects.append({
            "name": conv.get("name", ""),
            "object_type": conv.get("object_type", ""),
            "converted_sql": conv.get("converted_sql", ""),
        })

    from agents.deployment_agent import DeploymentAgent
    agent = DeploymentAgent()
    results = agent.deploy(objects, creds, dry_run=True)
    rollback_results = agent.rollback(results, creds)

    return JSONResponse({
        "rollback": [
            {"name": r.object_name, "ok": r.success, "error": r.error}
            for r in rollback_results
        ],
    })


@app.post("/api/save-s3-creds/{run_id}")
async def save_s3_creds(
    run_id: str,
    cloud_provider: str = Form("aws"),
    s3_bucket: str = Form(None),
    s3_region: str = Form(None),
    s3_access_key: str = Form(None),
    s3_secret_key: str = Form(None),
    s3_iam_role: str = Form(None),
    s3_storage_integration: str = Form(None),
    azure_account: str = Form(None),
    azure_container: str = Form(None),
    azure_sas_token: str = Form(None),
    gcs_bucket: str = Form(None),
    gcs_service_account: str = Form(None),
):
    run = _get_run(run_id)
    if not run:
        return JSONResponse({"error": "Run not found"}, status_code=404)
    run["cloud_provider"] = _strip(cloud_provider)
    run["s3_bucket"] = _strip(s3_bucket)
    run["s3_region"] = _strip(s3_region)
    run["s3_access_key"] = _strip(s3_access_key)
    run["s3_secret_key"] = _strip(s3_secret_key)
    run["s3_iam_role"] = _strip(s3_iam_role)
    run["s3_storage_integration"] = _strip(s3_storage_integration)
    run["azure_account"] = _strip(azure_account)
    run["azure_container"] = _strip(azure_container)
    run["azure_sas_token"] = _strip(azure_sas_token)
    run["gcs_bucket"] = _strip(gcs_bucket)
    run["gcs_service_account"] = _strip(gcs_service_account)
    _store_run(run_id, run)
    return JSONResponse({"ok": True})


@app.post("/api/data-migrate/{run_id}")
async def data_migrate(
    run_id: str,
    cloud_provider: str = Form("aws"),
    s3_bucket: str = Form(None),
    s3_region: str = Form(None),
    s3_access_key: str = Form(None),
    s3_secret_key: str = Form(None),
    s3_iam_role: str = Form(None),
    s3_storage_integration: str = Form(None),
    azure_account: str = Form(None),
    azure_container: str = Form(None),
    azure_sas_token: str = Form(None),
    gcs_bucket: str = Form(None),
    gcs_service_account: str = Form(None),
):
    run = _get_run(run_id)
    if not run:
        return JSONResponse({"error": "Run not found"}, status_code=404)

    try:
        report_data = run.get("storage_report")
        if not report_data:
            return JSONResponse({"error": "No storage report found — run /api/discover-storage first"}, status_code=400)

        project = _get_project(run.get("project_id"))

        sf_creds = {
            "sf_account": run.get("sf_account") or (project or {}).get("sf_account", ""),
            "sf_user": run.get("sf_user") or (project or {}).get("sf_user", ""),
            "sf_password": run.get("sf_password") or (project or {}).get("sf_password", ""),
            "sf_warehouse": run.get("sf_warehouse") or (project or {}).get("sf_warehouse", ""),
            "sf_role": run.get("sf_role") or (project or {}).get("sf_role"),
            "sf_database": run.get("sf_database") or (project or {}).get("sf_database"),
            "sf_schema": run.get("sf_schema") or (project or {}).get("sf_schema"),
        }
        db_creds = {
            "db_hostname": run.get("db_hostname", ""),
            "db_http_path": run.get("db_http_path", ""),
            "db_token": run.get("db_token", ""),
            "db_catalog": run.get("db_catalog"),
            "db_schema": run.get("db_schema"),
        }
        s3_creds = None
        if cloud_provider == "aws":
            s3_creds = {
                "bucket": _strip(s3_bucket) or run.get("s3_bucket", ""),
                "region": _strip(s3_region) or run.get("s3_region", ""),
                "access_key": _strip(s3_access_key) or run.get("s3_access_key", ""),
                "secret_key": _strip(s3_secret_key) or run.get("s3_secret_key", ""),
                "iam_role": _strip(s3_iam_role) or run.get("s3_iam_role", ""),
                "storage_integration": _strip(s3_storage_integration) or run.get("s3_storage_integration", ""),
            }
        elif cloud_provider == "azure":
            s3_creds = {
                "bucket": _strip(azure_container) or "",
                "azure_account": _strip(azure_account) or "",
                "azure_sas_token": _strip(azure_sas_token) or "",
            }
        elif cloud_provider == "gcs":
            s3_creds = {
                "bucket": _strip(gcs_bucket) or "",
                "gcp_service_account": _strip(gcs_service_account) or "",
            }

        if cloud_provider == "aws" and s3_bucket:
            run["s3_bucket"] = s3_bucket or ""
            run["s3_region"] = s3_region or ""
            run["s3_access_key"] = s3_access_key or ""
            run["s3_secret_key"] = s3_secret_key or ""
            run["s3_iam_role"] = s3_iam_role or ""
            run["s3_storage_integration"] = s3_storage_integration or ""
            _store_run(run_id, run)

        from agents.storage_discovery_agent import StorageDiscoveryAgent
        from connectors.snowflake_connector import SnowflakeConnector

        sf = SnowflakeConnector(
            account=sf_creds["sf_account"],
            user=sf_creds["sf_user"],
            password=sf_creds["sf_password"],
            warehouse=sf_creds["sf_warehouse"],
            role=sf_creds.get("sf_role"),
            database=sf_creds.get("sf_database"),
            schema=sf_creds.get("sf_schema"),
        )
        try:
            sf.test_connection()
            agent = StorageDiscoveryAgent()
            report = agent.discover(sf)
        finally:
            sf.close()

        from agents.data_migration_engine import DataMigrationEngine
        engine = DataMigrationEngine()
        results = engine.migrate(report, sf_creds, db_creds, s3_creds, cloud_provider=cloud_provider)

        result_list = [
            {
                "table": r.table,
                "storage_type": r.storage_type,
                "strategy": r.strategy,
                "success": r.success,
                "duration_ms": r.duration_ms,
                "error": r.error,
            }
            for r in results
        ]

        s = _get_run(run_id)
        if s:
            s["data_migration_results"] = result_list
            _store_run(run_id, s)

        return JSONResponse({
            "results": result_list,
            "summary": {
                "total": len(results),
                "success": sum(1 for r in results if r.success),
                "failed": sum(1 for r in results if not r.success),
            },
        })

    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/data-validate/{run_id}")
async def data_validate(run_id: str):
    run = _get_run(run_id)
    if not run:
        return JSONResponse({"error": "Run not found"}, status_code=404)

    try:
        sf_creds = {
            "sf_account": run.get("sf_account", ""),
            "sf_user": run.get("sf_user", ""),
            "sf_password": run.get("sf_password", ""),
            "sf_warehouse": run.get("sf_warehouse", ""),
            "sf_role": run.get("sf_role"),
            "sf_database": run.get("sf_database"),
            "sf_schema": run.get("sf_schema"),
        }
        db_creds = {
            "db_hostname": run.get("db_hostname", ""),
            "db_http_path": run.get("db_http_path", ""),
            "db_token": run.get("db_token", ""),
            "db_catalog": run.get("db_catalog"),
            "db_schema": run.get("db_schema"),
        }

        tables = [conv for conv in run.get("conversions", []) if conv.get("object_type") == "table"]

        from agents.data_validation_engine import DataValidationEngine
        engine = DataValidationEngine()
        results = engine.validate(tables, sf_creds, db_creds)

        return JSONResponse({
            "results": [
                {
                    "table": r.table,
                    "schema_match": r.schema_match,
                    "row_count_match": r.row_count_match,
                    "checksum_match": r.checksum_match,
                    "source_rows": r.source_rows,
                    "target_rows": r.target_rows,
                    "passed": r.passed,
                    "discrepancies": r.discrepancies,
                }
                for r in results
            ],
            "passed": sum(1 for r in results if r.passed),
            "failed": sum(1 for r in results if not r.passed),
            "total": len(results),
        })

    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ═══════════════════════════════════════════════════════════════════
# Debug
# ═══════════════════════════════════════════════════════════════════

@app.get("/api/debug/{run_id}")
async def debug_run(run_id: str):
    run = _get_run(run_id)
    if not run:
        return JSONResponse({"error": "Run not found"})
    items = []
    for conv in run.get("conversions", []):
        items.append({
            "name": conv.get("name"),
            "object_type": conv.get("object_type"),
            "converted_sql": conv.get("converted_sql", "")[:500],
        })
    tmp_dir = run.get("tmp_dir", "")
    dir_tree = ""
    ch = _creds_hash(run)
    project_tree = database.get_cached_ddl(ch)
    if project_tree:
        lines = sorted(project_tree.keys())[:50]
        dir_tree = "\n".join(f"  {p}" for p in lines)
    return JSONResponse({
        "catalog_ddl": run.get("catalog_ddl"),
        "schema_ddl": run.get("schema_ddl"),
        "project_dir_tree": dir_tree[:3000],
        "objects": items,
    })


# ═══════════════════════════════════════════════════════════════════
# Review & Approval API
# ═══════════════════════════════════════════════════════════════════

def _find_conversion(run: dict, name: str) -> dict | None:
    for conv in run.get("conversions", []):
        if conv.get("name") == name:
            return conv
    return None


@app.get("/api/review/{run_id}/conversions")
async def list_conversions(run_id: str):
    run = _get_run(run_id)
    if not run:
        return JSONResponse({"error": "Run not found"}, status_code=404)
    conversions = run.get("conversions", [])
    summary = {
        "total": len(conversions),
        "pending": sum(1 for c in conversions if c.get("review_status") == "pending_review"),
        "approved": sum(1 for c in conversions if c.get("review_status") == "approved"),
        "rejected": sum(1 for c in conversions if c.get("review_status") == "rejected"),
    }
    return JSONResponse({"conversions": conversions, "summary": summary})


@app.put("/api/review/{run_id}/conversions/{name:path}")
async def edit_conversion(run_id: str, name: str, body: dict):
    run = _get_run(run_id)
    if not run:
        return JSONResponse({"error": "Run not found"}, status_code=404)
    conv = _find_conversion(run, name)
    if not conv:
        return JSONResponse({"error": f"Conversion '{name}' not found"}, status_code=404)
    if "approved_sql" in body:
        conv["approved_sql"] = body["approved_sql"]
    if "reviewer_notes" in body:
        conv["reviewer_notes"] = body["reviewer_notes"]
    _store_run(run_id, run)
    return JSONResponse({"ok": True, "conversion": conv})


@app.post("/api/review/{run_id}/conversions/{name:path}/approve")
async def approve_conversion(run_id: str, name: str):
    run = _get_run(run_id)
    if not run:
        return JSONResponse({"error": "Run not found"}, status_code=404)
    conv = _find_conversion(run, name)
    if not conv:
        return JSONResponse({"error": f"Conversion '{name}' not found"}, status_code=404)
    conv["review_status"] = "approved"
    _store_run(run_id, run)
    return JSONResponse({"ok": True, "review_status": "approved"})


@app.post("/api/review/{run_id}/conversions/{name:path}/reject")
async def reject_conversion(run_id: str, name: str):
    run = _get_run(run_id)
    if not run:
        return JSONResponse({"error": "Run not found"}, status_code=404)
    conv = _find_conversion(run, name)
    if not conv:
        return JSONResponse({"error": f"Conversion '{name}' not found"}, status_code=404)
    conv["review_status"] = "rejected"
    _store_run(run_id, run)
    return JSONResponse({"ok": True, "review_status": "rejected"})


@app.post("/api/review/{run_id}/approve-all")
async def approve_all(run_id: str):
    run = _get_run(run_id)
    if not run:
        return JSONResponse({"error": "Run not found"}, status_code=404)
    count = 0
    for conv in run.get("conversions", []):
        if conv.get("review_status") != "rejected" and conv.get("converted_sql"):
            conv["review_status"] = "approved"
            count += 1
    _store_run(run_id, run)
    return JSONResponse({"ok": True, "approved_count": count})


@app.post("/api/review/{run_id}/reject-all")
async def reject_all(run_id: str):
    run = _get_run(run_id)
    if not run:
        return JSONResponse({"error": "Run not found"}, status_code=404)
    for conv in run.get("conversions", []):
        conv["review_status"] = "rejected"
    _store_run(run_id, run)
    return JSONResponse({"ok": True})


# ═══════════════════════════════════════════════════════════════════
# Deploy
# ═══════════════════════════════════════════════════════════════════

@app.post("/deploy/{run_id}")
async def deploy_run(run_id: str, mode: str = Form("deploy")):
    run = _get_run(run_id)
    if not run:
        return _tr("results.html", {
            "error": "Run not found or expired. Please run migration again.",
            "run_id": None, "deploy_allowed": False,
            "conversions": [], "report": "", "summary": None, "deploy_results": None,
        }, status_code=404)

    if mode == "skip":
        return RedirectResponse(url=f"/results/{run_id}", status_code=302)

    import threading
    thread = threading.Thread(target=_run_deploy_background, args=(run_id, mode), daemon=True)
    thread.start()

    return RedirectResponse(url=f"/results/{run_id}?deploying=1", status_code=302)


# ═══════════════════════════════════════════════════════════════════
# History
# ═══════════════════════════════════════════════════════════════════

@app.get("/history", response_class=HTMLResponse)
async def history_page():
    runs = _list_runs()
    return _tr("history.html", {"runs": runs})


@app.get("/api/runs")
async def list_runs():
    return JSONResponse(_list_runs())


def _list_runs() -> list[dict]:
    runs = database.list_runs()
    result = []
    for data in runs:
        created = data.get("created_at", 0)
        dm = data.get("data_migration_results")
        if isinstance(dm, str):
            dm = json.loads(dm)
        result.append({
            "run_id": data["id"],
            "timestamp": time.strftime("%Y-%m-%d %H:%M", time.localtime(created)) if created else "",
            "summary": data.get("summary"),
            "done": data.get("done", False),
            "error": data.get("error"),
            "deploy_allowed": data.get("deploy_allowed", False),
            "sf_account": data.get("sf_account", ""),
            "data_migration_results": dm or [],
        })
    return result


@app.get("/api/runs/{run_id}/report")
async def download_report(run_id: str):
    run = _get_run(run_id)
    if not run:
        return JSONResponse({"error": "Not found"}, status_code=404)
    report_text = run.get("report", "")
    if not report_text:
        return JSONResponse({"error": "No report"}, status_code=404)
    is_html = report_text.strip().startswith("<!DOCTYPE html>") or report_text.strip().startswith("<html")
    return Response(
        content=report_text,
        media_type="text/html" if is_html else "text/plain",
    )


# ═══════════════════════════════════════════════════════════════════
# Test Databricks connection
# ═══════════════════════════════════════════════════════════════════

@app.post("/api/test-databricks")
async def test_databricks(
    db_hostname: str = Form(...),
    db_http_path: str = Form(...),
    db_token: str = Form(""),
):
    try:
        db = DatabricksConnector(
            server_hostname=db_hostname,
            http_path=db_http_path,
            access_token=db_token,
        )
        msg = db.test_connection()
        db.close()
        return JSONResponse({"ok": True, "message": msg})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
