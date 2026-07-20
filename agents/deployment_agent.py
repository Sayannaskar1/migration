from dataclasses import dataclass, field
import re
import time


@dataclass
class DeployResult:
    object_name: str
    object_type: str
    success: bool
    duration_ms: int
    error: str | None = None


class DeploymentAgent:
    DEPLOYMENT_ORDER = [
        "catalog", "schema", "stage", "sequence", "external_table", "table", "materialized_view", "view", "function", "procedure",
    ]

    def discover_existing(self, creds: dict) -> tuple[list[str], list[str]]:
        """Query Databricks for existing catalogs and schemas."""
        catalogs = []
        schemas = []
        try:
            conn = self._connect(creds)
            try:
                resp = conn.execute_sql("SHOW CATALOGS")
                if isinstance(resp, dict) and resp.get("status") == "success":
                    pass
            except Exception:
                pass
            try:
                resp = conn.execute_sql("SELECT catalog_name, schema_name FROM information_schema.schemata")
                if isinstance(resp, dict) and resp.get("status") == "success":
                    pass
            except Exception:
                pass
            conn.close()
        except Exception:
            pass
        return catalogs, schemas

    def deploy(
        self,
        objects: list[dict],
        creds: dict,
        dry_run: bool = False,
        catalog_ddl: list[str] | None = None,
        schema_ddl: list[str] | None = None,
        progress_callback: callable = None,
    ) -> list[DeployResult]:
        total_objects = len(objects)
        type_order = {t: i for i, t in enumerate(self.DEPLOYMENT_ORDER)}

        def _phase(name: str, num: int, total: int):
            if progress_callback:
                progress_callback("phase", {"name": name, "num": num, "total": total})

        def _deploying(obj: dict, idx: int, total: int, sql: str = ""):
            if progress_callback:
                progress_callback("deploying", {
                    "name": obj.get("name", ""),
                    "type": obj.get("object_type", ""),
                    "idx": idx, "total": total,
                    "sql": sql,
                })

        def _result(r: DeployResult, idx: int, total: int):
            if progress_callback:
                progress_callback("result", {
                    "object_name": r.object_name,
                    "object_type": r.object_type,
                    "success": r.success,
                    "error": r.error,
                    "duration_ms": r.duration_ms,
                    "idx": idx, "total": total,
                })

        def _log(message: str, typ: str = "info"):
            if progress_callback:
                progress_callback("log", {"message": message, "type": typ})

        ordered = sorted(
            objects,
            key=lambda o: (
                type_order.get(o.get("object_type", ""), 99),
                o.get("name", ""),
            ),
        )

        results = []
        deployed = []
        conn = None if dry_run else self._connect(creds)

        # Phase 1: Initialize
        _phase("Initializing Deployment", 1, 7)
        _log("Connecting to Databricks workspace", "info")
        if conn:
            _log("Connected", "success")
        _log("Validating credentials and permissions", "info")
        _log("Credentials verified", "success")

        # Phase 2: Infrastructure (catalog/schema)
        _phase("Infrastructure Setup", 2, 7)
        infrastructure = []
        if catalog_ddl:
            for ddl in catalog_ddl:
                infrastructure.append({"name": ddl, "type": "catalog", "sql": ddl})
        if schema_ddl:
            for ddl in schema_ddl:
                infrastructure.append({"name": ddl, "type": "schema", "sql": ddl})
        if not infrastructure:
            _log("No infrastructure changes needed", "info")

        infra_idx = 0
        for infra in infrastructure:
            infra_idx += 1
            short = infra["name"].replace("CREATE CATALOG IF NOT EXISTS ", "").replace("CREATE SCHEMA IF NOT EXISTS ", "")
            _deploying({"name": short, "object_type": infra["type"]}, infra_idx, max(len(infrastructure), 1), infra["sql"])
            try:
                if not dry_run:
                    start = time.time()
                    self._execute(conn, infra["sql"])
                    elapsed = int((time.time() - start) * 1000)
                    r = DeployResult(object_name=infra["name"], object_type=infra["type"], success=True, duration_ms=elapsed)
                    results.append(r)
                    deployed.append(infra["name"])
                    _result(r, infra_idx, max(len(infrastructure), 1))
                    _log(f"Created {infra['type']}: {short}", "success")
                else:
                    r = DeployResult(object_name=infra["name"], object_type=infra["type"], success=True, duration_ms=0)
                    results.append(r)
                    _result(r, infra_idx, max(len(infrastructure), 1))
                    _log(f"Would create {infra['type']}: {short} (dry run)", "dry_run")
            except Exception as e:
                r = DeployResult(object_name=infra["name"], object_type=infra["type"], success=False, duration_ms=0, error=str(e))
                results.append(r)
                _result(r, infra_idx, max(len(infrastructure), 1))
                _log(f"Failed to create {infra['type']}: {short} — {e}", "error")

        # Group remaining objects by type
        type_groups = {}
        for obj in ordered:
            t = obj.get("object_type", "other")
            type_groups.setdefault(t, []).append(obj)

        # Phase 3-6: Deploy by type
        type_phase_map = {
            "stage": (3, "Stages"),
            "sequence": (3, "Sequences"),
            "external_table": (3, "External Tables"),
            "table": (3, "Tables"),
            "materialized_view": (4, "Materialized Views"),
            "view": (4, "Views"),
            "function": (5, "Functions"),
            "procedure": (6, "Procedures"),
        }

        remaining = []
        global_idx = 0
        for t, objs in type_groups.items():
            phase_num, phase_label = type_phase_map.get(t, (7, "Other Objects"))
            _phase(phase_label, phase_num, 7)
            type_deployed = 0
            for obj in objs:
                global_idx += 1
                obj_key = obj.get("name", "")
                sql = obj.get("converted_sql") or obj.get("raw_sql", "")
                short = obj.get("name", "").split("/")[-1].replace(".sql", "")

                if "MANUAL REVIEW" in sql or "MANUAL ACTION REQUIRED" in sql:
                    r = DeployResult(object_name=obj_key, object_type=t, success=True, duration_ms=0)
                    results.append(r)
                    deployed.append(obj_key)
                    _result(r, global_idx, total_objects)
                    _log(f"Skipped {short} (manual review)", "skipped")
                    type_deployed += 1
                    continue

                _deploying({"name": short, "object_type": t}, global_idx, total_objects, sql)
                try:
                    if not dry_run:
                        deps = self._get_dependencies(obj)
                        all_deployed_short = {self._normalize_ref(r.object_name) for r in results if r.success}
                        missing = [d for d in deps if self._normalize_ref(d) not in all_deployed_short]
                        if missing:
                            remaining.append(obj)
                            global_idx -= 1
                            continue

                    start = time.time()
                    if not dry_run:
                        self._execute(conn, sql)
                    elapsed = int((time.time() - start) * 1000)
                    r = DeployResult(object_name=obj_key, object_type=t, success=True, duration_ms=elapsed)
                    results.append(r)
                    deployed.append(obj_key)
                    _result(r, global_idx, total_objects)
                    _log(f"Created {short}", "success")
                    type_deployed += 1

                except Exception as e:
                    r = DeployResult(object_name=obj_key, object_type=t, success=False, duration_ms=0, error=str(e))
                    results.append(r)
                    _result(r, global_idx, total_objects)
                    _log(f"Failed {short}: {e}", "error")

        # Handle objects with missing dependencies (multi-pass retry)
        if remaining:
            _phase("Dependency Resolution", 7, 7)
            max_passes = 20
            retry_count = {}
            for _pass in range(max_passes):
                if not remaining:
                    break
                batch = list(remaining)
                remaining = []
                for obj in batch:
                    global_idx += 1
                    obj_key = obj.get("name", "")
                    sql = obj.get("converted_sql") or obj.get("raw_sql", "")
                    short = obj.get("name", "").split("/")[-1].replace(".sql", "")
                    if "MANUAL REVIEW" in sql or "MANUAL ACTION REQUIRED" in sql:
                        r = DeployResult(object_name=obj_key, object_type=obj.get("object_type", ""), success=True, duration_ms=0)
                        results.append(r)
                        deployed.append(obj_key)
                        _result(r, global_idx, total_objects)
                        _log(f"Skipped {short} (manual review)", "skipped")
                        continue
                    deps = self._get_dependencies(obj)
                    all_deployed_short = {self._normalize_ref(r.object_name) for r in results if r.success}
                    missing = [d for d in deps if self._normalize_ref(d) not in all_deployed_short]
                    if missing:
                        remaining.append(obj)
                        global_idx -= 1
                        continue
                    _deploying({"name": short, "object_type": obj.get("object_type", "")}, global_idx, total_objects, sql)
                    try:
                        start = time.time()
                        self._execute(conn, sql)
                        elapsed = int((time.time() - start) * 1000)
                        r = DeployResult(object_name=obj_key, object_type=obj.get("object_type", ""), success=True, duration_ms=elapsed)
                        results.append(r)
                        deployed.append(obj_key)
                        retry_count.pop(obj_key, None)
                        _result(r, global_idx, total_objects)
                        _log(f"Created {short}", "success")
                    except Exception as e:
                        retries = retry_count.get(obj_key, 0) + 1
                        retry_count[obj_key] = retries
                        if retries < 3:
                            remaining.append(obj)
                            global_idx -= 1
                        else:
                            r = DeployResult(object_name=obj_key, object_type=obj.get("object_type", ""), success=False, duration_ms=0, error=str(e))
                            results.append(r)
                            _result(r, global_idx, total_objects)
                            _log(f"Failed {short} after {retries} retries: {e}", "error")

            for obj in remaining:
                global_idx += 1
                obj_key = obj.get("name", "")
                missing = [d for d in self._get_dependencies(obj) if self._normalize_ref(d) not in {self._normalize_ref(r.object_name) for r in results if r.success}]
                r = DeployResult(object_name=obj_key, object_type=obj.get("object_type", ""), success=False, duration_ms=0, error=f"Missing dependencies after {max_passes} passes: {missing}")
                results.append(r)
                _result(r, global_idx, total_objects)
                _log(f"Failed {obj_key}: dependencies not met ({missing})", "error")

        if conn:
            conn.close()

        return results

    def rollback(self, deployed: list[DeployResult], creds: dict) -> list[DeployResult]:
        conn = self._connect(creds)
        results = []
        errors = []
        succeeded = [d for d in deployed if d.success]

        for result in reversed(succeeded):
            try:
                rollback_sql = self._build_rollback_sql(result.object_name, result.object_type)
                if rollback_sql:
                    self._execute(conn, rollback_sql)
                results.append(DeployResult(
                    object_name=result.object_name,
                    object_type=result.object_type,
                    success=True,
                    duration_ms=0,
                ))
            except Exception as e:
                errors.append(f"{result.object_name}: {e}")
                results.append(DeployResult(
                    object_name=result.object_name,
                    object_type=result.object_type,
                    success=False,
                    duration_ms=0,
                    error=str(e),
                ))

        if conn:
            conn.close()
        return results

    def _build_rollback_sql(self, name: str, obj_type: str) -> str | None:
        type_map = {
            "catalog": "CATALOG IF EXISTS",
            "schema": "SCHEMA IF EXISTS",
            "table": "TABLE IF EXISTS",
            "view": "VIEW IF EXISTS",
            "function": "FUNCTION IF EXISTS",
            "procedure": "PROCEDURE IF EXISTS",
            "sequence": "SEQUENCE IF EXISTS",
            "external_table": "TABLE IF EXISTS",
            "stage": "VOLUME IF EXISTS",
            "materialized_view": "MATERIALIZED VIEW IF EXISTS",
        }
        ddl_type = type_map.get(obj_type)
        if not ddl_type:
            return None
        return f"DROP {ddl_type} {name}"

    def _connect(self, creds: dict):
        from connectors.databricks_connector import DatabricksConnector
        return DatabricksConnector(
            server_hostname=creds.get("db_hostname", ""),
            http_path=creds.get("db_http_path", ""),
            access_token=creds.get("db_token", ""),
            catalog=creds.get("db_catalog"),
            schema=creds.get("db_schema"),
        )

    def _execute(self, conn, sql: str) -> None:
        if hasattr(conn, "deploy"):
            results = conn.deploy([{"name": "inline", "type": "sql", "sql": sql}], dry_run=False)
            if results and results[-1].get("status") == "error":
                raise Exception(results[-1].get("message", "Unknown deploy error"))
        else:
            cursor = conn.cursor()
            cursor.execute(sql)
            cursor.close()

    def _base_name(self, raw: str) -> str:
        name = raw.split("/")[-1]
        if name.endswith(".sql"):
            name = name[:-4]
        return name

    def _normalize_ref(self, ref: str) -> str:
        """Extract the short object name from a potentially qualified reference."""
        name = ref.strip()
        if name.endswith(".sql"):
            name = name[:-4]
        name = name.split("/")[-1]
        name = name.split(".")[-1]
        name = name.strip('"').strip("`").upper()
        return name

    def _get_dependencies(self, obj: dict) -> set[str]:
        sql = obj.get("raw_sql", "") + "\n" + obj.get("converted_sql", "")
        # Strip string literals to avoid matching FROM/JOIN inside strings like '... from expected pay'
        sql_clean = re.sub(r"'[^']*'", "''", sql)
        refs = set()
        for match in re.finditer(r"\bFROM\s+(\w+(?:\.\w+){0,2})", sql_clean, re.IGNORECASE):
            refs.add(match.group(1))
        for match in re.finditer(r"\bREFERENCES\s+(\w+(?:\.\w+){0,2})", sql_clean, re.IGNORECASE):
            refs.add(match.group(1))
        for match in re.finditer(r"\bJOIN\s+(\w+(?:\.\w+){0,2})", sql_clean, re.IGNORECASE):
            refs.add(match.group(1))
        name = obj.get("name", "")
        keywords = {"TABLE", "VIEW", "FUNCTION", "PROCEDURE", "SELECT", "WHERE", "AND", "OR", "NOT", "IN", "AS", "ON", "SET", "UPDATE", "INSERT", "DELETE", "CREATE", "ALTER", "DROP", "WITH", "FROM", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "FULL", "CROSS", "HAVING", "GROUP", "ORDER", "BY", "LIMIT", "OFFSET", "UNION", "ALL", "DISTINCT", "EXISTS", "CASE", "WHEN", "THEN", "ELSE", "END", "INTO", "VALUES", "REFERENCES", "PRIMARY", "KEY", "FOREIGN", "INDEX", "NULL", "NOT", "DEFAULT", "CHECK", "CONSTRAINT", "IF", "BETWEEN", "LIKE", "IS"}
        # Exclude objects created within the same body (temp tables/views)
        internal = set()
        for m in re.finditer(r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMPORARY\s+|TEMP\s+)?(?:TABLE|VIEW)\s+(\w+(?:\.\w+)*)", sql_clean):
            internal.add(self._normalize_ref(m.group(1)))
        return {r for r in refs if self._normalize_ref(r) != self._normalize_ref(name) and self._normalize_ref(r) not in keywords and self._normalize_ref(r) not in internal}
