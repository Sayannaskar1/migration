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
    ) -> list[DeployResult]:
        ordered = sorted(
            objects,
            key=lambda o: (
                self.DEPLOYMENT_ORDER.index(o.get("object_type"))
                if o.get("object_type") in self.DEPLOYMENT_ORDER
                else 99,
                o.get("name", ""),
            ),
        )

        results = []
        deployed = []
        conn = None if dry_run else self._connect(creds)

        infrastructure = []
        if catalog_ddl:
            for ddl in catalog_ddl:
                infrastructure.append({"name": ddl, "type": "catalog", "sql": ddl})
        if schema_ddl:
            for ddl in schema_ddl:
                infrastructure.append({"name": ddl, "type": "schema", "sql": ddl})

        for infra in infrastructure:
            try:
                if not dry_run:
                    start = time.time()
                    self._execute(conn, infra["sql"])
                    elapsed = int((time.time() - start) * 1000)
                    results.append(DeployResult(
                        object_name=infra["name"],
                        object_type=infra["type"],
                        success=True,
                        duration_ms=elapsed,
                    ))
                    deployed.append(infra["name"])
                else:
                    results.append(DeployResult(
                        object_name=infra["name"],
                        object_type=infra["type"],
                        success=True,
                        duration_ms=0,
                    ))
            except Exception as e:
                results.append(DeployResult(
                    object_name=infra["name"],
                    object_type=infra["type"],
                    success=False,
                    duration_ms=0,
                    error=str(e),
                ))

        remaining = list(ordered)
        max_passes = 20
        retry_count = {}
        for _pass in range(max_passes):
            if not remaining:
                break
            batch = list(remaining)
            remaining = []
            for obj in batch:
                obj_key = obj.get("name", "")
                try:
                    sql = obj.get("converted_sql") or obj.get("raw_sql", "")
                    if "MANUAL REVIEW" in sql:
                        results.append(DeployResult(
                            object_name=obj.get("name", ""),
                            object_type=obj.get("object_type", ""),
                            success=True,
                            duration_ms=0,
                        ))
                        deployed.append(obj.get("name"))
                        continue

                    if not dry_run:
                        deps = self._get_dependencies(obj)
                        all_deployed_short = {
                            self._normalize_ref(r.object_name)
                            for r in results if r.success
                        }
                        missing = [
                            d for d in deps
                            if self._normalize_ref(d) not in all_deployed_short
                        ]
                        if missing:
                            remaining.append(obj)
                            continue

                    start = time.time()

                    if not dry_run:
                        self._execute(conn, sql)

                    elapsed = int((time.time() - start) * 1000)
                    result = DeployResult(
                        object_name=obj.get("name", ""),
                        object_type=obj.get("object_type", ""),
                        success=True,
                        duration_ms=elapsed,
                    )
                    results.append(result)
                    deployed.append(obj.get("name"))
                    retry_count.pop(obj_key, None)

                except Exception as e:
                    retries = retry_count.get(obj_key, 0) + 1
                    retry_count[obj_key] = retries
                    if retries < 3:
                        remaining.append(obj)
                    else:
                        results.append(DeployResult(
                            object_name=obj.get("name", ""),
                            object_type=obj.get("object_type", ""),
                            success=False,
                            duration_ms=0,
                            error=str(e),
                        ))

        for obj in remaining:
            missing = [d for d in self._get_dependencies(obj) if self._normalize_ref(d) not in {self._normalize_ref(r.object_name) for r in results if r.success}]
            results.append(DeployResult(
                object_name=obj.get("name", ""),
                object_type=obj.get("object_type", ""),
                success=False,
                duration_ms=0,
                error=f"Missing dependencies after {max_passes} passes: {missing}",
            ))

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
