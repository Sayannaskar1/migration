import re
from pathlib import Path
from typing import Optional


from connectors.base import SourceConnector


class SnowflakeConnector(SourceConnector):
    def __init__(self, account: str, user: str, password: str, warehouse: str, role: str = None, database: str = None, schema: str = None):
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.role = role
        self.database = database
        self.schema = schema
        self._conn = None

    def _connect(self):
        if self._conn is None:
            import snowflake.connector
            kwargs = {
                "account": self.account,
                "user": self.user,
                "password": self.password,
                "warehouse": self.warehouse,
            }
            if self.role:
                kwargs["role"] = self.role
            if self.database:
                kwargs["database"] = self.database
            if self.schema:
                kwargs["schema"] = self.schema
            kwargs.setdefault("application", "MigrationAgent")
            kwargs.setdefault("login_timeout", 30)
            kwargs.setdefault("network_timeout", 120)
            self._conn = snowflake.connector.connect(**kwargs)
            cur = self._conn.cursor()
            cur.execute(f"USE WAREHOUSE {self.warehouse}")
            cur.execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 600")
            cur.close()
        return self._conn

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def test_connection(self) -> str:
        try:
            conn = self._connect()
            cur = conn.cursor()
            cur.execute("SELECT CURRENT_VERSION(), CURRENT_USER(), CURRENT_DATABASE()")
            row = cur.fetchone()
            return f"Connected to Snowflake v{row[0]} as {row[1]} | DB: {row[2]}"
        except Exception as e:
            raise Exception(f"Snowflake connection failed: {e}")

    def execute_sql(self, sql: str) -> dict:
        try:
            conn = self._connect()
            cur = conn.cursor()
            cur.execute(sql)
            results = cur.fetchall()
            cur.close()
            return {"status": "success", "rows": len(results), "data": results}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def list_databases(self) -> list[str]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SHOW DATABASES")
        return [row[1] for row in cur.fetchall()]

    def list_schemas(self, database: str) -> list[str]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(f"SHOW SCHEMAS IN {database}")
        return [row[1] for row in cur.fetchall() if row[1] not in ("INFORMATION_SCHEMA", "PUBLIC")]

    def list_tables(self, database: str, schema: str) -> list[dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(f"SHOW TABLES IN {database}.{schema}")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def list_views(self, database: str, schema: str) -> list[dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(f"SHOW VIEWS IN {database}.{schema}")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def list_procedures(self, database: str, schema: str) -> list[dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(f"SHOW PROCEDURES IN {database}.{schema}")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def list_functions(self, database: str, schema: str) -> list[dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(f"SHOW FUNCTIONS IN {database}.{schema}")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def list_external_tables(self, database: str, schema: str) -> list[dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(f"SHOW EXTERNAL TABLES IN {database}.{schema}")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def list_stages(self, database: str, schema: str) -> list[dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(f"SHOW STAGES IN {database}.{schema}")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def list_materialized_views(self, database: str, schema: str) -> list[dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(f"SHOW MATERIALIZED VIEWS IN {database}.{schema}")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def list_sequences(self, database: str, schema: str) -> list[dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(f"SHOW SEQUENCES IN {database}.{schema}")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def list_pipes(self, database: str, schema: str) -> list[dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(f"SHOW PIPES IN {database}.{schema}")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def list_tasks(self, database: str, schema: str) -> list[dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(f"SHOW TASKS IN {database}.{schema}")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def list_streams(self, database: str, schema: str) -> list[dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(f"SHOW STREAMS IN {database}.{schema}")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def list_file_formats(self, database: str, schema: str) -> list[dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(f"SHOW FILE FORMATS IN {database}.{schema}")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def list_alerts(self, database: str, schema: str) -> list[dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(f"SHOW ALERTS IN {database}.{schema}")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def list_masking_policies(self, database: str, schema: str) -> list[dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(f"SHOW MASKING POLICIES IN {database}.{schema}")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def list_row_access_policies(self, database: str, schema: str) -> list[dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(f"SHOW ROW ACCESS POLICIES IN {database}.{schema}")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def list_dynamic_tables(self, database: str, schema: str) -> list[dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(f"SHOW DYNAMIC TABLES IN {database}.{schema}")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    # ── Account-level object listings ──

    def list_tags(self) -> list[dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SHOW TAGS")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def list_warehouses(self) -> list[dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SHOW WAREHOUSES")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def list_resource_monitors(self) -> list[dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SHOW RESOURCE MONITORS")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def list_network_policies(self) -> list[dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SHOW NETWORK POLICIES")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def list_shares(self) -> list[dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SHOW SHARES")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def list_storage_integrations(self) -> list[dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SHOW STORAGE INTEGRATIONS")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def list_notification_integrations(self) -> list[dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SHOW NOTIFICATION INTEGRATIONS")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def list_security_integrations(self) -> list[dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SHOW SECURITY INTEGRATIONS")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def list_api_integrations(self) -> list[dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SHOW API INTEGRATIONS")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def list_roles(self) -> list[dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SHOW ROLES")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def list_users(self) -> list[dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SHOW USERS")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def list_grants_to_role(self, role_name: str) -> list[dict]:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(f"SHOW GRANTS TO ROLE {role_name}")
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def get_ddl(self, object_type: str, fully_qualified_name: str) -> str:
        conn = self._connect()
        cur = conn.cursor()
        try:
            cur.execute(f"SELECT GET_DDL('{object_type}', '{fully_qualified_name}')")
            row = cur.fetchone()
            return row[0] if row else ""
        except Exception as e:
            return f"-- Failed to get DDL for {fully_qualified_name}: {e}"

    def _safe_name(self, name: str) -> str:
        safe = name.strip().replace('"', "")
        safe = safe.replace("/", "_").replace("\\", "_").replace(":", "_")
        return safe if safe else "unnamed"

    def _is_valid_ddl(self, ddl: str) -> bool:
        return bool(ddl) and not ddl.startswith("-- Failed")

    def reconstruct_ddl(self, object_type: str, row: dict, db: str = "", schema: str = "") -> str:
        type_map = {
            "STAGE": self._reconstruct_stage,
            "MASKING POLICY": self._reconstruct_masking_policy,
            "ROW ACCESS POLICY": self._reconstruct_row_access_policy,
            "TAG": self._reconstruct_tag,
            "SHARE": self._reconstruct_share,
            "ROLE": self._reconstruct_role,
            "USER": self._reconstruct_user,
            "STREAM": self._reconstruct_stream,
            "PIPE": self._reconstruct_pipe,
            "TASK": self._reconstruct_task,
            "FILE FORMAT": self._reconstruct_file_format,
            "ALERT": self._reconstruct_alert,
            "WAREHOUSE": self._reconstruct_warehouse,
            "RESOURCE MONITOR": self._reconstruct_resource_monitor,
            "NETWORK POLICY": self._reconstruct_network_policy,
            "STORAGE INTEGRATION": self._reconstruct_storage_integration,
            "NOTIFICATION INTEGRATION": self._reconstruct_notification_integration,
            "SECURITY INTEGRATION": self._reconstruct_security_integration,
            "API INTEGRATION": self._reconstruct_api_integration,
        }
        fn = type_map.get(object_type)
        if fn:
            return fn(row, db, schema)
        return ""

    def _reconstruct_stage(self, row: dict, db: str, schema: str) -> str:
        name = row.get("name", "unknown")
        q = f"CREATE OR REPLACE STAGE {db}.{schema}.{name}"
        if row.get("directory_enabled") == "Y":
            q += " DIRECTORY = (ENABLE = TRUE)"
        ff = row.get("file_format_name")
        if ff:
            q += f" FILE_FORMAT = {ff}"
        if row.get("url"):
            q += f" URL = '{row['url']}'"
        if row.get("storage_integration"):
            q += f" STORAGE_INTEGRATION = {row['storage_integration']}"
        q += ";"
        return q

    def _reconstruct_masking_policy(self, row: dict, db: str, schema: str) -> str:
        name = row.get("name", "unknown")
        return (
            f"CREATE OR REPLACE MASKING POLICY {db}.{schema}.{name} AS (val STRING) RETURNS STRING -> val;\n"
            f"-- WARNING: Masking policy body could not be extracted via GET_DDL"
        )

    def _reconstruct_row_access_policy(self, row: dict, db: str, schema: str) -> str:
        name = row.get("name", "unknown")
        return (
            f"CREATE OR REPLACE ROW ACCESS POLICY {db}.{schema}.{name} AS (val BOOLEAN) RETURNS BOOLEAN -> true;\n"
            f"-- WARNING: Row access policy body could not be extracted via GET_DDL"
        )

    def _reconstruct_tag(self, row: dict, db: str, schema: str) -> str:
        name = row.get("name", "unknown")
        tag_db = row.get("database_name") or db
        tag_schema = row.get("schema_name") or schema
        allowed = row.get("allowed_values")
        q = f"CREATE OR REPLACE TAG {tag_db}.{tag_schema}.{name}"
        if allowed:
            vals = ", ".join(f"'{v.strip()}'" for v in allowed.strip("[]").split(",") if v.strip())
            if vals:
                q += f" ALLOWED_VALUES {vals}"
        q += ";"
        return q

    def _reconstruct_share(self, row: dict, db: str = "", schema: str = "") -> str:
        kind = row.get("kind", "")
        name = row.get("name", "unknown")
        if kind == "INBOUND":
            return f"-- INBOUND SHARE: {name} from {row.get('owner_account', 'unknown')} (read-only, cannot be migrated)"
        db_name = row.get("database_name", "")
        if db_name:
            return f"CREATE OR REPLACE SHARE {name};\n-- SHARE on database {db_name}"
        return f"CREATE OR REPLACE SHARE {name};"

    def _reconstruct_role(self, row: dict, db: str = "", schema: str = "") -> str:
        name = row.get("name", "unknown")
        comment = row.get("comment", "")
        q = f"CREATE OR REPLACE ROLE {name};"
        if comment:
            q += f"\n-- Comment: {comment}"
        return q

    def _reconstruct_user(self, row: dict, db: str = "", schema: str = "") -> str:
        name = row.get("name", "unknown")
        parts = [f"CREATE OR REPLACE USER {name}"]
        dw = row.get("default_warehouse")
        if dw:
            parts.append(f"DEFAULT_WAREHOUSE = '{dw}'")
        dr = row.get("default_role")
        if dr:
            parts.append(f"DEFAULT_ROLE = '{dr}'")
        dn = row.get("display_name")
        if dn:
            parts.append(f"DISPLAY_NAME = '{dn}'")
        email = row.get("email")
        if email:
            parts.append(f"EMAIL = '{email}'")
        parts.append("-- PASSWORD not extracted for security")
        return " " + "\n  ".join(parts) + ";"

    def _reconstruct_stream(self, row: dict, db: str, schema: str) -> str:
        name = row.get("name", "unknown")
        table_name = row.get("table_name", "")
        source_type = row.get("source_type", "")
        mode = row.get("mode", "")
        q = f"CREATE OR REPLACE STREAM {db}.{schema}.{name}"
        if source_type:
            q += f" ON {source_type}"
        if table_name:
            q += f" {table_name}"
        if mode:
            q += f" {mode}"
        q += ";"
        return q

    def _reconstruct_pipe(self, row: dict, db: str, schema: str) -> str:
        name = row.get("name", "unknown")
        integration = row.get("integration", "")
        q = f"CREATE OR REPLACE PIPE {db}.{schema}.{name}"
        if integration:
            q += f" AUTO_INGEST = TRUE INTEGRATION = '{integration}'"
        q += ";\n-- WARNING: Pipe COPY statement body could not be extracted via GET_DDL"
        return q

    def _reconstruct_task(self, row: dict, db: str, schema: str) -> str:
        name = row.get("name", "unknown")
        warehouse = row.get("warehouse", "")
        schedule = row.get("schedule", "")
        q = f"CREATE OR REPLACE TASK {db}.{schema}.{name}"
        if warehouse and warehouse != "null":
            q += f"\n  WAREHOUSE = {warehouse}"
        if schedule:
            q += f"\n  SCHEDULE = '{schedule}'"
        q += ";\n-- WARNING: Task SQL body could not be extracted via GET_DDL"
        return q

    def _reconstruct_file_format(self, row: dict, db: str, schema: str) -> str:
        name = row.get("name", "unknown")
        format_type = row.get("format_type", "")
        q = f"CREATE OR REPLACE FILE FORMAT {db}.{schema}.{name}"
        if format_type:
            q += f"\n  TYPE = {format_type}"
        q += ";"
        return q

    def _reconstruct_alert(self, row: dict, db: str, schema: str) -> str:
        name = row.get("name", "unknown")
        warehouse = row.get("warehouse", "")
        schedule = row.get("schedule", "")
        q = f"CREATE OR REPLACE ALERT {db}.{schema}.{name}"
        if warehouse and warehouse != "null":
            q += f"\n  WAREHOUSE = {warehouse}"
        if schedule:
            q += f"\n  SCHEDULE = '{schedule}'"
        q += ";\n-- WARNING: Alert condition and action body could not be extracted via GET_DDL"
        return q

    def _reconstruct_warehouse(self, row: dict, db: str = "", schema: str = "") -> str:
        name = row.get("name", "unknown")
        size = row.get("size", "XSMALL")
        auto_suspend = row.get("auto_suspend", "300")
        auto_resume = row.get("auto_resume", "true")
        q = f"CREATE OR REPLACE WAREHOUSE {name}"
        q += f"\n  WAREHOUSE_SIZE = {size}"
        q += f"\n  AUTO_SUSPEND = {auto_suspend}"
        q += f"\n  AUTO_RESUME = {auto_resume}"
        q += ";"
        return q

    def _reconstruct_resource_monitor(self, row: dict, db: str = "", schema: str = "") -> str:
        name = row.get("name", "unknown")
        credit_quota = row.get("credit_quota", "")
        frequency = row.get("frequency", "")
        q = f"CREATE OR REPLACE RESOURCE MONITOR {name}"
        if credit_quota:
            q += f"\n  CREDIT_QUOTA = {credit_quota}"
        if frequency:
            q += f"\n  FREQUENCY = {frequency}"
        q += ";"
        return q

    def _reconstruct_network_policy(self, row: dict, db: str = "", schema: str = "") -> str:
        name = row.get("name", "unknown")
        q = f"CREATE OR REPLACE NETWORK POLICY {name};\n-- WARNING: Network policy rules could not be extracted via GET_DDL"
        return q

    def _reconstruct_storage_integration(self, row: dict, db: str = "", schema: str = "") -> str:
        name = row.get("name", "unknown")
        q = f"CREATE OR REPLACE STORAGE INTEGRATION {name}\n  TYPE = EXTERNAL_STAGE\n  ENABLED = TRUE;\n-- WARNING: Storage integration details could not be extracted via GET_DDL"
        return q

    def _reconstruct_notification_integration(self, row: dict, db: str = "", schema: str = "") -> str:
        name = row.get("name", "unknown")
        q = f"CREATE OR REPLACE NOTIFICATION INTEGRATION {name}\n  TYPE = QUEUE\n  ENABLED = TRUE;\n-- WARNING: Notification integration details could not be extracted via GET_DDL"
        return q

    def _reconstruct_security_integration(self, row: dict, db: str = "", schema: str = "") -> str:
        name = row.get("name", "unknown")
        integration_type = row.get("integration_type", "EXTERNAL_OAUTH")
        q = f"CREATE OR REPLACE SECURITY INTEGRATION {name}\n  TYPE = {integration_type}\n  ENABLED = TRUE;\n-- WARNING: Security integration details could not be extracted via GET_DDL"
        return q

    def _reconstruct_api_integration(self, row: dict, db: str = "", schema: str = "") -> str:
        name = row.get("name", "unknown")
        q = f"CREATE OR REPLACE API INTEGRATION {name}\n  API_PROVIDER = aws_api_gateway\n  ENABLED = TRUE;\n-- WARNING: API integration details could not be extracted via GET_DDL"
        return q

    def _func_arg_types(self, arguments_str: str) -> str:
        # SHOW FUNCTIONS arguments column format: FUNC_NAME(TYPE1, TYPE2) RETURN TYPE
        match = re.search(r'\(([^)]*)\)', arguments_str)
        if match:
            types = [t.strip() for t in match.group(1).split(',') if t.strip()]
            return f"({', '.join(types)})" if types else "()"
        return "()"

    def extract_project(self, output_dir: str = "", databases: list[str] = None, on_progress: callable = None) -> dict:
        conn = self._connect()

        if databases:
            db_list = databases
        elif self.database:
            db_list = [self.database]
        else:
            db_list = self.list_databases()

        # Build list of (db, schema) pairs, excluding system databases
        pairs = []
        for db in db_list:
            db_clean = self._safe_name(db)
            if db_clean in ("SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"):
                continue
            if self.schema:
                schemas = [self.schema]
            else:
                schemas = self.list_schemas(db_clean)
            for schema in schemas:
                schema_clean = self._safe_name(schema)
                if schema_clean and schema_clean not in ("INFORMATION_SCHEMA",):
                    pairs.append((db_clean, schema_clean))

        total_pairs = len(pairs)
        summary = {"schemas": 0, "tables": 0, "views": 0, "procedures": 0, "functions": 0, "external_tables": 0, "stages": 0, "materialized_views": 0, "sequences": 0, "pipes": 0, "tasks": 0, "streams": 0, "file_formats": 0, "alerts": 0, "masking_policies": 0, "row_access_policies": 0, "dynamic_tables": 0, "iceberg_tables": 0, "tags": 0, "warehouses": 0, "resource_monitors": 0, "network_policies": 0, "shares": 0, "storage_integrations": 0, "notification_integrations": 0, "security_integrations": 0, "api_integrations": 0, "roles": 0, "users": 0}
        tree: dict[str, str] = {}

        for idx, (db_clean, schema_clean) in enumerate(pairs):
            if on_progress:
                on_progress(db_clean, schema_clean, idx + 1, total_pairs)

            schema_prefix = f"{db_clean}/{schema_clean}"

            tree[f"{schema_prefix}/schema.sql"] = f"CREATE SCHEMA IF NOT EXISTS {db_clean}.{schema_clean};"
            summary["schemas"] += 1

            tables = self.list_tables(db_clean, schema_clean)
            for t in tables:
                name = self._safe_name(t.get("name", ""))
                ddl = self.get_ddl("TABLE", f"{db_clean}.{schema_clean}.{t.get('name','')}")
                if self._is_valid_ddl(ddl):
                    if "ICEBERG" in ddl.upper():
                        tree[f"{schema_prefix}/{name}.sql"] = ddl
                        tree[f"{schema_prefix}/{name}.sql_type"] = "iceberg_table"
                        summary["iceberg_tables"] += 1
                    else:
                        tree[f"{schema_prefix}/{name}.sql"] = ddl
                        tree[f"{schema_prefix}/{name}.sql_type"] = "table"
                        summary["tables"] += 1

            views = self.list_views(db_clean, schema_clean)
            for v in views:
                name = self._safe_name(v.get("name", ""))
                ddl = self.get_ddl("VIEW", f"{db_clean}.{schema_clean}.{v.get('name','')}")
                if self._is_valid_ddl(ddl):
                    tree[f"{schema_prefix}/{name}.sql"] = ddl
                    tree[f"{schema_prefix}/{name}.sql_type"] = "view"
                    summary["views"] += 1

            procs = self.list_procedures(db_clean, schema_clean)
            for p in procs:
                if p.get("is_builtin") == "Y":
                    continue
                raw_name = p.get("name", "")
                name = self._safe_name(raw_name)
                arg_types = self._func_arg_types(p.get("arguments", ""))
                fqn = f"{db_clean}.{schema_clean}.{raw_name}{arg_types}"
                ddl = self.get_ddl("PROCEDURE", fqn)
                if self._is_valid_ddl(ddl):
                    tree[f"{schema_prefix}/{name}.sql"] = ddl
                    tree[f"{schema_prefix}/{name}.sql_type"] = "procedure"
                    summary["procedures"] += 1

            funcs = self.list_functions(db_clean, schema_clean)
            for f in funcs:
                if f.get("is_builtin") == "Y":
                    continue
                raw_name = f.get("name", "")
                name = self._safe_name(raw_name)
                arg_types = self._func_arg_types(f.get("arguments", ""))
                fqn = f"{db_clean}.{schema_clean}.{raw_name}{arg_types}"
                ddl = self.get_ddl("FUNCTION", fqn)
                if self._is_valid_ddl(ddl):
                    tree[f"{schema_prefix}/{name}.sql"] = ddl
                    tree[f"{schema_prefix}/{name}.sql_type"] = "function"
                    summary["functions"] += 1

            ext_tables = self.list_external_tables(db_clean, schema_clean)
            for t in ext_tables:
                name = self._safe_name(t.get("name", ""))
                fqn = f"{db_clean}.{schema_clean}.{t.get('name','')}"
                ddl = self.get_ddl("TABLE", fqn)
                if self._is_valid_ddl(ddl):
                    tree[f"{schema_prefix}/{name}.sql"] = ddl
                    tree[f"{schema_prefix}/{name}.sql_type"] = "external_table"
                    summary["external_tables"] += 1

            stages = self.list_stages(db_clean, schema_clean)
            for s in stages:
                name = self._safe_name(s.get("name", ""))
                fqn = f"{db_clean}.{schema_clean}.{s.get('name','')}"
                ddl = self.get_ddl("STAGE", fqn)
                if self._is_valid_ddl(ddl):
                    tree[f"{schema_prefix}/{name}.sql"] = ddl
                    tree[f"{schema_prefix}/{name}.sql_type"] = "stage"
                    summary["stages"] += 1
                else:
                    ddl = self.reconstruct_ddl("STAGE", s, db_clean, schema_clean)
                    if ddl:
                        tree[f"{schema_prefix}/{name}.sql"] = ddl
                        tree[f"{schema_prefix}/{name}.sql_type"] = "stage"
                        summary["stages"] += 1

            mat_views = self.list_materialized_views(db_clean, schema_clean)
            for v in mat_views:
                name = self._safe_name(v.get("name", ""))
                fqn = f"{db_clean}.{schema_clean}.{v.get('name','')}"
                ddl = self.get_ddl("MATERIALIZED VIEW", fqn)
                if self._is_valid_ddl(ddl):
                    tree[f"{schema_prefix}/{name}.sql"] = ddl
                    tree[f"{schema_prefix}/{name}.sql_type"] = "materialized_view"
                    summary["materialized_views"] += 1

            seqs = self.list_sequences(db_clean, schema_clean)
            for seq in seqs:
                name = self._safe_name(seq.get("name", ""))
                fqn = f"{db_clean}.{schema_clean}.{seq.get('name','')}"
                ddl = self.get_ddl("SEQUENCE", fqn)
                if self._is_valid_ddl(ddl):
                    tree[f"{schema_prefix}/{name}.sql"] = ddl
                    tree[f"{schema_prefix}/{name}.sql_type"] = "sequence"
                    summary["sequences"] += 1

            pipes = self.list_pipes(db_clean, schema_clean)
            for p in pipes:
                name = self._safe_name(p.get("name", ""))
                fqn = f"{db_clean}.{schema_clean}.{p.get('name','')}"
                ddl = self.get_ddl("PIPE", fqn)
                if self._is_valid_ddl(ddl):
                    tree[f"{schema_prefix}/{name}.sql"] = ddl
                    tree[f"{schema_prefix}/{name}.sql_type"] = "pipe"
                    summary["pipes"] += 1
                else:
                    ddl = self.reconstruct_ddl("PIPE", p, db_clean, schema_clean)
                    if ddl:
                        tree[f"{schema_prefix}/{name}.sql"] = ddl
                        tree[f"{schema_prefix}/{name}.sql_type"] = "pipe"
                        summary["pipes"] += 1

            tasks = self.list_tasks(db_clean, schema_clean)
            for t in tasks:
                name = self._safe_name(t.get("name", ""))
                fqn = f"{db_clean}.{schema_clean}.{t.get('name','')}"
                ddl = self.get_ddl("TASK", fqn)
                if self._is_valid_ddl(ddl):
                    tree[f"{schema_prefix}/{name}.sql"] = ddl
                    tree[f"{schema_prefix}/{name}.sql_type"] = "task"
                    summary["tasks"] += 1
                else:
                    ddl = self.reconstruct_ddl("TASK", t, db_clean, schema_clean)
                    if ddl:
                        tree[f"{schema_prefix}/{name}.sql"] = ddl
                        tree[f"{schema_prefix}/{name}.sql_type"] = "task"
                        summary["tasks"] += 1

            streams = self.list_streams(db_clean, schema_clean)
            for s in streams:
                name = self._safe_name(s.get("name", ""))
                fqn = f"{db_clean}.{schema_clean}.{s.get('name','')}"
                ddl = self.get_ddl("STREAM", fqn)
                if self._is_valid_ddl(ddl):
                    tree[f"{schema_prefix}/{name}.sql"] = ddl
                    tree[f"{schema_prefix}/{name}.sql_type"] = "stream"
                    summary["streams"] += 1
                else:
                    ddl = self.reconstruct_ddl("STREAM", s, db_clean, schema_clean)
                    if ddl:
                        tree[f"{schema_prefix}/{name}.sql"] = ddl
                        tree[f"{schema_prefix}/{name}.sql_type"] = "stream"
                        summary["streams"] += 1

            file_formats = self.list_file_formats(db_clean, schema_clean)
            for ff in file_formats:
                name = self._safe_name(ff.get("name", ""))
                fqn = f"{db_clean}.{schema_clean}.{ff.get('name','')}"
                ddl = self.get_ddl("FILE FORMAT", fqn)
                if self._is_valid_ddl(ddl):
                    tree[f"{schema_prefix}/{name}.sql"] = ddl
                    tree[f"{schema_prefix}/{name}.sql_type"] = "file_format"
                    summary["file_formats"] += 1
                else:
                    ddl = self.reconstruct_ddl("FILE FORMAT", ff, db_clean, schema_clean)
                    if ddl:
                        tree[f"{schema_prefix}/{name}.sql"] = ddl
                        tree[f"{schema_prefix}/{name}.sql_type"] = "file_format"
                        summary["file_formats"] += 1

            alerts = self.list_alerts(db_clean, schema_clean)
            for a in alerts:
                name = self._safe_name(a.get("name", ""))
                fqn = f"{db_clean}.{schema_clean}.{a.get('name','')}"
                ddl = self.get_ddl("ALERT", fqn)
                if self._is_valid_ddl(ddl):
                    tree[f"{schema_prefix}/{name}.sql"] = ddl
                    tree[f"{schema_prefix}/{name}.sql_type"] = "alert"
                    summary["alerts"] += 1
                else:
                    ddl = self.reconstruct_ddl("ALERT", a, db_clean, schema_clean)
                    if ddl:
                        tree[f"{schema_prefix}/{name}.sql"] = ddl
                        tree[f"{schema_prefix}/{name}.sql_type"] = "alert"
                        summary["alerts"] += 1

            masking_policies = self.list_masking_policies(db_clean, schema_clean)
            for mp in masking_policies:
                name = self._safe_name(mp.get("name", ""))
                fqn = f"{db_clean}.{schema_clean}.{mp.get('name','')}"
                ddl = self.get_ddl("MASKING POLICY", fqn)
                if self._is_valid_ddl(ddl):
                    tree[f"{schema_prefix}/{name}.sql"] = ddl
                    tree[f"{schema_prefix}/{name}.sql_type"] = "masking_policy"
                    summary["masking_policies"] += 1
                else:
                    ddl = self.reconstruct_ddl("MASKING POLICY", mp, db_clean, schema_clean)
                    if ddl:
                        tree[f"{schema_prefix}/{name}.sql"] = ddl
                        tree[f"{schema_prefix}/{name}.sql_type"] = "masking_policy"
                        summary["masking_policies"] += 1

            row_access_policies = self.list_row_access_policies(db_clean, schema_clean)
            for rap in row_access_policies:
                name = self._safe_name(rap.get("name", ""))
                fqn = f"{db_clean}.{schema_clean}.{rap.get('name','')}"
                ddl = self.get_ddl("ROW ACCESS POLICY", fqn)
                if self._is_valid_ddl(ddl):
                    tree[f"{schema_prefix}/{name}.sql"] = ddl
                    tree[f"{schema_prefix}/{name}.sql_type"] = "row_access_policy"
                    summary["row_access_policies"] += 1
                else:
                    ddl = self.reconstruct_ddl("ROW ACCESS POLICY", rap, db_clean, schema_clean)
                    if ddl:
                        tree[f"{schema_prefix}/{name}.sql"] = ddl
                        tree[f"{schema_prefix}/{name}.sql_type"] = "row_access_policy"
                        summary["row_access_policies"] += 1

            dynamic_tables = self.list_dynamic_tables(db_clean, schema_clean)
            for dt in dynamic_tables:
                name = self._safe_name(dt.get("name", ""))
                fqn = f"{db_clean}.{schema_clean}.{dt.get('name','')}"
                ddl = self.get_ddl("TABLE", fqn)
                if self._is_valid_ddl(ddl):
                    tree[f"{schema_prefix}/{name}.sql"] = ddl
                    tree[f"{schema_prefix}/{name}.sql_type"] = "dynamic_table"
                    summary["dynamic_tables"] += 1

        # ── Account-level object extraction (only when no specific database targeted) ──
        account_prefix = "__account__"

        if self.database:
            return {"tree": tree, "summary": summary}

        warehouses = self.list_warehouses()
        for w in warehouses:
            name = self._safe_name(w.get("name", ""))
            ddl = self.get_ddl("WAREHOUSE", name)
            if self._is_valid_ddl(ddl):
                tree[f"{account_prefix}/warehouses/{name}.sql"] = ddl
                tree[f"{account_prefix}/warehouses/{name}.sql_type"] = "warehouse"
                summary["warehouses"] += 1
            else:
                ddl = self.reconstruct_ddl("WAREHOUSE", w)
                if ddl:
                    tree[f"{account_prefix}/warehouses/{name}.sql"] = ddl
                    tree[f"{account_prefix}/warehouses/{name}.sql_type"] = "warehouse"
                    summary["warehouses"] += 1

        resource_monitors = self.list_resource_monitors()
        for rm in resource_monitors:
            name = self._safe_name(rm.get("name", ""))
            ddl = self.get_ddl("RESOURCE MONITOR", name)
            if self._is_valid_ddl(ddl):
                tree[f"{account_prefix}/resource_monitors/{name}.sql"] = ddl
                tree[f"{account_prefix}/resource_monitors/{name}.sql_type"] = "resource_monitor"
                summary["resource_monitors"] += 1
            else:
                ddl = self.reconstruct_ddl("RESOURCE MONITOR", rm)
                if ddl:
                    tree[f"{account_prefix}/resource_monitors/{name}.sql"] = ddl
                    tree[f"{account_prefix}/resource_monitors/{name}.sql_type"] = "resource_monitor"
                    summary["resource_monitors"] += 1

        network_policies = self.list_network_policies()
        for np in network_policies:
            name = self._safe_name(np.get("name", ""))
            ddl = self.get_ddl("NETWORK POLICY", name)
            if self._is_valid_ddl(ddl):
                tree[f"{account_prefix}/network_policies/{name}.sql"] = ddl
                tree[f"{account_prefix}/network_policies/{name}.sql_type"] = "network_policy"
                summary["network_policies"] += 1
            else:
                ddl = self.reconstruct_ddl("NETWORK POLICY", np)
                if ddl:
                    tree[f"{account_prefix}/network_policies/{name}.sql"] = ddl
                    tree[f"{account_prefix}/network_policies/{name}.sql_type"] = "network_policy"
                    summary["network_policies"] += 1

        tags = self.list_tags()
        for tg in tags:
            name = self._safe_name(tg.get("name", ""))
            tag_db = tg.get("database_name") or ""
            tag_schema = tg.get("schema_name") or ""
            fqn = f"{tag_db}.{tag_schema}.{name}" if tag_db and tag_schema else name
            ddl = self.get_ddl("TAG", fqn)
            if self._is_valid_ddl(ddl):
                tree[f"{account_prefix}/tags/{name}.sql"] = ddl
                tree[f"{account_prefix}/tags/{name}.sql_type"] = "tag"
                summary["tags"] += 1
            else:
                ddl = self.reconstruct_ddl("TAG", tg)
                if ddl:
                    tree[f"{account_prefix}/tags/{name}.sql"] = ddl
                    tree[f"{account_prefix}/tags/{name}.sql_type"] = "tag"
                    summary["tags"] += 1

        shares = self.list_shares()
        for sh in shares:
            name = self._safe_name(sh.get("name", ""))
            ddl = self.get_ddl("SHARE", name)
            if self._is_valid_ddl(ddl):
                tree[f"{account_prefix}/shares/{name}.sql"] = ddl
                tree[f"{account_prefix}/shares/{name}.sql_type"] = "share"
                summary["shares"] += 1
            else:
                ddl = self.reconstruct_ddl("SHARE", sh)
                if ddl:
                    tree[f"{account_prefix}/shares/{name}.sql"] = ddl
                    tree[f"{account_prefix}/shares/{name}.sql_type"] = "share"
                    summary["shares"] += 1

        storage_integrations = self.list_storage_integrations()
        for si in storage_integrations:
            name = self._safe_name(si.get("name", ""))
            ddl = self.get_ddl("STORAGE INTEGRATION", name)
            if self._is_valid_ddl(ddl):
                tree[f"{account_prefix}/storage_integrations/{name}.sql"] = ddl
                tree[f"{account_prefix}/storage_integrations/{name}.sql_type"] = "storage_integration"
                summary["storage_integrations"] += 1
            else:
                ddl = self.reconstruct_ddl("STORAGE INTEGRATION", si)
                if ddl:
                    tree[f"{account_prefix}/storage_integrations/{name}.sql"] = ddl
                    tree[f"{account_prefix}/storage_integrations/{name}.sql_type"] = "storage_integration"
                    summary["storage_integrations"] += 1

        notification_integrations = self.list_notification_integrations()
        for ni in notification_integrations:
            name = self._safe_name(ni.get("name", ""))
            ddl = self.get_ddl("NOTIFICATION INTEGRATION", name)
            if self._is_valid_ddl(ddl):
                tree[f"{account_prefix}/notification_integrations/{name}.sql"] = ddl
                tree[f"{account_prefix}/notification_integrations/{name}.sql_type"] = "notification_integration"
                summary["notification_integrations"] += 1
            else:
                ddl = self.reconstruct_ddl("NOTIFICATION INTEGRATION", ni)
                if ddl:
                    tree[f"{account_prefix}/notification_integrations/{name}.sql"] = ddl
                    tree[f"{account_prefix}/notification_integrations/{name}.sql_type"] = "notification_integration"
                    summary["notification_integrations"] += 1

        security_integrations = self.list_security_integrations()
        for si in security_integrations:
            name = self._safe_name(si.get("name", ""))
            ddl = self.get_ddl("SECURITY INTEGRATION", name)
            if self._is_valid_ddl(ddl):
                tree[f"{account_prefix}/security_integrations/{name}.sql"] = ddl
                tree[f"{account_prefix}/security_integrations/{name}.sql_type"] = "security_integration"
                summary["security_integrations"] += 1
            else:
                ddl = self.reconstruct_ddl("SECURITY INTEGRATION", si)
                if ddl:
                    tree[f"{account_prefix}/security_integrations/{name}.sql"] = ddl
                    tree[f"{account_prefix}/security_integrations/{name}.sql_type"] = "security_integration"
                    summary["security_integrations"] += 1

        api_integrations = self.list_api_integrations()
        for ai in api_integrations:
            name = self._safe_name(ai.get("name", ""))
            ddl = self.get_ddl("API INTEGRATION", name)
            if self._is_valid_ddl(ddl):
                tree[f"{account_prefix}/api_integrations/{name}.sql"] = ddl
                tree[f"{account_prefix}/api_integrations/{name}.sql_type"] = "api_integration"
                summary["api_integrations"] += 1
            else:
                ddl = self.reconstruct_ddl("API INTEGRATION", ai)
                if ddl:
                    tree[f"{account_prefix}/api_integrations/{name}.sql"] = ddl
                    tree[f"{account_prefix}/api_integrations/{name}.sql_type"] = "api_integration"
                    summary["api_integrations"] += 1

        roles = self.list_roles()
        for r in roles:
            name = self._safe_name(r.get("name", ""))
            ddl = self.get_ddl("ROLE", name)
            if self._is_valid_ddl(ddl):
                tree[f"{account_prefix}/roles/{name}.sql"] = ddl
                tree[f"{account_prefix}/roles/{name}.sql_type"] = "role"
                summary["roles"] += 1
            else:
                ddl = self.reconstruct_ddl("ROLE", r)
                if ddl:
                    tree[f"{account_prefix}/roles/{name}.sql"] = ddl
                    tree[f"{account_prefix}/roles/{name}.sql_type"] = "role"
                    summary["roles"] += 1

        users = self.list_users()
        for u in users:
            name = self._safe_name(u.get("name", ""))
            ddl = self.get_ddl("USER", name)
            if self._is_valid_ddl(ddl):
                tree[f"{account_prefix}/users/{name}.sql"] = ddl
                tree[f"{account_prefix}/users/{name}.sql_type"] = "user"
                summary["users"] += 1
            else:
                ddl = self.reconstruct_ddl("USER", u)
                if ddl:
                    tree[f"{account_prefix}/users/{name}.sql"] = ddl
                    tree[f"{account_prefix}/users/{name}.sql_type"] = "user"
                    summary["users"] += 1

        # ── Grants: query per custom role and store as reference ──
        grant_lines = []
        custom_roles = [r for r in roles if r.get("name", "").upper() not in
            ("ACCOUNTADMIN", "SYSADMIN", "USERADMIN", "SECURITYADMIN",
             "PUBLIC", "ORGADMIN", "SNOWFLAKE_LEARNING_ROLE")]
        for r in custom_roles:
            role_name = r.get("name", "")
            if not role_name:
                continue
            try:
                grants = self.list_grants_to_role(role_name)
                for g in grants:
                    priv = g.get("privilege", "")
                    obj_type = g.get("granted_on", "")
                    obj_name = g.get("name", "")
                    grantee_name = g.get("grantee_name", "")
                    if self.database:
                        obj_upper = obj_name.upper()
                        db_upper = self.database.upper()
                        if obj_type == "DATABASE":
                            if obj_upper != db_upper:
                                continue
                        elif "." in obj_name:
                            first_part = obj_name.split(".")[0].upper()
                            if first_part != db_upper:
                                continue
                    grant_lines.append(
                        f"GRANT {priv} ON {obj_type} {obj_name} TO ROLE {grantee_name};"
                    )
            except Exception:
                pass
        if grant_lines:
            tree[f"{account_prefix}/grants/grants.sql"] = "\n".join(grant_lines)
            tree[f"{account_prefix}/grants/grants.sql_type"] = "grant"

        return {"tree": tree, "summary": summary}
