import time
import re
from typing import Optional

from connectors.base import TargetConnector


class DatabricksConnector(TargetConnector):
    def __init__(
        self,
        server_hostname: str,
        http_path: str,
        access_token: str,
        catalog: str = None,
        schema: str = None,
    ):
        self.server_hostname = self._clean_hostname(server_hostname)
        self.http_path = http_path
        self.access_token = access_token
        self.catalog = catalog
        self.schema = schema
        self._warehouse_id = self._parse_warehouse_id(http_path)
        self._client = None

    @staticmethod
    def _clean_hostname(host: str) -> str:
        host = host.strip()
        host = host.replace("https://", "").replace("http://", "")
        idx = host.find("/")
        if idx > 0:
            host = host[:idx]
        idx = host.find("?")
        if idx > 0:
            host = host[:idx]
        return host

    @staticmethod
    def _parse_warehouse_id(http_path: str) -> str | None:
        m = re.search(r"/warehouses/([a-zA-Z0-9]+)", http_path)
        if m:
            return m.group(1)
        m = re.search(r"/o/\d+/([a-zA-Z0-9_-]+)", http_path)
        if m:
            return m.group(1)
        return None

    def _get_client(self):
        if self._client is None:
            from databricks.sdk import WorkspaceClient
            self._client = WorkspaceClient(
                host=f"https://{self.server_hostname}",
                token=self.access_token,
            )
        return self._client

    def close(self):
        self._client = None

    def _check_dns(self) -> str:
        import socket
        try:
            socket.getaddrinfo(self.server_hostname, 443, socket.AF_INET)
            return "ok"
        except socket.gaierror as e:
            return f"Cannot resolve hostname '{self.server_hostname}' — check Server Hostname (DNS error: {e})"

    def test_connection(self) -> str:
        errs = []
        if not self.server_hostname or "." not in self.server_hostname:
            errs.append("Server hostname looks invalid (must be like dbc-xxxx.cloud.databricks.com)")
        if not self.http_path.startswith("/"):
            errs.append("HTTP Path must start with / (e.g. /sql/1.0/warehouses/xxxx)")
        elif "warehouses" not in self.http_path:
            errs.append("HTTP Path should contain '/warehouses/' — are you using a SQL Warehouse HTTP Path? (not a cluster endpoint)")
        if not self.access_token:
            errs.append("Access token is required")
        elif len(self.access_token) < 20:
            errs.append("Access token seems too short — check it's a valid Databricks PAT")
        if not self._warehouse_id:
            errs.append("Could not extract warehouse ID from HTTP Path — check format (/sql/1.0/warehouses/XXXX)")
        if errs:
            raise Exception("; ".join(errs))

        dns = self._check_dns()
        if dns != "ok":
            raise Exception(dns)

        try:
            client = self._get_client()
            client.statement_execution.execute_statement(
                "SELECT 1",
                warehouse_id=self._warehouse_id,
            )
            return "Connected to Databricks successfully"
        except Exception as e:
            msg = str(e).lower()
            if "dns" in msg or "resolve" in msg or "name or service not known" in msg:
                hint = " — check your Server Hostname"
            elif "permission" in msg or "access" in msg:
                hint = " — check your token has 'Can Use' access on this SQL Warehouse"
            elif "401" in msg or "unauthorized" in msg or "token" in msg:
                hint = " — check your Access Token (may be expired or invalid)"
            elif "timeout" in msg or "timed out" in msg:
                hint = " — network timeout. Check firewall/VPN"
            else:
                hint = ""
            raise Exception(f"Databricks connection failed: {e}{hint}")

    def execute_sql(self, sql: str) -> dict:
        try:
            client = self._get_client()
            resp = client.statement_execution.execute_statement(
                sql,
                warehouse_id=self._warehouse_id,
                catalog=self.catalog,
                schema=self.schema,
            )
            result = getattr(resp, "result", None)
            columns = getattr(result, "column_names", []) if result else []
            data = getattr(result, "data_array", []) if result else []
            from databricks.sdk.service.sql import StatementState
            state = getattr(getattr(resp, "status", None), "state", StatementState.SUCCEEDED)
            if state == StatementState.FAILED:
                error = getattr(getattr(resp, "status", None), "error", None)
                msg = getattr(error, "message", "Statement failed") if error else "Statement failed"
                return {"status": "error", "message": msg}
            return {
                "status": "success",
                "row_count": getattr(resp, "row_count", 0),
                "columns": columns,
                "rows": data,
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}

    @staticmethod
    def _extract_target_schema(sql: str) -> tuple[str | None, str | None]:
        """Extract catalog.schema from DDL target like CREATE VIEW catalog.schema.name."""
        _IDENT = r"(?:`[^`]+`|\w+)"
        m = re.match(
            rf"\s*(?:CREATE|CREATE\s+OR\s+REPLACE|DROP|ALTER)\s+(?:\w+\s+)*?"
            rf"(?:IF\s+(?:NOT\s+EXISTS|EXISTS)\s+)?"
            rf"({_IDENT})\.({_IDENT})\.{_IDENT}",
            sql,
            re.IGNORECASE,
        )
        if m:
            return m.group(1), m.group(2)
        m = re.match(
            rf"\s*(?:CREATE|CREATE\s+OR\s+REPLACE|DROP|ALTER)\s+(?:\w+\s+)*?"
            rf"(?:IF\s+(?:NOT\s+EXISTS|EXISTS)\s+)?"
            rf"({_IDENT})\.({_IDENT})",
            sql,
            re.IGNORECASE,
        )
        if m:
            return None, m.group(1)
        return None, None

    def deploy(
        self,
        sql_statements: list[dict],
        dry_run: bool = False,
        on_error: str = "stop",
    ) -> list[dict]:
        results = []
        client = self._get_client()

        for item in sql_statements:
            obj_name = item.get("name", "unknown")
            obj_type = item.get("type", "unknown")
            sql = item.get("sql", "").strip()

            if not sql:
                continue

            if "MANUAL REVIEW" in sql:
                results.append({
                    "object": obj_name,
                    "type": obj_type,
                    "status": "skipped",
                    "message": "Manual review required",
                })
                continue

            if dry_run:
                results.append({
                    "object": obj_name,
                    "type": obj_type,
                    "status": "dry_run",
                    "message": "Would execute (dry run)",
                })
                continue

            target_catalog, target_schema = self._extract_target_schema(sql)
            exec_catalog = target_catalog or self.catalog
            exec_schema = target_schema or self.schema

            try:
                resp = client.statement_execution.execute_statement(
                    sql,
                    warehouse_id=self._warehouse_id,
                    catalog=exec_catalog,
                    schema=exec_schema,
                )
                from databricks.sdk.service.sql import StatementState
                state = getattr(getattr(resp, "status", None), "state", StatementState.SUCCEEDED)
                if state == StatementState.FAILED:
                    error = getattr(getattr(resp, "status", None), "error", None)
                    msg = getattr(error, "message", "Statement failed") if error else "Statement failed"
                    results.append({
                        "object": obj_name,
                        "type": obj_type,
                        "status": "error",
                        "message": msg,
                    })
                    if on_error == "stop":
                        break
                else:
                    results.append({
                        "object": obj_name,
                        "type": obj_type,
                        "status": "success",
                    })
            except Exception as e:
                msg = str(e)
                results.append({
                    "object": obj_name,
                    "type": obj_type,
                    "status": "error",
                    "message": msg,
                })
                if on_error == "stop":
                    break

        return results
