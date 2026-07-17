import time
from .mover_base import DataMover, MigrateResult


class ExternalDataMover(DataMover):
    def migrate(self, table_info, sf_creds: dict, db_creds: dict, storage_creds: dict | None = None) -> MigrateResult:
        start = time.time()
        try:
            location = table_info.storage_location
            if not location:
                return MigrateResult(
                    table=table_info.name, storage_type="external", strategy="direct_register",
                    rows=0, duration_ms=int((time.time() - start) * 1000),
                    success=False, error="No storage location found",
                )

            from connectors.databricks_connector import DatabricksConnector
            db = DatabricksConnector(
                server_hostname=db_creds.get("db_hostname", ""),
                http_path=db_creds.get("db_http_path", ""),
                access_token=db_creds.get("db_token", ""),
                catalog=db_creds.get("db_catalog"),
                schema=db_creds.get("db_schema"),
            )

            create_sql = self._build_external_table_ddl(table_info, location, table_info.cloud_provider or "AWS")
            db.deploy([{"name": table_info.name, "type": "table", "sql": create_sql}], dry_run=False)
            db.close()

            elapsed = int((time.time() - start) * 1000)
            return MigrateResult(
                table=table_info.name, storage_type="external", strategy="direct_register",
                rows=0, duration_ms=elapsed, success=True,
            )

        except Exception as e:
            elapsed = int((time.time() - start) * 1000)
            return MigrateResult(
                table=table_info.name, storage_type="external", strategy="direct_register",
                rows=0, duration_ms=elapsed, success=False, error=str(e),
            )

    def _build_external_table_ddl(self, table_info, location: str, provider: str) -> str:
        fqn = f"{table_info.database}.{table_info.schema}.{table_info.name}"
        provider_sql = provider.upper() if provider.upper() == "AWS" else provider
        return f"""
CREATE OR REPLACE TABLE {fqn}
USING {provider_sql}
LOCATION '{location}'
"""
