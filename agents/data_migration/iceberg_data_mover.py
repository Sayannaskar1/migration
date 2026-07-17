import time
from .mover_base import DataMover, MigrateResult


class IcebergDataMover(DataMover):
    def migrate(self, table_info, sf_creds: dict, db_creds: dict, storage_creds: dict | None = None) -> MigrateResult:
        start = time.time()
        try:
            from connectors.databricks_connector import DatabricksConnector
            db = DatabricksConnector(
                server_hostname=db_creds.get("db_hostname", ""),
                http_path=db_creds.get("db_http_path", ""),
                access_token=db_creds.get("db_token", ""),
                catalog=db_creds.get("db_catalog"),
                schema=db_creds.get("db_schema"),
            )

            target = f"{table_info.database}.{table_info.schema}.{table_info.name}"
            sync_sql = f"SYNC FROM ICEBERG TABLE {target}"
            db.deploy([{"name": table_info.name, "type": "table", "sql": sync_sql}], dry_run=False)
            db.close()

            elapsed = int((time.time() - start) * 1000)
            return MigrateResult(
                table=table_info.name, storage_type="iceberg", strategy="direct_sync",
                rows=0, duration_ms=elapsed, success=True,
            )

        except Exception as e:
            elapsed = int((time.time() - start) * 1000)
            return MigrateResult(
                table=table_info.name, storage_type="iceberg", strategy="direct_sync",
                rows=0, duration_ms=elapsed, success=False, error=str(e),
            )
