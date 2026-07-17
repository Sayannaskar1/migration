import re
import time
from .mover_base import DataMover, MigrateResult


def _sanitize(val: str) -> str:
    """Strip brackets, quotes, and whitespace from credential values."""
    if not val:
        return val
    val = val.strip()
    for open_c, close_c in [("[" ,"]"), ("\"", "\""), ("'", "'"), ("(", ")")]:
        if val.startswith(open_c) and val.endswith(close_c):
            val = val[1:-1]
    return val.strip()


def _load_s3_to_databricks(s3_path: str, db_target_fqn: str, access_key: str, secret_key: str, region: str, db_creds: dict) -> str | None:
    """Fallback: read Parquet from S3, write to Databricks via pandas DataFrame.
    Returns error string or None on success.
    """
    try:
        import boto3
        import pyarrow.parquet as pq
        import pandas as pd
        import io

        s3 = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region or "us-east-1",
        )

        # Parse s3://bucket/prefix/ to bucket + prefix
        path_part = s3_path.replace("s3://", "")
        parts = path_part.split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""

        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        parquet_keys = [
            o["Key"] for o in resp.get("Contents", [])
            if o["Key"].endswith(".parquet")
        ]
        if not parquet_keys:
            return f"No Parquet files found at {s3_path}"

        # Read all Parquet files into a single pandas DataFrame
        frames = []
        for key in parquet_keys:
            obj = s3.get_object(Bucket=bucket, Key=key)
            table = pq.read_table(io.BytesIO(obj["Body"].read()))
            frames.append(table.to_pandas())
        df = pd.concat(frames, ignore_index=True)

        if df.empty:
            return "No rows found in Parquet files"

        # Infer Databricks types from PyArrow schema of first file
        first_table = pq.read_table(io.BytesIO(s3.get_object(Bucket=bucket, Key=parquet_keys[0])["Body"].read()))
        type_map = {
            "int64": "BIGINT", "int32": "INT", "int16": "SMALLINT", "int8": "TINYINT",
            "float64": "DOUBLE", "float32": "FLOAT", "bool": "BOOLEAN",
            "string": "STRING", "binary": "BINARY",
            "date32": "DATE", "timestamp[ns]": "TIMESTAMP", "timestamp[us]": "TIMESTAMP",
            "timestamp[ms]": "TIMESTAMP", "timestamp[ns, tz=UTC]": "TIMESTAMP",
            "timestamp[us, tz=UTC]": "TIMESTAMP", "timestamp[ms, tz=UTC]": "TIMESTAMP",
        }
        col_defs = []
        for c in df.columns:
            pa_type = str(first_table.schema.field(c).type)
            db_type = type_map.get(pa_type, "STRING")
            col_defs.append(f"`{c}` {db_type}")
        col_def_sql = ", ".join(col_defs)
        col_list = ", ".join(f"`{c}`" for c in df.columns)

        from connectors.databricks_connector import DatabricksConnector
        db = DatabricksConnector(
            server_hostname=db_creds.get("db_hostname", ""),
            http_path=db_creds.get("db_http_path", ""),
            access_token=db_creds.get("db_token", ""),
            catalog=db_creds.get("db_catalog"),
            schema=None,
        )

        # Create table with correct schema
        create_result = db.execute_sql(f"CREATE TABLE IF NOT EXISTS {db_target_fqn} ({col_def_sql})")
        if create_result.get("status") == "error":
            db.close()
            return f"CREATE TABLE failed: {create_result.get('message')}"

        # Write DataFrame to Databricks in small batches to avoid statement size limits
        # and timeouts. 1000 rows keeps each INSERT under ~200KB of SQL.
        batch_size = 1000
        total_rows = len(df)
        total_inserted = 0
        failed_batches = []
        max_retries = 3
        import time

        for start_idx in range(0, total_rows, batch_size):
            batch_df = df.iloc[start_idx:start_idx + batch_size]
            value_rows = []
            for _, row in batch_df.iterrows():
                vals = ", ".join(_sql_literal(v) for v in row)
                value_rows.append(f"({vals})")
            sql = f"INSERT INTO {db_target_fqn} ({col_list}) VALUES {', '.join(value_rows)}"

            batch_ok = False
            last_error = None
            for attempt in range(max_retries):
                result = db.execute_sql(sql)
                if result.get("status") == "error":
                    last_error = result.get("message", "unknown insert error")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                else:
                    total_inserted += len(batch_df)
                    batch_ok = True
                    break

            if not batch_ok:
                failed_batches.append((start_idx, last_error))

            # Progress: after every 10 batches, log progress
            batch_num = start_idx // batch_size + 1
            if batch_num % 10 == 0:
                print(f"    ... {min(start_idx + batch_size, total_rows)}/{total_rows} rows processed")

        # Verify data landed
        time.sleep(1)  # brief pause for Delta Lake commit visibility
        count_result = db.execute_sql(f"SELECT COUNT(*) FROM {db_target_fqn}")
        db.close()

        actual_count = 0
        rows_data = count_result.get("rows", [])
        if rows_data:
            actual_count = int(rows_data[0][0]) if rows_data[0] else 0

        if actual_count == 0 and total_rows > 0:
            err_detail = ""
            if failed_batches:
                first_err = failed_batches[0][1]
                err_detail = f"; first batch error: {first_err}"
            return f"0 rows in target after inserting {total_rows} rows{err_detail}"

        if failed_batches and actual_count < total_rows:
            return f"Partial load: {actual_count}/{total_rows} rows landed ({len(failed_batches)} batches failed)"

        return None
    except Exception as e:
        return str(e)


def _sql_literal(val):
    """Convert a Python value to a SQL literal string."""
    import math
    import numpy as np
    import pandas as pd
    from decimal import Decimal
    from datetime import datetime, date
    if val is None:
        return "NULL"
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return "NULL"
    try:
        if pd.isna(val):
            return "NULL"
    except (TypeError, ValueError):
        pass
    if isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    if isinstance(val, (int, np.integer)):
        return str(int(val))
    if isinstance(val, (float, np.floating)):
        return str(float(val))
    if isinstance(val, Decimal):
        return str(val)
    if isinstance(val, (datetime, pd.Timestamp)):
        return f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'"
    if isinstance(val, date):
        return f"'{val.strftime('%Y-%m-%d')}'"
    s = str(val).replace("'", "''")
    return f"'{s}'"


def _verify_row_count(db_fqn: str, db_creds: dict) -> int:
    """Run SELECT COUNT(*) on Databricks and return actual row count."""
    try:
        from connectors.databricks_connector import DatabricksConnector
        db = DatabricksConnector(
            server_hostname=db_creds.get("db_hostname", ""),
            http_path=db_creds.get("db_http_path", ""),
            access_token=db_creds.get("db_token", ""),
            catalog=db_creds.get("db_catalog"),
        )
        cr = db.execute_sql(f"SELECT COUNT(*) FROM {db_fqn}")
        db.close()
        rows_data = cr.get("rows", [])
        if rows_data:
            return int(rows_data[0][0]) if rows_data[0] else 0
    except Exception:
        pass
    return 0


class InternalDataMover(DataMover):
    CLOUD_PROVIDERS = {
        "AWS": {"scheme": "s3", "staging": "STORAGE_INTEGRATION"},
        "AZURE": {"scheme": "abfss", "staging": "AZURE_STORAGE_INTEGRATION"},
        "GCS": {"scheme": "gcs", "staging": "STORAGE_INTEGRATION"},
    }

    def migrate(self, table_info, sf_creds: dict, db_creds: dict, storage_creds: dict | None = None, cloud_override: str | None = None) -> MigrateResult:
        start = time.time()
        try:
            provider = (cloud_override or table_info.cloud_provider or "AWS").upper()
            if provider not in self.CLOUD_PROVIDERS:
                return MigrateResult(
                    table=table_info.name, storage_type="internal", strategy="copy_into",
                    rows=0, duration_ms=int((time.time() - start) * 1000),
                    success=False, error=f"Unsupported cloud provider: {provider}",
                )

            bucket = (storage_creds or {}).get("bucket", "")
            region = (storage_creds or {}).get("region", "")
            if not bucket:
                return MigrateResult(
                    table=table_info.name, storage_type="internal", strategy="copy_into",
                    rows=0, duration_ms=int((time.time() - start) * 1000),
                    success=False, error=f"Cloud storage bucket required for {provider} export",
                )

            # Pre-check: verify target Databricks table exists
            db_fqn = self._db_target_fqn(table_info, db_creds)
            try:
                from connectors.databricks_connector import DatabricksConnector
                db = DatabricksConnector(
                    server_hostname=db_creds.get("db_hostname", ""),
                    http_path=db_creds.get("db_http_path", ""),
                    access_token=db_creds.get("db_token", ""),
                    catalog=db_creds.get("db_catalog"),
                )
                cr = db.execute_sql(f"DESCRIBE TABLE {db_fqn}")
                db.close()
                if cr.get("error"):
                    return MigrateResult(
                        table=table_info.name, storage_type="internal", strategy="skipped",
                        rows=0, duration_ms=int((time.time() - start) * 1000),
                        success=False, error=f"Target table {db_fqn} does not exist. Deploy the table DDL first.",
                    )
            except Exception:
                pass

            if provider == "AWS":
                return self._migrate_aws(table_info, sf_creds, db_creds, storage_creds, start)
            elif provider == "AZURE":
                return self._migrate_azure(table_info, sf_creds, db_creds, storage_creds, start)
            elif provider == "GCS":
                return self._migrate_gcs(table_info, sf_creds, db_creds, storage_creds, start)
            else:
                return self._migrate_aws(table_info, sf_creds, db_creds, storage_creds, start)

        except Exception as e:
            elapsed = int((time.time() - start) * 1000)
            return MigrateResult(
                table=table_info.name, storage_type="internal", strategy="copy_into",
                rows=0, duration_ms=elapsed, success=False, error=str(e),
            )

    @staticmethod
    def _db_target_fqn(table_info, db_creds: dict) -> str:
        """Map Snowflake FQN to Databricks catalog.schema.table."""
        catalog = db_creds.get("db_catalog") or "workspace"
        schema = table_info.schema.lower() if table_info.schema else "default"
        return f"{catalog}.{schema}.`{table_info.name}`"

    def _migrate_aws(self, table_info, sf_creds, db_creds, storage_creds, start) -> MigrateResult:
        bucket = _sanitize((storage_creds or {}).get("bucket", ""))
        region = _sanitize((storage_creds or {}).get("region", ""))
        storage_integration = _sanitize((storage_creds or {}).get("storage_integration", ""))
        access_key = _sanitize((storage_creds or {}).get("access_key", ""))
        secret_key = _sanitize((storage_creds or {}).get("secret_key", ""))
        iam_role = _sanitize((storage_creds or {}).get("iam_role", ""))

        sf_fqn = f"{table_info.database}.{table_info.schema}.{table_info.name}"
        db_fqn = self._db_target_fqn(table_info, db_creds)
        stage_name = f"migration_stage_{table_info.database}_{table_info.schema}"
        s3_path = f"s3://{bucket}/migration/{table_info.database}/{table_info.schema}/{table_info.name}/"

        from connectors.snowflake_connector import SnowflakeConnector
        sf = SnowflakeConnector(
            account=sf_creds.get("sf_account", ""),
            user=sf_creds.get("sf_user", ""),
            password=sf_creds.get("sf_password", ""),
            warehouse=sf_creds.get("sf_warehouse", ""),
            role=sf_creds.get("sf_role"),
            database=table_info.database,
            schema=table_info.schema,
        )
        conn = sf._connect()
        cur = conn.cursor()

        creds_clause = ""
        if storage_integration:
            creds_clause = f"STORAGE_INTEGRATION = {storage_integration}"
        elif access_key and secret_key:
            creds_clause = f"CREDENTIALS = (AWS_KEY_ID = '{access_key}' AWS_SECRET_KEY = '{secret_key}')"
        elif iam_role:
            creds_clause = f"STORAGE_INTEGRATION = {iam_role}" if iam_role.startswith("si_") else f"AWS_ROLE = ARN='{iam_role}'"

        cur.execute(f"""
            CREATE OR REPLACE STAGE {stage_name}
            URL = '{s3_path}'
            {creds_clause}
        """)

        unload_path = f"@{stage_name}/{table_info.name}/"
        cur.execute(f"""
            COPY INTO '{unload_path}'
            FROM {sf_fqn}
            FILE_FORMAT = (TYPE = PARQUET)
            HEADER = TRUE
            MAX_FILE_SIZE = 1073741824
            OVERWRITE = TRUE
        """)
        cur.close()
        sf.close()

        elapsed = int((time.time() - start) * 1000)
        return MigrateResult(
            table=table_info.name, storage_type="internal", strategy="snowflake_to_s3",
            rows=0, duration_ms=elapsed, success=True,
            error=None,
        )

    def _migrate_azure(self, table_info, sf_creds, db_creds, storage_creds, start) -> MigrateResult:
        container = _sanitize((storage_creds or {}).get("bucket", ""))
        account = _sanitize((storage_creds or {}).get("azure_account", ""))
        sas_token = _sanitize((storage_creds or {}).get("azure_sas_token", ""))

        sf_fqn = f"{table_info.database}.{table_info.schema}.{table_info.name}"
        db_fqn = self._db_target_fqn(table_info, db_creds)
        stage_name = f"migration_stage_{table_info.database}_{table_info.schema}"
        abfss_path = f"abfss://{container}@{account}.dfs.core.windows.net/migration/{table_info.database}/{table_info.schema}/{table_info.name}/"

        from connectors.snowflake_connector import SnowflakeConnector
        sf = SnowflakeConnector(
            account=sf_creds.get("sf_account", ""),
            user=sf_creds.get("sf_user", ""),
            password=sf_creds.get("sf_password", ""),
            warehouse=sf_creds.get("sf_warehouse", ""),
            role=sf_creds.get("sf_role"),
            database=table_info.database,
            schema=table_info.schema,
        )
        conn = sf._connect()
        cur = conn.cursor()

        creds_clause = f'CREDENTIALS = (AZURE_SAS_TOKEN="{sas_token}")' if sas_token else ""
        cur.execute(f"""
            CREATE OR REPLACE STAGE {stage_name}
            URL = '{abfss_path}'
            {creds_clause}
        """)

        unload_path = f"@{stage_name}/{table_info.name}/"
        cur.execute(f"""
            COPY INTO '{unload_path}'
            FROM {sf_fqn}
            FILE_FORMAT = (TYPE = PARQUET)
            HEADER = TRUE
            MAX_FILE_SIZE = 1073741824
            OVERWRITE = TRUE
        """)
        cur.close()
        sf.close()

        elapsed = int((time.time() - start) * 1000)
        return MigrateResult(
            table=table_info.name, storage_type="internal", strategy="snowflake_to_s3",
            rows=0, duration_ms=elapsed, success=True,
        )

    def _migrate_gcs(self, table_info, sf_creds, db_creds, storage_creds, start) -> MigrateResult:
        bucket = _sanitize((storage_creds or {}).get("bucket", ""))
        gcp_account = _sanitize((storage_creds or {}).get("gcp_service_account", ""))

        sf_fqn = f"{table_info.database}.{table_info.schema}.{table_info.name}"
        db_fqn = self._db_target_fqn(table_info, db_creds)
        stage_name = f"migration_stage_{table_info.database}_{table_info.schema}"
        gcs_path = f"gcs://{bucket}/migration/{table_info.database}/{table_info.schema}/{table_info.name}/"

        from connectors.snowflake_connector import SnowflakeConnector
        sf = SnowflakeConnector(
            account=sf_creds.get("sf_account", ""),
            user=sf_creds.get("sf_user", ""),
            password=sf_creds.get("sf_password", ""),
            warehouse=sf_creds.get("sf_warehouse", ""),
            role=sf_creds.get("sf_role"),
            database=table_info.database,
            schema=table_info.schema,
        )
        conn = sf._connect()
        cur = conn.cursor()

        creds_clause = ""
        if gcp_account:
            creds_clause = f'STORAGE_INTEGRATION = {gcp_account}'
        cur.execute(f"""
            CREATE OR REPLACE STAGE {stage_name}
            URL = '{gcs_path}'
            {creds_clause}
        """)

        unload_path = f"@{stage_name}/{table_info.name}/"
        cur.execute(f"""
            COPY INTO '{unload_path}'
            FROM {sf_fqn}
            FILE_FORMAT = (TYPE = PARQUET)
            HEADER = TRUE
            MAX_FILE_SIZE = 1073741824
            OVERWRITE = TRUE
        """)
        cur.close()
        sf.close()

        elapsed = int((time.time() - start) * 1000)
        return MigrateResult(
            table=table_info.name, storage_type="internal", strategy="snowflake_to_s3",
            rows=0, duration_ms=elapsed, success=True,
        )
