from dataclasses import dataclass


@dataclass
class ValidationResult:
    table: str
    schema_match: bool
    row_count_match: bool
    source_rows: int
    target_rows: int
    checksum_match: bool | None = None
    passed: bool = False
    discrepancies: list[str] = None

    def __post_init__(self):
        if self.discrepancies is None:
            self.discrepancies = []
        self.passed = (
            self.schema_match
            and self.row_count_match
            and (self.checksum_match is None or self.checksum_match)
        )


class DataValidationEngine:
    def validate(self, tables: list[dict], sf_creds: dict, db_creds: dict) -> list[ValidationResult]:
        results = []
        for table in tables:
            try:
                result = self._validate_single(table, sf_creds, db_creds)
                results.append(result)
            except Exception as e:
                results.append(ValidationResult(
                    table=table.get("name", "unknown"),
                    schema_match=False, row_count_match=False,
                    source_rows=0, target_rows=0,
                    passed=False, discrepancies=[str(e)],
                ))
        return results

    def _validate_single(self, table: dict, sf_creds: dict, db_creds: dict) -> ValidationResult:
        name = table.get("name", "unknown")

        source_rows = self._count_source_rows(name, sf_creds)
        target_rows = self._count_target_rows(name, db_creds)
        schema_source = self._get_schema(name, "source", sf_creds)
        schema_target = self._get_schema(name, "target", db_creds)

        schema_match = schema_source == schema_target
        row_count_match = source_rows == target_rows
        discrepancies = []

        if not schema_match:
            discrepancies.append(
                f"Schema mismatch: {set(schema_source) ^ set(schema_target)}"
            )
        if not row_count_match:
            discrepancies.append(
                f"Row count: source={source_rows} target={target_rows}"
            )

        checksum_match = None
        if row_count_match and source_rows > 0:
            checksum_match = self._compare_checksums(name, sf_creds, db_creds)

        return ValidationResult(
            table=name,
            schema_match=schema_match,
            row_count_match=row_count_match,
            source_rows=source_rows,
            target_rows=target_rows,
            checksum_match=checksum_match,
            discrepancies=discrepancies,
        )

    def _get_sf_conn(self, sf_creds: dict):
        from connectors.snowflake_connector import SnowflakeConnector
        sf = SnowflakeConnector(
            account=sf_creds.get("sf_account", ""),
            user=sf_creds.get("sf_user", ""),
            password=sf_creds.get("sf_password", ""),
            warehouse=sf_creds.get("sf_warehouse", ""),
            role=sf_creds.get("sf_role"),
            database=sf_creds.get("sf_database"),
            schema=sf_creds.get("sf_schema"),
        )
        return sf

    def _get_db_conn(self, db_creds: dict):
        from connectors.databricks_connector import DatabricksConnector
        return DatabricksConnector(
            server_hostname=db_creds.get("db_hostname", ""),
            http_path=db_creds.get("db_http_path", ""),
            access_token=db_creds.get("db_token", ""),
            catalog=db_creds.get("db_catalog"),
            schema=db_creds.get("db_schema"),
        )

    def _count_source_rows(self, table: str, sf_creds: dict) -> int:
        try:
            sf = self._get_sf_conn(sf_creds)
            conn = sf._connect()
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            row = cursor.fetchone()
            cursor.close()
            sf.close()
            return row[0] if row else 0
        except Exception:
            return 0

    def _count_target_rows(self, table: str, db_creds: dict) -> int:
        try:
            db = self._get_db_conn(db_creds)
            resp = db.execute_sql(f"SELECT COUNT(*) AS cnt FROM {table}")
            db.close()
            if resp.get("status") == "success" and resp.get("rows"):
                return int(resp["rows"][0][0])
            return 0
        except Exception:
            return 0

    def _get_schema(self, table: str, side: str, creds: dict) -> list[tuple]:
        try:
            if side == "source":
                sf = self._get_sf_conn(creds)
                conn = sf._connect()
                cursor = conn.cursor()
                cursor.execute(f"DESCRIBE TABLE {table}")
                cols = [(row[0], row[1]) for row in cursor.fetchall()]
                cursor.close()
                sf.close()
                return cols
            else:
                db = self._get_db_conn(creds)
                resp = db.execute_sql(f"DESCRIBE TABLE {table}")
                db.close()
                if resp.get("status") == "success" and resp.get("rows"):
                    return [(row[0], row[1]) for row in resp["rows"]]
                return []
        except Exception:
            return []

    def _compare_checksums(self, table: str, sf_creds: dict, db_creds: dict) -> bool | None:
        source_hash = self._get_checksum(table, "source", sf_creds)
        target_hash = self._get_checksum(table, "target", db_creds)
        if source_hash is not None and target_hash is not None:
            return source_hash == target_hash
        return None

    def _get_checksum(self, table: str, side: str, creds: dict) -> str | None:
        try:
            if side == "source":
                sf = self._get_sf_conn(creds)
                conn = sf._connect()
                cursor = conn.cursor()
                cursor.execute(f"SELECT HASH_AGG(*) FROM (SELECT * FROM {table} ORDER BY ALL)")
                row = cursor.fetchone()
                cursor.close()
                sf.close()
                return str(row[0]) if row else None
            else:
                db = self._get_db_conn(creds)
                schema_resp = db.execute_sql(f"DESCRIBE TABLE {table}")
                if schema_resp.get("status") != "success" or not schema_resp.get("rows"):
                    db.close()
                    return None
                cols = [r[0] for r in schema_resp["rows"]]
                if not cols:
                    db.close()
                    return None
                col_list = ", ".join(cols)
                sql = (
                    f"SELECT MD5(TO_JSON(STRUCT({col_list}))) AS checksum "
                    f"FROM (SELECT {col_list} FROM {table} ORDER BY ALL)"
                )
                resp = db.execute_sql(sql)
                db.close()
                if resp.get("status") == "success" and resp.get("rows"):
                    return str(resp["rows"][0][0])
                return None
        except Exception:
            return None
