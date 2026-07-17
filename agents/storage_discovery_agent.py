from dataclasses import dataclass, field


@dataclass
class TableStorageInfo:
    name: str
    database: str
    schema: str
    storage_type: str  # "internal" | "external" | "iceberg"
    stage: str | None = None
    storage_location: str | None = None
    cloud_provider: str | None = None  # "AWS" | "AZURE" | "GCS"
    file_format: str | None = None
    row_count: int | None = None
    needs_export: bool = True


@dataclass
class StageInfo:
    name: str
    type: str  # "internal" | "external"
    storage_location: str | None = None
    cloud_provider: str | None = None
    file_format: str | None = None


@dataclass
class StorageReport:
    total_tables: int
    internal_tables: list[TableStorageInfo]
    external_tables: list[TableStorageInfo]
    iceberg_tables: list[TableStorageInfo]
    stages: list[StageInfo]
    storage_integrations: list[str]
    cloud_providers: set[str]
    needs_export: bool
    needs_s3_credentials: bool
    summary: str


class StorageDiscoveryAgent:
    def discover(self, sf_connector) -> StorageReport:
        conn = sf_connector._connect()
        cur = conn.cursor()

        databases = self._get_databases(cur, sf_connector)
        schemas = self._get_all_schemas(cur, databases, sf_connector)

        tables = self._get_all_tables(cur, schemas)
        stages = self._get_stages(cur)
        integrations = self._get_storage_integrations(cur)
        cur.close()

        internal = []
        external = []
        iceberg = []
        cloud_providers = set()

        for t in tables:
            storage_type = self._classify_table(t)
            info = TableStorageInfo(
                name=t["name"],
                database=t["database"],
                schema=t["schema"],
                storage_type=storage_type,
            )
            if storage_type == "external":
                ext_info = self._get_external_table_info(cur, t)
                info.stage = ext_info.get("stage")
                info.storage_location = ext_info.get("location")
                info.cloud_provider = ext_info.get("cloud_provider")
                info.file_format = ext_info.get("file_format")
                info.needs_export = False
                external.append(info)
                if ext_info.get("cloud_provider"):
                    cloud_providers.add(ext_info["cloud_provider"])
            elif storage_type == "iceberg":
                info.needs_export = False
                iceberg.append(info)
            else:
                internal.append(info)

        for s in stages:
            if s.cloud_provider:
                cloud_providers.add(s.cloud_provider)

        needs_export = len(internal) > 0
        needs_s3 = any(p == "AWS" for p in cloud_providers) or (
            needs_export and self._any_stage_s3(stages)
        )

        if needs_export:
            summary = (
                f"{len(internal)} internal table(s) need export to cloud storage. "
                f"{len(external)} external table(s) can be registered directly. "
                f"{len(iceberg)} Iceberg table(s) can be read directly."
            )
        elif not needs_export and not external and not iceberg:
            summary = "No tables found."
        elif not needs_export:
            summary = (
                f"All {len(tables)} table(s) are external or Iceberg. "
                "No data export needed — register existing locations."
            )
        else:
            summary = f"{len(tables)} table(s) analyzed."

        return StorageReport(
            total_tables=len(tables),
            internal_tables=internal,
            external_tables=external,
            iceberg_tables=iceberg,
            stages=stages,
            storage_integrations=integrations,
            cloud_providers=cloud_providers,
            needs_export=needs_export,
            needs_s3_credentials=needs_s3,
            summary=summary,
        )

    def _get_databases(self, cur, sf_connector):
        if sf_connector.database:
            return [sf_connector.database]
        cur.execute("SHOW DATABASES")
        return [
            row[1]
            for row in cur.fetchall()
            if row[1] not in ("SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA")
        ]

    def _get_all_schemas(self, cur, databases, sf_connector):
        schemas = []
        for db in databases:
            db_clean = db.replace('"', "")
            if sf_connector.schema:
                schemas.append((db_clean, sf_connector.schema))
            else:
                cur.execute(f"SHOW SCHEMAS IN \"{db_clean}\"")
                schemas.extend(
                    (db_clean, row[1])
                    for row in cur.fetchall()
                    if row[1] not in ("INFORMATION_SCHEMA",)
                )
        return schemas

    def _get_all_tables(self, cur, schemas):
        tables = []
        for db, schema in schemas:
            try:
                cur.execute(f"SHOW TABLES IN \"{db}\".\"{schema}\"")
                cols = [desc[0] for desc in cur.description]
                for row in cur.fetchall():
                    t = dict(zip(cols, row))
                    t["database"] = db
                    t["schema"] = schema
                    tables.append(t)
            except Exception:
                pass
        return tables

    def _classify_table(self, table: dict) -> str:
        if table.get("is_external", "").upper() == "Y":
            return "external"
        return "internal"

    def _get_external_table_info(self, cur, table: dict) -> dict:
        info = {"stage": None, "location": None, "cloud_provider": None, "file_format": None}
        try:
            fqn = f'"{table["database"]}"."{table["schema"]}"."{table["name"]}"'
            cur.execute(f"DESCRIBE TABLE {fqn}")
            for row in cur.fetchall():
                key = (row[0] or "").strip().upper()
                val = (row[1] or "").strip() if len(row) > 1 else ""
                if key == "STAGE":
                    info["stage"] = val
                elif key == "LOCATION":
                    info["location"] = val
                elif key == "FILE_FORMAT":
                    info["file_format"] = val
            if info["location"]:
                loc = info["location"].lower()
                if loc.startswith("s3://") or loc.startswith("s3n://"):
                    info["cloud_provider"] = "AWS"
                elif loc.startswith("wasb://") or loc.startswith("abfss://"):
                    info["cloud_provider"] = "AZURE"
                elif loc.startswith("gcs://") or loc.startswith("gs://"):
                    info["cloud_provider"] = "GCS"
        except Exception:
            pass
        return info

    def _get_stages(self, cur) -> list[StageInfo]:
        stages = []
        try:
            cur.execute("SHOW STAGES")
            cols = [desc[0] for desc in cur.description]
            for row in cur.fetchall():
                s = dict(zip(cols, row))
                loc = s.get("storage_location") or ""
                cloud = None
                if loc.startswith("s3://") or loc.startswith("s3n://"):
                    cloud = "AWS"
                elif loc.startswith("wasb://") or loc.startswith("abfss://"):
                    cloud = "AZURE"
                elif loc.startswith("gcs://") or loc.startswith("gs://"):
                    cloud = "GCS"
                stages.append(
                    StageInfo(
                        name=s.get("name", ""),
                        type="external" if loc else "internal",
                        storage_location=loc or None,
                        cloud_provider=cloud,
                        file_format=s.get("file_format"),
                    )
                )
        except Exception:
            pass
        return stages

    def _get_storage_integrations(self, cur) -> list[str]:
        try:
            cur.execute("SHOW STORAGE INTEGRATIONS")
            return [row[1] for row in cur.fetchall()]
        except Exception:
            return []

    def _any_stage_s3(self, stages: list[StageInfo]) -> bool:
        return any(s.cloud_provider == "AWS" for s in stages)
