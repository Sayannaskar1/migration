from dataclasses import dataclass, field


CATALOG_STRATEGY_PRESERVE = "preserve"
CATALOG_STRATEGY_MERGE = "merge"
CATALOG_STRATEGY_RENAME = "rename"
CATALOG_STRATEGY_CUSTOM = "custom"


@dataclass
class CatalogMapping:
    strategy: str = CATALOG_STRATEGY_PRESERVE
    snowflake_database: str = ""
    databricks_catalog: str = ""
    snowflake_schema: str = ""
    databricks_schema: str = ""


@dataclass
class CatalogMapResult:
    strategy: str = CATALOG_STRATEGY_PRESERVE
    mappings: list[CatalogMapping] = field(default_factory=list)
    catalog_create_sql: list[str] = field(default_factory=list)
    schema_create_sql: list[str] = field(default_factory=list)
    existing_catalogs: list[str] = field(default_factory=list)
    existing_schemas: list[str] = field(default_factory=list)

    @property
    def mapping_table(self) -> list[dict]:
        return [
            {
                "snowflake_database": m.snowflake_database,
                "snowflake_schema": m.snowflake_schema,
                "databricks_catalog": m.databricks_catalog,
                "databricks_schema": m.databricks_schema,
            }
            for m in self.mappings
        ]


class CatalogMappingEngine:
    def build_mapping(
        self,
        inventory,
        strategy: str = CATALOG_STRATEGY_PRESERVE,
        merge_target_catalog: str = "",
        rename_map: dict[str, str] | None = None,
        custom_mappings: list[CatalogMapping] | None = None,
        existing_catalogs: list[str] | None = None,
        existing_schemas: list[str] | None = None,
    ) -> CatalogMapResult:
        databases = self._discover_databases(inventory)
        schemas = self._discover_schemas(inventory)

        result = CatalogMapResult(
            strategy=strategy,
            existing_catalogs=existing_catalogs or [],
            existing_schemas=existing_schemas or [],
        )

        if strategy == CATALOG_STRATEGY_PRESERVE:
            self._apply_preserve(result, databases, schemas)
        elif strategy == CATALOG_STRATEGY_MERGE:
            self._apply_merge(result, databases, schemas, merge_target_catalog)
        elif strategy == CATALOG_STRATEGY_RENAME:
            self._apply_rename(result, databases, schemas, rename_map or {})
        elif strategy == CATALOG_STRATEGY_CUSTOM:
            self._apply_custom(result, custom_mappings or [])
        else:
            self._apply_preserve(result, databases, schemas)

        self._generate_ddl(result)
        return result

    def _discover_databases(self, inventory) -> set[str]:
        dbs = set()
        for obj in inventory.all_objects:
            parts = (obj.name or "").split(".")
            if len(parts) >= 2:
                dbs.add(parts[0])
            sql = obj.raw_sql or ""
            self._extract_fqn_from_sql(sql, dbs, None)
        return dbs

    def _discover_schemas(self, inventory) -> set[str]:
        schemas = set()
        for obj in inventory.all_objects:
            parts = (obj.name or "").split(".")
            if len(parts) >= 2:
                schemas.add(f"{parts[0]}.{parts[1]}")
            sql = obj.raw_sql or ""
            self._extract_fqn_from_sql(sql, None, schemas)
        return schemas

    def _extract_fqn_from_sql(self, sql: str, dbs: set[str] | None, schemas: set[str] | None):
        import re
        for match in re.finditer(
            r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW|PROCEDURE|FUNCTION)\s+(\w+)\.(\w+)\.(\w+)",
            sql, re.IGNORECASE,
        ):
            db = match.group(1)
            schema = match.group(2)
            if dbs is not None:
                dbs.add(db)
            if schemas is not None:
                schemas.add(f"{db}.{schema}")

    def _apply_preserve(self, result: CatalogMapResult, databases: set[str], schemas: set[str]):
        for db in sorted(databases):
            result.mappings.append(CatalogMapping(
                strategy=CATALOG_STRATEGY_PRESERVE,
                snowflake_database=db,
                databricks_catalog=db,
            ))
        for skey in sorted(schemas):
            parts = skey.split(".")
            result.mappings.append(CatalogMapping(
                strategy=CATALOG_STRATEGY_PRESERVE,
                snowflake_database=parts[0],
                snowflake_schema=parts[1],
                databricks_catalog=parts[0],
                databricks_schema=parts[1],
            ))

    def _apply_merge(self, result: CatalogMapResult, databases: set[str], schemas: set[str], target: str):
        target_name = target or "MERGED"
        for db in sorted(databases):
            result.mappings.append(CatalogMapping(
                strategy=CATALOG_STRATEGY_MERGE,
                snowflake_database=db,
                databricks_catalog=target_name,
            ))
        for skey in sorted(schemas):
            parts = skey.split(".")
            merged_schema = f"{parts[0]}_{parts[1]}"
            result.mappings.append(CatalogMapping(
                strategy=CATALOG_STRATEGY_MERGE,
                snowflake_database=parts[0],
                snowflake_schema=parts[1],
                databricks_catalog=target_name,
                databricks_schema=merged_schema,
            ))

    def _apply_rename(self, result: CatalogMapResult, databases: set[str], schemas: set[str], rename_map: dict[str, str]):
        for db in sorted(databases):
            target = rename_map.get(db, db)
            result.mappings.append(CatalogMapping(
                strategy=CATALOG_STRATEGY_RENAME,
                snowflake_database=db,
                databricks_catalog=target,
            ))
        for skey in sorted(schemas):
            parts = skey.split(".")
            db_target = rename_map.get(parts[0], parts[0])
            result.mappings.append(CatalogMapping(
                strategy=CATALOG_STRATEGY_RENAME,
                snowflake_database=parts[0],
                snowflake_schema=parts[1],
                databricks_catalog=db_target,
                databricks_schema=parts[1],
            ))

    def _apply_custom(self, result: CatalogMapResult, custom_mappings: list[CatalogMapping]):
        for m in custom_mappings:
            result.mappings.append(m)

    def _generate_ddl(self, result: CatalogMapResult):
        seen_catalogs = set()
        seen_schemas = set()

        for m in result.mappings:
            cat = m.databricks_catalog
            if cat and cat not in seen_catalogs:
                if cat not in result.existing_catalogs:
                    result.catalog_create_sql.append(f"CREATE CATALOG IF NOT EXISTS {cat}")
                seen_catalogs.add(cat)

            if m.databricks_schema:
                schema_fqn = f"{cat}.{m.databricks_schema}"
                if schema_fqn not in seen_schemas:
                    schema_key = f"{cat}.{m.databricks_schema}"
                    if schema_key not in result.existing_schemas:
                        result.schema_create_sql.append(f"CREATE SCHEMA IF NOT EXISTS {schema_key}")
                    seen_schemas.add(schema_key)

    def resolve_name(self, obj_name: str, mapping_result: CatalogMapResult) -> str:
        parts = obj_name.split(".")
        if len(parts) < 2:
            return obj_name

        source_db = parts[0]
        source_schema = parts[1] if len(parts) > 1 else ""
        object_name = parts[2] if len(parts) > 2 else parts[1]

        target_catalog = source_db
        target_schema = source_schema

        for m in mapping_result.mappings:
            if m.snowflake_database == source_db:
                if m.databricks_catalog:
                    target_catalog = m.databricks_catalog
                if m.snowflake_schema and m.snowflake_schema == source_schema and m.databricks_schema:
                    target_schema = m.databricks_schema

        if target_schema:
            return f"{target_catalog}.{target_schema}.{object_name}"
        return f"{target_catalog}.{object_name}"
