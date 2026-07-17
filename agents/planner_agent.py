from dataclasses import dataclass, field
from agents.catalog_mapping_engine import CatalogMappingEngine, CatalogMapResult, CATALOG_STRATEGY_PRESERVE


@dataclass
class PlanStep:
    object_name: str
    object_type: str
    depends_on: list[str] = field(default_factory=list)
    estimated_complexity: str = "low"
    blockers: list[str] = field(default_factory=list)


@dataclass
class MigrationPlan:
    project_name: str
    execution_order: list[PlanStep]
    total_objects: int
    estimated_complexity: str
    blockers: list[str]
    storage_summary: str | None = None
    needs_export: bool = False
    export_strategy: str | None = None
    catalog_mapping: CatalogMapResult | None = None
    target_version: str = "current"


class PlannerAgent:
    DEPLOYMENT_ORDER = [
        "catalog", "schema", "sequence", "table", "view", "function", "procedure",
    ]

    def __init__(self):
        self._catalog_engine = CatalogMappingEngine()

    def build_plan(
        self,
        inventory,
        storage_report=None,
        capability_results: dict[str, list[dict]] | None = None,
        target_version: str = "current",
        catalog_strategy: str = CATALOG_STRATEGY_PRESERVE,
        catalog_merge_target: str = "",
        catalog_rename_map: dict[str, str] | None = None,
        catalog_custom_mappings: list | None = None,
        existing_catalogs: list[str] | None = None,
        existing_schemas: list[str] | None = None,
    ) -> MigrationPlan:
        steps = []
        blockers = []
        complexities = []

        for obj in inventory.all_objects:
            deps = self._find_dependencies(obj)
            obj_blockers = self._find_blockers(obj)
            blockers.extend(obj_blockers)
            complexity = self._estimate_complexity(obj)
            complexities.append(complexity)
            steps.append(
                PlanStep(
                    object_name=obj.name,
                    object_type=obj.object_type,
                    depends_on=deps,
                    estimated_complexity=complexity,
                    blockers=obj_blockers,
                )
            )

        ordered = self._sort_by_dependency(steps)
        overall = "high" if blockers else self._overall_complexity(complexities)

        storage_summary = None
        needs_export = False
        export_strategy = "none"
        if storage_report:
            storage_summary = storage_report.summary
            needs_export = storage_report.needs_export
            if storage_report.external_tables or storage_report.iceberg_tables:
                export_strategy = "direct_register"
            if storage_report.needs_export:
                export_strategy = "copy_into"
            if storage_report.external_tables and storage_report.internal_tables:
                export_strategy = "mixed"

        if capability_results:
            unsupported = 0
            for obj_name, caps in capability_results.items():
                for c in caps:
                    if c.get("capability") in ("not_supported", "architectural_change"):
                        unsupported += 1
                        if unsupported <= 5:
                            blockers.append(f"{obj_name}: {c.get('capability', 'issue')} - {c.get('feature', '')}")
            if unsupported:
                overall = "high"

        catalog_mapping = self._catalog_engine.build_mapping(
            inventory,
            strategy=catalog_strategy,
            merge_target_catalog=catalog_merge_target,
            rename_map=catalog_rename_map,
            custom_mappings=catalog_custom_mappings,
            existing_catalogs=existing_catalogs,
            existing_schemas=existing_schemas,
        )

        return MigrationPlan(
            project_name=inventory.project_path.name,
            execution_order=ordered,
            total_objects=len(steps),
            estimated_complexity=overall,
            blockers=blockers,
            storage_summary=storage_summary,
            needs_export=needs_export,
            export_strategy=export_strategy,
            catalog_mapping=catalog_mapping,
            target_version=target_version,
        )

    def _find_dependencies(self, obj) -> list[str]:
        import re
        deps = []
        sql = obj.raw_sql or ""
        refs = re.findall(r"\bFROM\s+(\w+)", sql, re.IGNORECASE)
        refs += re.findall(r"\bJOIN\s+(\w+)", sql, re.IGNORECASE)
        refs += re.findall(r"\bREFERENCES\s+(\w+)", sql, re.IGNORECASE)
        return list(set(r for r in refs if r != obj.name))

    def _find_blockers(self, obj) -> list[str]:
        blockers = []
        sql = obj.raw_sql or ""
        import re
        if re.search(r"\bCLONE\b", sql, re.IGNORECASE):
            blockers.append(f"{obj.name}: CLONE not supported")
        if re.search(r"\bLANGUAGE\s+JAVASCRIPT\b", sql, re.IGNORECASE):
            blockers.append(f"{obj.name}: JavaScript UDF needs manual rewrite")
        return blockers

    def _estimate_complexity(self, obj) -> str:
        sql = obj.raw_sql or ""
        lines = len(sql.split("\n"))
        if lines > 100:
            return "high"
        if lines > 30:
            return "medium"
        return "low"

    def _overall_complexity(self, complexities: list[str]) -> str:
        if "high" in complexities:
            return "high"
        if "medium" in complexities:
            return "medium"
        return "low"

    def _sort_by_dependency(self, steps: list[PlanStep]) -> list[PlanStep]:
        ordered = []
        remaining = list(steps)
        order_map = {t: i for i, t in enumerate(self.DEPLOYMENT_ORDER)}

        level = 0
        while remaining and level < 10:
            ready = [
                s
                for s in remaining
                if all(
                    any(d == r.object_name for r in ordered) for d in s.depends_on
                )
            ]
            if not ready:
                remaining.sort(
                    key=lambda s: (
                        order_map.get(s.object_type, 99),
                        s.estimated_complexity != "low",
                    )
                )
                ordered.append(remaining.pop(0))
                level += 1
                continue
            ready.sort(
                key=lambda s: (
                    order_map.get(s.object_type, 99),
                    s.estimated_complexity != "low",
                )
            )
            ordered.extend(ready)
            for r in ready:
                remaining.remove(r)
            level += 1

        ordered.extend(remaining)
        return ordered
