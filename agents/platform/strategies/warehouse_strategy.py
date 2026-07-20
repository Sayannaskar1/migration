import re
from agents.platform.strategy_base import MigrationStrategy, StrategyAnalysis, StrategyPlan
from parser.sql_parser import ParsedObject
from agents.project_loader import ProjectInventory


class WarehouseStrategy(MigrationStrategy):
    def can_handle(self, object_type: str) -> bool:
        return object_type == "warehouse"

    def analyze(self, obj: ParsedObject, inventory: ProjectInventory) -> StrategyAnalysis:
        sql = obj.raw_sql or ""
        size = self._extract_size(sql)
        auto_suspend = self._extract_auto_suspend(sql)
        notes = []
        if size:
            notes.append(f"Size: {size}")
        if auto_suspend:
            notes.append(f"Auto-suspend: {auto_suspend}s")
        target, reason = self._recommend(sql, size)
        return StrategyAnalysis(
            obj_name=obj.name,
            object_type="warehouse",
            status="ARCHITECTURAL_MIGRATION",
            recommended_target=target,
            additional_services=["SQL Warehouse", "Job Clusters", "Serverless"],
            manual_steps=[
                reason,
                "Create the SQL Warehouse via UI or API",
                "Configure warehouse size, auto-stop, and scaling",
            ],
            automation_percentage=30,
            notes=notes,
            converted_sql=sql,
        )

    def generate_plan(self, analysis: StrategyAnalysis) -> StrategyPlan:
        sql = analysis.converted_sql or ""
        name = analysis.obj_name
        # Emit CREATE WAREHOUSE as-is (Databricks supports it)
        return StrategyPlan(
            analysis=analysis,
            deployment_sql=[sql] if sql else [],
            deployment_artifacts={"warehouse_name": name},
        )

    @staticmethod
    def _extract_size(sql: str) -> str:
        m = re.search(r"(?i)warehouse_size\s*=\s*['\"]?(\w[\w-]*)['\"]?", sql)
        return m.group(1) if m else ""

    @staticmethod
    def _extract_auto_suspend(sql: str) -> str:
        m = re.search(r"(?i)auto_suspend\s*=\s*(\d+)", sql)
        return m.group(1) if m else ""

    @staticmethod
    def _recommend(sql: str, size: str) -> tuple[str, str]:
        upper = sql.upper()
        if "SYSTEM$STREAMLIT" in upper:
            return "Serverless Notebook Cluster", "System-managed warehouse for Streamlit — use a Serverless cluster instead"
        if size and WarehouseStrategy._size_rank(size) > 2:
            return "SQL Warehouse (Premium)", f"Large warehouse ({size}) — use a Multi-cluster SQL Warehouse"
        return "SQL Warehouse (Standard)", f"General-purpose warehouse ({size or 'default'}) — use a SQL Warehouse"

    @staticmethod
    def _size_rank(size: str) -> int:
        mapping = {
            "X-SMALL": 0, "SMALL": 1, "MEDIUM": 2,
            "LARGE": 3, "X-LARGE": 4, "2X-LARGE": 5,
            "3X-LARGE": 6, "4X-LARGE": 7, "5X-LARGE": 8,
            "6X-LARGE": 9,
        }
        for k, v in mapping.items():
            if size.upper().replace(" ", "").startswith(k.replace("-", "")):
                return v
        return 1
