import re
from agents.platform.strategy_base import MigrationStrategy, StrategyAnalysis, StrategyPlan
from parser.sql_parser import ParsedObject
from agents.project_loader import ProjectInventory


_POLICY_TYPES = {"masking_policy", "row_access_policy"}


class PolicyStrategy(MigrationStrategy):
    def can_handle(self, object_type: str) -> bool:
        return object_type in _POLICY_TYPES

    def analyze(self, obj: ParsedObject, inventory: ProjectInventory) -> StrategyAnalysis:
        sql = obj.raw_sql or ""
        if obj.object_type == "masking_policy":
            converted = self._convert_masking(sql)
            target = "Function-based column mask"
            manual = [
                "Apply the function to columns using: ALTER TABLE ... ALTER COLUMN col SET MASK func_name",
            ]
        else:
            converted = self._convert_row_access(sql)
            target = "Function-based row filter"
            manual = [
                "Apply the function to tables using: ALTER TABLE ... SET ROW FILTER func_name",
            ]
        return StrategyAnalysis(
            obj_name=obj.name,
            object_type=obj.object_type,
            status="AUTOMATED",
            recommended_target=target,
            additional_services=["Unity Catalog"],
            manual_steps=manual,
            automation_percentage=60,
            notes=["Converted to Databricks function. Requires ALTER TABLE to apply."],
            converted_sql=converted,
        )

    def generate_plan(self, analysis: StrategyAnalysis) -> StrategyPlan:
        return StrategyPlan(
            analysis=analysis,
            deployment_sql=[analysis.converted_sql] if analysis.converted_sql else [],
        )

    @staticmethod
    def _convert_masking(sql: str) -> str:
        return re.sub(
            r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?MASKING\s+POLICY\s+(\S+(?:\.\S+)?)\s+AS\s*\(([^)]+)\)\s+RETURNS\s+(\S+)\s*->\s*(.*)",
            r"CREATE OR REPLACE FUNCTION \1(\2) RETURNS \3 RETURN \4",
            sql,
        )

    @staticmethod
    def _convert_row_access(sql: str) -> str:
        return re.sub(
            r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?ROW\s+ACCESS\s+POLICY\s+(\S+(?:\.\S+)?)\s+AS\s*\(([^)]+)\)\s+RETURNS\s+(\S+)\s*->\s*(.*)",
            r"CREATE OR REPLACE FUNCTION \1(\2) RETURNS \3 RETURN \4",
            sql,
        )
