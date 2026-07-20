import re
from agents.platform.strategy_base import MigrationStrategy, StrategyAnalysis, StrategyPlan
from parser.sql_parser import ParsedObject
from agents.project_loader import ProjectInventory


_BUILTIN_SNOWFLAKE_ROLES = {
    "ACCOUNTADMIN", "SYSADMIN", "SECURITYADMIN", "USERADMIN",
    "PUBLIC", "ORGADMIN",
}


class RoleStrategy(MigrationStrategy):
    def can_handle(self, object_type: str) -> bool:
        return object_type == "role"

    def analyze(self, obj: ParsedObject, inventory: ProjectInventory) -> StrategyAnalysis:
        name = obj.name.upper()
        is_builtin = name in _BUILTIN_SNOWFLAKE_ROLES
        sql = obj.raw_sql or ""
        converted = re.sub(r"(?i)CREATE\s+OR\s+REPLACE\s+ROLE", "CREATE ROLE", sql, count=1)
        status = "SKIPPED" if is_builtin else "AUTOMATED"
        notes = [f"Built-in Snowflake role — no Databricks equivalent"] if is_builtin else []
        return StrategyAnalysis(
            obj_name=obj.name,
            object_type="role",
            status=status,
            recommended_target="Unity Catalog Role" if not is_builtin else "Not applicable",
            additional_services=["Unity Catalog", "Account Console"] if not is_builtin else [],
            manual_steps=[] if not is_builtin else [],
            automation_percentage=100 if not is_builtin else 0,
            notes=notes,
            converted_sql=converted if not is_builtin else None,
        )

    def generate_plan(self, analysis: StrategyAnalysis) -> StrategyPlan:
        return StrategyPlan(
            analysis=analysis,
            deployment_sql=[analysis.converted_sql] if analysis.converted_sql else [],
            deployment_artifacts={"role_name": analysis.obj_name, "skipped": analysis.status == "SKIPPED"},
        )
