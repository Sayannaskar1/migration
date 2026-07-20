import re
from agents.platform.strategy_base import MigrationStrategy, StrategyAnalysis, StrategyPlan
from parser.sql_parser import ParsedObject
from agents.project_loader import ProjectInventory


class ResourceMonitorStrategy(MigrationStrategy):
    def can_handle(self, object_type: str) -> bool:
        return object_type == "resource_monitor"

    def analyze(self, obj: ParsedObject, inventory: ProjectInventory) -> StrategyAnalysis:
        sql = obj.raw_sql or ""
        quota = self._extract_quota(sql)
        frequency = self._extract_frequency(sql)
        notes = []
        if quota:
            notes.append(f"Credit quota: {quota}")
        if frequency:
            notes.append(f"Frequency: {frequency}")
        return StrategyAnalysis(
            obj_name=obj.name,
            object_type="resource_monitor",
            status="ARCHITECTURAL_MIGRATION",
            recommended_target="Unity Catalog Budget + Cluster Policy",
            additional_services=["Unity Catalog Budgets", "Cluster Policies", "Cost Management Console"],
            manual_steps=[
                "Set up Unity Catalog budget for the account",
                "Create cluster policies with cost limits",
                "Configure cost monitoring dashboards",
            ],
            automation_percentage=10,
            notes=notes,
        )

    def generate_plan(self, analysis: StrategyAnalysis) -> StrategyPlan:
        return StrategyPlan(
            analysis=analysis,
            deployment_artifacts={
                "resource_monitor": analysis.obj_name,
                "budget_recommendation": f"Create budget with limit matching credit quota",
            },
        )

    @staticmethod
    def _extract_quota(sql: str) -> str:
        m = re.search(r"(?i)CREDIT_QUOTA\s*=\s*(\d+)", sql)
        return m.group(1) if m else ""

    @staticmethod
    def _extract_frequency(sql: str) -> str:
        m = re.search(r"(?i)FREQUENCY\s*=\s*'([^']+)'", sql)
        return m.group(1) if m else ""
