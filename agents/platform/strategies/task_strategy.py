import re
from agents.platform.strategy_base import MigrationStrategy, StrategyAnalysis, StrategyPlan, StrategyArtifacts
from parser.sql_parser import ParsedObject
from agents.project_loader import ProjectInventory


class TaskStrategy(MigrationStrategy):
    def can_handle(self, object_type: str) -> bool:
        return object_type == "task"

    def analyze(self, obj: ParsedObject, inventory: ProjectInventory) -> StrategyAnalysis:
        sql = obj.raw_sql or ""
        schedule = self._extract_schedule(sql)
        warehouse = self._extract_warehouse(sql)
        body = self._extract_body(sql)
        notes = []
        if schedule:
            notes.append(f"Schedule: {schedule}")
        if warehouse:
            notes.append(f"Warehouse: {warehouse}")
        return StrategyAnalysis(
            obj_name=obj.name,
            object_type="task",
            status="ARCHITECTURAL_MIGRATION",
            recommended_target="Databricks Job",
            additional_services=["Databricks Workflows", "Job Schedules"],
            manual_steps=[
                "Create a Databricks Job with the task SQL or notebook",
                "Configure schedule via Jobs API or UI",
                "Set up cluster or use SQL Warehouse for execution",
            ],
            automation_percentage=40,
            notes=notes,
            converted_sql=body,
        )

    def generate_plan(self, analysis: StrategyAnalysis) -> StrategyPlan:
        sql = analysis.converted_sql or ""
        job_def = {
            "name": analysis.obj_name,
            "schedule": self._parse_cron(analysis.notes) if analysis.notes else {},
            "tasks": [
                {
                    "task_key": "main",
                    "description": f"Migrated from Snowflake task {analysis.obj_name}",
                    "sql_task": {"query": {"query": sql}} if sql else {},
                }
            ],
        }
        return StrategyPlan(
            analysis=analysis,
            deployment_artifacts={"job_definition": job_def},
        )

    def generate_artifacts(self, plan: StrategyPlan) -> StrategyArtifacts:
        job = plan.deployment_artifacts.get("job_definition", {})
        import json
        return StrategyArtifacts(
            yaml_config=json.dumps(job, indent=2),
        )

    @staticmethod
    def _extract_schedule(sql: str) -> str:
        m = re.search(r"(?i)schedule\s*=\s*'([^']+)'", sql)
        return m.group(1) if m else ""

    @staticmethod
    def _extract_warehouse(sql: str) -> str:
        m = re.search(r"(?i)warehouse\s*=\s*(\S+)", sql)
        return m.group(1) if m else ""

    @staticmethod
    def _extract_body(sql: str) -> str:
        m = re.search(r"(?i)\bas\s+(.+)$", sql, re.DOTALL)
        return m.group(1).strip() if m else ""

    @staticmethod
    def _parse_cron(notes: list[str]) -> dict:
        for note in notes:
            if note.startswith("Schedule: "):
                raw = note[len("Schedule: "):]
                cron_m = re.search(r"(?i)USING\s+CRON\s+([^']+)", raw)
                if cron_m:
                    return {"quartz_cron_expression": cron_m.group(1).strip()}
                if raw.upper().strip().endswith("MINUTE"):
                    import re
                    num_m = re.search(r"(\d+)\s+MINUTE", raw, re.IGNORECASE)
                    if num_m:
                        return {"pause_status": "UNPAUSED", "interval": int(num_m.group(1)), "time_unit": "MINUTES"}
        return {}
