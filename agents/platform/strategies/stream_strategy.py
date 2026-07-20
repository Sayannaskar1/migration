import re
from agents.platform.strategy_base import MigrationStrategy, StrategyAnalysis, StrategyPlan, StrategyArtifacts
from parser.sql_parser import ParsedObject
from agents.project_loader import ProjectInventory


class StreamStrategy(MigrationStrategy):
    def can_handle(self, object_type: str) -> bool:
        return object_type == "stream"

    def analyze(self, obj: ParsedObject, inventory: ProjectInventory) -> StrategyAnalysis:
        sql = obj.raw_sql or ""
        on_table = self._extract_table(sql)
        return StrategyAnalysis(
            obj_name=obj.name,
            object_type="stream",
            status="ARCHITECTURAL_MIGRATION",
            recommended_target="Delta Change Data Feed",
            additional_services=["Structured Streaming", "Auto Loader"],
            manual_steps=[
                "Enable CDF on the source table",
                "Create a streaming table or Structured Streaming pipeline",
                "Replace stream references in downstream jobs",
            ],
            automation_percentage=30,
            notes=[f"Stream on table: {on_table}"] if on_table else [],
        )

    def generate_plan(self, analysis: StrategyAnalysis) -> StrategyPlan:
        sql_statements = []
        table_name = None
        for note in analysis.notes:
            if note.startswith("Stream on table: "):
                table_name = note[len("Stream on table: "):]
                break
        if table_name and table_name != "unknown":
            sql_statements.append(
                f"ALTER TABLE {table_name} SET TBLPROPERTIES (\n"
                f"  delta.enableChangeDataFeed = true\n"
                f");"
            )
        return StrategyPlan(
            analysis=analysis,
            deployment_sql=sql_statements,
            deployment_artifacts={
                "type": "cdf_enablement",
                "source_table": table_name,
            },
        )

    def generate_artifacts(self, plan: StrategyPlan) -> StrategyArtifacts:
        table = plan.deployment_artifacts.get("source_table", "source_table")
        notebook = (
            f"# Databricks notebook: Stream migration from Snowflake STREAM\n"
            f"# Source table: {table}\n\n"
            f"from pyspark.sql.functions import col, to_timestamp, input_file_name\n\n"
            f'df = (spark.readStream\n'
            f'  .format("delta")\n'
            f'  .option("readChangeFeed", "true")\n'
            f'  .option("startingVersion", "latest")\n'
            f'  .table("{table}")\n'
            f')\n\n'
            f"# Write the stream. Replace with your target table and checkpoint location.\n"
            f"# query = (df.writeStream\n"
            f"#   .outputMode(\"append\")\n"
            f"#   .format(\"delta\")\n"
            f"#   .option(\"checkpointLocation\", \"/Volumes/.../_checkpoints/...\")\n"
            f"#   .table(\"target_catalog.target_schema.target_table\")\n"
            f"# )\n"
        )
        return StrategyArtifacts(
            sql_statements=plan.deployment_sql,
            notebook_code=notebook,
        )

    @staticmethod
    def _extract_table(sql: str) -> str:
        m = re.search(r"(?i)on\s+table\s+(\S+)", sql)
        return m.group(1) if m else "unknown"
