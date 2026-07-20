import re
from agents.platform.strategy_base import MigrationStrategy, StrategyAnalysis, StrategyPlan, StrategyArtifacts
from parser.sql_parser import ParsedObject
from agents.project_loader import ProjectInventory


class PipeStrategy(MigrationStrategy):
    def can_handle(self, object_type: str) -> bool:
        return object_type == "pipe"

    def analyze(self, obj: ParsedObject, inventory: ProjectInventory) -> StrategyAnalysis:
        sql = obj.raw_sql or ""
        copy_into = self._extract_copy_into(sql)
        return StrategyAnalysis(
            obj_name=obj.name,
            object_type="pipe",
            status="ARCHITECTURAL_MIGRATION",
            recommended_target="Auto Loader",
            additional_services=["Structured Streaming", "Databricks Workflows"],
            manual_steps=[
                "Replace Snowpipe with Auto Loader (cloudFiles in Databricks)",
                "Configure cloud storage notification services (SQS, Pub/Sub, Event Grid)",
                "Set up checkpoint directory in Volumes or DBFS",
            ],
            automation_percentage=25,
            notes=[f"COPY INTO: {copy_into[:200]}"] if copy_into else [],
        )

    def generate_plan(self, analysis: StrategyAnalysis) -> StrategyPlan:
        return StrategyPlan(
            analysis=analysis,
            deployment_artifacts={"copy_into_sql": analysis.notes[0] if analysis.notes else ""},
        )

    def generate_artifacts(self, plan: StrategyPlan) -> StrategyArtifacts:
        copy_sql = plan.deployment_artifacts.get("copy_into_sql", "")
        target_table = self._extract_target_table(copy_sql)
        notebook = (
            f"# Databricks notebook: Auto Loader migration from Snowpipe\n"
            f"# Target table: {target_table}\n\n"
            f'from pyspark.sql.functions import col, input_file_name, current_timestamp\n\n'
            f"cloud_file_path = \"/path/to/source/files\"  # Replace with your cloud storage path\n"
            f"checkpoint_path = \"/Volumes/.../_checkpoints/pipe_migration\"\n\n"
            f"df = (\n"
            f'  spark.readStream\n'
            f'  .format("cloudFiles")\n'
            f'  .option("cloudFiles.format", "json")  # Change to csv, parquet, etc.\n'
            f'  .option("cloudFiles.schemaLocation", checkpoint_path + "/schema")\n'
            f'  .option("cloudFiles.inferColumnTypes", "true")\n'
            f"  .load(cloud_file_path)\n"
            f"  .withColumn(\"_file_name\", input_file_name())\n"
            f"  .withColumn(\"_ingested_at\", current_timestamp())\n"
            f")\n\n"
            f"# query = (\n"
            f"#   df.writeStream\n"
            f"#   .trigger(availableNow=True)  # or .trigger(processingTime=\"5 minutes\"))\n"
            f"#   .format(\"delta\")\n"
            f"#   .option(\"checkpointLocation\", checkpoint_path)\n"
            f"#   .table(\"{target_table}\")\n"
            f"# )\n"
        )
        return StrategyArtifacts(
            notebook_code=notebook,
        )

    @staticmethod
    def _extract_copy_into(sql: str) -> str:
        m = re.search(r"(?i)\bas\s+(.+)", sql, re.DOTALL)
        return m.group(1).strip() if m else ""

    @staticmethod
    def _extract_target_table(text: str) -> str:
        m = re.search(r"(?i)COPY\s+INTO\s+(\S+)", text)
        return m.group(1) if m else "target_table"
