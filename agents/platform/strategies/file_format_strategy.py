import re
from agents.platform.strategy_base import MigrationStrategy, StrategyAnalysis, StrategyPlan
from parser.sql_parser import ParsedObject
from agents.project_loader import ProjectInventory


class FileFormatStrategy(MigrationStrategy):
    def can_handle(self, object_type: str) -> bool:
        return object_type == "file_format"

    def analyze(self, obj: ParsedObject, inventory: ProjectInventory) -> StrategyAnalysis:
        sql = obj.raw_sql or ""
        fmt_type = self._extract_type(sql)
        notes = [f"File format type: {fmt_type}"] if fmt_type else []
        return StrategyAnalysis(
            obj_name=obj.name,
            object_type="file_format",
            status="ARCHITECTURAL_MIGRATION",
            recommended_target="Inline file format in COPY INTO or Auto Loader",
            additional_services=["Auto Loader", "COPY INTO"],
            manual_steps=[
                "Replace CREATE FILE FORMAT with inline format specification",
                "In COPY INTO: use FILEFORMAT = JSON / CSV / PARQUET",
                "In Auto Loader: use .option('cloudFiles.format', 'json')",
            ],
            automation_percentage=5,
            notes=notes,
        )

    @staticmethod
    def _extract_type(sql: str) -> str:
        m = re.search(r"(?i)TYPE\s*=\s*['\"]?(\w+)['\"]?", sql)
        return m.group(1) if m else ""
