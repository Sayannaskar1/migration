import re
from agents.platform.strategy_base import MigrationStrategy, StrategyAnalysis, StrategyPlan, StrategyArtifacts
from parser.sql_parser import ParsedObject
from agents.project_loader import ProjectInventory


class StageStrategy(MigrationStrategy):
    def can_handle(self, object_type: str) -> bool:
        return object_type == "stage"

    def analyze(self, obj: ParsedObject, inventory: ProjectInventory) -> StrategyAnalysis:
        sql = obj.raw_sql or ""
        url = self._extract_url(sql)
        is_external = url is not None

        if is_external:
            integration = self._extract_integration(sql)
            file_format = self._extract_file_format(sql)
            encryption = self._extract_encryption(sql)
            notes = []
            notes.append(f"URL: {url}")
            if integration:
                notes.append(f"Storage Integration: {integration} \u2192 Databricks Storage Credential")
            if file_format:
                notes.append(f"File Format: {file_format}")
            if encryption:
                notes.append(f"Encryption: {encryption}")
            notes.append("\u26a0 IAM role ARN required for Storage Credential — not available from Snowflake")

            return StrategyAnalysis(
                obj_name=obj.name,
                object_type="stage",
                status="ARCHITECTURAL_MIGRATION",
                recommended_target="Storage Credential + External Location + External Volume",
                additional_services=["Unity Catalog", "Storage Credential", "External Location", "External Volume"],
                manual_steps=[
                    "1. Replace <IAM_ROLE_ARN> in CREATE STORAGE CREDENTIAL with your AWS IAM role ARN",
                    "2. Execute CREATE STORAGE CREDENTIAL (one-time cloud IAM setup)",
                    "3. Execute CREATE EXTERNAL LOCATION (binds URL to credential)",
                    "4. Execute CREATE EXTERNAL VOLUME (enables /Volumes/ path access)",
                    "5. Update all @stage_name references to external location or volume path",
                ],
                automation_percentage=70,
                notes=notes,
                confidence=96,
                warnings=["Storage Credential requires manual IAM configuration"],
            )
        else:
            return StrategyAnalysis(
                obj_name=obj.name,
                object_type="stage",
                status="AUTOMATED",
                recommended_target="Managed Volume (Unity Catalog)",
                additional_services=["Unity Catalog"],
                manual_steps=[
                    "Update all @stage_name references to /Volumes/... path",
                ],
                automation_percentage=90,
                confidence=99,
            )

    def generate_plan(self, analysis: StrategyAnalysis) -> StrategyPlan:
        sql = []
        is_external = analysis.automation_percentage < 80
        if is_external:
            sql.append("-- ARCHITECTURAL CHANGE: External Stage \u2192 Storage Credential + External Location + External Volume")
            sql.append("-- See the schema conversion for full SQL templates with IAM placeholder")
            sql.append("-- MANUAL ACTION REQUIRED: IAM role ARN must be configured manually")
        else:
            sql.append(f"CREATE VOLUME IF NOT EXISTS {analysis.obj_name};")
        return StrategyPlan(
            analysis=analysis,
            deployment_sql=sql,
            deployment_artifacts={"volume_name": analysis.obj_name},
        )

    @staticmethod
    def _extract_url(sql: str) -> str | None:
        m = re.search(r"(?i)URL\s*=\s*['\"]([^'\"]+)['\"]", sql)
        return m.group(1) if m else None

    @staticmethod
    def _extract_integration(sql: str) -> str | None:
        m = re.search(r"(?i)STORAGE_INTEGRATION\s*=\s*(\w+)", sql)
        return m.group(1) if m else None

    @staticmethod
    def _extract_file_format(sql: str) -> str | None:
        m = re.search(r"(?i)FILE_FORMAT\s*=\s*\(([^)]*)\)", sql)
        return m.group(1).strip() if m else None

    @staticmethod
    def _extract_encryption(sql: str) -> str | None:
        m = re.search(r"(?i)ENCRYPTION\s*=\s*\(([^)]*)\)", sql)
        return m.group(1).strip() if m else None
