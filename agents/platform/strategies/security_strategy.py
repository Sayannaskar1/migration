from agents.platform.strategy_base import MigrationStrategy, StrategyAnalysis, StrategyPlan
from parser.sql_parser import ParsedObject
from agents.project_loader import ProjectInventory


_SECURITY_TYPES = {
    "security_integration", "storage_integration",
    "notification_integration", "api_integration", "network_policy",
}


class SecurityStrategy(MigrationStrategy):
    def can_handle(self, object_type: str) -> bool:
        return object_type in _SECURITY_TYPES

    def analyze(self, obj: ParsedObject, inventory: ProjectInventory) -> StrategyAnalysis:
        target_map = {
            "security_integration": "OAuth / SCIM / IDP configuration",
            "storage_integration": "Storage credential in Unity Catalog",
            "notification_integration": "Notification destination in Databricks",
            "api_integration": "External gateway / API proxy",
            "network_policy": "IP ACL / Private Link / VPC peering",
        }
        target = target_map.get(obj.object_type, "Manual configuration")
        return StrategyAnalysis(
            obj_name=obj.name,
            object_type=obj.object_type,
            status="ARCHITECTURAL_MIGRATION",
            recommended_target=target,
            additional_services=["Unity Catalog", "Account Console", "Cloud Console"],
            manual_steps=[
                f"Snowflake {obj.object_type.upper()} has no direct SQL equivalent",
                f"Configure the equivalent capability in Databricks Account Console",
                "Review security policies and access controls",
            ],
            automation_percentage=5,
            notes=[f"Requires manual setup in Databricks Account Console"],
        )

    def generate_plan(self, analysis: StrategyAnalysis) -> StrategyPlan:
        return StrategyPlan(
            analysis=analysis,
            deployment_artifacts={
                "integration_type": analysis.object_type,
                "integration_name": analysis.obj_name,
            },
        )
