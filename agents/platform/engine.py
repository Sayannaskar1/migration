from agents.project_loader import ProjectInventory
from agents.platform.strategy_base import MigrationStrategy, StrategyAnalysis, StrategyPlan, StrategyArtifacts
from agents.platform.strategies.stream_strategy import StreamStrategy
from agents.platform.strategies.task_strategy import TaskStrategy
from agents.platform.strategies.warehouse_strategy import WarehouseStrategy
from agents.platform.strategies.pipe_strategy import PipeStrategy
from agents.platform.strategies.stage_strategy import StageStrategy
from agents.platform.strategies.resource_monitor_strategy import ResourceMonitorStrategy
from agents.platform.strategies.role_strategy import RoleStrategy
from agents.platform.strategies.security_strategy import SecurityStrategy
from agents.platform.strategies.file_format_strategy import FileFormatStrategy
from agents.platform.strategies.policy_strategy import PolicyStrategy


_BUILTIN_STRATEGIES: list[MigrationStrategy] = [
    StreamStrategy(),
    TaskStrategy(),
    WarehouseStrategy(),
    PipeStrategy(),
    StageStrategy(),
    ResourceMonitorStrategy(),
    RoleStrategy(),
    SecurityStrategy(),
    FileFormatStrategy(),
    PolicyStrategy(),
]


class PlatformMigrationEngine:
    def __init__(self, strategies: list[MigrationStrategy] | None = None):
        self.strategies = strategies or list(_BUILTIN_STRATEGIES)

    def analyze(self, inventory: ProjectInventory) -> list[StrategyAnalysis]:
        results: list[StrategyAnalysis] = []
        for obj in inventory.all_objects:
            strategy = self._find_strategy(obj.object_type)
            if strategy is None:
                continue
            try:
                analysis = strategy.analyze(obj, inventory)
                results.append(analysis)
            except Exception as e:
                results.append(StrategyAnalysis(
                    obj_name=obj.name,
                    object_type=obj.object_type,
                    status="ERROR",
                    notes=[f"Strategy analysis failed: {e}"],
                ))
        return results

    def plan(self, analyses: list[StrategyAnalysis]) -> list[StrategyPlan]:
        plans: list[StrategyPlan] = []
        for analysis in analyses:
            strategy = self._find_strategy(analysis.object_type)
            if strategy is None:
                continue
            try:
                plan = strategy.generate_plan(analysis)
                plans.append(plan)
            except Exception as e:
                plans.append(StrategyPlan(
                    analysis=analysis,
                    deployment_artifacts={"error": str(e)},
                ))
        return plans

    def generate_artifacts(self, plans: list[StrategyPlan]) -> list[StrategyArtifacts]:
        artifacts: list[StrategyArtifacts] = []
        for plan in plans:
            strategy = self._find_strategy(plan.analysis.object_type)
            if strategy is None:
                continue
            try:
                artifact = strategy.generate_artifacts(plan)
                artifacts.append(artifact)
            except Exception:
                artifacts.append(StrategyArtifacts())
        return artifacts

    def analyze_and_plan(self, inventory: ProjectInventory) -> list[StrategyPlan]:
        analyses = self.analyze(inventory)
        return self.plan(analyses)

    def _find_strategy(self, object_type: str) -> MigrationStrategy | None:
        for s in self.strategies:
            if s.can_handle(object_type):
                return s
        return None
