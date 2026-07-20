from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional
from parser.sql_parser import ParsedObject
from agents.project_loader import ProjectInventory


@dataclass
class StrategyAnalysis:
    obj_name: str
    object_type: str
    status: str = "ARCHITECTURAL_MIGRATION"
    recommended_target: str = ""
    additional_services: list[str] = field(default_factory=list)
    manual_steps: list[str] = field(default_factory=list)
    automation_percentage: int = 0
    notes: list[str] = field(default_factory=list)
    converted_sql: Optional[str] = None
    confidence: int = 95
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "obj_name": self.obj_name,
            "object_type": self.object_type,
            "status": self.status,
            "recommended_target": self.recommended_target,
            "additional_services": self.additional_services,
            "manual_steps": self.manual_steps,
            "automation_percentage": self.automation_percentage,
            "notes": self.notes,
            "converted_sql": self.converted_sql,
            "confidence": self.confidence,
            "warnings": self.warnings,
        }


@dataclass
class StrategyPlan:
    analysis: StrategyAnalysis
    deployment_sql: list[str] = field(default_factory=list)
    deployment_artifacts: dict = field(default_factory=dict)
    dependencies: list[str] = field(default_factory=list)


@dataclass
class StrategyArtifacts:
    sql_statements: list[str] = field(default_factory=list)
    job_definitions: list[dict] = field(default_factory=list)
    notebook_code: str = ""
    yaml_config: str = ""
    terraform_config: str = ""


class MigrationStrategy(ABC):
    @abstractmethod
    def can_handle(self, object_type: str) -> bool:
        ...

    @abstractmethod
    def analyze(self, obj: ParsedObject, inventory: ProjectInventory) -> StrategyAnalysis:
        ...

    def generate_plan(self, analysis: StrategyAnalysis) -> StrategyPlan:
        return StrategyPlan(analysis=analysis)

    def generate_artifacts(self, plan: StrategyPlan) -> StrategyArtifacts:
        return StrategyArtifacts()
