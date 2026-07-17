from dataclasses import dataclass, field
from typing import Protocol, Any
from pathlib import Path


class AgentInterface(Protocol):
    def run(self, state: "MigrationState") -> "MigrationState":
        ...


@dataclass
class MigrationState:
    migration_id: str
    config: dict = field(default_factory=dict)
    project_path: str = ""
    output_dir: str = "output"

    completed_steps: list[str] = field(default_factory=list)
    current_step: str | None = None
    step_logs: dict[str, list[str]] = field(default_factory=dict)

    inventory: Any = None
    dep_graph: Any = None
    storage_report: Any = None
    migration_plan: Any = None
    capability_results: dict = field(default_factory=dict)
    validation_results: dict = field(default_factory=dict)
    confidence_scores: list = field(default_factory=list)
    healing_results: list = field(default_factory=list)

    deployment_results: list = field(default_factory=list)
    deployment_approved: bool = False
    rollback_results: list = field(default_factory=list)

    data_migration_results: list = field(default_factory=list)
    data_validation_results: list = field(default_factory=list)

    assessment_report: Any = None
    manifest: Any = None
    optimization_suggestions: list = field(default_factory=list)
    step_timings: dict = field(default_factory=dict)
    catalog_map: dict = field(default_factory=dict)
    source_hashes: dict = field(default_factory=dict)
    dry_run: bool = False
    target_version: str = ""

    extra: dict = field(default_factory=dict)

    start_time: float | None = None
    end_time: float | None = None
    error: str | None = None

    @property
    def is_complete(self) -> bool:
        return bool(self.error) or self.current_step is None

    def has_completed(self, step: str) -> bool:
        return step in self.completed_steps

    def log(self, step: str, message: str):
        if step not in self.step_logs:
            self.step_logs[step] = []
        self.step_logs[step].append(message)
