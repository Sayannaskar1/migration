import json
import time
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Any


@dataclass
class MigrationManifest:
    manifest_version: str = "1.0"
    migration_id: str = ""
    source: str = "snowflake"
    target: str = "databricks"
    started_at: str = ""
    completed_at: str = ""
    duration_seconds: float = 0.0
    status: str = ""  # "completed" | "failed" | "dry_run"

    assessment: dict = field(default_factory=dict)
    storage_summary: dict = field(default_factory=dict)
    plan: dict = field(default_factory=dict)

    total_objects: int = 0
    translated: int = 0
    passed: int = 0
    warnings: int = 0
    errors: int = 0
    llm_calls: int = 0
    deployed: int = 0
    deployed_failed: int = 0

    confidence_buckets: dict = field(default_factory=dict)
    step_timings: dict = field(default_factory=dict)
    blockers: list[str] = field(default_factory=list)
    features_detected: list[str] = field(default_factory=list)
    validation_summary: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {k: v for k, v in asdict(self).items() if v}

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def save(self, path: Path):
        path.write_text(self.to_json())


class ManifestGenerator:
    def generate(self, state) -> MigrationManifest:
        manifest = MigrationManifest()
        manifest.migration_id = getattr(state, "migration_id", str(int(time.time())))

        if hasattr(state, "step_timings"):
            manifest.step_timings = getattr(state, "step_timings", {})

        if hasattr(state, "assessment_report") and state.assessment_report:
            r = state.assessment_report
            manifest.assessment = {
                "databases": r.databases,
                "schemas": r.schemas,
                "tables": r.tables,
                "views": r.views,
                "procedures": r.procedures,
                "functions": r.functions,
                "overall_confidence": r.overall_confidence,
                "migration_strategy": r.migration_strategy,
                "estimated_runtime_min": r.estimated_translation_time_min,
                "estimated_llm_cost_usd": r.estimated_llm_cost_usd,
                "estimated_manual_effort_hours": r.estimated_manual_effort_hours,
            }
            manifest.features_detected = r.snowflake_features_detected
            manifest.blockers = r.blockers

        if hasattr(state, "storage_report") and state.storage_report:
            r = state.storage_report
            manifest.storage_summary = {
                "total_tables": r.total_tables,
                "internal": len(getattr(r, "internal_tables", [])),
                "external": len(getattr(r, "external_tables", [])),
                "iceberg": len(getattr(r, "iceberg_tables", [])),
                "needs_export": r.needs_export,
            }

        if hasattr(state, "migration_plan") and state.migration_plan:
            p = state.migration_plan
            manifest.plan = {
                "total_objects": p.total_objects,
                "complexity": p.estimated_complexity,
                "export_strategy": p.export_strategy,
            }

        if hasattr(state, "inventory") and state.inventory:
            self._populate_inventory(manifest, state.inventory)

        if hasattr(state, "validation_results") and state.validation_results:
            self._populate_validation(manifest, state.validation_results)

        if hasattr(state, "confidence_scores"):
            manifest.confidence_buckets = self._bucket_confidence(state.confidence_scores)

        if hasattr(state, "deployment_results") and state.deployment_results:
            for r in state.deployment_results:
                if r.success:
                    manifest.deployed += 1
                else:
                    manifest.deployed_failed += 1

        return manifest

    def _populate_inventory(self, manifest: MigrationManifest, inventory):
        summary = inventory.summary()
        manifest.total_objects = summary["total_objects"]
        manifest.translated = sum(
            1 for obj in inventory.all_objects if hasattr(obj, "converted_sql") and obj.converted_sql
        )

    def _populate_validation(self, manifest: MigrationManifest, validation_results):
        manifest.passed = sum(1 for v in validation_results.values() if v.status in ("PASS", "WARNING"))
        manifest.warnings = sum(1 for v in validation_results.values() if v.status == "WARNING")
        manifest.errors = sum(1 for v in validation_results.values() if v.status in ("ERROR", "ISSUE"))
        manifest.validation_summary = {
            "passed": manifest.passed,
            "warnings": manifest.warnings,
            "errors": manifest.errors,
        }

    def _bucket_confidence(self, scores: list[dict]) -> dict:
        buckets = {"Automatic": 0, "LLM Assisted": 0, "Manual Review": 0}
        for s in scores:
            label = s.get("confidence_label", "")
            if label in buckets:
                buckets[label] += 1
        return buckets
