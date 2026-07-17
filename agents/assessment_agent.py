from dataclasses import dataclass, field
import time


@dataclass
class AssessmentReport:
    databases: int = 0
    schemas: int = 0
    tables: int = 0
    views: int = 0
    procedures: int = 0
    functions: int = 0

    internal_tables: int = 0
    external_tables: int = 0
    iceberg_tables: int = 0

    ddl_complexity: str = "low"
    view_compatibility: float = 100.0
    function_compatibility: float = 100.0
    procedure_compatibility: float = 100.0
    overall_confidence: float = 100.0

    estimated_translation_time_min: int = 0
    estimated_llm_calls: int = 0
    estimated_llm_cost_usd: float = 0.0
    estimated_manual_effort_hours: float = 0.0

    snowflake_features_detected: list[str] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)

    migration_strategy: str = ""  # "automatic" | "llm_assisted" | "manual"

    raw: dict = field(default_factory=dict)

    def summary_lines(self) -> list[str]:
        lines = []
        lines.append("=" * 58)
        lines.append("  Migration Assessment Report")
        lines.append("=" * 58)
        lines.append(f"  Databases          : {self.databases}")
        lines.append(f"  Schemas            : {self.schemas}")
        lines.append(f"  Tables             : {self.tables}")
        lines.append(f"  Views              : {self.views}")
        lines.append(f"  Procedures         : {self.procedures}")
        lines.append(f"  Functions          : {self.functions}")
        lines.append("")
        lines.append(f"  Internal Tables    : {self.internal_tables}")
        lines.append(f"  External Tables    : {self.external_tables}")
        lines.append(f"  Iceberg Tables     : {self.iceberg_tables}")
        lines.append("")
        lines.append(f"  Estimated Runtime  : ~{self.estimated_translation_time_min}m")
        lines.append(f"  LLM Calls          : ~{self.estimated_llm_calls}")
        lines.append(f"  LLM Cost           : ~${self.estimated_llm_cost_usd:.2f}")
        lines.append(f"  Manual Effort      : ~{self.estimated_manual_effort_hours}h")
        lines.append("")
        if self.blockers:
            lines.append(f"  Blockers           : {len(self.blockers)}")
            for b in self.blockers[:5]:
                lines.append(f"    - {b}")
            lines.append("")
        lines.append("  Migration Complexity")
        lines.append(f"    DDL              100%")
        lines.append(f"    Views             {self.view_compatibility:.0f}%")
        lines.append(f"    Functions         {self.function_compatibility:.0f}%")
        lines.append(f"    Procedures        {self.procedure_compatibility:.0f}%")
        lines.append("")
        lines.append(f"  Overall Confidence : {self.overall_confidence:.0f}%")
        lines.append("")
        lines.append("  Recommended Strategy")
        lines.append(f"    {self.migration_strategy}")
        lines.append("")
        for rec in self.recommendations:
            lines.append(f"  > {rec}")
        lines.append("=" * 58)
        return lines


class MigrationAssessmentAgent:
    def assess(self, inventory, storage_report=None, capability_results=None) -> AssessmentReport:
        summary = inventory.summary()

        report = AssessmentReport(
            databases=self._count_databases(inventory),
            schemas=self._count_schemas(inventory),
            tables=summary["tables"],
            views=summary["views"],
            procedures=summary["procedures"],
            functions=summary["functions"],
            raw={
                "databases": self._get_databases(inventory),
                "schemas": self._get_schemas(inventory),
            },
        )

        self._assess_storage(report, storage_report)
        self._assess_capabilities(report, capability_results)
        self._estimate_runtime(report)
        self._estimate_costs(report)
        self._estimate_effort(report)
        self._compute_confidence(report)
        self._recommend_strategy(report)

        return report

    def _count_databases(self, inventory) -> int:
        dbs = set()
        for obj in inventory.all_objects:
            parts = (obj.name or "").split(".")
            if len(parts) >= 2:
                dbs.add(parts[0])
        return max(len(dbs), 1)

    def _count_schemas(self, inventory) -> int:
        schemas = set()
        for obj in inventory.all_objects:
            parts = (obj.name or "").split(".")
            if len(parts) >= 2:
                schemas.add(f"{parts[0]}.{parts[1]}")
        return max(len(schemas), 1)

    def _get_databases(self, inventory) -> list[str]:
        dbs = set()
        for obj in inventory.all_objects:
            parts = (obj.name or "").split(".")
            if len(parts) >= 2:
                dbs.add(parts[0])
        return sorted(dbs)

    def _get_schemas(self, inventory) -> list[str]:
        schemas = set()
        for obj in inventory.all_objects:
            parts = (obj.name or "").split(".")
            if len(parts) >= 2:
                schemas.add(f"{parts[0]}.{parts[1]}")
        return sorted(schemas)

    def _assess_storage(self, report: AssessmentReport, storage_report):
        if not storage_report:
            return
        report.internal_tables = len(getattr(storage_report, "internal_tables", []))
        report.external_tables = len(getattr(storage_report, "external_tables", []))
        report.iceberg_tables = len(getattr(storage_report, "iceberg_tables", []))

    def _assess_capabilities(self, report: AssessmentReport, capability_results):
        if not capability_results:
            return
        features = set()
        unsupported = 0
        total_features = 0
        for obj_name, caps in capability_results.items():
            for c in caps:
                total_features += 1
                feat = c.get("feature", "")
                cap = c.get("capability", "")
                if feat:
                    features.add(feat)
                if cap == "not_supported":
                    unsupported += 1
                elif cap == "architectural_change":
                    unsupported += 1
        report.snowflake_features_detected = sorted(features)

        objs_with_findings = len(capability_results)
        total_objs = report.tables + report.views + report.procedures + report.functions
        if total_objs > 0:
            compat = max(0, 100 - (unsupported / max(total_objs, 1)) * 100)
            report.view_compatibility = compat if report.views > 0 else 100.0
            report.function_compatibility = compat if report.functions > 0 else 100.0
            report.procedure_compatibility = compat if report.procedures > 0 else 100.0

    def _estimate_runtime(self, report: AssessmentReport):
        total = report.tables + report.views + report.procedures + report.functions
        sqlglot = total * 0.05
        llm = report.estimated_llm_calls * 5.0
        validation = total * 0.02
        total_min = int((sqlglot + llm + validation) / 60)
        report.estimated_translation_time_min = max(total_min, 1)

    def _estimate_costs(self, report: AssessmentReport):
        total = report.tables + report.views + report.procedures + report.functions
        llm_calls = int(total * 0.15)
        report.estimated_llm_calls = llm_calls
        report.estimated_llm_cost_usd = round(llm_calls * 0.002, 2)

    def _estimate_effort(self, report: AssessmentReport):
        total = report.tables + report.views + report.procedures + report.functions
        report.estimated_manual_effort_hours = round(total * 0.02, 1)

    def _compute_confidence(self, report: AssessmentReport):
        scores = [report.view_compatibility, report.function_compatibility, report.procedure_compatibility]
        report.overall_confidence = round(sum(scores) / len(scores), 1)

    def _recommend_strategy(self, report: AssessmentReport):
        if report.overall_confidence >= 90:
            report.migration_strategy = "Automatic Migration"
            report.recommendations.append("No blockers — full automatic migration recommended")
        elif report.overall_confidence >= 70:
            report.migration_strategy = "LLM Assisted Migration"
            report.recommendations.append("LLM review recommended for low-confidence objects")
            report.recommendations.append("Plan for manual review of procedures and complex views")
        else:
            report.migration_strategy = "Manual Review Required"
            report.recommendations.append("Manual review strongly recommended before production migration")
            report.recommendations.append("Consider running a pilot migration on a subset of objects first")

        if report.blockers:
            report.recommendations.append(f"Resolve {len(report.blockers)} blocker(s) before migration")
