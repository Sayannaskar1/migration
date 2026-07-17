from dataclasses import dataclass, field
import re


@dataclass
class OptimizationSuggestion:
    object_name: str
    object_type: str
    suggestion_type: str  # "optimize" | "vacuum" | "zorder" | "partition" | "incremental"
    sql: str
    priority: str  # "high" | "medium" | "low"
    reason: str


class PerformanceOptimizer:
    def analyze(self, inventory) -> list[OptimizationSuggestion]:
        suggestions = []
        for obj in inventory.all_objects:
            sql = obj.converted_sql or obj.raw_sql or ""
            if obj.object_type == "table":
                suggestions.extend(self._suggest_for_table(obj, sql))
            elif obj.object_type == "view":
                suggestions.extend(self._suggest_for_view(obj, sql))
        return suggestions

    def _suggest_for_table(self, obj, sql: str) -> list[OptimizationSuggestion]:
        suggestions = []
        name = obj.name

        suggestions.append(OptimizationSuggestion(
            object_name=name, object_type="table",
            suggestion_type="optimize",
            sql=f"OPTIMIZE {name}",
            priority="high",
            reason="Improve query performance and file layout",
        ))

        suggestions.append(OptimizationSuggestion(
            object_name=name, object_type="table",
            suggestion_type="vacuum",
            sql=f"VACUUM {name}",
            priority="medium",
            reason="Remove old snapshots and reclaim storage",
        ))

        if "PARTITION BY" in sql.upper() or "CLUSTER BY" in sql.upper():
            suggestions.append(OptimizationSuggestion(
                object_name=name, object_type="table",
                suggestion_type="zorder",
                sql=f"OPTIMIZE {name} ZORDER BY (key_column)",
                priority="medium",
                reason="Consider ZORDER on frequently filtered columns",
            ))

        if self._detects_large_table(sql):
            suggestions.append(OptimizationSuggestion(
                object_name=name, object_type="table",
                suggestion_type="partition",
                sql=f"ALTER TABLE {name} SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
                priority="medium",
                reason="Enable auto-optimize for tables with frequent writes",
            ))

        return suggestions

    def _suggest_for_view(self, obj, sql: str) -> list[OptimizationSuggestion]:
        suggestions = []
        if "EXISTS" in sql or "NOT EXISTS" in sql:
            suggestions.append(OptimizationSuggestion(
                object_name=obj.name, object_type="view",
                suggestion_type="incremental",
                sql=f"CREATE OR REFRESH MATERIALIZED VIEW {obj.name} AS {sql}",
                priority="low",
                reason="Consider materialized view if query pattern is repeated",
            ))
        return suggestions

    def _detects_large_table(self, sql: str) -> bool:
        return False  # placeholder; would need table stats

    def generate_report(self, suggestions: list[OptimizationSuggestion]) -> str:
        if not suggestions:
            return "# Performance Optimization\n\nNo suggestions — all objects look well-optimized."

        lines = ["# Performance Optimization Recommendations", ""]
        for s in suggestions:
            lines.append(f"## {s.suggestion_type.upper()}: {s.object_name}")
            lines.append(f"  Priority: {s.priority}")
            lines.append(f"  Reason:   {s.reason}")
            lines.append(f"  ```sql")
            lines.append(f"  {s.sql}")
            lines.append(f"  ```")
            lines.append("")
        return "\n".join(lines)
