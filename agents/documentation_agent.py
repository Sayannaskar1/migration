from pathlib import Path
from agents.project_loader import ProjectInventory, ObjectType
from agents.validation_agent import ValidationResult
from agents.dependency_agent import DependencyGraph
from parser.ast_parser import detect_snowflake_features


def _type_display_name(raw: str) -> str:
    return raw.replace("_", " ").title()


OBJECT_TYPE_LABELS = {}
for _attr_name in dir(ObjectType):
    if _attr_name.startswith("_") or _attr_name == "UNKNOWN":
        continue
    _val = getattr(ObjectType, _attr_name)
    if isinstance(_val, str):
        OBJECT_TYPE_LABELS[_val] = _type_display_name(_val)
OBJECT_TYPE_LABELS[ObjectType.UNKNOWN] = "Unknown"


def _complexity_level(obj, result: ValidationResult = None) -> tuple[str, int]:
    """Score object migration complexity on 1-10 scale, returning (label, score)."""
    base = {"table": 1, "view": 2, "function": 3, "procedure": 4}.get(obj.object_type, 3)
    feature_count = len(detect_snowflake_features(obj.raw_sql))
    base += feature_count
    if result:
        base += len(result.warnings) * 1
        base += len(result.issues) * 2
        base += len(result.errors) * 3
        if result.status == "ARCHITECTURAL CHANGE":
            base += 2
    if base <= 2:
        return "Easy", base
    elif base <= 4:
        return "Medium", base
    elif base <= 7:
        return "Hard", base
    else:
        return "Expert", base


def generate_migration_report(
    inventory: ProjectInventory,
    dep_graph: DependencyGraph,
    validation_results: dict[str, ValidationResult],
    output_dir: Path,
) -> Path:
    summary = inventory.summary()
    ACH = "ARCHITECTURAL CHANGE"
    val_summary = {
        "total": len(validation_results),
        "passed": sum(1 for r in validation_results.values() if r.is_pass()),
        "errors": sum(1 for r in validation_results.values() if r.status == "ERROR"),
        "issues": sum(1 for r in validation_results.values() if r.status == "ISSUE"),
        "warnings": sum(
            1 for r in validation_results.values() if r.status == "WARNING"
        ),
        "architectural": sum(
            1 for r in validation_results.values() if r.status == ACH
        ),
    }
    val_summary["pass_rate"] = (
        f"{val_summary['passed'] / val_summary['total'] * 100:.1f}%"
        if val_summary["total"] > 0
        else "N/A"
    )

    sql_udf_count = 0
    js_udf_count = 0
    for obj in inventory.by_type.get(ObjectType.FUNCTION, []):
        if "LANGUAGE JAVASCRIPT" in obj.raw_sql.upper():
            js_udf_count += 1
        else:
            sql_udf_count += 1
    sql_proc_count = 0
    js_proc_count = 0
    for obj in inventory.by_type.get(ObjectType.PROCEDURE, []):
        if "LANGUAGE JAVASCRIPT" in obj.raw_sql.upper():
            js_proc_count += 1
        else:
            sql_proc_count += 1

    auto_count = sum(1 for r in validation_results.values() if r.is_pass() and not r.warnings)

    # Split warnings into informational vs manual-review-required
    _INFO_WARNINGS = {
        "PRIMARY KEY is informational",
        "FOREIGN KEY is informational",
        "VARIANT column(s) converted",
        "SECURE VIEW access controls lost",
        "Syntax error expected (architectural change)",
    }
    info_warning_count = 0
    manual_review_count = 0
    for r in validation_results.values():
        for w in r.warnings:
            if any(kw in w for kw in _INFO_WARNINGS):
                info_warning_count += 1
            else:
                manual_review_count += 1

    architectural_count = val_summary["architectural"]
    unsupported_count = val_summary["errors"]
    total_val = max(val_summary["total"], 1)
    auto_pct = auto_count / total_val * 100
    info_pct = info_warning_count / total_val * 100
    manual_review_pct = manual_review_count / total_val * 100
    arch_pct = architectural_count / total_val * 100
    unsupported_pct = unsupported_count / total_val * 100

    sqlglot_count = sum(1 for obj in inventory.all_objects
                        if obj.object_type not in ("procedure",)
                        and "$$" not in (obj.raw_sql or "")
                        and "LANGUAGE JAVASCRIPT" not in (obj.raw_sql or "").upper())
    rule_engine_count = auto_count
    llm_count = 0
    manual_ai_count = total_val - auto_count - info_warning_count - manual_review_count
    ai_total = max(rule_engine_count + sqlglot_count + llm_count + manual_ai_count, 1)

    complexity_buckets = {"Easy": 0, "Medium": 0, "Hard": 0, "Expert": 0}
    complexity_total = 0
    for obj in inventory.all_objects:
        r = validation_results.get(obj.name)
        label, score = _complexity_level(obj, r)
        complexity_buckets[label] = complexity_buckets.get(label, 0) + 1
        complexity_total += score
    avg_complexity = round(complexity_total / max(len(inventory.all_objects), 1), 1)
    norm_score = max(0, min(100, round((1 - avg_complexity / 15) * 100)))

    report_lines = [
        "=" * 70,
        "  SNOWFLAKE TO DATABRICKS MIGRATION REPORT",
        "=" * 70,
        "",
        f"Project:       {summary['project']}",
        "",
        "--- Objects ---",
    ]

    type_order = sorted(
        ObjectType.__dict__.values(),
        key=lambda t: len(inventory.by_type.get(t, [])),
        reverse=True,
    )
    seen_type_keys = set()
    for type_val in type_order:
        if not isinstance(type_val, str) or type_val == ObjectType.UNKNOWN:
            continue
        items = inventory.by_type.get(type_val, [])
        if not items:
            continue
        if type_val in seen_type_keys:
            continue
        seen_type_keys.add(type_val)
        count = len(items)
        if type_val in (ObjectType.TABLE, ObjectType.VIEW, ObjectType.PROCEDURE, ObjectType.FUNCTION):
            count = len({o.name.lower() for o in items})
        label = _type_display_name(type_val)
        report_lines.append(f"  {label:25s} {count:>4d}")
        if type_val == ObjectType.FUNCTION:
            report_lines.append(f"    SQL UDF                {sql_udf_count:>4d}")
            report_lines.append(f"    JS UDF                 {js_udf_count:>4d}")
        elif type_val == ObjectType.PROCEDURE:
            report_lines.append(f"    SQL Proc               {sql_proc_count:>4d}")
            report_lines.append(f"    JS Proc                {js_proc_count:>4d}")

    unknown_count = len(inventory.by_type.get(ObjectType.UNKNOWN, []))
    if unknown_count > 0:
        report_lines.append(f"  {'Unknown':25s} {unknown_count:>4d}")
    report_lines.append(f"  {'─' * 25}")
    report_lines.append(f"  Total                  {summary['unique_objects']:>4d}")
    report_lines.append("")
    report_lines.append("--- Migration Coverage ---")
    report_lines.append(f"  Automatic                      {auto_pct:>5.0f}%  ({auto_count:>4d})")
    report_lines.append(f"  Informational Warnings         {info_pct:>5.0f}%  ({info_warning_count:>4d})")
    report_lines.append(f"  Manual Review Required         {manual_review_pct:>5.0f}%  ({manual_review_count:>4d})")
    report_lines.append(f"  Architectural Rewrite          {arch_pct:>5.0f}%  ({architectural_count:>4d})")
    report_lines.append(f"  Unsupported                    {unsupported_pct:>5.0f}%  ({unsupported_count:>4d})")
    report_lines.append("")
    report_lines.append("--- Complexity ---")
    report_lines.append(f"  Easy                   {complexity_buckets['Easy']:>4d}")
    report_lines.append(f"  Medium                 {complexity_buckets['Medium']:>4d}")
    report_lines.append(f"  Hard                   {complexity_buckets['Hard']:>4d}")
    report_lines.append(f"  Expert                 {complexity_buckets['Expert']:>4d}")
    report_lines.append(f"  {'─' * 25}")
    report_lines.append(f"  Overall Complexity     {avg_complexity:>5.1f} / 10")
    report_lines.append("")
    report_lines.append("--- Migration Quality ---")
    report_lines.append(f"  PASS                   {val_summary['passed']:>4d}")
    report_lines.append(f"  WARNING                 {val_summary['warnings']:>4d}")
    report_lines.append(f"  ARCHITECTURAL           {val_summary['architectural']:>4d}")
    report_lines.append(f"  UNSUPPORTED             {val_summary['issues']:>4d}")
    report_lines.append(f"  ERROR                   {val_summary['errors']:>4d}")
    report_lines.append(f"  {'─' * 25}")
    report_lines.append(f"  Overall Migration Score  {norm_score:>3d} / 100")
    report_lines.append(f"  Enterprise Ready        {'YES' if norm_score >= 85 and manual_review_count == 0 else 'CONDITIONAL — Manual Review Required: ' + str(manual_review_count) + ' Object(s)' if manual_review_count > 0 else 'NO'}")
    report_lines.append("")

    ordered = dep_graph.get_deployment_order()
    for i, obj in enumerate(ordered, 1):
        label = OBJECT_TYPE_LABELS.get(obj.object_type, obj.object_type)
        report_lines.append(f"  {i:3d}. [{label:10s}] {obj.name}")

    report_lines.extend(
        [
            "",
            "--- Object Details ---",
        ]
    )

    for obj in ordered:
        label = OBJECT_TYPE_LABELS.get(obj.object_type, obj.object_type)
        report_lines.append("")
        report_lines.append(f"  {label}: {obj.name}")
        report_lines.append(f"    Source:      {obj.file_path}")
        report_lines.append(f"    Converted:   {'Yes' if obj.converted_sql else 'No'}")

        val = validation_results.get(obj.name)
        if val:
            report_lines.append(f"    Status:      {val.status}")
            if val.status != "ERROR":
                report_lines.append(f"    Confidence:  {val.confidence * 100:.0f}%")
            for err in val.errors:
                report_lines.append(f"    ERROR:       {err}")
            for issue in val.issues:
                report_lines.append(f"    ISSUE:       {issue}")
            for warn in val.warnings:
                report_lines.append(f"    WARNING:     {warn}")

        if obj.raw_sql:
            report_lines.append("")
            report_lines.append("    --- Original (Snowflake) ---")
            for line in obj.raw_sql.strip().split("\n"):
                report_lines.append(f"      {line}")
        if obj.converted_sql and "MANUAL REVIEW" not in obj.converted_sql:
            report_lines.append("")
            report_lines.append("    --- Converted (Databricks) ---")
            for line in obj.converted_sql.strip().split("\n"):
                report_lines.append(f"      {line}")

    report_lines.extend(
        [
            "",
            "--- Architectural Changes Required ---",
        ]
    )

    arch_objects = [
        obj for obj in ordered
        if validation_results.get(obj.name) and validation_results[obj.name].status == ACH
    ]
    if arch_objects:
        for obj in arch_objects:
            report_lines.append(f"  - {obj.name} ({obj.object_type})")
    else:
        report_lines.append("  None.")

    report_lines.extend(
        [
            "",
            "--- Objects Requiring Manual Review ---",
        ]
    )

    manual_count = 0
    for obj in ordered:
        if obj.converted_sql and "MANUAL REVIEW" in obj.converted_sql:
            manual_count += 1
            report_lines.append(f"  - {obj.name} ({obj.object_type})")

    if manual_count == 0:
        report_lines.append("  None - all objects fully converted.")

    report_lines.extend(
        [
            "",
            "--- Dependency Graph ---",
        ]
    )

    dep_graph.has_cycles()
    if dep_graph.cycles:
        report_lines.append("  WARNING: Circular dependencies detected:")
        for cycle in dep_graph.cycles:
            report_lines.append(f"    - {cycle}")
    else:
        report_lines.append("  No circular dependencies detected.")

    report_lines.append("")
    report_lines.append("  Object dependency chain (table → view → function → procedure):")
    dep_printed = False
    ordered = dep_graph.get_deployment_order()
    for obj in ordered:
        deps = dep_graph.edges.get(obj.name.lower(), [])
        if deps:
            dep_printed = True
            label = OBJECT_TYPE_LABELS.get(obj.object_type, obj.object_type)
            dep_names = ", ".join(deps)
            report_lines.append(f"    {label} [{obj.name}]  depends on  [{dep_names}]")
    if not dep_printed:
        report_lines.append("    (no explicit dependencies detected)")

    report_lines.extend(
        [
            "",
            "--- Summary ---",
            f"  Total objects:             {summary['total_objects']:>4d}",
            f"  Unique named objects:       {len(validation_results):>4d}",
            f"  Overall complexity:         {avg_complexity:>5.1f} / 10",
            f"  Overall migration score:    {norm_score:>3d} / 100",
            f"  Enterprise ready:           {'YES' if norm_score >= 85 and manual_review_count == 0 else 'CONDITIONAL — Manual Review Required: ' + str(manual_review_count) + ' Object(s)' if manual_review_count > 0 else 'NO'}",
            "",
            "  Migration Coverage:",
            f"    Automatically Converted:  {auto_count:>4d}  ({auto_pct:.0f}%)",
            f"    Informational Warnings:   {info_warning_count:>4d}  ({info_pct:.0f}%)",
            f"    Manual Review Required:   {manual_review_count:>4d}  ({manual_review_pct:.0f}%)",
            f"    Architectural Rewrite:    {architectural_count:>4d}  ({arch_pct:.0f}%)",
            f"    Unsupported:              {unsupported_count:>4d}  ({unsupported_pct:.0f}%)",
            "",
            "  Validation:",
            f"    PASS:              {val_summary['passed']:>4d}",
            f"    WARNING:           {val_summary['warnings']:>4d}",
            f"    ARCHITECTURAL:     {val_summary['architectural']:>4d}",
            f"    UNSUPPORTED:       {val_summary['issues']:>4d}",
            f"    ERROR:             {val_summary['errors']:>4d}",
            "",
            "=" * 70,
        ]
    )

    report_path = output_dir / "migration_report.txt"
    report_path.write_text("\n".join(report_lines), encoding="utf-8")
    return report_path


def generate_object_inventory_csv(
    inventory: ProjectInventory, output_dir: Path
) -> Path:
    csv_path = output_dir / "object_inventory.csv"
    lines = ["object_type,object_name,schema_name,source_file,status"]
    for obj in inventory.all_objects:
        schema = obj.schema_name or ""
        status = "converted" if obj.converted_sql else "pending"
        lines.append(
            f"{obj.object_type},{obj.name},{schema},{obj.file_path},{status}"
        )
    csv_path.write_text("\n".join(lines), encoding="utf-8")
    return csv_path


def generate_dependency_diagram(dep_graph: DependencyGraph, output_dir: Path) -> Path:
    diagram_path = output_dir / "dependency_diagram.txt"
    lines = ["Dependency Diagram", "=" * 50, ""]

    ordered = dep_graph.get_deployment_order()
    for obj in ordered:
        deps = dep_graph.edges.get(obj.name.lower(), [])
        if deps:
            dep_names = ", ".join(deps)
            lines.append(f"  {obj.name}  -->  [{dep_names}]")
        else:
            lines.append(f"  {obj.name}  (no dependencies)")

    diagram_path.write_text("\n".join(lines), encoding="utf-8")
    return diagram_path
