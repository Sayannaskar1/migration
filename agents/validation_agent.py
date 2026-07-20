import re
from typing import Optional
import sqlglot
from agents.project_loader import ParsedObject, ProjectInventory
from parser.ast_parser import validate_sql_syntax, detect_snowflake_features


class ValidationResult:
    ARCHITECTURAL_CHANGE = "ARCHITECTURAL CHANGE"

    def __init__(self, obj: ParsedObject):
        self.obj = obj
        self.status: str = "PASS"
        self.issues: list[str] = []
        self.warnings: list[str] = []
        self.errors: list[str] = []
        self.notes: list[str] = []
        self.confidence: float = 1.0

    def is_pass(self) -> bool:
        return self.status == "PASS"

    def compute_confidence(self) -> float:
        c = 1.0
        c -= len(self.warnings) * 0.1
        c -= len(self.issues) * 0.2
        c -= len(self.errors) * 0.3
        if self.status == self.ARCHITECTURAL_CHANGE:
            c = min(c, 0.7)
        self.confidence = max(0.0, round(c, 2))
        return self.confidence

    def to_dict(self) -> dict:
        return {
            "object_name": self.obj.name,
            "object_type": self.obj.object_type,
            "status": self.status,
            "confidence": self.confidence,
            "errors": self.errors,
            "issues": self.issues,
            "warnings": self.warnings,
            "notes": self.notes,
        }


def _count_columns(sql: str) -> int:
    col_pattern = r"(\w+)\s+(?:NUMBER|VARCHAR|VARIANT|OBJECT|ARRAY|TIMESTAMP(?:_NTZ|_LTZ|_TZ)?|TIMESTAMPNTZ|TIMESTAMPLTZ|TIMESTAMP_TZ|DATE|BOOLEAN|INT|INTEGER|FLOAT|FLOAT8|DOUBLE|STRING|TEXT|BINARY|DECIMAL|BIGINT|SMALLINT|TINYINT|BYTE|BYTES|INTERVAL|GEOGRAPHY|GEOMETRY)"
    try:
        tree = sqlglot.parse_one(sql)
        cols = list(tree.find_all(sqlglot.exp.ColumnDef))
        if cols:
            return len(cols)
    except Exception:
        pass
    # SQLGlot may fall back to Command for backtick-quoted tables — retry after stripping backticks
    no_backticks = sql.replace("`", "")
    if no_backticks != sql:
        try:
            tree = sqlglot.parse_one(no_backticks)
            cols = list(tree.find_all(sqlglot.exp.ColumnDef))
            if cols:
                return len(cols)
        except Exception:
            pass
    matches = [m for m in re.findall(col_pattern, sql, re.IGNORECASE)
               if m.upper() not in ("AS", "NOT", "KEY", "BY", "ON", "IN", "SET", "ALL", "FOR", "AND", "OR")]
    return len(matches)


def _validate_schema_match(source: ParsedObject, target_sql: str) -> tuple[list[str], list[str]]:
    notes: list[str] = []
    issues: list[str] = []
    source_count = _count_columns(source.raw_sql)
    target_count = _count_columns(target_sql)
    if source_count != target_count:
        issues.append(
            f"Column count mismatch: source={source_count}, target={target_count}"
        )
    else:
        notes.append(f"Column count verified: {source_count} columns")
    return notes, issues


def _check_remaining_snowflake(target_sql: str) -> list[str]:
    remaining = detect_snowflake_features(target_sql)
    return [f"Unconverted Snowflake feature: {f}" for f in remaining]


def _check_manual_review(target_sql: str) -> list[str]:
    warnings: list[str] = []
    if "MANUAL REVIEW" in target_sql:
        warnings.append("Object contains manual review markers")
    return warnings


def _check_invalid_databricks_types(target_sql: str) -> list[str]:
    errors: list[str] = []
    invalid = re.findall(r"\b(STRING|BINARY)\s*\(\s*\d+\s*\)", target_sql, re.IGNORECASE)
    for match in invalid:
        errors.append(
            f"Invalid type: {match} — Databricks does not support length-qualified STRING/BINARY"
        )
    return errors


def _check_variant_mapping(source: ParsedObject, target_sql: str) -> list[str]:
    warnings: list[str] = []
    if "VARIANT" in source.raw_sql.upper() and "VARIANT" not in target_sql:
        warnings.append(
            "VARIANT column(s) converted to STRING — semantic information may be lost"
        )
    return warnings


def _check_constraints(source: ParsedObject, target_sql: str) -> tuple[list[str], list[str]]:
    notes: list[str] = []
    warnings: list[str] = []
    if re.search(r"\bPRIMARY\s+KEY\b", source.raw_sql, re.IGNORECASE):
        notes.append(
            "PRIMARY KEY metadata preserved — Snowflake and Databricks both treat PRIMARY KEY as informational metadata for standard tables"
        )
    if re.search(r"\bFOREIGN\s+KEY\b", source.raw_sql, re.IGNORECASE):
        notes.append(
            "FOREIGN KEY metadata preserved — Snowflake and Databricks both treat FOREIGN KEY as informational metadata for standard tables"
        )
    return notes, warnings


def validate_object(
    obj: ParsedObject, inventory: ProjectInventory
) -> ValidationResult:
    result = ValidationResult(obj)

    _ARCHITECTURAL_TYPES = {
        "pipe", "task", "file_format", "stream", "stage",
        "masking_policy", "row_access_policy", "sequence",
    }

    if obj.converted_sql is None:
        result.status = "ERROR"
        result.errors.append("No converted SQL available")
        return result

    if obj.object_type in _ARCHITECTURAL_TYPES:
        result.notes.append(
            f"SQL validation skipped — {obj.object_type} converted to architecture artifact, not executable SQL"
        )
        result.status = ValidationResult.ARCHITECTURAL_CHANGE
    else:
        syntax_errors = validate_sql_syntax(obj.converted_sql)
        for err in syntax_errors:
            result.errors.append(f"Syntax error: {err}")

        if obj.object_type == "table":
            schema_notes, schema_issues = _validate_schema_match(obj, obj.converted_sql)
            for n in schema_notes:
                result.notes.append(n)
            for issue in schema_issues:
                result.issues.append(issue)

        remaining = _check_remaining_snowflake(obj.converted_sql)
        for issue in remaining:
            result.issues.append(issue)

        invalid_types = _check_invalid_databricks_types(obj.converted_sql)
        for err in invalid_types:
            result.errors.append(err)

        variant_warnings = _check_variant_mapping(obj, obj.converted_sql)
        for w in variant_warnings:
            result.warnings.append(w)

        warnings = _check_manual_review(obj.converted_sql)
        for w in warnings:
            result.warnings.append(w)

    constraint_notes, constraint_warnings = _check_constraints(obj, obj.converted_sql)
    for n in constraint_notes:
        result.notes.append(n)
    for w in constraint_warnings:
        result.warnings.append(w)

    if re.search(r"\bSECURE\s+VIEW\b", obj.raw_sql, re.IGNORECASE):
        result.notes.append(
            "SECURE VIEW converted to standard Databricks VIEW — Snowflake SECURE VIEW has no direct equivalent. "
            "Review Unity Catalog permissions, row filters, and column masks to preserve the intended security model."
        )

    if re.search(r"\bLANGUAGE\s+JAVASCRIPT\b", obj.converted_sql or "", re.IGNORECASE):
        result.errors.append(
            "LANGUAGE JAVASCRIPT is not supported in Databricks — convert to Python or Scala"
        )

    is_architectural = "ARCHITECTURAL CHANGE" in (obj.converted_sql or "")
    is_manual_review = "MANUAL REVIEW" in (obj.converted_sql or "")
    is_js = re.search(r"\bLANGUAGE\s+JAVASCRIPT\b", obj.raw_sql, re.IGNORECASE)

    if re.search(r"\bCLONE\b", obj.converted_sql, re.IGNORECASE):
        result.warnings.append(
            "CLONE has no Databricks equivalent — use SHALLOW CLONE or CREATE TABLE AS SELECT (MANUAL REVIEW)"
        )
        result.status = ValidationResult.ARCHITECTURAL_CHANGE

    if (is_architectural or is_manual_review) and result.errors:
        syntax_errors = [e for e in result.errors if e.startswith("Syntax error")]
        other_errors = [e for e in result.errors if not e.startswith("Syntax error")]
        for e in syntax_errors:
            result.warnings.append(f"Syntax error expected (architectural change): {e}")
        result.errors = other_errors
        if not result.errors and result.status != ValidationResult.ARCHITECTURAL_CHANGE:
            result.status = ValidationResult.ARCHITECTURAL_CHANGE

    if is_js and result.errors:
        syntax_errors = [e for e in result.errors if e.startswith("Syntax error")]
        other_errors = [e for e in result.errors if not e.startswith("Syntax error")]
        for e in syntax_errors:
            result.warnings.append(f"Syntax error expected (architectural change): {e}")
        result.errors = other_errors

    if result.status == ValidationResult.ARCHITECTURAL_CHANGE:
        pass
    elif result.errors:
        result.status = "ERROR"
    elif result.issues:
        result.status = "ISSUE"
    elif result.warnings:
        result.status = "WARNING"
    else:
        result.status = "PASS"

    result.compute_confidence()
    return result


def validate_inventory(inventory: ProjectInventory) -> dict[str, ValidationResult]:
    results: dict[str, ValidationResult] = {}
    for obj in inventory.all_objects:
        results[obj.name] = validate_object(obj, inventory)
    return results


def generate_validation_summary(
    results: dict[str, ValidationResult],
) -> dict:
    total = len(results)
    passed = sum(1 for r in results.values() if r.is_pass())
    errors = sum(1 for r in results.values() if r.status == "ERROR")
    issues = sum(1 for r in results.values() if r.status == "ISSUE")
    warnings = sum(1 for r in results.values() if r.status == "WARNING")
    architectural = sum(
        1 for r in results.values() if r.status == ValidationResult.ARCHITECTURAL_CHANGE
    )

    return {
        "total": total,
        "passed": passed,
        "errors": errors,
        "issues": issues,
        "warnings": warnings,
        "architectural": architectural,
        "pass_rate": f"{passed / total * 100:.1f}%" if total > 0 else "N/A",
    }
