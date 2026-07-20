import re
from typing import Optional
from parser.sql_parser import ParsedObject
from agents.project_loader import ProjectInventory
from parser.ast_parser import detect_snowflake_features


class Capability:
    SUPPORTED = "supported"
    SUPPORTED_WITH_REWRITE = "supported_with_rewrite"
    ARCHITECTURAL_CHANGE = "architectural_change"
    NOT_SUPPORTED = "not_supported"


FEATURE_CAPABILITY: dict[str, tuple[str, str]] = {
    "CLONE": (
        Capability.NOT_SUPPORTED,
        "CLONE has no Databricks equivalent; use INSERT INTO or CREATE TABLE AS SELECT",
    ),
    "TIME TRAVEL": (
        Capability.ARCHITECTURAL_CHANGE,
        "Time travel syntax differs: use Delta Lake VERSION AS OF / TIMESTAMP AS OF",
    ),
    "SECURE": (
        Capability.ARCHITECTURAL_CHANGE,
        "SECURE attribute is a Snowflake security concept; review access controls in Databricks",
    ),
    "IDENTITY": (
        Capability.SUPPORTED_WITH_REWRITE,
        "IDENTITY/AUTOINCREMENT converted to GENERATED ALWAYS AS IDENTITY by rule engine",
    ),
    "AUTOINCREMENT": (
        Capability.SUPPORTED_WITH_REWRITE,
        "AUTOINCREMENT converted to GENERATED ALWAYS AS IDENTITY by rule engine",
    ),
    "NEXTVAL": (
        Capability.SUPPORTED,
        "NEXTVAL syntax is identical between Snowflake and Databricks (seq.NEXTVAL)",
    ),
    "CURRVAL": (
        Capability.SUPPORTED,
        "CURRVAL syntax is identical between Snowflake and Databricks (seq.CURRVAL)",
    ),
    "LANGUAGE JAVASCRIPT": (
        Capability.NOT_SUPPORTED,
        "JavaScript UDFs/procedures are not supported in Databricks; convert to Python or Scala",
    ),
    "DATA_RETENTION_TIME_IN_DAYS": (
        Capability.ARCHITECTURAL_CHANGE,
        "DATA_RETENTION_TIME_IN_DAYS is not a TBLPROPERTY in Databricks; configure at catalog/schema level",
    ),
    "MATCH_RECOGNIZE": (
        Capability.ARCHITECTURAL_CHANGE,
        "MATCH_RECOGNIZE requires manual rewrite using window functions or custom logic",
    ),
    "CONNECT BY": (
        Capability.ARCHITECTURAL_CHANGE,
        "CONNECT BY requires manual rewrite using recursive CTEs",
    ),
    "SAMPLE": (
        Capability.ARCHITECTURAL_CHANGE,
        "TABLESAMPLE syntax differs; use Databricks TABLESAMPLE or LIMIT",
    ),
    "TABLESAMPLE": (
        Capability.ARCHITECTURAL_CHANGE,
        "TABLESAMPLE syntax differs; use Databricks TABLESAMPLE or LIMIT",
    ),
    "PIVOT": (
        Capability.SUPPORTED,
        "PIVOT syntax is identical between Snowflake and Databricks",
    ),
    "UNPIVOT": (
        Capability.SUPPORTED,
        "UNPIVOT syntax is identical between Snowflake and Databricks",
    ),
    "GROUP BY ALL": (
        Capability.SUPPORTED,
        "GROUP BY ALL is supported in Databricks SQL",
    ),
    "QUALIFY": (
        Capability.SUPPORTED_WITH_REWRITE,
        "QUALIFY converted to subquery by rule engine",
    ),
    "IFF": (
        Capability.SUPPORTED_WITH_REWRITE,
        "IFF converted to CASE WHEN by rule engine",
    ),
    "LATERAL FLATTEN": (
        Capability.SUPPORTED_WITH_REWRITE,
        "LATERAL FLATTEN converted to LATERAL VIEW EXPLODE by rule engine",
    ),
    "FLATTEN": (
        Capability.SUPPORTED_WITH_REWRITE,
        "FLATTEN converted to EXPLODE by rule engine",
    ),
    "ARRAY_AGG": (
        Capability.SUPPORTED_WITH_REWRITE,
        "ARRAY_AGG converted to COLLECT_LIST by rule engine",
    ),
    "OBJECT_CONSTRUCT": (
        Capability.SUPPORTED_WITH_REWRITE,
        "OBJECT_CONSTRUCT converted to NAMED_STRUCT by rule engine",
    ),
    "LISTAGG": (
        Capability.SUPPORTED_WITH_REWRITE,
        "LISTAGG converted to CONCAT_WS/COLLECT_LIST by rule engine",
    ),
    "OBJECT_AGG": (
        Capability.ARCHITECTURAL_CHANGE,
        "OBJECT_AGG has no direct Databricks equivalent; requires manual rewrite",
    ),
    "VARIANT": (
        Capability.ARCHITECTURAL_CHANGE,
        "VARIANT mapped to STRING — semantic information may be lost",
    ),
    "ZEROIFNULL": (
        Capability.SUPPORTED_WITH_REWRITE,
        "ZEROIFNULL converted to COALESCE by rule engine",
    ),
    "NULLIFZERO": (
        Capability.SUPPORTED_WITH_REWRITE,
        "NULLIFZERO converted to IF by rule engine",
    ),
    "TO_VARCHAR": (
        Capability.SUPPORTED_WITH_REWRITE,
        "TO_VARCHAR converted to format_number (numeric) or CAST AS STRING (non-numeric) by rule engine",
    ),
    "TO_NUMBER": (
        Capability.SUPPORTED_WITH_REWRITE,
        "TO_NUMBER converted to CAST AS DECIMAL by rule engine",
    ),
    "SEQUENCE": (
        Capability.SUPPORTED_WITH_REWRITE,
        "SEQ[1248] converted to ROW_NUMBER() OVER (ORDER BY 1) by rule engine",
    ),
    "CREATE SEQUENCE": (
        Capability.SUPPORTED,
        "CREATE SEQUENCE syntax is identical between Snowflake and Databricks",
    ),
    "TO_ARRAY": (
        Capability.ARCHITECTURAL_CHANGE,
        "TO_ARRAY has no direct equivalent; review conversion",
    ),
    "TO_OBJECT": (
        Capability.ARCHITECTURAL_CHANGE,
        "TO_OBJECT has no direct equivalent; review conversion",
    ),
    "TO_BOOLEAN": (
        Capability.ARCHITECTURAL_CHANGE,
        "TO_BOOLEAN has no direct equivalent; use CASE/CAST",
    ),
    "PARSE_JSON": (
        Capability.SUPPORTED,
        "PARSE_JSON is supported natively in Databricks SQL (pass-through)",
    ),
    "TRY_PARSE_JSON": (
        Capability.SUPPORTED,
        "TRY_PARSE_JSON is supported natively in Databricks SQL (pass-through)",
    ),
    "CHECK_JSON": (
        Capability.NOT_SUPPORTED,
        "CHECK_JSON has no direct Databricks equivalent; review validation approach",
    ),
    "JSON_TYPEOF": (
        Capability.NOT_SUPPORTED,
        "JSON_TYPEOF has no direct Databricks equivalent; review conversion",
    ),
    "STRIP_NULL_VALUE": (
        Capability.ARCHITECTURAL_CHANGE,
        "STRIP_NULL_VALUE has no direct equivalent; review conversion",
    ),
    "OBJECT_DELETE": (
        Capability.ARCHITECTURAL_CHANGE,
        "OBJECT_DELETE has no direct equivalent; review conversion",
    ),
    "OBJECT_PICK": (
        Capability.ARCHITECTURAL_CHANGE,
        "OBJECT_PICK has no direct equivalent; review conversion",
    ),
    "ARRAY_SIZE": (
        Capability.SUPPORTED_WITH_REWRITE,
        "ARRAY_SIZE converted to SIZE by rule engine",
    ),
    "GET": (
        Capability.SUPPORTED_WITH_REWRITE,
        "GET converted to array bracket access by rule engine",
    ),
    "RANDSTR": (
        Capability.ARCHITECTURAL_CHANGE,
        "RANDSTR has no direct equivalent; review conversion",
    ),
    "MONTHNAME": (
        Capability.SUPPORTED_WITH_REWRITE,
        "MONTHNAME converted to DATE_FORMAT by rule engine",
    ),
    "DAYNAME": (
        Capability.SUPPORTED_WITH_REWRITE,
        "DAYNAME converted to DATE_FORMAT by rule engine",
    ),
    "CONVERT_TIMEZONE": (
        Capability.ARCHITECTURAL_CHANGE,
        "CONVERT_TIMEZONE syntax differs; verify conversion",
    ),
    "CURRENT_ACCOUNT": (
        Capability.NOT_SUPPORTED,
        "CURRENT_ACCOUNT* functions have no Databricks equivalent",
    ),
    "SYSTEM$": (
        Capability.NOT_SUPPORTED,
        "SYSTEM$ functions have no Databricks equivalent",
    ),
    "STREAMS": (
        Capability.NOT_SUPPORTED,
        "Snowflake STREAMS (CDC) have no Databricks equivalent; use Delta Lake Change Data Feed",
    ),
    "TASKS": (
        Capability.NOT_SUPPORTED,
        "Snowflake TASKS (scheduling) have no Databricks equivalent; use Delta Live Tables or Databricks Workflows",
    ),
    "NVL": (
        Capability.SUPPORTED_WITH_REWRITE,
        "NVL converted to COALESCE by rule engine",
    ),
    "MATERIALIZED VIEW": (
        Capability.ARCHITECTURAL_CHANGE,
        "CREATE MATERIALIZED VIEW syntax differs; use Databricks materialized views or streaming tables",
    ),
    "MERGE": (
        Capability.SUPPORTED,
        "MERGE INTO syntax is compatible between Snowflake and Databricks",
    ),
    "TRY_CAST": (
        Capability.SUPPORTED,
        "TRY_CAST is supported natively in Databricks SQL (pass-through)",
    ),
    "RATIO_TO_REPORT": (
        Capability.SUPPORTED_WITH_REWRITE,
        "RATIO_TO_REPORT converted to expr / SUM(expr) OVER () by rule engine",
    ),
    "TO_DATE": (
        Capability.SUPPORTED,
        "TO_DATE is supported natively in Databricks SQL (pass-through)",
    ),
    "TO_TIMESTAMP": (
        Capability.SUPPORTED,
        "TO_TIMESTAMP is supported natively in Databricks SQL (pass-through)",
    ),
    "DATE_TRUNC": (
        Capability.SUPPORTED,
        "DATE_TRUNC syntax is compatible between Snowflake and Databricks",
    ),
    "DATEADD": (
        Capability.SUPPORTED,
        "DATEADD syntax is compatible between Snowflake and Databricks",
    ),
}

SQL_UDF_CAPABILITY: tuple[str, str] = (
    Capability.SUPPORTED,
    "SQL scalar UDFs are directly convertible to Databricks SQL functions",
)

FEATURE_PATTERNS: dict[str, str] = {
    "CLONE": r"\bCLONE\b",
    "TIME TRAVEL": r"\b(AT|BEFORE)\s*\(",
    "STREAMS": r"\bSTREAM\b",
    "TASKS": r"\bTASK\b",
    "SECURE": r"\bSECURE\s+(VIEW|FUNCTION|TABLE)\b",
    "IDENTITY": r"\bIDENTITY\b",
    "AUTOINCREMENT": r"\bAUTOINCREMENT\b",
    "NEXTVAL": r"\bNEXTVAL\b",
    "CURRVAL": r"\bCURRVAL\b",
    "LANGUAGE JAVASCRIPT": r"\bLANGUAGE\s+JAVASCRIPT\b",
    "DATA_RETENTION_TIME_IN_DAYS": r"\bDATA_RETENTION_TIME_IN_DAYS\b",
    "MATCH_RECOGNIZE": r"\bMATCH_RECOGNIZE\b",
    "CONNECT BY": r"\bCONNECT\s+BY\b",
    "MATERIALIZED VIEW": r"\bMATERIALIZED\s+VIEW\b",
    "MERGE": r"\bMERGE\s+INTO\b",
    "TRY_CAST": r"\bTRY_CAST\s*\(",
    "RATIO_TO_REPORT": r"\bRATIO_TO_REPORT\s*\(",
    "TO_DATE": r"\bTO_DATE\s*\(",
    "TO_TIMESTAMP": r"\bTO_TIMESTAMP\s*\(",
    "DATE_TRUNC": r"\bDATE_TRUNC\s*\(",
    "DATEADD": r"\bDATEADD\s*\(",
    "SAMPLE": r"\bSAMPLE\b",
    "TABLESAMPLE": r"\bTABLESAMPLE\b",
    "PIVOT": r"\bPIVOT\s*\(",
    "UNPIVOT": r"\bUNPIVOT\s*\(",
    "OBJECT_AGG": r"\bOBJECT_AGG\s*\(",
    "VARIANT": r"\bVARIANT\b",
    "TO_ARRAY": r"\bTO_ARRAY\s*\(",
    "TO_OBJECT": r"\bTO_OBJECT\s*\(",
    "TO_BOOLEAN": r"\bTO_BOOLEAN\s*\(",
    "RANDSTR": r"\bRANDSTR\s*\(",
    "CONVERT_TIMEZONE": r"\bCONVERT_TIMEZONE\s*\(",
    "CURRENT_ACCOUNT": r"\bCURRENT_ACCOUNT\w*\s*\(",
    "SYSTEM$": r"\bSYSTEM\$\w*\s*\(",
    "GROUP BY ALL": r"GROUP\s+BY\s+ALL\b",
    "CREATE SEQUENCE": r"\bCREATE\s+(?:OR\s+REPLACE\s+)?SEQUENCE\b",
}


def check_object_capabilities(obj: ParsedObject) -> list[dict]:
    findings: list[dict] = []
    sql = obj.raw_sql

    for feature, pattern in FEATURE_PATTERNS.items():
        if re.search(pattern, sql, re.IGNORECASE):
            capability, message = FEATURE_CAPABILITY.get(
                feature, (Capability.ARCHITECTURAL_CHANGE, f"Snowflake feature '{feature}' requires review")
            )
            findings.append({
                "feature": feature,
                "capability": capability,
                "message": message,
                "object_name": obj.name,
                "object_type": obj.object_type,
            })

    if obj.object_type == "function" and not re.search(
        r"\bLANGUAGE\s+JAVASCRIPT\b", sql, re.IGNORECASE
    ):
        capability, message = SQL_UDF_CAPABILITY
        findings.append({
            "feature": "SQL UDF",
            "capability": capability,
            "message": message,
            "object_name": obj.name,
            "object_type": obj.object_type,
        })

    return findings


def check_inventory_capabilities(inventory: ProjectInventory) -> dict[str, list[dict]]:
    results: dict[str, list[dict]] = {}
    for obj in inventory.all_objects:
        findings = check_object_capabilities(obj)
        if findings:
            results[obj.name] = findings
    return results


def generate_capability_summary(results: dict[str, list[dict]]) -> dict:
    total_features = sum(len(v) for v in results.values())
    buckets: dict[str, int] = {
        Capability.SUPPORTED: 0,
        Capability.SUPPORTED_WITH_REWRITE: 0,
        Capability.ARCHITECTURAL_CHANGE: 0,
        Capability.NOT_SUPPORTED: 0,
    }
    for findings in results.values():
        for f in findings:
            cap = f.get("capability", "")
            if cap in buckets:
                buckets[cap] += 1

    return {
        "total_objects_with_findings": len(results),
        "total_features_detected": total_features,
        "buckets": buckets,
    }
