import re
import sqlglot
from sqlglot import exp
from typing import Optional


def validate_sql_syntax(sql: str, dialect: str = "databricks") -> list[str]:
    errors: list[str] = []
    if "$$" in sql:
        return errors
    # SQLGlot can't parse Databricks procedural SQL (BEGIN/DECLARE/END/EXCEPTION)
    if re.search(r"(?i)\b(BEGIN|DECLARE|END|EXCEPTION|WHEN\s+.*THEN)\b", sql):
        return errors
    # SQLGlot doesn't support GET DIAGNOSTICS — strip it before parsing
    cleaned = re.sub(
        r"(?i)GET\s+DIAGNOSTICS\s+\w+\s*=\s*ROW_COUNT\s*;",
        "SELECT 1;",
        sql,
    )
    try:
        parsed = sqlglot.parse(cleaned, dialect=dialect)
        for i, stmt in enumerate(parsed):
            if stmt is None:
                errors.append(f"Statement {i + 1}: failed to parse")
    except sqlglot.errors.ParseError as e:
        errors.append(str(e))
    except Exception as e:
        errors.append(f"Unexpected error: {e}")
    return errors


def extract_table_references(ast: exp.Expression) -> list[str]:
    references: list[str] = []
    for table in ast.find_all(exp.Table):
        ref = table.sql(dialect="databricks")
        if ref not in references:
            references.append(ref)
    return references


def extract_column_definitions(ast: exp.Expression) -> list[dict]:
    columns: list[dict] = []
    for schema in ast.find_all(exp.Schema):
        for column in schema.expressions:
            if isinstance(column, exp.ColumnDef):
                col_info = {
                    "name": column.name,
                    "type": column.args.get("kind").sql(dialect="databricks")
                    if column.args.get("kind")
                    else None,
                    "nullable": True,
                    "default": None,
                    "comment": None,
                }
                for constraint in column.args.get("constraints", []):
                    if isinstance(constraint, exp.NotNullColumnConstraint):
                        col_info["nullable"] = False
                    elif isinstance(constraint, exp.DefaultColumnConstraint):
                        col_info["default"] = constraint.expression.sql(
                            dialect="databricks"
                        )
                columns.append(col_info)
    return columns


def detect_snowflake_features(sql: str) -> list[str]:
    features: list[str] = []
    snowflake_patterns = {
        "QUALIFY": r"\bQUALIFY\b",
        "LATERAL FLATTEN": r"\bLATERAL\s+FLATTEN\b",
        "IFF": r"\bIFF\s*\(",
        "ARRAY_AGG": r"\bARRAY_AGG\s*\(",
        "OBJECT_CONSTRUCT": r"\bOBJECT_CONSTRUCT\s*\(",
        "OBJECT_AGG": r"\bOBJECT_AGG\s*\(",
        "LISTAGG": r"\bLISTAGG\s*\(",
        "FLATTEN": r"\bFLATTEN\s*\(",
        "LATERAL VIEW": r"\bLATERAL\s+VIEW\b",
        "PIVOT": r"\bPIVOT\s*\(",
        "UNPIVOT": r"\bUNPIVOT\s*\(",
        "SAMPLE": r"\bSAMPLE\b",
        "TABLESAMPLE": r"\bTABLESAMPLE\b",
        "MATCH_RECOGNIZE": r"\bMATCH_RECOGNIZE\b",
        "CONNECT BY": r"\bCONNECT\s+BY\b",
        "ZEROIFNULL": r"\bZEROIFNULL\s*\(",
        "NULLIFZERO": r"\bNULLIFZERO\s*\(",
        "TO_VARCHAR": r"\bTO_VARCHAR\s*\(",
        "TO_NUMBER": r"\bTO_NUMBER\s*\(",
        "TO_BOOLEAN": r"\bTO_BOOLEAN\s*\(",
        "TO_ARRAY": r"\bTO_ARRAY\s*\(",
        "TO_OBJECT": r"\bTO_OBJECT\s*\(",
        "NVL": r"\bNVL\s*\(",
        "NVL2": r"\bNVL2\s*\(",
        "DECODE": r"\bDECODE\s*\(",
        "PARSE_JSON": r"\bPARSE_JSON\s*\(",
        "ARRAY_SIZE": r"\bARRAY_SIZE\s*\(",
        "GET": r"\bGET\s*\(",
        "SEQUENCE": r"\bSEQ[1248]\b",
        "RANDSTR": r"\bRANDSTR\s*\(",
        "MONTHNAME": r"\bMONTHNAME\s*\(",
        "DAYNAME": r"\bDAYNAME\s*\(",
        "GROUP BY ALL": r"GROUP\s+BY\s+ALL\b",
        "CONVERT_TIMEZONE": r"\bCONVERT_TIMEZONE\s*\(",
        "CLONE": r"\bCLONE\b",
        "STREAMS": r"\bSTREAM\b",
        "TASKS": r"\bTASK\b",
        "AUTOINCREMENT": r"\bAUTOINCREMENT\b",
        "LANGUAGE JAVASCRIPT": r"\bLANGUAGE\s+JAVASCRIPT\b",
        "NEXTVAL": r"\bNEXTVAL\b",
        "CURRVAL": r"\bCURRVAL\b",
        "CREATE SEQUENCE": r"\bCREATE\s+(?:OR\s+REPLACE\s+)?SEQUENCE\b",
    }

    for feature, pattern in snowflake_patterns.items():
        if re.search(pattern, sql, re.IGNORECASE):
            features.append(feature)
    return features


def convert_data_type(snowflake_type: str) -> str:
    type_mapping = {
        "NUMBER": "DECIMAL",
        "DECIMAL": "DECIMAL",
        "NUMERIC": "DECIMAL",
        "INT": "INT",
        "INTEGER": "INT",
        "BIGINT": "BIGINT",
        "SMALLINT": "SMALLINT",
        "TINYINT": "TINYINT",
        "BYTEINT": "TINYINT",
        "FLOAT": "DOUBLE",
        "FLOAT4": "FLOAT",
        "FLOAT8": "DOUBLE",
        "DOUBLE": "DOUBLE",
        "REAL": "DOUBLE",
        "VARCHAR": "STRING",
        "CHAR": "STRING",
        "CHARACTER": "STRING",
        "STRING": "STRING",
        "TEXT": "STRING",
        "NVARCHAR": "STRING",
        "NCHAR": "STRING",
        "BINARY": "BINARY",
        "VARBINARY": "BINARY",
        "BOOLEAN": "BOOLEAN",
        "DATE": "DATE",
        "DATETIME": "TIMESTAMP",
        "TIME": "TIMESTAMP",
        "TIMESTAMP": "TIMESTAMP",
        "TIMESTAMP_NTZ": "TIMESTAMP",
        "TIMESTAMP_LTZ": "TIMESTAMP",
        "TIMESTAMP_TZ": "TIMESTAMP",
        "VARIANT": "STRING",
        "OBJECT": "STRING",
        "ARRAY": "ARRAY<STRING>",
        "GEOGRAPHY": "STRING",
    }

    upper = snowflake_type.upper().strip()
    for sf_type in sorted(type_mapping, key=len, reverse=True):
        db_type = type_mapping[sf_type]
        if upper.startswith(sf_type):
            return upper.replace(sf_type, db_type, 1)
    return snowflake_type
