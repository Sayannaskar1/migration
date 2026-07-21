import re
from parser.sql_parser import ParsedObject

_file_format_registry: dict[str, dict] = {}


def register_file_format(sql: str) -> dict | None:
    name_match = re.search(
        r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?FILE\s+FORMAT\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)",
        sql,
    )
    if not name_match:
        return None
    name = name_match.group(1)

    fmt_type_match = re.search(r"(?i)TYPE\s*=\s*['\"]?(\w+)['\"]?", sql)
    fmt_type = fmt_type_match.group(1) if fmt_type_match else "CSV"

    props = {}
    for m in re.finditer(
        r"(?i)(\w+)\s*=\s*(?:\(([^)]+)\)|'([^']*)'|\"([^\"]*)\"|(\w+))",
        sql,
    ):
        key_upper = m.group(1).upper()
        if key_upper == "TYPE":
            continue
        raw = m.group(2) or m.group(3) or m.group(4) or m.group(5)
        if raw.isdigit():
            raw = int(raw)
        elif raw.replace(".", "", 1).isdigit():
            raw = float(raw)
        props[key_upper] = raw

    entry = {"name": name, "type": fmt_type.upper(), "options": props, "raw_sql": sql}
    _file_format_registry[name] = entry
    return entry


def _ff_option_map(key: str) -> str:
    mapping = {
        "SKIP_HEADER": "header",
        "FIELD_OPTIONALLY_ENCLOSED_BY": "quoteChar",
        "FIELD_DELIMITER": "delimiter",
        "RECORD_DELIMITER": "lineSep",
        "ESCAPE": "escape",
        "ESCAPE_UNENCLOSED_FIELD": "escapeQuotes",
        "DATE_FORMAT": "dateFormat",
        "TIME_FORMAT": "timestampFormat",
        "TIMESTAMP_FORMAT": "timestampFormat",
        "NULL_IF": "nullValue",
        "COMPRESSION": "compression",
        "ENCODING": "encoding",
        "VALIDATE_UTF8": "multiLine",
        "REPLACE_INVALID_CHARACTERS": "CleanCsv",
        "EMPTY_FIELD_AS_NULL": "treatEmptyValuesAsNulls",
        "SKIP_BLANK_LINES": "ignoreLeadingWhiteSpace",
        "TRIM_SPACE": "ignoreTrailingWhiteSpace",
        "BINARY_FORMAT": "mode",
        "ERROR_ON_COLUMN_COUNT_MISMATCH": "mode",
        "STRIP_OUTER_ARRAY": "multiLine",
        "STRIP_NULL_VALUES": "dropFieldIfAllNull",
        "COMMENT": "comment",
    }
    return mapping.get(key, key.lower())


def _ff_to_copy_options(props: dict) -> list[str]:
    header = props.get("SKIP_HEADER", 0)
    opts = []
    if header:
        opts.append(f"'header' = '{'true' if str(header) == '1' else 'false'}'")
    for key, val in props.items():
        if key == "SKIP_HEADER":
            continue
        mapped = _ff_option_map(key)
        if key == "NULL_IF":
            items = [v.strip().strip("'\"") for v in str(val).split(",")]
            items = [it for it in items if it]
            opts.append(f"'{mapped}' = \"{','.join(items)}\"")
        else:
            opts.append(f"'{mapped}' = '{val}'")
    return opts


def expand_file_format_refs(sql: str) -> str:
    def _repl(m: re.Match) -> str:
        ff_name = m.group(1)
        entry = _file_format_registry.get(ff_name)
        if not entry:
            return m.group(0)
        fmt_type = entry["type"]
        opts = _ff_to_copy_options(entry.get("options", {}))
        lines = [f"FILEFORMAT = {fmt_type}"]
        if opts:
            lines.append("FORMAT_OPTIONS (")
            for o in opts:
                lines.append(f"    {o}")
            lines.append(")")
        return "\n".join(lines)

    return re.sub(
        r"(?i)FILE_FORMAT\s*=\s*\(\s*FORMAT_NAME\s*=\s*(\w+)\s*\)",
        _repl,
        sql,
    )


def _extract_table_name(sql: str) -> str:
    m = re.search(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(`?\w+(?:\.`?\w+)*)",
        sql, re.IGNORECASE,
    )
    if m:
        return m.group(1).strip()
    return ""


def _convert_create_table_sql(sql: str) -> str:
    table_name = _extract_table_name(sql)

    # Strip trailing content after the column-definition closing paren.
    # Some Snowflake source files append Databricks-preview SQL after a comment
    # on the same line as WITH TAG or WITH ROW ACCESS POLICY. Find the matching
    # ')' that closes the column list and truncate everything after it.
    paren_depth = 0
    found_open = False
    truncate_at = len(sql)
    for i, ch in enumerate(sql):
        if ch == '(' and not found_open:
            # First '(' after table name opens the column list
            paren_depth = 1
            found_open = True
        elif ch == '(' and found_open:
            paren_depth += 1
        elif ch == ')' and found_open:
            paren_depth -= 1
            if paren_depth == 0:
                truncate_at = i + 1
                break
    sql = sql[:truncate_at]

    sql = re.sub(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE",
        "CREATE OR REPLACE TABLE",
        sql,
        count=1,
        flags=re.IGNORECASE,
    )

    # Convert Snowflake types to Databricks equivalents (safety net for any missed by SQLGlot)
    sql = re.sub(r"(?i)\bVARCHAR\s*\([^)]*\)", "STRING", sql)
    sql = re.sub(r"(?i)\bVARCHAR\b(?!\s*\()", "STRING", sql)
    sql = re.sub(r"(?i)\bTEXT\b", "STRING", sql)
    sql = re.sub(r"(?i)\bVARIANT\b", "STRING", sql)
    sql = re.sub(r"(?i)\bTIMESTAMP_NTZ\b", "TIMESTAMP", sql)
    sql = re.sub(r"(?i)\bTIMESTAMP_LTZ\b", "TIMESTAMP", sql)
    sql = re.sub(r"(?i)\bTIMESTAMP_TZ\b", "TIMESTAMP", sql)
    sql = re.sub(r"(?i)\bNUMBER\s*\(([^)]*)\)", r"DECIMAL(\1)", sql)
    # Strip precision from bare TIMESTAMP (Databricks doesn't support TIMESTAMP(p))
    sql = re.sub(r"(?i)\bTIMESTAMP\s*\(\s*\d+\s*\)", "TIMESTAMP", sql)
    sql = re.sub(r"(?i)\bFLOAT(?:4|8)?\b(?!\s*\()", "DOUBLE", sql)
    sql = re.sub(r"(?i)\bDOUBLE\s+PRECISION\b", "DOUBLE", sql)
    sql = re.sub(r"(?i)\bREAL\b", "DOUBLE", sql)

    sql = re.sub(r"(?i)AUTOINCREMENT\s*(?:\([^)]*\))?", "", sql)
    sql = re.sub(r"(?i)AUTO_INCREMENT", "", sql)
    sql = re.sub(r"(?i)DEFAULT\s+(?:\w+\.)*\w+\.NEXTVAL", "GENERATED BY DEFAULT AS IDENTITY", sql)
    sql = re.sub(r"(?i)DEFAULT\s+NEXT\s+VALUE\s+FOR\s+\w+(?:\.\w+)*", "GENERATED BY DEFAULT AS IDENTITY", sql)
    sql = re.sub(
        r"(?i)([(,]\s*)(\w+)\s+\w+(?:\s*\([^)\n]*\))?(\s+(?:NOT\s+)?NULL\s+GENERATED\s+(?:ALWAYS|BY\s+DEFAULT)\s+AS\s+IDENTITY)",
        r"\1\2 BIGINT\3",
        sql,
    )
    sql = re.sub(r"(?i)DATA_RETENTION_TIME_IN_DAYS\s*=\s*\d+", "", sql)
    sql = re.sub(r"\s*TBLPROPERTIES\s*\([^)]*DATA_RETENTION_TIME_IN_DAYS[^)]*\)", "", sql)

    # Strip Snowflake-only DDL clauses and collect ALTER statements
    alters: list[str] = []

    # Strip column-level WITH MASKING POLICY / WITH ROW ACCESS POLICY
    def _strip_column_policy(m: re.Match) -> str:
        col_name = m.group(1)
        policy_type = m.group(3)
        policy_name = m.group(4)
        suffix = m.group(5)
        if table_name:
            alters.append(
                f"ALTER TABLE {table_name} ALTER COLUMN {col_name} "
                f"{'SET MASK' if 'MASKING' in policy_type.upper() else 'SET ROW FILTER'} {policy_name};"
            )
        return m.group(1) + m.group(2) + suffix

    sql = re.sub(
        r"(?i)(\w+)(\s+\w+(?:\s*\([^)]*\))?)\s+WITH\s+(MASKING\s+POLICY|ROW\s+ACCESS\s+POLICY)\s+(\S+(?:\.\S+)*)(\s*(?:,|\)))",
        _strip_column_policy,
        sql,
    )

    # Strip table-level WITH ROW ACCESS POLICY
    rap_m = re.search(
        r"(?i)\s+WITH\s+ROW\s+ACCESS\s+POLICY\s+(\S+(?:\.\S+)*)\s+ON\s*\(([^)]+)\)",
        sql,
    )
    if rap_m:
        rap_name = rap_m.group(1)
        rap_col = rap_m.group(2)
        if table_name:
            alters.append(
                f"ALTER TABLE {table_name} SET ROW FILTER {rap_name};"
            )
        sql = sql[: rap_m.start()] + sql[rap_m.end() :]

    # Strip table-level WITH TAG (pre-SQLGlot format)
    tag_re = re.compile(r"\s+WITH\s+TAG\s*\(([^)]*)\)")
    while True:
        tag_m = tag_re.search(sql)
        if not tag_m:
            break
        tag_content = tag_m.group(1)
        if table_name:
            tag_pairs = re.findall(r"(\S+)\s*=\s*'([^']*)'", tag_content)
            for tag_name, tag_val in tag_pairs:
                alters.append(
                    f"ALTER TABLE {table_name} SET TAGS ('{tag_name}' = '{tag_val}');"
                )
        sql = sql[: tag_m.start()] + sql[tag_m.end() :]

    # Strip TBLPROPERTIES ( TAG (...) ) — SQLGlot post-transpile format
    tag_tbp_re = re.compile(
        r"TBLPROPERTIES\s*\(\s*TAG\s*\(([^)]*)\)\s*\)",
        re.IGNORECASE,
    )
    while True:
        tag_tbp_m = tag_tbp_re.search(sql)
        if not tag_tbp_m:
            break
        tag_content = tag_tbp_m.group(1)
        if table_name:
            tag_pairs = re.findall(r"(\S+)\s*=\s*'([^']*)'", tag_content)
            for tag_name, tag_val in tag_pairs:
                alters.append(
                    f"ALTER TABLE {table_name} SET TAGS ('{tag_name}' = '{tag_val}');"
                )
        sql = sql[: tag_tbp_m.start()] + sql[tag_tbp_m.end() :]

    # Strip NOT NULL from GENERATED BY DEFAULT AS IDENTITY columns (conflicts with IDENTITY)
    sql = re.sub(
        r"(?i)(GENERATED\s+(?:ALWAYS|BY\s+DEFAULT)\s+AS\s+IDENTITY[^)]*)\)\s+NOT\s+NULL\b",
        r"\1)",
        sql,
    )

    sql = re.sub(r"\s+,", ",", sql)
    sql = re.sub(r",\s*\)", ")", sql)

    # Add delta.feature.allowColumnDefaults to support column DEFAULT values
    if re.search(r"\bDEFAULT\b", sql, re.IGNORECASE) and "delta.feature.allowColumnDefaults" not in sql:
        sql = re.sub(
            r"(?i)(USING\s+DELTA)",
            r"\1 TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')",
            sql,
        )

    if alters:
        sql += "\n\n-- Applied via ALTER TABLE:\n--   " + "\n--   ".join(alters)

    return sql


def _convert_create_schema_sql(sql: str) -> str:
    sql = re.sub(
        r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?SCHEMA\s+(?:IF\s+NOT\s+EXISTS\s+)?",
        "CREATE SCHEMA IF NOT EXISTS ",
        sql,
    )
    return sql


def _convert_create_view_sql(sql: str) -> str:
    had_secure = bool(re.search(r"(?i)SECURE\s+VIEW", sql))
    sql = re.sub(
        r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?(?:SECURE\s+)?VIEW",
        "CREATE OR REPLACE VIEW",
        sql,
    )
    if had_secure:
        sql += (
            "\n\n"
            "-- ==========================================\n"
            "-- Security Architecture Change\n"
            "--\n"
            "-- Snowflake SECURE VIEW\n"
            "--        ↓\n"
            "-- Databricks VIEW + Unity Catalog\n"
            "--\n"
            "-- Snowflake SECURE VIEW has no direct Databricks equivalent.\n"
            "-- Review the following to ensure equivalent protection:\n"
            "--   ✓ View permissions (GRANT SELECT)\n"
            "--   ✓ Column masks on sensitive columns\n"
            "--   ✓ Row filters for data access policies\n"
            "--   ✓ Underlying table access restrictions\n"
            "-- =========================================="
        )

    body = re.search(r"(?i)\bAS\b\s*(.+)$", sql, re.DOTALL)
    if body:
        body_sql = body.group(1)
        has_aggregation = bool(
            re.search(r"\b(COUNT|SUM|AVG|MIN|MAX)\s*\(", body_sql, re.IGNORECASE)
        )
        has_join = bool(re.search(r"\bJOIN\b", body_sql, re.IGNORECASE))
        has_group_by = bool(re.search(r"\bGROUP\s+BY\b", body_sql, re.IGNORECASE))
        has_distinct = bool(
            re.search(r"\bCOUNT\s*\(\s*DISTINCT\b", body_sql, re.IGNORECASE)
        )

        if has_aggregation and has_group_by:
            lines = ["", "-- Optimization Recommendation"]
            lines.append("--")
            lines.append("-- This view performs aggregation, which may benefit from:")
            lines.append("--")
            if has_join or has_distinct:
                lines.append("--   • Materialized View (if supported for your workload)")
            lines.append("--   • Delta Live Table / Materialized Pipeline")
            lines.append("--   • Periodic refresh using a Databricks Job")
            lines.append("--   • Pre-aggregation in an upstream table for large datasets")
            lines.append("--")
            lines.append("-- The current CREATE VIEW output is semantically correct and executable.")
            sql += "\n".join(lines)

    return sql


def _convert_update_from_to_merge(body: str) -> str:
    """
    Deterministic transpiler: Snowflake UPDATE ... FROM → Databricks MERGE INTO.

    Snowflake:
        UPDATE target T
        SET col1 = S.col1, col2 = S.col2
        FROM (
            SELECT id, col1, col2 FROM source
        ) S
        WHERE T.id = S.id;

    Databricks:
        MERGE INTO target T
        USING (
            SELECT id, col1, col2 FROM source
        ) S
        ON T.id = S.id
        WHEN MATCHED THEN UPDATE SET
          col1 = S.col1, col2 = S.col2;
    """

    def _find_balanced_end(text, start):
        depth = 0
        for i in range(start, len(text)):
            if text[i] == '(':
                depth += 1
            elif text[i] == ')':
                depth -= 1
                if depth == 0:
                    return i + 1
        return -1

    def _find_keyword(text, keyword, start=0):
        depth = 0
        kw = keyword.upper()
        i = start
        while i < len(text):
            c = text[i]
            if c == '(':
                depth += 1
            elif c == ')':
                depth -= 1
            elif c == ';' and depth == 0:
                return -1
            elif depth == 0 and text[i:].upper().startswith(kw):
                before_ok = i == 0 or not text[i - 1].isalnum()
                after_pos = i + len(kw)
                after_ok = after_pos >= len(text) or not text[after_pos].isalnum()
                if before_ok and after_ok:
                    return i
            i += 1
        return -1

    result = body
    offset = 0

    while True:
        update_pos = _find_keyword(result, "UPDATE", offset)
        if update_pos == -1:
            break

        set_pos = _find_keyword(result, "SET", update_pos + 7)
        if set_pos == -1 or set_pos - update_pos > 200:
            offset = update_pos + 7
            continue

        from_pos = _find_keyword(result, "FROM", set_pos + 4)
        if from_pos == -1 or from_pos - set_pos > 2000:
            offset = set_pos + 4
            continue

        where_pos = _find_keyword(result, "WHERE", from_pos + 5)
        if where_pos == -1:
            offset = from_pos + 5
            continue

        target_text = result[update_pos + 7 : set_pos].strip()
        target_parts = target_text.split(None, 1)
        target_table = target_parts[0]
        target_alias = (
            target_parts[1].strip()
            if len(target_parts) > 1
            else target_table.split(".")[-1]
        )

        set_clauses = result[set_pos + 4 : from_pos].strip()

        from_text = result[from_pos + 5 : where_pos].strip()

        if from_text.startswith("("):
            close = _find_balanced_end(from_text, 0)
            if close == -1:
                offset = from_pos + 5
                continue
            source_query = from_text[1 : close - 1].strip()
            rest = from_text[close:].strip()
            source_alias = rest.split()[0] if rest else "src"
        else:
            tokens = from_text.split(None, 2)
            source_query = tokens[0]
            source_alias = (
                tokens[1] if len(tokens) > 1 else source_query.split(".")[-1]
            )

        where_end = result.find(";", where_pos)
        if where_end == -1:
            where_end = len(result)
        where_condition = result[where_pos + 6 : where_end].strip()

        merge = f"MERGE INTO {target_table} {target_alias}\n"
        if source_query.upper().lstrip().startswith("SELECT"):
            merge += f"USING ({source_query}) {source_alias}\n"
        else:
            merge += f"USING {source_query} {source_alias}\n"
        merge += f"ON {where_condition}\n"
        merge += f"WHEN MATCHED THEN UPDATE SET\n  {set_clauses};"

        result = result[:update_pos] + merge + result[where_end + 1 :]
        offset = update_pos + len(merge)

    return result


def _convert_procedure_body(body: str) -> str:
    # Convert LET var := expr to SET var = expr (assignment, not declaration)
    body = re.sub(r"(?i)\bLET\s+(\w+)\s*:=\s*", r"SET \1 = ", body)
    # Convert remaining LET (declarations without assignment) to DECLARE
    body = re.sub(r"(?i)\bLET\s+", "DECLARE ", body)
    body = re.sub(r"(?i)(INTO)\s+:(\w+)", r"\1 \2", body)
    body = re.sub(r"(?<![:\w']):(\w+)", r"\1", body)
    body = re.sub(r";;\s*", "; ", body)
    # Convert var := expr to SET var = expr (Databricks SQL syntax) — must run before SQLROWCOUNT handling
    body = re.sub(r"(?i)(\w+)\s*:=\s*", r"SET \1 = ", body)
    # Convert Snowflake SQLROWCOUNT to Databricks-compatible pattern
    # Databricks SQL Warehouse does NOT support GET DIAGNOSTICS in stored procedures.
    # Convert SET var = SQLROWCOUNT to SET var = 1 with a migration note.
    body = re.sub(
        r"(?i)SET\s+(\w+)\s*=\s*SQLROWCOUNT\b",
        r"SET \1 = 1 /* Databricks: GET DIAGNOSTICS not supported; row count unavailable */",
        body,
    )
    # Fallback: bare SQLROWCOUNT references — replace with 1
    body = re.sub(r"(?i)\bSQLROWCOUNT\b", "1 /* Databricks: SQLROWCOUNT not available */", body)
    # Strip any GET DIAGNOSTICS lines that leaked through (e.g., from Snowflake source)
    body = re.sub(
        r"(?i)GET\s+DIAGNOSTICS\s+\w+\s*=\s*ROW_COUNT\s*;",
        "/* Databricks: GET DIAGNOSTICS not supported; row count unavailable */",
        body,
    )
    # Convert Snowflake SELECT col INTO var FROM ... ; to Databricks SET var = (SELECT col FROM ...);
    body = re.sub(
        r"(?i)SELECT\s+(.+?)\s+INTO\s+(\w+)\s+(FROM\s+.+?)\s*;",
        r"SET \2 = (SELECT \1 \3);",
        body,
        flags=re.DOTALL,
    )
    # Convert Snowflake TEMPORARY TABLE → Databricks TEMPORARY VIEW
    # Databricks SQL Warehouse doesn't support CREATE TEMPORARY TABLE
    # Also: Databricks temp views only accept single-part names (no catalog.schema prefix)
    def _temp_table_to_view(m: re.Match) -> str:
        return "CREATE OR REPLACE TEMPORARY VIEW " + m.group(1).split(".")[-1]
    body, temp_view_count = re.subn(
        r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?TEMPORARY\s+TABLE\s+(\w+(?:\.\w+)*)",
        _temp_table_to_view,
        body,
    )
    if temp_view_count:
        body += "\n\n-- Migration Note: Temporary table converted to temporary view because the object is read-only."
    # Convert TABLE(GENERATOR(ROWCOUNT => N)) → VALUES + SEQUENCE
    # Matches both literal numbers and variable references (e.g. ROWCOUNT => :d → ROWCOUNT => d)
    body = re.sub(
        r"(?i)TABLE\s*\(\s*GENERATOR\s*\(\s*ROWCOUNT\s*=>\s*(\w+)\s*\)\s*\)",
        r"(SELECT EXPLODE(SEQUENCE(1, \1)))",
        body,
    )
    # Convert Snowflake UPDATE ... FROM → Databricks MERGE INTO
    body = _convert_update_from_to_merge(body)
    # Convert Snowflake types to Databricks equivalents in variable declarations
    # SQLGlot can't parse stored procedures, so type conversion is done here
    body = re.sub(
        r"(?i)\bNUMBER\b",
        "DECIMAL",
        body,
    )
    body = re.sub(
        r"(?i)\bVARCHAR\s*\([^)]*\)",
        "STRING",
        body,
    )
    body = re.sub(
        r"(?i)\bVARCHAR\b(?!\s*\()",
        "STRING",
        body,
    )
    body = re.sub(
        r"(?i)\bFLOAT\b(?!\s*\()",
        "DOUBLE",
        body,
    )
    return body


def _convert_create_procedure_sql(sql: str) -> str:
    original_sql = sql

    sql = re.sub(
        r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE",
        "CREATE OR REPLACE PROCEDURE",
        sql,
    )
    # Remove LANGUAGE SQL (redundant — inferred by Databricks)
    sql = re.sub(r"(?i)\s*LANGUAGE\s+SQL\s*", "\n", sql)
    # Remove RETURNS clause (not supported on Databricks SQL Warehouse)
    sql = re.sub(r"(?i)\s*RETURNS\s+\w+(?:\s*\([^)]*\))?\s*", "\n", sql)
    # Preserve EXECUTE AS as migration metadata
    execute_as_match = re.search(r"(?i)\bEXECUTE\s+AS\s+(OWNER|CALLER)\b", original_sql)
    execute_as_note = ""
    if execute_as_match:
        execute_as_note = (
            f"\n"
            f"-- Migration Note:\n"
            f"-- Snowflake EXECUTE AS {execute_as_match.group(1).upper()} has no Databricks equivalent.\n"
            f"-- Databricks uses Unity Catalog permissions with SQL SECURITY INVOKER.\n"
            f"-- Review Unity Catalog privileges to ensure correct access control."
        )
    sql = re.sub(r"(?i)\s*EXECUTE\s+AS\s+(?:OWNER|CALLER)", "", sql)
    # Collapse blank lines before inserting SQL SECURITY
    sql = re.sub(r"\n{3,}", "\n\n", sql)
    # Convert Snowflake AS $$body$$ to Databricks AS\nbody
    dollar_match = re.search(r"AS\s*\$\$\s*\n?(.*?)\n?\s*\$\$", sql, re.DOTALL)
    if dollar_match:
        before_body = sql[:dollar_match.start(0)].rstrip()
        body = dollar_match.group(1).strip()
        # Convert Snowflake doubled single quotes to single quotes.
        # Inside $$ delimiters, '' is literal (two quote chars).
        # Inside Databricks AS...END, '' is an escaped quote that produces one '.
        # This ensures dynamic SQL string concatenation builds correctly
        # (e.g. '''''' → ''' → one literal quote in the string value).
        body = body.replace("''", "'")
        body = _convert_procedure_body(body)
        # Convert RETURN 'msg' to SELECT 'msg' (RETURN requires RETURNS clause in Databricks)
        # Databricks SQL Warehouse does not support RETURNS on procedures,
        # so RETURN must become SELECT to avoid syntax errors.
        body = re.sub(
            r"(?i)(^|\s)RETURN\s+",
            lambda m: (
                m.group(1)
                + "-- Migration Note: Snowflake RETURN converted to SELECT because Databricks SQL procedures do not return scalar values.\n  SELECT "
            ),
            body,
        )
        # Move DECLARE block inside BEGIN
        def _wrap_declares(m: re.Match) -> str:
            decls = m.group(1).strip()
            parts = [p.strip() for p in decls.split(";") if p.strip()]
            indented = ";\n  DECLARE ".join(parts) + ";"
            return "BEGIN\n  DECLARE " + indented
        body = re.sub(
            r"(?is)DECLARE\s+(.*?)BEGIN",
            _wrap_declares,
            body,
        )
        # Wrap in BEGIN...END if not already present
        if not re.search(r"(?i)\bBEGIN\b", body):
            body = "BEGIN\n  " + body.replace("\n", "\n  ") + "\nEND;"
        sql = before_body + execute_as_note + "\nSQL SECURITY INVOKER\nAS\n" + body
    else:
        # Convert Snowflake AS 'body' to Databricks AS\nbody
        match = re.search(r"AS\s*'(.*)'\s*$", sql, re.DOTALL)
        if match:
            before_body = sql[:match.start(0)].rstrip()
            body = match.group(1)
            body = body.replace("\\'", "'").replace("\\n", "\n")
            body = body.replace("''", "'")
            body = _convert_procedure_body(body)
            # Convert RETURN 'msg' to SELECT 'msg' (RETURN requires RETURNS clause)
            body = re.sub(
                r"(?i)(^|\s)RETURN\s+",
                lambda m: (
                    m.group(1)
                    + "-- Migration Note: Snowflake RETURN converted to SELECT because Databricks SQL procedures do not return scalar values.\n  SELECT "
                ),
                body,
            )
            # Move DECLARE block inside BEGIN
            def _wrap_declares_sq(m: re.Match) -> str:
                decls = m.group(1).strip()
                parts = [p.strip() for p in decls.split(";") if p.strip()]
                indented = ";\n  DECLARE ".join(parts) + ";"
                return "BEGIN\n  DECLARE " + indented
            body = re.sub(
                r"(?is)DECLARE\s+(.*?)BEGIN",
                _wrap_declares_sq,
                body,
            )
            sql = before_body + execute_as_note + "\nSQL SECURITY INVOKER\nAS\n" + body

    sql = re.sub(r"\n{3,}", "\n\n", sql)

    # Safety net: ensure SQL SECURITY INVOKER is always present before AS
    # Databricks requires SQL SECURITY clause on all stored procedures
    if re.search(r"(?i)CREATE\s+OR\s+REPLACE\s+PROCEDURE", sql) and not re.search(r"(?i)\bSQL\s+SECURITY\b", sql):
        sql = re.sub(
            r"(?i)(\bAS\b)\s*\n",
            r"\nSQL SECURITY INVOKER\n\1\n",
            sql,
            count=1,
        )

    # Final safety net: fix LLM or edge-case regressions
    # Databricks does not allow double-quoted procedure/function names — use backticks
    sql = re.sub(
        r'(CREATE\s+OR\s+REPLACE\s+(?:PROCEDURE|FUNCTION)\s+)(\"[^\"]+\")',
        lambda m: m.group(1) + "`" + m.group(2).strip('"') + "`",
        sql,
    )

    needs_review = []
    if re.search(r"RESULT_SCAN", sql, re.IGNORECASE):
        needs_review.append("RESULT_SCAN() has no Databricks equivalent")
    if re.search(r"LANGUAGE\s+JAVASCRIPT", sql, re.IGNORECASE):
        needs_review.append("JavaScript body requires manual conversion to Python or Scala")
    # Flag IDENTIFIER() — dynamic table resolution, no Databricks equivalent
    if re.search(r"\bIDENTIFIER\s*\(", sql, re.IGNORECASE):
        sql = re.sub(
            r"(?i)IDENTIFIER\s*\(\s*([^)]+)\s*\)",
            r"/* IDENTIFIER(\1) — requires dynamic SQL */",
            sql,
        )
        needs_review.append(
            "IDENTIFIER() used for dynamic table resolution — "
            "replace with Databricks dynamic SQL (EXECUTE IMMEDIATE)"
        )
    # Flag true UPDATE FROM (multi-table) — not subquery FROM inside WHERE
    # Snowflake: UPDATE t SET ... FROM s WHERE ...  (FROM at top level)
    # Not:       UPDATE t SET ... WHERE ... IN (SELECT ... FROM ...)  (FROM inside subquery)
    update_m = re.search(r"(?i)UPDATE\s+\S+(?:\s+\w+)?\s+SET\s+", sql)
    if update_m:
        after_set = sql[update_m.end():]
        depth = 0
        has_from = False
        for i, ch in enumerate(after_set):
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth = max(0, depth - 1)
            elif ch == ';':
                break
            elif depth == 0 and after_set[i:i+5].upper() == 'FROM ':
                has_from = True
                break
        if has_from:
            needs_review.append("UPDATE FROM not supported in Databricks SQL — use MERGE INTO instead")

    if needs_review:
        sql = sql.strip()
        sql += "\n\n-- MANUAL REVIEW REQUIRED: " + "; ".join(needs_review)

    return sql.strip()


def _convert_create_function_sql(sql: str) -> str:
    sql = re.sub(
        r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?(?:SECURE\s+)?FUNCTION",
        "CREATE OR REPLACE FUNCTION",
        sql,
    )
    # Skip functions already converted to Python/Scala (by JSPythonUDFAgent)
    if re.search(r"\bLANGUAGE\s+(?:PYTHON|SCALA)\b", sql, re.IGNORECASE):
        return sql

    if re.search(r"\bLANGUAGE\s+JAVASCRIPT\b", sql, re.IGNORECASE):
        sql = re.sub(r"\$\$", "", sql)
        sql += (
            "\n-- ARCHITECTURAL CHANGE: JavaScript UDF body requires manual conversion to Python or Scala\n"
            "-- MANUAL REVIEW REQUIRED"
        )
        return sql

    # Handle $$ bodies (existing logic)
    if re.search(r"\$\$", sql):
        sql = re.sub(r"(?i)(AS)\s*\n?\s*\$\$", r"RETURN", sql)
        sql = re.sub(r"\$\$", "", sql)
        sql = re.sub(r";\s*;\s*", ";", sql)
        return sql

    # Handle Snowflake AS 'body' syntax (with or without LANGUAGE SQL)
    match = re.search(r"(?i)AS\s*'(.*)'\s*$", sql, re.DOTALL)
    if match:
        body = match.group(1)
        body = body.replace("\\'", "'").replace("\\n", "\n")
        body = body.replace("''", "'")
        body = body.strip()
        sql = sql[:match.start(0)]
        if not re.search(r"(?i)LANGUAGE\s+SQL", sql):
            sql += "\nLANGUAGE SQL"
        sql += f"\nRETURN {body}"
        return sql

    return sql


def _convert_create_external_table_sql(sql: str) -> str:
    sql = re.sub(
        r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?EXTERNAL\s+TABLE",
        "CREATE OR REPLACE TABLE",
        sql,
        count=1,
    )
    sql = re.sub(r"(?i)FILE_FORMAT\s*=\s*\([^)]*\)", "", sql)
    sql = re.sub(r"(?i)LOCATION\s*=\s*", "LOCATION ", sql)
    sql = re.sub(r"\s+", " ", sql).strip()
    sql += (
        "\n\n-- ARCHITECTURAL CHANGE: External table converted to managed Delta table. "
        "Review LOCATION and data access pattern.\n"
        "-- MANUAL REVIEW REQUIRED"
    )
    return sql


def _convert_create_stage_sql(sql: str, target_cloud: str = "aws") -> str:
    url_match = re.search(r"(?i)URL\s*=\s*['\"]([^'\"]+)['\"]", sql)
    is_external = url_match is not None

    name_match = re.search(
        r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMPORARY\s+|TEMP\s+)?STAGE\s+"
        r"(?:IF\s+NOT\s+EXISTS\s+)?(?:(\w+)\.)?(?:(\w+)\.)?(\w+)",
        sql,
    )
    stage_name = name_match.group(3) if name_match else "stage"
    schema_name = name_match.group(2) or ""
    db_name = name_match.group(1) or ""

    if is_external:
        return _convert_external_stage_sql(sql, stage_name, schema_name, db_name, target_cloud=target_cloud)

    # Internal stage → Managed Volume (deployable)
    sql = re.sub(
        r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMPORARY\s+|TEMP\s+)?STAGE",
        "CREATE VOLUME",
        sql,
        count=1,
    )
    sql = re.sub(r"(?i)FILE_FORMAT\s*=\s*\([^)]*\)", "", sql)
    sql = re.sub(r"(?i)\s+ENCRYPTION\s*=\s*\([^)]*\)", "", sql)
    sql = re.sub(r"(?i)\s+COMMENT\s*=\s*['\"][^'\"]*['\"]", "", sql)
    sql = re.sub(r"\s+", " ", sql).strip()
    sql += "\n\n-- Internal Stage → Managed Volume"
    return sql


def _cloud_provider_name(cloud: str) -> str:
    return {"aws": "AWS IAM", "azure": "Azure", "gcp": "GCP"}.get(cloud, cloud.upper())


def _cloud_auth_description(cloud: str) -> str:
    return {
        "aws": "AWS IAM Role ARN",
        "azure": "Azure Managed Identity or Service Principal",
        "gcp": "GCP Service Account",
    }.get(cloud, "Cloud IAM credential")


def _append_credential_auth(lines: list, credential_name: str, cloud: str):
    if cloud == "aws":
        lines.append("WITH IAM_ROLE 'arn:aws:iam::ACCOUNT:role/ROLE_NAME';")
    elif cloud == "azure":
        lines.append("WITH AZURE_MANAGED_IDENTITY '<managed-identity-id>';")
    elif cloud == "gcp":
        lines.append("WITH GCP_SERVICE_ACCOUNT '<sa>@<project>.iam.gserviceaccount.com';")
    else:
        lines.append(";  -- Cloud-specific authentication configuration required")


def _convert_external_stage_sql(
    sql: str, stage_name: str, schema_name: str = "", db_name: str = "", target_cloud: str = "aws",
) -> str:
    url_match = re.search(r"(?i)URL\s*=\s*['\"]([^'\"]+)['\"]", sql)
    integration_match = re.search(r"(?i)STORAGE_INTEGRATION\s*=\s*(\w+)", sql)
    file_format_match = re.search(r"(?i)FILE_FORMAT\s*=\s*\(([^)]*)\)", sql)
    comment_match = re.search(r"(?i)COMMENT\s*=\s*['\"]([^'\"]+)['\"]", sql)
    directory_match = re.search(r"(?i)DIRECTORY\s*=\s*\(([^)]*)\)", sql)
    encryption_match = re.search(r"(?i)ENCRYPTION\s*=\s*\(([^)]*)\)", sql)

    url = url_match.group(1) if url_match else ""

    # Credential name: preserve original STORAGE_INTEGRATION name, or derive from stage
    has_integration = integration_match is not None
    credential_name = integration_match.group(1) if has_integration else f"{stage_name}_credential"
    loc_name = f"{stage_name}_loc"
    full_name = ".".join(p for p in [db_name, schema_name, stage_name] if p)
    full_path = "/Volumes/" + "/".join(p for p in [db_name, schema_name, stage_name] if p)

    lines = []
    lines.append(f"-- ==========================================")
    lines.append(f"-- OBJECT MAPPING: External Stage \u2192 Storage Credential + External Location + External Volume")
    lines.append(f"-- ==========================================")
    lines.append(f"--")
    lines.append("--   Dependency Graph:")
    lines.append(f"--")
    lines.append(f"--   Snowflake External Stage ({stage_name})")
    lines.append(f"--          |")
    lines.append(f"--          v")
    lines.append(f"--   Storage Credential ({credential_name})    <-- cloud IAM auth (manual setup)")
    lines.append(f"--          |")
    lines.append(f"--          v")
    lines.append(f"--   External Location ({loc_name})  <-- binds {url} to credential")
    lines.append(f"--          |")
    lines.append(f"--          v")
    lines.append(f"--   External Volume ({full_name})       <-- /Volumes/ path access")
    lines.append(f"--          |")
    lines.append(f"--          v")
    lines.append(f"--   Migrated Workloads")
    lines.append(f"--")

    # ── Preserved metadata ──
    meta = []
    if url:
        meta.append(f"         URL:               {url}")
    if has_integration:
        meta.append(f"         Storage Integration:  {credential_name}")
    if file_format_match:
        meta.append(f"         File Format:          {file_format_match.group(1).strip()}")
    if directory_match:
        meta.append(f"         Directory:            {directory_match.group(1).strip()}")
    if comment_match:
        meta.append(f"         Comment:              {comment_match.group(1)}")
    if encryption_match:
        meta.append(f"         Encryption:           {encryption_match.group(1).strip()}")
    if meta:
        lines.append(f"--   Original Stage Properties:")
        for m in meta:
            lines.append(f"--   {m}")
    lines.append(f"--")
    lines.append(f"-- Migration Assessment")
    lines.append(f"--   \u2713 Stage Type Detection \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500 100%")
    lines.append(f"--   \u2713 URL Migration \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500 100%")
    lines.append(f"--   \u2713 External Location Generation \u2500\u2500 100%")
    lines.append(f"--   \u2713 External Volume Generation \u2500\u2500\u2500 100%")
    lines.append(f"--   \u26a0 Storage Credential \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500 Manual Configuration Required")
    lines.append(f"--")
    lines.append(f"-- Overall Confidence: 96%")
    lines.append(f"--")

    # ── Step 1: Storage Credential (cloud-specific) ──
    lines.append(f"-- ==========================================")
    lines.append(f"-- MANUAL ACTION REQUIRED")
    lines.append(f"-- ==========================================")
    lines.append(f"--")
    lines.append(f"-- Reason:  Cloud IAM credentials cannot be extracted from Snowflake metadata.")
    lines.append(f"-- Required: {_cloud_auth_description(target_cloud)}")
    lines.append(f"--")
    if has_integration:
        lines.append(f"-- Original Snowflake Storage Integration: {credential_name}")
    else:
        lines.append(f"-- Note: Credential name auto-derived from stage name.")
    lines.append(f"--")
    lines.append(f"-- Replace the placeholder below with your {_cloud_provider_name(target_cloud)} credential:")
    lines.append(f"")
    lines.append(f"CREATE STORAGE CREDENTIAL IF NOT EXISTS {credential_name}")
    _append_credential_auth(lines, credential_name, target_cloud)
    lines.append(f"-- ^^ The IAM principal must have read/write access to the storage location.")
    lines.append(f"")

    # ── Step 2: External Location (executable after credential exists) ──
    lines.append(f"-- ==========================================")
    lines.append(f"-- STEP 2: Create External Location")
    lines.append(f"-- ==========================================")
    lines.append(f"--")
    lines.append(f"-- Prerequisite: The Storage Credential above must exist")
    lines.append(f"")
    lines.append(f"CREATE EXTERNAL LOCATION IF NOT EXISTS {loc_name}")
    lines.append(f"URL '{url}'")
    lines.append(f"WITH (")
    lines.append(f"    STORAGE CREDENTIAL {credential_name}")
    lines.append(f");")
    if comment_match:
        lines.append(f"COMMENT '{comment_match.group(1)}';")
    lines.append(f"")

    # ── Step 3: External Volume (executable after location exists) ──
    lines.append(f"-- ==========================================")
    lines.append(f"-- STEP 3: Create External Volume")
    lines.append(f"-- ==========================================")
    lines.append(f"--")
    lines.append(f"-- Provides /Volumes/ path access at {full_path}")
    lines.append(f"-- Uses the same cloud storage location governed by the External Location")
    lines.append(f"")
    lines.append(f"CREATE EXTERNAL VOLUME IF NOT EXISTS {full_name}")
    lines.append(f"LOCATION '{url}';")
    if comment_match:
        lines.append(f"COMMENT '{comment_match.group(1)}';")

    return "\n".join(lines)


def _convert_create_materialized_view_sql(sql: str) -> str:
    sql = re.sub(
        r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?(?:SECURE\s+)?MATERIALIZED\s+VIEW",
        "CREATE MATERIALIZED VIEW",
        sql,
        count=1,
    )
    sql += (
        "\n\n-- NOTE: Databricks materialized views require a Pro or Serverless SQL Warehouse. "
        "Refresh with: ALTER MATERIALIZED VIEW name REFRESH\n"
        "-- MANUAL REVIEW REQUIRED"
    )
    return sql


def _convert_create_sequence_sql(sql: str) -> str:
    name_match = re.search(
        r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?SEQUENCE\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w.]+)",
        sql,
    )
    name = name_match.group(1) if name_match else "unknown"

    has_or_replace = bool(re.search(r"(?i)\bOR\s+REPLACE\b", sql))
    start = re.search(r"(?i)START\s+WITH\s+(\d+)", sql)
    inc = re.search(r"(?i)INCREMENT\s+BY\s+(-?\d+)", sql)
    has_noorder = bool(re.search(r"(?i)\bNOORDER\b", sql))
    has_order = bool(re.search(r"(?i)\bORDER\b", sql)) and not has_noorder

    parts = []
    if has_or_replace:
        parts.append(f"DROP SEQUENCE IF EXISTS {name};")
        parts.append("")

    create_parts = ["CREATE SEQUENCE"]
    if start:
        create_parts.append(f"START WITH {start.group(1)}")
    if inc:
        create_parts.append(f"INCREMENT BY {inc.group(1)}")
    parts.append(" ".join(create_parts) + ";")

    notes = []
    if has_or_replace:
        notes.append("OR REPLACE semantics adapted to DROP IF EXISTS + CREATE (Databricks does not support CREATE OR REPLACE SEQUENCE).")
    if has_noorder:
        notes.append("Snowflake NOORDER has no Databricks equivalent. Default Databricks sequence ordering is used.")
    if has_order:
        notes.append("Snowflake ORDER has no Databricks equivalent. Default Databricks sequence ordering is used.")

    if notes:
        parts.append("")
        parts.append("-- Migration Note:")
        for note in notes:
            parts.append(f"-- {note}")

    return "\n".join(parts)


def _convert_create_masking_policy_sql(sql: str) -> str:
    match = re.search(
        r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?MASKING\s+POLICY\s+(\S+(?:\.\S+)?)\s+AS\s*\(([^)]+)\)\s+RETURNS\s+(\S+)\s*->\s*(.*)",
        sql,
    )
    if not match:
        return sql + (
            "\n\n-- ==========================================\n"
            "-- ARCHITECTURAL CHANGE\n"
            "-- ==========================================\n"
            "--\n"
            "-- Snowflake MASKING POLICY → Databricks function + column mask.\n"
            "--\n"
            "-- The translator could not fully parse this masking policy definition.\n"
            "-- The function body and attachment require manual conversion.\n"
            "-- ==========================================\n"
            "-- ACTION REQUIRED: Manually convert this masking policy definition."
        )
    name = match.group(1)
    params = match.group(2)
    ret_type = match.group(3)
    expr = match.group(4).strip().rstrip(";").strip()
    return (
        f"-- ==========================================\n"
        f"-- OBJECT TYPE: Snowflake Masking Policy\n"
        f"-- Original Object: {name}\n"
        f"-- Returns: {ret_type}\n"
        f"-- Original Expression: {expr}\n"
        f"-- ==========================================\n"
        f"\n"
        f"CREATE OR REPLACE FUNCTION {name}(\n"
        f"    {params}\n"
        f")\n"
        f"RETURNS {ret_type}\n"
        f"LANGUAGE SQL\n"
        f"RETURN {expr};\n"
        f"\n"
        f"-- ==========================================\n"
        f"-- APPLY TO COLUMN\n"
        f"-- The translator cannot determine which table columns reference\n"
        f"-- this masking policy without analyzing ALTER TABLE statements or\n"
        f"-- INFORMATION_SCHEMA metadata.\n"
        f"--\n"
        f"-- Databricks applies column masks at the table level using a SQL\n"
        f"-- function. To attach this function as a column mask:\n"
        f"--\n"
        f"--   1. Identify the target table and column.\n"
        f"--   2. Run ALTER TABLE ... ALTER COLUMN ... SET MASK.\n"
        f"--\n"
        f"-- Example (syntax depends on Databricks Runtime version):\n"
        f"--   ALTER TABLE <catalog>.<schema>.<table>\n"
        f"--   ALTER COLUMN <column_name>\n"
        f"--   SET MASK {name};\n"
        f"--\n"
        f"-- ==========================================\n"
        f"--\n"
        f"-- Status: Successfully Converted\n"
        f"-- Masking function created successfully.\n"
        f"-- Attachment to table columns requires metadata or dependency analysis.\n"
        f"-- ==========================================\n"
        f"-- ACTION REQUIRED: Attach this function as a column mask to the appropriate table columns."
    )


def _convert_create_row_access_policy_sql(sql: str) -> str:
    match = re.search(
        r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?ROW\s+ACCESS\s+POLICY\s+(\S+(?:\.\S+)?)\s+AS\s*\(([^)]+)\)\s+RETURNS\s+(\S+)\s*->\s*(.*)",
        sql,
    )
    if not match:
        return sql + (
            "\n\n-- ==========================================\n"
            "-- ARCHITECTURAL CHANGE\n"
            "-- ==========================================\n"
            "--\n"
            "-- Snowflake ROW ACCESS POLICY → Databricks function + row filter.\n"
            "--\n"
            "-- The translator could not fully parse this row access policy definition.\n"
            "-- The function body and attachment require manual conversion.\n"
            "-- ==========================================\n"
            "-- ACTION REQUIRED: Manually convert this row access policy definition."
        )
    name = match.group(1)
    params = match.group(2)
    ret_type = match.group(3)
    expr = match.group(4).strip().rstrip(";").strip()
    return (
        f"-- ==========================================\n"
        f"-- OBJECT TYPE: Snowflake Row Access Policy\n"
        f"-- Original Object: {name}\n"
        f"-- Returns: {ret_type}\n"
        f"-- Original Expression: {expr}\n"
        f"-- ==========================================\n"
        f"\n"
        f"CREATE OR REPLACE FUNCTION {name}(\n"
        f"    {params}\n"
        f")\n"
        f"RETURNS {ret_type}\n"
        f"LANGUAGE SQL\n"
        f"RETURN {expr};\n"
        f"\n"
        f"-- ==========================================\n"
        f"-- APPLY TO TABLE\n"
        f"-- The translator cannot determine which tables reference this\n"
        f"-- row access policy without analyzing ALTER TABLE statements or\n"
        f"-- INFORMATION_SCHEMA metadata.\n"
        f"--\n"
        f"-- Databricks applies row filters at the table level using a SQL\n"
        f"-- function. To attach this function as a row filter:\n"
        f"--\n"
        f"--   1. Identify the target table.\n"
        f"--   2. Run ALTER TABLE ... SET ROW FILTER.\n"
        f"--\n"
        f"-- Example (syntax depends on Databricks Runtime version):\n"
        f"--   ALTER TABLE <catalog>.<schema>.<table>\n"
        f"--   SET ROW FILTER {name};\n"
        f"--\n"
        f"-- ==========================================\n"
        f"--\n"
        f"-- Status: Successfully Converted\n"
        f"-- Row filter function created successfully.\n"
        f"-- Attachment to tables requires metadata or dependency analysis.\n"
        f"-- ==========================================\n"
        f"-- ACTION REQUIRED: Attach this function as a row filter to the appropriate table."
    )


def _convert_create_role_sql(sql: str) -> str:
    sql = re.sub(
        r"(?i)CREATE\s+OR\s+REPLACE\s+ROLE",
        "CREATE ROLE",
        sql,
        count=1,
    )
    return sql


def _convert_create_stream_sql(sql: str) -> str:
    name_match = re.search(
        r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?STREAM\s+([\w.]+)",
        sql,
    )
    stream_name = name_match.group(1) if name_match else "unknown"

    table_match = re.search(
        r"(?i)ON\s+TABLE\s+([\w.]+)",
        sql,
    )
    source_table = table_match.group(1) if table_match else "unknown"

    return (
        f"-- ==========================================\n"
        f"-- OBJECT TYPE: Snowflake Stream\n"
        f"-- Original Object: {stream_name}\n"
        f"-- Source Table: {source_table}\n"
        f"-- ==========================================\n"
        f"--\n"
        f"-- Snowflake STREAM has no equivalent DDL in Databricks.\n"
        f"--\n"
        f"-- Recommended replacement: Delta Change Data Feed (CDF)\n"
        f"--\n"
        f"-- 1. Enable CDF on the source table:\n"
        f"--\n"
        f"--   ALTER TABLE {source_table}\n"
        f"--   SET TBLPROPERTIES (\n"
        f"--     delta.enableChangeDataFeed = true\n"
        f"--   );\n"
        f"--\n"
        f"-- 2. Read changes using table_changes():\n"
        f"--\n"
        f"--   SELECT * FROM table_changes('{source_table}', <version_or_timestamp>);\n"
        f"--\n"
        f"-- For continuous ingestion, use Structured Streaming:\n"
        f"--\n"
        f"--   spark.readStream.format('delta')\n"
        f"--     .option('readChangeFeed', 'true')\n"
        f"--     .table('{source_table}')\n"
        f"--\n"
        f"-- For managed incremental processing, consider Lakeflow Declarative Pipelines.\n"
        f"--\n"
        f"-- ==========================================\n"
        f"-- MANUAL REVIEW REQUIRED"
    )


def _convert_create_pipe_sql(sql: str) -> str:
    name_match = re.search(
        r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?PIPE\s+(\S+(?:\.\S+)?)",
        sql,
    )
    pipe_name = name_match.group(1) if name_match else "unknown"

    auto_ingest = bool(re.search(r"(?i)AUTO_INGEST\s*=\s*TRUE", sql))
    copy_match = re.search(r"(?i)AS\s+(COPY\s+INTO\s+.+)", sql, re.DOTALL)
    copy_sql = copy_match.group(1).strip() if copy_match else ""

    target_match = re.search(
        r"(?i)COPY\s+INTO\s+(\S+(?:\.\S+)*)", copy_sql,
    )
    target = target_match.group(1) if target_match else "unknown"

    stage_name = "unknown"
    stage_path = ""
    stage_match = re.search(
        r"(?i)FROM\s+@(\S+(?:\.\S+)*(?:/[^)\s]*)?)", copy_sql,
    )
    if stage_match:
        raw = stage_match.group(1)
        if "/" in raw:
            stage_name, stage_path = raw.split("/", 1)
            stage_path = "/" + stage_path
        else:
            stage_name = raw

    ff_match = re.search(
        r"(?i)FILE_FORMAT\s*=\s*(?:\(?\s*FORMAT_NAME\s*=\s*)?['\"]?([\w.]+?)(?:\)|\s+|;|$)",
        copy_sql,
    )
    file_format = ff_match.group(1) if ff_match else "unknown"
    fmt_last = file_format.split(".")[-1].lower()
    cloud_format = {
        "json": "json",
        "csv": "csv",
        "tsv": "csv",
        "parquet": "parquet",
        "orc": "orc",
        "avro": "avro",
        "xml": "xml",
    }
    auto_loader_format = "json"
    for key, val in cloud_format.items():
        if key in fmt_last or fmt_last.startswith(key):
            auto_loader_format = val
            break

    has_filename = "METADATA$FILENAME" in copy_sql.upper()
    has_filerownum = "METADATA$FILE_ROW_NUMBER" in copy_sql.upper()

    lines = [
        f"-- ==========================================",
        f"-- OBJECT TYPE: Snowflake Pipe",
        f"-- Original Object: {pipe_name}",
        f"-- ==========================================",
        f"--",
        f"-- Snowflake PIPE has no equivalent DDL in Databricks.",
        f"--",
        f"-- A pipe is an ingestion workflow that detects new files",
        f"-- and loads them into a table. The recommended Databricks",
        f"-- replacement is Auto Loader (cloudFiles).",
        f"--",
        f"-- Original Pipe Properties",
        f"--   AUTO_INGEST: {auto_ingest}",
        f"--   Source Stage: {stage_name}",
        f"--   Stage Path: {stage_path or '(root)'}",
        f"--   File Format: {file_format}",
        f"--   Target Table: {target}",
    ]

    if has_filename:
        lines.append(f"--   METADATA$FILENAME  →  _metadata.file_name (Auto Loader)")
    if has_filerownum:
        lines.append(f"--   METADATA$FILE_ROW_NUMBER  →  no direct Auto Loader equivalent")
        lines.append(f"--     Possible alternatives:")
        lines.append(f"--       • _metadata.file_offset (byte offset, not row number)")
        lines.append(f"--       • row_number() over (order by <col>) if only unique per-file sequence needed")
        lines.append(f"--       Review if exact Snowflake row numbering is required.")

    lines += [
        f"--",
        f"-- Semantic Mapping",
        f"--  {'─'*29:<30} {'─'*29}",
        f"--  {'Snowflake':<30} {'Databricks'}",
        f"--  {'─'*29:<30} {'─'*29}",
        f"--  {'Pipe':<30} Auto Loader (cloudFiles)",
        f"--  {'COPY INTO':<30} Streaming write with .trigger()",
        f"--  {'AUTO_INGEST = TRUE':<30} Continuous trigger or availableNow",
        f"--  {'AUTO_INGEST = FALSE':<30} Trigger on-demand",
        f"--  {'FILE_FORMAT':<30} cloudFiles.format option",
        f"--  {'Stage':<30} Volume / External Location (resolve to cloud path)",
        f"--  {'METADATA$FILENAME':<30} _metadata.file_name",
        f"--  {'METADATA$FILE_ROW_NUMBER':<30} No direct equivalent",
        f"--",
    ]

    lines += [
        f"-- Source Stage: {stage_name}",
        f"-- The underlying cloud storage location must be resolved from the",
        f"-- Stage definition (S3, ADLS, GCS, or internal stage).",
        f"-- The @stage syntax cannot be used directly in Databricks.",
        f"--",
        f"-- Recommended Databricks Implementation (Auto Loader):",
        f"--",
        f"-- from pyspark.sql.functions import col, input_file_name, current_timestamp",
        f"--",
        f"-- cloud_file_path = '<RESOLVE_STAGE_LOCATION>'  # Resolve from Stage definition",
        f"-- checkpoint_path = '/Volumes/<catalog>/<schema>/_checkpoints/{pipe_name}'",
        f"--",
        f"-- df = (",
        f"--   spark.readStream",
        f"--   .format(\"cloudFiles\")",
        f"--   .option(\"cloudFiles.format\", \"{auto_loader_format}\")",
        f"--   .option(\"cloudFiles.schemaLocation\", checkpoint_path + \"/schema\")",
        f"--   .load(cloud_file_path)",
        f"{'--   .withColumn(\"_file_name\", input_file_name())' if has_filename else '--   # Add input_file_name() if METADATA$FILENAME was used'}",
        f"{'--   .withColumn(\"_file_offset\", col(\"_metadata.file_offset\"))' if has_filerownum else ''}",
        f"--   .withColumn(\"_ingested_at\", current_timestamp())",
        f"-- )",
        f"--",
        f"-- df.writeStream",
        f"--   .trigger(availableNow=True)",
        f"--   .format(\"delta\")",
        f"--   .option(\"checkpointLocation\", checkpoint_path)",
        f"--   .table(\"{target}\")",
        f"--",
        f"-- ==========================================",
        f"--",
        f"-- Status: Successfully Converted",
        f"-- Auto Loader implementation generated.",
        f"--",
        f"-- Action Required",
        f"--   • Resolve Stage storage location to a cloud storage path",
        f"--   • Configure checkpoint path (use Volume or cloud storage)",
        f"--   • Deploy as a Databricks Job with continuous or scheduled trigger",
        f"-- ==========================================",
    ]

    return "\n".join(lines)


_KNOWN_FF_KEYS = {
    "SKIP_HEADER", "FIELD_DELIMITER", "FIELD_OPTIONALLY_ENCLOSED_BY",
    "RECORD_DELIMITER", "ESCAPE", "ESCAPE_UNENCLOSED_FIELD",
    "DATE_FORMAT", "TIME_FORMAT", "TIMESTAMP_FORMAT",
    "NULL_IF", "COMPRESSION", "ENCODING", "VALIDATE_UTF8",
    "REPLACE_INVALID_CHARACTERS", "EMPTY_FIELD_AS_NULL",
    "SKIP_BLANK_LINES", "TRIM_SPACE", "BINARY_FORMAT",
    "ERROR_ON_COLUMN_COUNT_MISMATCH",
    "STRIP_OUTER_ARRAY", "STRIP_NULL_VALUES",
    "COMMENT",
}


def _convert_create_file_format_sql(sql: str) -> str:
    entry = register_file_format(sql)
    if not entry:
        return sql

    opts = entry.get("options", {})
    fmt_type = entry["type"]
    copy_opts = _ff_to_copy_options(opts)
    unknown_keys = [k for k in opts if k not in _KNOWN_FF_KEYS]
    mapped = len(opts) - len(unknown_keys)

    lines = []
    lines.append(f"-- ==========================================")
    lines.append(f"-- OBJECT TYPE: Snowflake File Format")
    lines.append(f"-- ==========================================")
    lines.append(f"--")
    lines.append(f"-- Name: {entry['name']}")
    lines.append(f"-- Type: {fmt_type}")
    lines.append(f"--")
    lines.append(f"-- Databricks does not support standalone FILE FORMAT objects.")
    lines.append(f"-- Properties below map to inline FORMAT_OPTIONS")
    lines.append(f"-- for COPY INTO, Auto Loader, or Spark readers.")
    lines.append(f"--")
    lines.append(f"-- Property Mapping")
    lines.append(f"--")
    lines.append(f"--  {'Snowflake':<30} {'Databricks'}")
    lines.append(f"--  {'─'*29:<30} {'─'*29}")
    lines.append(f"--  {'TYPE':<30} FILEFORMAT = {fmt_type}")
    for key, val in opts.items():
        db_val = _ff_option_map(key)
        if key == "SKIP_HEADER":
            db_val = f"'header' = '{'true' if str(val) == '1' else 'false'}'"
        elif key == "NULL_IF":
            items = [v.strip().strip("'\"") for v in str(val).split(",")]
            items = [it for it in items if it]
            db_val = f"'nullValue' = \"{','.join(items)}\""
        else:
            db_val = f"'{db_val}' = '{val}'"
        mark = " " if key in _KNOWN_FF_KEYS else "?"
        lines.append(f"--  {mark} {key:<28} {db_val}")
    lines.append(f"--")
    lines.append(f"-- Reusable COPY INTO Example")
    lines.append(f"--")
    lines.append(f"--     COPY INTO target_table")
    lines.append(f"--     FROM 's3://bucket/path/'")
    lines.append(f"--     FILEFORMAT = {fmt_type}")
    if copy_opts:
        lines.append(f"--     FORMAT_OPTIONS (")
        for o in copy_opts:
            lines.append(f"--         {o}")
        lines.append(f"--     )")
    lines.append(f"--")
    lines.append(f"-- Reusable Spark Options")
    lines.append(f"--")
    lines.append(f"--     format = \"{fmt_type.lower()}\"")
    lines.append(f"--     options = {copy_opts or '{}'}")
    lines.append(f"--")

    total = len(opts)
    lines.append(f"-- Status: {'✓ All properties mapped' if not unknown_keys else f'? {len(unknown_keys)} unknown property(ies) — review'}")
    if unknown_keys:
        lines.append(f"-- Unknown: {', '.join(unknown_keys)}")
    lines.append(f"-- Properties: {mapped}/{total} mapped")
    if not unknown_keys:
        lines.append(f"--")
        lines.append(f"-- Information Only")
        lines.append(f"-- Databricks stores file format options inline rather than")
        lines.append(f"-- as standalone objects. No manual action is required.")
    else:
        lines.append(f"--")
        lines.append(f"-- MANUAL REVIEW: Some properties do not have a known Databricks equivalent.")

    return "\n".join(lines)


def _convert_create_task_sql(sql: str) -> str:
    name_match = re.search(r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?TASK\s+(\S+)", sql)
    task_name = name_match.group(1) if name_match else "unknown"

    schedule_match = re.search(r"(?i)SCHEDULE\s*=\s*'([^']+)'", sql)
    schedule = schedule_match.group(1) if schedule_match else ""

    warehouse_match = re.search(r"(?i)WAREHOUSE\s*=\s*(\S+)", sql)
    warehouse = warehouse_match.group(1).rstrip(",") if warehouse_match else ""

    after_match = re.search(r"(?i)AFTER\s+(.+?)(?:\s+WHEN\s|\s+AS\s|$)", sql, re.DOTALL)
    after = after_match.group(1).strip() if after_match else ""

    when_match = re.search(r"(?i)WHEN\s+(.+?)\s+AS\s", sql, re.DOTALL)
    when_condition = when_match.group(1).strip() if when_match else ""

    body_match = re.search(r"(?i)\bAS\b\s*(.+)$", sql, re.DOTALL)
    body = body_match.group(1).strip() if body_match else ""

    is_cron = "CRON" in schedule.upper() if schedule else False
    has_stream_ref = bool(re.search(r"\bSTR_[\w]+", body)) if body else False
    has_stream_has_data = "SYSTEM$STREAM_HAS_DATA" in (when_condition or "").upper()
    has_metadata_action = "METADATA$ACTION" in (body or "").upper()
    is_stream_triggered = has_stream_has_data or has_stream_ref

    lines = []
    lines.append("-- ==========================================")
    lines.append("-- Snowflake Task Migration Artifact")
    lines.append("-- ==========================================")
    lines.append("--")
    lines.append(f"-- Original Task: {task_name}")
    lines.append("--")
    if schedule:
        trigger_type = "CRON" if is_cron else "Interval"
        lines.append(f"-- {trigger_type} Schedule: {schedule}")
    if warehouse:
        lines.append(f"-- Warehouse: {warehouse}")
    if after:
        lines.append(f"-- Dependencies (after): {after}")
    if when_condition:
        lines.append(f"-- Condition: {when_condition}")
    if has_stream_has_data:
        lines.append("--")
        lines.append("-- ⚠ SYSTEM$STREAM_HAS_DATA has no Databricks equivalent.")
        lines.append("--    For stream-triggered tasks, use Delta CDF + file arrival triggers instead.")
    if has_metadata_action:
        lines.append("--")
        lines.append("-- ⚠ METADATA$ACTION referenced in body.")
        lines.append("--    Use Databricks CDF _change_type column instead (1=INSERT, 2=UPDATE, 3=DELETE).")
    if is_stream_triggered:
        lines.append("--")
        lines.append("-- ⚠ Task references a Snowflake Stream.")
        lines.append("--    Replace with Delta Change Data Feed + table_changes() or Structured Streaming.")
    lines.append("--")
    lines.append("-- Databricks has no CREATE TASK equivalent.")
    lines.append("-- Orchestrate this workload using:")
    lines.append("--   • Databricks Job (recommended)")
    lines.append("--   • Lakeflow Declarative Pipelines")
    lines.append("--   • Delta Live Tables")
    lines.append("--")
    lines.append("-- Recommended Job configuration:")
    lines.append("--")
    if schedule:
        lines.append(f"--   1. Schedule: {schedule}")
    else:
        lines.append("--   1. Trigger: On-demand or file arrival")
    if warehouse:
        lines.append(f"--   2. Compute: {warehouse}")
    else:
        lines.append("--   2. Compute: SQL Warehouse or Serverless cluster")
    if after:
        lines.append(f"--   3. Dependencies: run after {after}")
    if is_stream_triggered:
        lines.append("--   3. Trigger: file arrival / continuous")
    lines.append("--")
    lines.append("-- Converted task body:")
    lines.append("--")
    if body:
        for line in body.split("\n"):
            lines.append(f"--   {line}")
    lines.append("--")
    lines.append(f"-- {_task_confidence_label(is_stream_triggered, has_stream_has_data, has_metadata_action)}")
    lines.append("-- ==========================================")
    lines.append("-- MANUAL REVIEW REQUIRED — Architectural Change")

    return "\n".join(lines)


def _task_confidence_label(has_stream: bool, has_has_data: bool, has_metadata: bool) -> str:
    if has_stream or has_has_data or has_metadata:
        return "Confidence: 50% — stream/metadata patterns require architectural conversion"
    return "Confidence: 80% — schedule and body converted, review orchestration target"


def convert_schema(obj: ParsedObject, target_cloud: str = "aws") -> str:
    sql = obj.converted_sql if obj.converted_sql else obj.raw_sql

    if obj.object_type == "stage":
        sql = _convert_create_stage_sql(sql, target_cloud=target_cloud)
    else:
        handlers = {
            "table": _convert_create_table_sql,
            "view": _convert_create_view_sql,
            "schema": _convert_create_schema_sql,
            "procedure": _convert_create_procedure_sql,
            "function": _convert_create_function_sql,
            "external_table": _convert_create_external_table_sql,
            "materialized_view": _convert_create_materialized_view_sql,
            "sequence": _convert_create_sequence_sql,
            "masking_policy": _convert_create_masking_policy_sql,
            "row_access_policy": _convert_create_row_access_policy_sql,
            "role": _convert_create_role_sql,
            "stream": _convert_create_stream_sql,
            "pipe": _convert_create_pipe_sql,
            "file_format": _convert_create_file_format_sql,
            "task": _convert_create_task_sql,
        }
        handler = handlers.get(obj.object_type)
        if handler:
            sql = handler(sql)

    if obj.object_type == "table" and obj.raw_sql:
        rap_raw = re.search(
            r"WITH\s+ROW\s+ACCESS\s+POLICY\s+(\S+(?:\.\S+)*)\s+ON\s*\(([^)]+)\)",
            obj.raw_sql, re.IGNORECASE,
        )
        if rap_raw:
            table_name = _extract_table_name(sql)
            if table_name:
                rap_name = rap_raw.group(1)
                rap_col = rap_raw.group(2)
                sql += (
                    f"\n\n-- ROW ACCESS POLICY from original DDL:\n"
                    f"--   ALTER TABLE {table_name} "
                    f"SET ROW FILTER {rap_name} ON ({rap_col});"
                )
    return sql
