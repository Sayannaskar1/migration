import re
from typing import Optional
from parser.sql_parser import ParsedObject


TYPE_MAP: dict[str, str] = {
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
    "FLOAT4": "DOUBLE",
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


TYPE_PATTERN = re.compile(
    r"\b(" + "|".join(TYPE_MAP) + r")\b",
    re.IGNORECASE,
)


def _replace_type(match: re.Match) -> str:
    upper = match.group(0).upper()
    for sf_type in sorted(TYPE_MAP, key=len, reverse=True):
        if upper.startswith(sf_type.upper()):
            return TYPE_MAP[sf_type]
    return match.group(0)


def _strip_unsupported_args(sql: str) -> str:
    return re.sub(
        r"\b(STRING|BINARY)\s*\([^)]*\)",
        r"\1",
        sql,
        flags=re.IGNORECASE,
    )


def _strip_varchar_n(sql: str) -> str:
    return re.sub(
        r"\b(VARCHAR|CHAR|NVARCHAR)\s*\(\s*\d+\s*\)",
        "STRING",
        sql,
        flags=re.IGNORECASE,
    )


def _find_matching_paren(sql: str, start: int) -> int:
    depth = 0
    for i in range(start, len(sql)):
        ch = sql[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i
    return -1


def _apply_ddl_rules(sql: str) -> str:
    sql = re.sub(r"(?i)CLUSTER\s+BY\s*\(", "ZORDER BY (", sql)
    sql = re.sub(r"(?i)TABLESPACE\s+\S+", "", sql)
    match = re.search(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(\S+)",
        sql,
        re.IGNORECASE,
    )
    if match:
        table_end = match.end()
        rest = sql[table_end:]
        rest_stripped = rest.lstrip()

        if re.search(r"\bCLONE\b", rest, re.IGNORECASE):
            return sql

        if rest_stripped.startswith("("):
            paren_start = table_end + len(rest) - len(rest_stripped)
            close_paren = _find_matching_paren(sql, paren_start)
            if close_paren != -1:
                after_paren = close_paren + 1
                rest_after = sql[after_paren:]
                if not re.match(r"\s*USING\s", rest_after, re.IGNORECASE):
                    sql = (
                        sql[:after_paren]
                        + "\nUSING DELTA"
                        + rest_after
                    )
        else:
            if not re.match(r"\s*USING\s", rest, re.IGNORECASE):
                sql = (
                    sql[:table_end]
                    + " (\n  dummy_col STRING\n)\nUSING DELTA\n"
                    + rest
                )
    return sql


def _convert_iff(sql: str) -> str:
    while re.search(r"(?<![A-Z_])IFF\s*\(", sql, re.IGNORECASE):
        pattern = re.compile(r"(?<![A-Z_])IFF\s*\(", re.IGNORECASE)
        match = pattern.search(sql)
        if not match:
            break
        start = match.start()
        paren_start = match.end() - 1
        close = _find_matching_paren(sql, paren_start)
        if close == -1:
            break
        inner = sql[paren_start + 1 : close]
        parts = _extract_args(inner)
        if len(parts) >= 3:
            condition, true_val, false_val = parts[0], parts[1], ",".join(parts[2:])
            replacement = f"CASE WHEN {condition} THEN {true_val} ELSE {false_val} END"
            sql = sql[:start] + replacement + sql[close + 1 :]
        else:
            break
    return sql


def _convert_qualify(sql: str) -> str:
    qualify_idx = re.search(r"\bQUALIFY\b", sql, re.IGNORECASE)
    if not qualify_idx:
        return sql

    before_qualify = sql[: qualify_idx.start()].rstrip()
    after_qualify_raw = sql[qualify_idx.end() :]

    over_match = re.search(r"OVER\s*\(", after_qualify_raw, re.IGNORECASE)
    if over_match:
        over_paren = over_match.end() - 1
        close_paren = _find_matching_paren(after_qualify_raw, over_paren)
        if close_paren != -1:
            space_before = after_qualify_raw[: close_paren + 1].rfind(
                "ROW_NUMBER", 0, close_paren + 1
            )
            if space_before != -1:
                rn_expr = after_qualify_raw[space_before : close_paren + 1].strip()
                rest = after_qualify_raw[close_paren + 1 :]
                equals_match = re.search(r"\s*=\s*(\d+)", rest)
                if equals_match:
                    rn_value = equals_match.group(1)
                    qualify_end_idx = (
                        qualify_idx.end() + close_paren + 1 + equals_match.end()
                    )
                    after_sql = sql[qualify_end_idx:]
                    return _wrap_subquery(
                        before_qualify, rn_expr, "rn", rn_value
                    ) + after_sql

    alias_match = re.match(r"\s*(\w+)\s*=\s*(\d+)", after_qualify_raw)
    if alias_match:
        alias = alias_match.group(1)
        rn_value = alias_match.group(2)
        select_end = before_qualify.rstrip()
        as_idx = re.search(r"\bAS\s*$", select_end, re.IGNORECASE)
        if as_idx:
            select_end = select_end[: as_idx.start()].rstrip()

        win_pattern = re.compile(
            r"ROW_NUMBER\s*\(\s*\)\s*OVER\s*\(.*?\)\s+(?:AS\s+)?" + re.escape(alias),
            re.IGNORECASE | re.DOTALL,
        )
        win_match = win_pattern.search(select_end)
        if win_match:
            rn_expr = win_match.group(0).strip()
            rn_expr = re.sub(
                r"\s+(?:AS\s+)?" + re.escape(alias) + r"\s*$",
                "",
                rn_expr,
                flags=re.IGNORECASE,
            ).strip()
            alias_end = alias_match.end()
            after_sql = after_qualify_raw[alias_end:]
            return (
                _wrap_subquery(select_end, rn_expr, alias, rn_value)
                + after_sql
            )

    return sql


def _wrap_subquery(
    before_qualify: str, rn_expr: str, alias: str, rn_value: str
) -> str:
    as_idx = re.search(r"\bAS\s*$", before_qualify, re.IGNORECASE)
    if as_idx:
        before_qualify = before_qualify[: as_idx.start()].rstrip()

    inner_select_match = re.search(r"\bSELECT\b", before_qualify, re.IGNORECASE)
    if not inner_select_match:
        return before_qualify + f" QUALIFY {alias} = {rn_value}"

    inner_select = before_qualify[inner_select_match.start() :]
    outer_prefix = before_qualify[: inner_select_match.start()]
    from_match = re.search(r"\bFROM\b", inner_select, re.IGNORECASE)

    if from_match:
        cols_part = inner_select[len("SELECT") : from_match.start()].strip()
        from_part = inner_select[from_match.start() :]
        orig_win_pattern = re.compile(
            r"ROW_NUMBER\s*\(\s*\)\s*OVER\s*\(.*?\)\s*(?:AS\s+)?" + re.escape(alias) + r"\s*,?\s*",
            re.IGNORECASE | re.DOTALL,
        )
        cols_part = orig_win_pattern.sub("", cols_part).strip()
        if cols_part.endswith(","):
            cols_part = cols_part[:-1].strip()
        inner_sql = f"SELECT {cols_part}, {rn_expr} AS {alias} {from_part}"
    else:
        cols_part = "*"
        inner_sql = f"SELECT *, {rn_expr} AS {alias} FROM {inner_select}"

    cols_part_outer = ', '.join(
        re.sub(r'^\s*\w+\.', '', col) for col in cols_part.split(',')
    )
    return f"{outer_prefix}SELECT {cols_part_outer} FROM (\n  {inner_sql}\n) WHERE {alias} = {rn_value}"


def _extract_args(args_str: str) -> list[str]:
    args = []
    depth = 0
    current = ""
    for ch in args_str:
        if ch == "(":
            depth += 1
            current += ch
        elif ch == ")":
            depth -= 1
            current += ch
        elif ch == "," and depth == 0:
            args.append(current.strip())
            current = ""
        else:
            current += ch
    if current.strip():
        args.append(current.strip())
    return args


def _convert_lateral_flatten(sql: str) -> str:
    result = sql
    pattern = re.compile(r"LATERAL\s+(?:FLATTEN|EXPLODE)\s*\(", re.IGNORECASE)
    while True:
        match = pattern.search(result)
        if not match:
            break
        start = match.start()
        paren_start = match.end() - 1
        close = _find_matching_paren(result, paren_start)
        if close == -1:
            break
        inner = result[paren_start + 1 : close]
        inner_clean = re.sub(r"(?i)INPUT\s*=>", "", inner).strip()
        replacement = f"LATERAL VIEW EXPLODE({inner_clean})"
        result = result[:start] + replacement + result[close + 1 :]
    return result


def _replace_func(sql: str, func_name: str, template: str) -> str:
    pattern = re.compile(rf"{func_name}\s*\(", re.IGNORECASE)
    result = sql
    while True:
        match = pattern.search(result)
        if not match:
            break
        start = match.start()
        paren_start = match.end() - 1
        close = _find_matching_paren(result, paren_start)
        if close == -1:
            break
        inner = result[paren_start + 1 : close]
        replacement = template.replace("{inner}", inner)
        result = result[:start] + replacement + result[close + 1 :]
    return result


def _convert_get(sql: str) -> str:
    result = sql
    pattern = re.compile(r"GET\s*\(", re.IGNORECASE)
    while True:
        match = pattern.search(result)
        if not match:
            break
        start = match.start()
        paren_start = match.end() - 1
        close = _find_matching_paren(result, paren_start)
        if close == -1:
            break
        inner = result[paren_start + 1 : close]
        args = _extract_args(inner)
        if len(args) >= 2:
            replacement = f"{args[0]}[{args[1]}]"
            result = result[:start] + replacement + result[close + 1 :]
        else:
            break
    return result


def _convert_flatten(sql: str) -> str:
    result = []
    for line in sql.split("\n"):
        if re.search(r"LATERAL\s+FLATTEN", line, re.IGNORECASE):
            result.append(line)
        else:
            result.append(_replace_func(line, "FLATTEN", "EXPLODE({inner})"))
    return "\n".join(result)


def _skip_string_literals(sql: str, pos: int) -> bool:
    """Return True if *pos* falls inside a single-quoted string literal."""
    in_str = False
    i = 0
    while i < pos:
        ch = sql[i]
        if ch == "'":
            if in_str and i + 1 < len(sql) and sql[i + 1] == "'":
                i += 2
                continue
            in_str = not in_str
        i += 1
    return in_str


def _segments_to_path(segments: str) -> str:
    path = re.sub(r"^[:.]+", "", segments)
    path = re.sub(r"[:.]+", ".", path)
    return path


def _is_url_or_temporal(left: str, segments: str) -> bool:
    if left.upper() in ("HTTP", "HTTPS", "FTP"):
        return True
    first_seg = segments.lstrip(":.").split(".")[0].split("[")[0].strip()
    if first_seg.upper() in ("MINUTE", "HOUR", "DAY", "MONTH"):
        return True
    return False


COLON_ACCESS_RE = re.compile(
    r"\b((?:[A-Za-z_]\w*\.)*[A-Za-z_]\w*)"
    r"((?::[A-Za-z_]\w*(?:\[[^\]]*\])?)+(?:\.[A-Za-z_]\w*(?:\[[^\]]*\])?)*)",
    re.IGNORECASE,
)

COLON_BRACKET_RE = re.compile(
    r"\b((?:[A-Za-z_]\w*\.)*[A-Za-z_]\w*):\[([^\]]+)\]",
    re.IGNORECASE,
)

COLON_CAST_RE = re.compile(
    r"CAST\(\s*((?:[A-Za-z_]\w*\.)*[A-Za-z_]\w*)"
    r"((?::[A-Za-z_]\w*(?:\[[^\]]*\])?)+(?:\.[A-Za-z_]\w*(?:\[[^\]]*\])?)*)"
    r"\s+AS\s+(\w+(?:\s*\([^)]*\))?)\s*\)",
    re.IGNORECASE,
)


def _convert_colon_accessor(sql: str) -> str:
    result = sql
    while True:
        m = COLON_CAST_RE.search(result)
        if not m:
            break
        if _skip_string_literals(result, m.start()):
            break
        path = _segments_to_path(m.group(2))
        replacement = (
            f"CAST(GET_JSON_OBJECT({m.group(1)}, '$.{path}')"
            f" AS {m.group(3)})"
        )
        result = result[: m.start()] + replacement + result[m.end() :]

    while True:
        m = COLON_BRACKET_RE.search(result)
        if not m:
            break
        if _skip_string_literals(result, m.start()):
            break
        inner = m.group(2)
        replacement = f"GET_JSON_OBJECT({m.group(1)}, '$[{inner}]')"
        result = result[: m.start()] + replacement + result[m.end() :]

    while True:
        m = COLON_ACCESS_RE.search(result)
        if not m:
            break
        if _skip_string_literals(result, m.start()):
            break
        left = m.group(1)
        segments = m.group(2)
        if _is_url_or_temporal(left, segments):
            break
        path = _segments_to_path(segments)
        replacement = f"GET_JSON_OBJECT({left}, '$.{path}')"
        result = result[: m.start()] + replacement + result[m.end() :]

    return result


def _convert_nvl2(sql: str) -> str:
    result = sql
    pattern = re.compile(r"NVL2\s*\(", re.IGNORECASE)
    while True:
        match = pattern.search(result)
        if not match:
            break
        start = match.start()
        paren_start = match.end() - 1
        close = _find_matching_paren(result, paren_start)
        if close == -1:
            break
        inner = result[paren_start + 1 : close]
        parts = _extract_args(inner)
        if len(parts) >= 3:
            expr, val1, val2 = parts[0], parts[1], ",".join(parts[2:])
            replacement = f"(CASE WHEN {expr} IS NOT NULL THEN {val1} ELSE {val2} END)"
            result = result[:start] + replacement + result[close + 1 :]
        else:
            break
    return result


def _convert_decode(sql: str) -> str:
    result = sql
    pattern = re.compile(r"DECODE\s*\(", re.IGNORECASE)
    while True:
        match = pattern.search(result)
        if not match:
            break
        start = match.start()
        paren_start = match.end() - 1
        close = _find_matching_paren(result, paren_start)
        if close == -1:
            break
        inner = result[paren_start + 1 : close]
        parts = _extract_args(inner)
        if len(parts) >= 3:
            expr = parts[0]
            pairs = parts[1:]
            if len(pairs) % 2 == 1:
                default = pairs[-1]
                pairs = pairs[:-1]
            else:
                default = None
            case_parts = [f"WHEN {pairs[i]} THEN {pairs[i+1]}" for i in range(0, len(pairs), 2)]
            case_sql = f"CASE {expr}\n  " + "\n  ".join(case_parts)
            if default is not None:
                case_sql += f"\n  ELSE {default}"
            case_sql += "\nEND"
            result = result[:start] + case_sql + result[close + 1 :]
        else:
            break
    return result


def _clean_pivot(sql: str) -> str:
    sql = re.sub(
        r"(?i)('(?:[^']*)')\s+AS\s+`\1`",
        r"\1",
        sql,
    )
    return sql


def _apply_function_rules(sql: str) -> str:
    sql = _clean_pivot(sql)
    sql = _convert_colon_accessor(sql)
    sql = _convert_iff(sql)
    sql = _convert_nvl2(sql)
    sql = _convert_decode(sql)
    sql = _convert_qualify(sql)
    sql = _convert_lateral_flatten(sql)
    sql = _convert_flatten(sql)
    sql = re.sub(
        r"(?i)(DESC)\s+NULLS\s+FIRST\b",
        r"\1 NULLS LAST",
        sql,
    )
    sql = _replace_func(sql, "ARRAY_AGG", "COLLECT_LIST({inner})")
    sql = _replace_func(sql, "OBJECT_CONSTRUCT", "NAMED_STRUCT({inner})")
    def _convert_listagg(m: re.Match) -> str:
        col = m.group(1).strip()
        delim = m.group(2).strip()
        order_clause = re.sub(r"(?i)^ORDER\s+BY\s+", "", m.group(3)).strip()
        return f"CONCAT_WS('{delim}', COLLECT_LIST({col} ORDER BY {order_clause}))"

    sql = re.sub(
        r"LISTAGG\s*\(\s*(.*?)\s*,\s*['\"]([^'\"]*)['\"]\s*\)\s*WITHIN\s+GROUP\s*\((.*?)\)",
        _convert_listagg,
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    sql = _replace_func(sql, "ZEROIFNULL", "COALESCE({inner}, 0)")
    sql = _replace_func(sql, "NULLIFZERO", "IF({inner} = 0, NULL, {inner})")
    sql = _replace_func(sql, "TO_VARCHAR", "CAST({inner} AS STRING)")
    sql = _replace_func(sql, "TO_NUMBER", "CAST({inner} AS DECIMAL)")
    sql = _replace_func(sql, "TO_CHAR", "DATE_FORMAT({inner})")
    sql = _replace_func(sql, "MONTHNAME", "DATE_FORMAT({inner}, 'MMM')")
    sql = _replace_func(sql, "DAYNAME", "DATE_FORMAT({inner}, 'EEE')")
    sql = _replace_func(sql, "ARRAY_SIZE", "SIZE({inner})")
    sql = _convert_get(sql)
    sql = _replace_func(sql, "JSON_TYPEOF", "LOWER(TYPEOF({inner}))")
    sql = _replace_func(sql, "CHECK_JSON", "(TRY_PARSE_JSON({inner}) IS NOT NULL)")
    sql = _replace_func(sql, "STRIP_NULL_VALUE", "REGEXP_REPLACE(TO_JSON({inner}), '\"null\"', 'null')")
    sql = _replace_func(sql, "TO_ARRAY", "ARRAY({inner})")
    sql = _replace_func(sql, "NVL", "COALESCE({inner})")
    sql = re.sub(
        r"(?i)RATIO_TO_REPORT\s*\(\s*([^)]+)\)\s*OVER\s*\((.*?)\)",
        r"\1 / SUM(\1) OVER (\2)",
        sql,
    )
    # DATEDIFF(part, start, end) → Databricks equivalent
    _result = sql
    _dateparts = {"YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "WEEK", "QUARTER"}
    while True:
        _m = re.search(r"(?i)\bDATEDIFF\s*\(", _result)
        if not _m:
            break
        _paren_start = _m.end() - 1
        _close = _find_matching_paren(_result, _paren_start)
        if _close == -1:
            break
        _inner = _result[_paren_start + 1:_close]
        _args = _extract_args(_inner)
        if len(_args) == 3:
            _part, _start, _end = _args[0].strip(), _args[1].strip(), _args[2].strip()
            _p = _part.upper()
            if _p not in _dateparts:
                break
            if _p == "YEAR":
                _repl = f"FLOOR(CAST(DATEDIFF({_end}, {_start}) AS DOUBLE) / 365.25)"
            elif _p == "MONTH":
                _repl = f"CAST(MONTHS_BETWEEN({_end}, {_start}) AS INT)"
            elif _p == "DAY":
                _repl = f"DATEDIFF({_end}, {_start})"
            elif _p == "HOUR":
                _repl = f"DATEDIFF({_end}, {_start}) * 24"
            elif _p == "MINUTE":
                _repl = f"DATEDIFF({_end}, {_start}) * 1440"
            elif _p == "SECOND":
                _repl = f"DATEDIFF({_end}, {_start}) * 86400"
            elif _p == "WEEK":
                _repl = f"DATEDIFF({_end}, {_start}) / 7"
            elif _p == "QUARTER":
                _repl = f"DATEDIFF({_end}, {_start}) / 91.25"
            else:
                break
            _result = _result[:_m.start()] + _repl + _result[_close + 1:]
        else:
            # Snowflake 2-arg DATEDIFF doesn't exist; break to avoid infinite loop
            break
    sql = _result
    sql = re.sub(r"(?i)\bMINUS\b", "EXCEPT", sql)
    sql = re.sub(r"RANDOM\s*\(\s*\)", "RAND()", sql, flags=re.IGNORECASE)
    sql = re.sub(
        r"SEQ[1248]\s*\(\s*\)",
        "ROW_NUMBER() OVER (ORDER BY 1)",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"(?i)NEXTVAL\s*\(\s*'([^']+)'\s*\)",
        r"\1.NEXTVAL",
        sql,
    )
    sql = re.sub(
        r"(?i)CURRVAL\s*\(\s*'([^']+)'\s*\)",
        r"\1.CURRVAL",
        sql,
    )
    sql = sql.replace("__TRY_PARSE_JSON__(", "TRY_PARSE_JSON(")
    return sql


def apply_rules(sql: str, object_type: Optional[str] = None) -> str:
    sql = TYPE_PATTERN.sub(_replace_type, sql)
    sql = _strip_varchar_n(sql)
    sql = _strip_unsupported_args(sql)
    # UNIFORM(low, high, seed) → FLOOR(RAND() * (high - low + 1) + low)
    # Handle nested parens in seed arg (e.g. UNIFORM(a, b, RANDOM()))
    # UNIFORM(low, high, seed) → FLOOR(RAND() * (high - low + 1) + low)
    sql = re.sub(
        r"(?i)UNIFORM\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*[^()]*(?:\([^()]*\))?[^()]*\)",
        r"FLOOR(RAND() * ((\2) - (\1) + 1) + \1)",
        sql,
    )
    if object_type == "table":
        sql = _apply_ddl_rules(sql)
    sql = _apply_function_rules(sql)
    return sql
