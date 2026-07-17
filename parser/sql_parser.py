import re
from pathlib import Path
from typing import Optional


class ObjectType:
    SCHEMA = "schema"
    TABLE = "table"
    VIEW = "view"
    PROCEDURE = "procedure"
    FUNCTION = "function"
    EXTERNAL_TABLE = "external_table"
    STAGE = "stage"
    MATERIALIZED_VIEW = "materialized_view"
    SEQUENCE = "sequence"
    PIPE = "pipe"
    TASK = "task"
    STREAM = "stream"
    FILE_FORMAT = "file_format"
    ALERT = "alert"
    MASKING_POLICY = "masking_policy"
    ROW_ACCESS_POLICY = "row_access_policy"
    DYNAMIC_TABLE = "dynamic_table"
    TAG = "tag"
    WAREHOUSE = "warehouse"
    RESOURCE_MONITOR = "resource_monitor"
    NETWORK_POLICY = "network_policy"
    SHARE = "share"
    STORAGE_INTEGRATION = "storage_integration"
    NOTIFICATION_INTEGRATION = "notification_integration"
    SECURITY_INTEGRATION = "security_integration"
    API_INTEGRATION = "api_integration"
    ROLE = "role"
    USER_OBJ = "user"
    GRANT = "grant"
    ICEBERG_TABLE = "iceberg_table"
    UNKNOWN = "unknown"


def _name_group() -> str:
    return r"(\S+)"


OBJECT_PATTERNS = {
    ObjectType.TABLE: re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMPORARY\s+|TEMP\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)",
        re.IGNORECASE,
    ),
    ObjectType.VIEW: re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:SECURE\s+)?(?:TEMPORARY\s+|TEMP\s+)?VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)",
        re.IGNORECASE,
    ),
    ObjectType.PROCEDURE: re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+([^\s(]+)", re.IGNORECASE
    ),
    ObjectType.FUNCTION: re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:SECURE\s+)?FUNCTION\s+([^\s(]+)", re.IGNORECASE
    ),
    ObjectType.SCHEMA: re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?SCHEMA\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)",
        re.IGNORECASE,
    ),
    ObjectType.EXTERNAL_TABLE: re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?EXTERNAL\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)",
        re.IGNORECASE,
    ),
    ObjectType.STAGE: re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMPORARY\s+|TEMP\s+)?STAGE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)",
        re.IGNORECASE,
    ),
    ObjectType.MATERIALIZED_VIEW: re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:SECURE\s+)?MATERIALIZED\s+VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)",
        re.IGNORECASE,
    ),
    ObjectType.SEQUENCE: re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?SEQUENCE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)",
        re.IGNORECASE,
    ),
    ObjectType.PIPE: re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?PIPE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)",
        re.IGNORECASE,
    ),
    ObjectType.TASK: re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?TASK\s+(\S+)",
        re.IGNORECASE,
    ),
    ObjectType.STREAM: re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?STREAM\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)",
        re.IGNORECASE,
    ),
    ObjectType.FILE_FORMAT: re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?FILE\s+FORMAT\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)",
        re.IGNORECASE,
    ),
    ObjectType.ALERT: re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?ALERT\s+(\S+)",
        re.IGNORECASE,
    ),
    ObjectType.MASKING_POLICY: re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?MASKING\s+POLICY\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)",
        re.IGNORECASE,
    ),
    ObjectType.ROW_ACCESS_POLICY: re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?ROW\s+ACCESS\s+POLICY\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)",
        re.IGNORECASE,
    ),
    ObjectType.DYNAMIC_TABLE: re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?DYNAMIC\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)",
        re.IGNORECASE,
    ),
    ObjectType.TAG: re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?TAG\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)",
        re.IGNORECASE,
    ),
    ObjectType.WAREHOUSE: re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?WAREHOUSE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)",
        re.IGNORECASE,
    ),
    ObjectType.RESOURCE_MONITOR: re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?RESOURCE\s+MONITOR\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)",
        re.IGNORECASE,
    ),
    ObjectType.NETWORK_POLICY: re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?NETWORK\s+POLICY\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)",
        re.IGNORECASE,
    ),
    ObjectType.SHARE: re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?SHARE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)",
        re.IGNORECASE,
    ),
    ObjectType.STORAGE_INTEGRATION: re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?STORAGE\s+INTEGRATION\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)",
        re.IGNORECASE,
    ),
    ObjectType.NOTIFICATION_INTEGRATION: re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?NOTIFICATION\s+INTEGRATION\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)",
        re.IGNORECASE,
    ),
    ObjectType.SECURITY_INTEGRATION: re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?SECURITY\s+INTEGRATION\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)",
        re.IGNORECASE,
    ),
    ObjectType.API_INTEGRATION: re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?API\s+INTEGRATION\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)",
        re.IGNORECASE,
    ),
    ObjectType.ROLE: re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?ROLE\s+(\S+)",
        re.IGNORECASE,
    ),
    ObjectType.USER_OBJ: re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?USER\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)",
        re.IGNORECASE,
    ),
    ObjectType.GRANT: re.compile(
        r"(GRANT\s+\S+)",
        re.IGNORECASE,
    ),
    ObjectType.ICEBERG_TABLE: re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?ICEBERG\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)",
        re.IGNORECASE,
    ),
}


def parse_object_name(raw_name: str) -> tuple[Optional[str], Optional[str], str]:
    parts = raw_name.strip().strip('"').split(".")
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    elif len(parts) == 2:
        return parts[0], None, parts[1]
    else:
        return None, None, parts[0]


def identify_object_type(sql: str) -> str:
    for obj_type, pattern in OBJECT_PATTERNS.items():
        if pattern.match(sql):
            return obj_type
    return ObjectType.UNKNOWN


def extract_object_name(sql: str) -> Optional[str]:
    for obj_type, pattern in OBJECT_PATTERNS.items():
        match = pattern.match(sql)
        if match:
            return match.group(1).strip().strip('"')
    return None


def extract_dependencies(sql: str) -> list[str]:
    deps: list[str] = []
    patterns = [
        r"(?:FROM|JOIN|INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+JOIN|CROSS\s+JOIN|LATERAL\s+VIEW\s+EXPLODE)\s+(\S+)",
        r"USING\s+(\S+)",
        r"INSERT\s+(?:INTO|OVERWRITE)\s+(\S+)",
        r"UPDATE\s+(\S+)",
        r"DELETE\s+(?:FROM\s+)?(\S+)",
        r"MERGE\s+INTO\s+(\S+)",
        r"REFERENCES\s+(\S+)",
        r"LIKE\s+(\S+)",
        r"CLONE\s+(\S+)",
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, sql, re.IGNORECASE):
            name = match.group(1).strip().strip('"')
            if name and name not in deps:
                deps.append(name)
    return deps


def extract_cte_names(sql: str) -> list[str]:
    names: list[str] = []
    pattern = re.compile(r"WITH\s+(\w+)\s+AS\s*\(", re.IGNORECASE)
    for match in pattern.finditer(sql):
        names.append(match.group(1))
    return names


def split_sql_statements(sql: str) -> list[str]:
    statements: list[str] = []
    current = ""
    depth = 0
    in_string = False
    string_char = None
    in_dollar = False
    i = 0
    while i < len(sql):
        ch = sql[i]
        if in_dollar:
            current += ch
            if ch == "$" and i + 1 < len(sql) and sql[i + 1] == "$":
                current += "$"
                i += 2
                in_dollar = False
                continue
        elif in_string:
            current += ch
            if ch == string_char and (i == 0 or sql[i - 1] != "\\"):
                in_string = False
        elif (
            ch == "$"
            and i + 1 < len(sql)
            and sql[i + 1] == "$"
        ):
            current += "$$"
            i += 2
            in_dollar = True
            continue
        elif ch in ("'", '"'):
            in_string = True
            string_char = ch
            current += ch
        elif ch == "(":
            depth += 1
            current += ch
        elif ch == ")":
            depth -= 1
            current += ch
        elif ch == ";" and depth == 0 and not in_dollar:
            stripped = current.strip()
            if stripped:
                statements.append(stripped)
            current = ""
        elif ch == "-" and i + 1 < len(sql) and sql[i + 1] == "-":
            end = sql.find("\n", i)
            if end == -1:
                end = len(sql)
            current += sql[i:end]
            i = end - 1
        elif ch == "/" and i + 1 < len(sql) and sql[i + 1] == "*":
            end = sql.find("*/", i + 2)
            if end == -1:
                end = len(sql) - 2
            current += sql[i : end + 2]
            i = end + 1
        else:
            current += ch
        i += 1
    stripped = current.strip()
    if stripped:
        statements.append(stripped)
    return statements


class ParsedObject:
    def __init__(
        self,
        object_type: str,
        name: str,
        schema_name: Optional[str],
        raw_sql: str,
        file_path: Path,
        dependencies: list[str],
        cte_names: list[str],
    ):
        self.object_type = object_type
        self.name = name
        self.schema_name = schema_name
        self.raw_sql = raw_sql
        self.file_path = file_path
        self.dependencies = dependencies
        self.cte_names = cte_names
        self.converted_sql: Optional[str] = None
        self.validation_result: Optional[str] = None

    def __repr__(self):
        return (
            f"ParsedObject({self.object_type}, {self.name}, schema={self.schema_name})"
        )


def parse_sql_content(content: str, file_path: Optional[Path] = None) -> list[ParsedObject]:
    statements = split_sql_statements(content)
    objects: list[ParsedObject] = []

    for stmt in statements:
        if not stmt.strip():
            continue
        obj_type = identify_object_type(stmt)
        name = extract_object_name(stmt)
        if name is None:
            continue
        schema, _, obj_name = parse_object_name(name)
        deps = extract_dependencies(stmt)
        cte_names = extract_cte_names(stmt)
        obj = ParsedObject(
            object_type=obj_type,
            name=obj_name or name,
            schema_name=schema,
            raw_sql=stmt,
            file_path=file_path or Path("<memory>"),
            dependencies=deps,
            cte_names=cte_names,
        )
        objects.append(obj)

    return objects


def parse_sql_file(file_path: Path) -> list[ParsedObject]:
    content = file_path.read_text(encoding="utf-8")
    return parse_sql_content(content, file_path)
