import re
from dataclasses import dataclass, field
from parser.sql_parser import ParsedObject


@dataclass
class SemiStructuredResult:
    object_name: str
    object_type: str
    variant_columns_found: int = 0
    functions_converted: list[str] = field(default_factory=list)
    strategy: str = "native"
    warnings: list[str] = field(default_factory=list)


class SemiStructuredAgent:

    SEMI_STRUCT_FUNCTION_MAP = {
        "JSON_TYPEOF": "_convert_json_typeof",
        "CHECK_JSON": "_convert_check_json",
        "STRIP_NULL_VALUE": "_convert_strip_null_value",
        "OBJECT_DELETE": "_convert_object_delete",
        "OBJECT_PICK": "_convert_object_pick",
        "TO_ARRAY": "_convert_to_array",
        "TO_OBJECT": "_convert_to_object",
        "TYPEOF": "_convert_typeof",
    }

    def convert(
        self, inventory, strategy: str = "native"
    ) -> list[SemiStructuredResult]:
        results = []
        for obj in inventory.all_objects:
            if not obj.converted_sql and not obj.raw_sql:
                continue
            result = self._convert_object(obj, strategy)
            if result.variant_columns_found > 0 or result.functions_converted:
                results.append(result)
        return results

    def _convert_object(
        self, obj: ParsedObject, strategy: str
    ) -> SemiStructuredResult:
        result = SemiStructuredResult(
            object_name=obj.name,
            object_type=obj.object_type,
            strategy=strategy,
        )
        sql = obj.converted_sql if obj.converted_sql else obj.raw_sql
        if not sql:
            return result

        result.variant_columns_found = self._count_variant_columns(sql)
        sql = self._convert_functions(sql, result)
        obj.converted_sql = sql
        return result

    def _count_variant_columns(self, sql: str) -> int:
        count = 0
        for m in re.finditer(r"\bVARIANT\b", sql, re.IGNORECASE):
            context = sql[max(0, m.start() - 50) : m.start()].upper()
            if "CREATE" in context or "COLUMN" in context or "," in context:
                count += 1
        return count

    def _convert_functions(self, sql: str, result: SemiStructuredResult) -> str:
        for func_name, method_name in self.SEMI_STRUCT_FUNCTION_MAP.items():
            if re.search(rf"\b{func_name}\s*\(", sql, re.IGNORECASE):
                converter = getattr(self, method_name)
                new_sql = converter(sql)
                if new_sql != sql:
                    result.functions_converted.append(func_name)
                    sql = new_sql
        return sql

    def _convert_json_typeof(self, sql: str) -> str:
        pattern = re.compile(
            r"\bJSON_TYPEOF\s*\(\s*([^)]+)\s*\)", re.IGNORECASE
        )
        return pattern.sub(r"LOWER(TYPEOF(\1))", sql)

    def _convert_check_json(self, sql: str) -> str:
        pattern = re.compile(
            r"\bCHECK_JSON\s*\(\s*([^)]+)\s*\)", re.IGNORECASE
        )
        return pattern.sub(r"(TRY_PARSE_JSON(\1) IS NOT NULL)", sql)

    def _convert_strip_null_value(self, sql: str) -> str:
        pattern = re.compile(
            r"\bSTRIP_NULL_VALUE\s*\(\s*([^)]+)\s*\)", re.IGNORECASE
        )
        return pattern.sub(
            r"REGEXP_REPLACE(TO_JSON(\1), '\"null\"', 'null')", sql
        )

    def _convert_object_delete(self, sql: str) -> str:
        pattern = re.compile(
            r"\bOBJECT_DELETE\s*\(\s*([^,]+)\s*,\s*['\"](\w+)['\"]\s*\)",
            re.IGNORECASE,
        )
        result = sql
        seen = set()
        for m in pattern.finditer(sql):
            obj_ref = m.group(1).strip()
            key = m.group(2)
            if key in seen:
                continue
            seen.add(key)
            replacement = f"DROP({obj_ref}, '{key}')"
            result = result[: m.start()] + replacement + result[m.end() :]
        if result != sql:
            result += (
                "\n-- NOTE: OBJECT_DELETE converted to DROP(). "
                "Verify DROP() is available in your Databricks runtime."
            )
        return result

    def _convert_object_pick(self, sql: str) -> str:
        pattern = re.compile(
            r"\bOBJECT_PICK\s*\(\s*([^,]+)\s*,\s*((?:['\"]\w+['\"]\s*,?\s*)+)\)",
            re.IGNORECASE,
        )

        def _replace_pick(m: re.Match) -> str:
            obj_ref = m.group(1).strip()
            keys_str = m.group(2)
            keys = re.findall(r"['\"](\w+)['\"]", keys_str)
            if not keys:
                return m.group(0)
            struct_parts = []
            for k in keys:
                struct_parts.append(f"'{k}', {obj_ref}.{k}")
            return f"NAMED_STRUCT({', '.join(struct_parts)})"

        return pattern.sub(_replace_pick, sql)

    def _convert_to_array(self, sql: str) -> str:
        pattern = re.compile(
            r"\bTO_ARRAY\s*\(\s*([^)]+)\s*\)", re.IGNORECASE
        )
        return pattern.sub(r"ARRAY(\1)", sql)

    def _convert_to_object(self, sql: str) -> str:
        return sql

    def _convert_typeof(self, sql: str) -> str:
        pattern = re.compile(
            r"\bTYPEOF\s*\(\s*([^)]+)\s*\)", re.IGNORECASE
        )
        return pattern.sub(r"TYPEOF(\1)", sql)
