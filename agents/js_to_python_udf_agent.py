import re
import logging
from dataclasses import dataclass, field
from parser.sql_parser import ParsedObject

logger = logging.getLogger(__name__)

JS_TO_PYTHON_PROMPT = """You are an expert at converting Snowflake JavaScript UDFs and stored procedures to Databricks Python UDFs/stored procedures.

Convert the given Snowflake JavaScript function/procedure to a Databricks Python equivalent.

Mapping rules:
1. snowflake.execute({ sqlText: 'SELECT ...' }) -> spark.sql('SELECT ...')
2. snowflake.execute({ sqlText: '...', binds: [v1, v2] }) -> spark.sql('...').bind([v1, v2])
3. result.getColumnValue(1) -> row[0] (0-indexed)
4. result.getColumnValue('NAME') -> row['NAME'] or row.NAME
5. result.next() -> for row in result: (iterate over rows)
6. result.getRowCount() -> len(rows)
7. var result = snowflake.execute(...) -> rows = spark.sql(...).collect()
8. return value -> return value (same in Python)
9. function body -> def function_name(args): body
10. LET/DECLARE variables -> plain Python variables
11. IF/ELSEIF/ELSE -> if/elif/else
12. WHILE loops -> while loops
13. FOR loops -> for loops
14. String concatenation with || -> f-strings or + operator
15. NUMBER type -> float or int
16. VARCHAR/STRING type -> str
17. BOOLEAN type -> bool
18. ARRAY type -> list
19. OBJECT type -> dict
20. NULL checks: x == null -> x is None; x != null -> x is not None

For UDFs (functions):
- Return a Python function body that can be used in CREATE FUNCTION ... LANGUAGE PYTHON AS $$ ... $$
- The function should accept SQL types as parameters and return the correct type

For stored procedures:
- Return a Python script body that can be used in CREATE PROCEDURE ... LANGUAGE PYTHON AS $$ ... $$
- Use spark.sql() for SQL operations
- Use spark.catalog.tableExists() for table existence checks

Return ONLY the Python code with no explanation, no markdown, no code fences."""


@dataclass
class JSConversionResult:
    object_name: str
    object_type: str
    success: bool = False
    original_language: str = "JAVASCRIPT"
    converted_language: str = "PYTHON"
    converted_sql: str | None = None
    error: str | None = None
    strategy: str = "llm"


class JSPythonUDFAgent:

    def convert(self, inventory, llm_config: dict | None = None) -> list[JSConversionResult]:
        results = []
        for obj in inventory.all_objects:
            if not self._is_javascript(obj):
                continue
            result = self._convert_object(obj, llm_config)
            results.append(result)
        return results

    def _is_javascript(self, obj: ParsedObject) -> bool:
        sql = obj.raw_sql or ""
        return bool(re.search(r"\bLANGUAGE\s+JAVASCRIPT\b", sql, re.IGNORECASE))

    def _convert_object(self, obj: ParsedObject, llm_config: dict | None) -> JSConversionResult:
        result = JSConversionResult(
            object_name=obj.name,
            object_type=obj.object_type,
        )
        raw_sql = obj.raw_sql or ""
        js_body = self._extract_js_body(raw_sql)
        if not js_body:
            result.error = "Could not extract JavaScript body from DDL"
            return result

        python_body = self._call_llm(js_body, llm_config)
        if not python_body:
            result.error = "LLM conversion failed — no provider configured or call failed"
            fallback = self._generate_fallback(raw_sql, obj.object_type)
            result.converted_sql = fallback
            obj.converted_sql = fallback
            return result

        python_body = self._clean_llm_output(python_body)
        converted = self._generate_databricks_ddl(raw_sql, python_body, obj.object_type)
        if converted:
            result.success = True
            result.converted_sql = converted
            obj.converted_sql = converted
        else:
            result.error = "Failed to generate Databricks DDL"
        return result

    def _extract_js_body(self, sql: str) -> str | None:
        match = re.search(r"AS\s*\$\$\s*\n?(.*?)\n?\s*\$\$", sql, re.DOTALL)
        if match:
            return match.group(1).strip()
        match = re.search(r"AS\s+'(.*)'\s*$", sql, re.DOTALL)
        if match:
            body = match.group(1)
            body = body.replace("\\'", "'").replace("\\n", "\n")
            return body.strip()
        return None

    def _call_llm(self, js_body: str, llm_config: dict | None) -> str | None:
        from agents.llm_transpiler import _get_llm_config, _call_openai, _call_anthropic, _call_gemini

        cfg = llm_config or _get_llm_config()
        provider = cfg.get("provider") or ""
        if not provider:
            return None

        prompt = f"Convert this Snowflake JavaScript to Databricks Python:\n\n{js_body}"
        try:
            if provider == "openai":
                return _call_openai(JS_TO_PYTHON_PROMPT, prompt, cfg)
            elif provider == "anthropic":
                return _call_anthropic(JS_TO_PYTHON_PROMPT, prompt, cfg)
            elif provider == "gemini":
                return _call_gemini(JS_TO_PYTHON_PROMPT, prompt, cfg)
            else:
                logger.warning(f"Unsupported LLM provider for JS conversion: {provider}")
                return None
        except Exception as e:
            logger.warning(f"LLM call failed for JS conversion: {e}")
            return None

    def _clean_llm_output(self, text: str) -> str:
        text = text.strip()
        text = re.sub(r"^```python\s*\n?", "", text)
        text = re.sub(r"^```\s*\n?", "", text)
        text = re.sub(r"\n?```\s*$", "", text)
        return text.strip()

    def _generate_databricks_ddl(self, raw_sql: str, python_body: str, obj_type: str) -> str | None:
        if obj_type == "function":
            return self._generate_function_ddl(raw_sql, python_body)
        elif obj_type == "procedure":
            return self._generate_procedure_ddl(raw_sql, python_body)
        return None

    def _generate_function_ddl(self, raw_sql: str, python_body: str) -> str:
        name_match = re.search(
            r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:SECURE\s+)?FUNCTION\s+([\w.]+)",
            raw_sql, re.IGNORECASE,
        )
        name = name_match.group(1) if name_match else "unknown_func"

        params = self._extract_params(raw_sql)
        returns = self._extract_returns(raw_sql)

        ddl = f"CREATE OR REPLACE FUNCTION {name}({params})\n"
        if returns:
            ddl += f"RETURNS {returns}\n"
        ddl += "LANGUAGE PYTHON\n"
        ddl += "AS $$\n"
        ddl += python_body + "\n"
        ddl += "$$"
        return ddl

    def _generate_procedure_ddl(self, raw_sql: str, python_body: str) -> str:
        name_match = re.search(
            r"CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+([\w.]+)",
            raw_sql, re.IGNORECASE,
        )
        name = name_match.group(1) if name_match else "unknown_proc"

        params = self._extract_params(raw_sql)

        ddl = f"CREATE OR REPLACE PROCEDURE {name}({params})\n"
        ddl += "LANGUAGE PYTHON\n"
        ddl += "SQL SECURITY INVOKER\n"
        ddl += "AS $$\n"
        ddl += python_body + "\n"
        ddl += "$$"
        return ddl

    def _extract_params(self, sql: str) -> str:
        match = re.search(
            r"(?:FUNCTION|PROCEDURE)\s+\w+\s*\(([^)]*)\)",
            sql, re.IGNORECASE,
        )
        if not match:
            return ""
        params_raw = match.group(1).strip()
        if not params_raw:
            return ""
        params = []
        for param in params_raw.split(","):
            param = param.strip()
            param = re.sub(r"\bNUMBER\b", "DOUBLE", param, flags=re.IGNORECASE)
            param = re.sub(r"\bVARCHAR\b", "STRING", param, flags=re.IGNORECASE)
            param = re.sub(r"\bINT\b", "INT", param, flags=re.IGNORECASE)
            param = re.sub(r"\bINTEGER\b", "INT", param, flags=re.IGNORECASE)
            param = re.sub(r"\bBOOLEAN\b", "BOOLEAN", param, flags=re.IGNORECASE)
            param = re.sub(r"\bDATE\b", "DATE", param, flags=re.IGNORECASE)
            param = re.sub(r"\bTIMESTAMP\b", "TIMESTAMP", param, flags=re.IGNORECASE)
            params.append(param)
        return ", ".join(params)

    def _extract_returns(self, sql: str) -> str:
        match = re.search(r"\bRETURNS\s+(\w+)", sql, re.IGNORECASE)
        if not match:
            return ""
        ret_type = match.group(1).upper()
        type_map = {
            "NUMBER": "DOUBLE",
            "VARCHAR": "STRING",
            "STRING": "STRING",
            "INT": "INT",
            "INTEGER": "INT",
            "BOOLEAN": "BOOLEAN",
            "DATE": "DATE",
            "TIMESTAMP": "TIMESTAMP",
        }
        return type_map.get(ret_type, "STRING")

    def _generate_fallback(self, raw_sql: str, obj_type: str) -> str:
        name_match = re.search(
            r"(?:FUNCTION|PROCEDURE)\s+([\w.]+)",
            raw_sql, re.IGNORECASE,
        )
        name = name_match.group(1) if name_match else "unknown"
        keyword = "FUNCTION" if obj_type == "function" else "PROCEDURE"

        return (
            f"CREATE OR REPLACE {keyword} {name}()\n"
            f"LANGUAGE PYTHON\n"
            f"AS $$\n"
            f"# TODO: Convert JavaScript body to Python\n"
            f"pass\n"
            f"$$\n\n"
            f"-- ARCHITECTURAL CHANGE: JavaScript body requires manual conversion to Python\n"
            f"-- MANUAL REVIEW REQUIRED"
        )
