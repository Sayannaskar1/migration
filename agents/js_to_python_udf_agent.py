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

        # Try rule-based translation first for simple constructs
        rule_based = self._rule_based_translate(js_body)
        if rule_based is not None:
            result.success = True
            result.strategy = "rule_based"
            converted = self._generate_databricks_ddl(raw_sql, rule_based, obj.object_type, obj.name)
            if converted:
                result.converted_sql = converted
                obj.converted_sql = converted
            else:
                result.error = "Failed to generate Databricks DDL"
            return result

        # Fall back to LLM for complex constructs
        python_body = self._call_llm(js_body, llm_config)
        if not python_body:
            result.error = "LLM conversion failed — no provider configured or call failed"
            fallback = self._generate_fallback(raw_sql, obj.object_type, obj.name)
            result.converted_sql = fallback
            obj.converted_sql = fallback
            return result

        python_body = self._clean_llm_output(python_body)
        converted = self._generate_databricks_ddl(raw_sql, python_body, obj.object_type, obj.name)
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

    # ── Rule-based JS → Python translator ──

    # Constructs that require LLM (not translatable deterministically)
    _COMPLEX_PATTERNS = [
        r"\bsnowflake\b",          # Snowflake APIs
        r"\beval\s*\(",            # eval()
        r"\bnew\s+Date\b",         # Date objects
        r"\.prototype\b",          # prototype chain
        r"\barguments\b",          # arguments object
        r"\bclosures?\b",          # closures
        r"\bcatch\s*\(",           # try/catch
        r"\bfinally\s*\{",         # try/finally
        r"\basync\b",              # async/await
        r"\bawait\b",              # async/await
        r"\bclass\s+\w+",          # class declarations
        r"\bimport\s+",            # ES6 imports
        r"\bexport\s+",            # ES6 exports
        r"\bfor\s*\(\s*var\s+\w+\s+of\b",  # for...of
        r"\bfor\s*\(\s*var\s+\w+\s+in\b",  # for...in
        r"\bArray\.isArray\b",     # Array.isArray
        r"\bObject\.\w+",          # Object.keys/values/etc
        r"\bJSON\.parse\b",        # JSON.parse (complex)
        r"\bJSON\.stringify\b",    # JSON.stringify (complex)
        r"\bMap\s*\(",             # Map constructor
        r"\bSet\s*\(",             # Set constructor
        r"\bPromise\b",            # Promises
        r"\bthen\s*\(",            # Promise chains
        r"\bcatch\s*\(",           # Promise chains
    ]

    def _is_simple_js(self, js_body: str) -> bool:
        for pat in self._COMPLEX_PATTERNS:
            if re.search(pat, js_body, re.IGNORECASE):
                return False
        return True

    def _rule_based_translate(self, js_body: str) -> str | None:
        """Attempt deterministic JS→Python translation. Returns None if too complex."""
        if not self._is_simple_js(js_body):
            return None

        body = js_body.strip()

        # Strip trailing semicolons
        body = re.sub(r";\s*$", "", body, flags=re.MULTILINE)

        # var/let/const → plain assignment
        body = re.sub(r"\b(?:var|let|const)\s+", "", body)

        # Null/undefined checks
        body = re.sub(r"(!\s*(\w+))\s*(?=\)|\?|:|&&|\|\||$)", r"\2 is None", body)
        body = re.sub(r"(\w+)\s*===?\s*null\b", r"\1 is None", body)
        body = re.sub(r"(\w+)\s*!==?\s*null\b", r"\1 is not None", body)
        body = re.sub(r"(\w+)\s*===?\s*undefined\b", r"\1 is None", body)
        body = re.sub(r"(\w+)\s*!==?\s*undefined\b", r"\1 is not None", body)

        # Equality operators
        body = re.sub(r"===", "==", body)
        body = re.sub(r"!==", "!=", body)

        # Logical operators
        body = re.sub(r"\&\&", "and", body)
        body = re.sub(r"\|\|", "or", body)
        body = re.sub(r"\!\s*(?=\w)", "not ", body)

        # String methods
        body = re.sub(r"\.toLowerCase\(\)", ".lower()", body)
        body = re.sub(r"\.toUpperCase\(\)", ".upper()", body)
        body = re.sub(r"\.trim\(\)", ".strip()", body)
        body = re.sub(r"\.length\b", ".__len__()", body)
        body = re.sub(r"\.substring\((\d+),\s*(\d+)\)", r"[\1:\2]", body)
        body = re.sub(r"\.indexOf\(([^)]+)\)", r".find(\1)", body)
        body = re.sub(r"\.charAt\((\d+)\)", r"[\1]", body)
        body = re.sub(r"\.slice\(([^)]+)\)", r"[\1]", body)
        body = re.sub(r"\.includes\(([^)]+)\)", r" in \1", body)
        body = re.sub(r"\.replace\(([^,]+),\s*([^)]+)\)", r".replace(\1, \2)", body)
        body = re.sub(r"\.split\(([^)]+)\)", r".split(\1)", body)
        body = re.sub(r"\.join\(([^)]+)\)", r".join(\1)", body)
        body = re.sub(r"\.push\(([^)]+)\)", r".append(\1)", body)

        # Math methods
        body = re.sub(r"Math\.abs\(", "abs(", body)
        body = re.sub(r"Math\.ceil\(", "math.ceil(", body)
        body = re.sub(r"Math\.floor\(", "math.floor(", body)
        body = re.sub(r"Math\.round\(", "round(", body)
        body = re.sub(r"Math\.max\(", "max(", body)
        body = re.sub(r"Math\.min\(", "min(", body)
        body = re.sub(r"Math\.pow\(", "pow(", body)
        body = re.sub(r"Math\.sqrt\(", "math.sqrt(", body)

        # parseInt/parseFloat
        body = re.sub(r"\bparseInt\(([^)]+)\)", r"int(\1)", body)
        body = re.sub(r"\bparseFloat\(([^)]+)\)", r"float(\1)", body)
        body = re.sub(r"\bString\(([^)]+)\)", r"str(\1)", body)

        # Ternary: x ? y : z → y if x else z
        body = re.sub(
            r"(\w+(?:\s+\w+)*)\s*\?\s*([^:]+?)\s*:\s*(.+)",
            r"\2 if \1 else \3",
            body,
        )

        # if/else if/else → if/elif/else
        body = re.sub(r"\}?\s*else\s+if\s*\(", "\nelif ", body)
        body = re.sub(r"\}?\s*else\s*\{", "\nelse:", body)
        body = re.sub(r"\bif\s*\(", "if ", body)

        # Strip remaining braces and parentheses from control flow
        body = re.sub(r"\)\s*\{", ":", body)
        body = re.sub(r"^\s*\}\s*$", "", body, flags=re.MULTILINE)

        # return → return
        body = re.sub(r"\breturn\s+", "return ", body)

        # String concatenation: 'a' || 'b' → 'a' + 'b' (already handled by || → or above)
        # But we need to handle string || concat differently
        # Undo the || → or for string contexts if needed

        # Clean up
        body = re.sub(r"\n{3,}", "\n\n", body)
        body = body.strip()

        # Validate: ensure we have actual Python code
        if not body or body == js_body.strip():
            return None

        return body

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

    def _ensure_imports(self, body: str) -> str:
        module_imports = {
            "math.": "import math",
            "json.": "import json",
            "datetime.": "import datetime",
            "re.": "import re",
            "random.": "import random",
            "collections.": "import collections",
            "itertools.": "import itertools",
            "functools.": "import functools",
        }
        added = set()
        lines = body.split("\n")
        import_lines = [l for l in lines if l.strip().startswith("import ")]
        existing_imports = "\n".join(import_lines).lower()

        for marker, imp in module_imports.items():
            imp_name = imp.split()[-1]
            if marker in body.lower() and imp_name not in existing_imports:
                added.add(imp)

        if not added:
            return body

        new_lines = []
        for imp in sorted(added):
            new_lines.append(imp)
        if new_lines:
            new_lines.append("")
        new_lines.append(body)
        return "\n".join(new_lines)

    def _clean_llm_output(self, text: str) -> str:
        text = text.strip()
        text = re.sub(r"^```python\s*\n?", "", text)
        text = re.sub(r"^```\s*\n?", "", text)
        text = re.sub(r"\n?```\s*$", "", text)
        text = text.strip()
        # Remove def wrapper if LLM included it (not needed in Databricks $$ body)
        text = re.sub(r"^def\s+\w+\s*\([^)]*\):\s*\n?", "", text)
        # Remove str() wrapping around return values (LLMs often add it unnecessarily)
        text = re.sub(r"(?i)\breturn\s+str\s*\(", "return ", text)
        # Clean up extra closing paren left by str(...) removal
        # Only reduce 3+ consecutive closing parens at end to remove the extra
        text = re.sub(r"\){3,}\s*$", "))", text.strip())
        # Dedent the body
        text = re.sub(r"(?m)^    ", "", text)
        text = re.sub(r"(?m)^  ", "", text)
        return text.strip()

    def _generate_databricks_ddl(self, raw_sql: str, python_body: str, obj_type: str, obj_name: str | None = None) -> str | None:
        if obj_type == "function":
            return self._generate_function_ddl(raw_sql, python_body, obj_name)
        elif obj_type == "procedure":
            return self._generate_procedure_ddl(raw_sql, python_body, obj_name)
        return None

    def _generate_function_ddl(self, raw_sql: str, python_body: str, obj_name: str | None = None) -> str:
        if obj_name:
            name = obj_name
        else:
            name_match = re.search(
                r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:SECURE\s+)?FUNCTION\s+([\w.]+)",
                raw_sql, re.IGNORECASE,
            )
            name = name_match.group(1) if name_match else "unknown_func"

        params = self._extract_params(raw_sql)
        returns = self._extract_returns(raw_sql)
        python_body = self._ensure_imports(python_body)

        ddl = f"CREATE OR REPLACE FUNCTION {name}({params})\n"
        if returns:
            ddl += f"RETURNS {returns}\n"
        ddl += "LANGUAGE PYTHON\n"
        ddl += "AS $$\n"
        ddl += python_body + "\n"
        ddl += "$$"
        return ddl

    def _generate_procedure_ddl(self, raw_sql: str, python_body: str, obj_name: str | None = None) -> str:
        if obj_name:
            name = obj_name
        else:
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
        # Try the standard Snowflake DDL pattern first
        match = re.search(
            r"(?:FUNCTION|PROCEDURE)\s+[\w.]+\s*\(",
            sql, re.IGNORECASE,
        )
        if not match:
            # Fallback: look for any parenthesized param list after CREATE FUNCTION/PROCEDURE
            # Use [^\s(]+ to stop at '(' — \S+ would gobble "name"("first_param
            match = re.search(
                r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:SECURE\s+)?(?:FUNCTION|PROCEDURE)\s+[^\s(]+\s*",
                sql, re.IGNORECASE,
            )
            if match:
                after_name = sql[match.end():].strip()
                if after_name.startswith("("):
                    match = type("_", (), {"end": lambda: match.end()})()
                    match.end = lambda: match.end()
                    # recreate as a real match
                    match = re.search(
                        r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:SECURE\s+)?(?:FUNCTION|PROCEDURE)\s+[^\s(]+\s*\(",
                        sql, re.IGNORECASE,
                    )
            if not match:
                return ""
        paren_start = match.end() - 1
        depth = 1
        i = paren_start + 1
        while i < len(sql) and depth > 0:
            if sql[i] == "(":
                depth += 1
            elif sql[i] == ")":
                depth -= 1
            i += 1
        params_raw = sql[paren_start + 1 : i - 1].strip()
        if not params_raw:
            return ""
        params = []
        depth = 0
        current = ""
        for ch in params_raw:
            if ch in ("(",):
                depth += 1
                current += ch
            elif ch in (")",):
                depth -= 1
                current += ch
            elif ch == "," and depth == 0:
                params.append(current.strip())
                current = ""
            else:
                current += ch
        if current.strip():
            params.append(current.strip())
        type_map_params = {
            "NUMBER": "DOUBLE",
            "FLOAT": "DOUBLE",
            "FLOAT4": "DOUBLE",
            "FLOAT8": "DOUBLE",
            "DOUBLE": "DOUBLE",
            "DOUBLE_PRECISION": "DOUBLE",
            "REAL": "DOUBLE",
            "VARCHAR": "STRING",
            "STRING": "STRING",
            "TEXT": "STRING",
            "INT": "INT",
            "INTEGER": "INT",
            "BIGINT": "BIGINT",
            "SMALLINT": "INT",
            "TINYINT": "INT",
            "BOOLEAN": "BOOLEAN",
            "DATE": "DATE",
            "TIMESTAMP": "TIMESTAMP",
            "TIMESTAMP_NTZ": "TIMESTAMP",
            "TIMESTAMP_LTZ": "TIMESTAMP",
            "TIMESTAMP_TZ": "TIMESTAMP",
            "BINARY": "BINARY",
            "VARBINARY": "BINARY",
            "ARRAY": "ARRAY<STRING>",
            "OBJECT": "MAP<STRING, STRING>",
            "VARIANT": "STRING",
        }
        converted = []
        for param in params:
            param = param.strip()
            for src, dst in type_map_params.items():
                param = re.sub(
                    rf"\b{src}\b(?:\s*\([^)]*\))?",
                    dst,
                    param,
                    flags=re.IGNORECASE,
                )
            converted.append(param)
        return ", ".join(converted)

    def _extract_returns(self, sql: str) -> str:
        match = re.search(r"\bRETURNS\s+(\w+)", sql, re.IGNORECASE)
        if not match:
            return ""
        ret_type = match.group(1).upper()
        type_map = {
            "NUMBER": "DOUBLE",
            "FLOAT": "DOUBLE",
            "FLOAT4": "DOUBLE",
            "FLOAT8": "DOUBLE",
            "DOUBLE": "DOUBLE",
            "DOUBLE_PRECISION": "DOUBLE",
            "REAL": "DOUBLE",
            "INT": "INT",
            "INTEGER": "INT",
            "BIGINT": "BIGINT",
            "SMALLINT": "INT",
            "TINYINT": "INT",
            "BYTEINT": "INT",
            "VARCHAR": "STRING",
            "STRING": "STRING",
            "TEXT": "STRING",
            "CHAR": "STRING",
            "CHARACTER": "STRING",
            "NVARCHAR": "STRING",
            "NCHAR": "STRING",
            "BOOLEAN": "BOOLEAN",
            "DATE": "DATE",
            "TIMESTAMP": "TIMESTAMP",
            "TIMESTAMP_NTZ": "TIMESTAMP",
            "TIMESTAMP_LTZ": "TIMESTAMP",
            "TIMESTAMP_TZ": "TIMESTAMP",
            "BINARY": "BINARY",
            "VARBINARY": "BINARY",
        }
        return type_map.get(ret_type, "DOUBLE")

    def _generate_fallback(self, raw_sql: str, obj_type: str, obj_name: str | None = None) -> str:
        if obj_name:
            name = obj_name
        else:
            name_match = re.search(
                r"(?:FUNCTION|PROCEDURE)\s+([\w.]+)",
                raw_sql, re.IGNORECASE,
            )
            name = name_match.group(1) if name_match else "unknown"
        keyword = "FUNCTION" if obj_type == "function" else "PROCEDURE"

        params = self._extract_params(raw_sql)
        returns = self._extract_returns(raw_sql)
        return (
            f"CREATE OR REPLACE {keyword} {name}({params})\n"
            f"RETURNS {returns}\n"
            f"LANGUAGE PYTHON\n"
            f"AS $$\n"
            f"# TODO: Convert JavaScript body to Python\n"
            f"pass\n"
            f"$$\n\n"
            f"-- ARCHITECTURAL CHANGE: JavaScript body requires manual conversion to Python\n"
            f"-- MANUAL REVIEW REQUIRED"
        )
