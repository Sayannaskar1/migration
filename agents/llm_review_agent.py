import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from agents.project_loader import ProjectInventory, ParsedObject
from agents.validation_agent import ValidationResult

logger = logging.getLogger(__name__)

_LLM_AVAILABLE: Optional[bool] = None

REVIEW_SYSTEM_PROMPT = """You are an expert Snowflake-to-Databricks SQL migration reviewer.

Review the converted SQL for correctness, completeness, and Databricks compatibility.

Key rules to check:
1. Data types: NUMBER->DECIMAL, VARCHAR/CHAR->STRING, VARIANT->STRING, OBJECT->STRING, etc.
2. Functions: IFF->CASE WHEN, ARRAY_AGG->COLLECT_LIST, OBJECT_CONSTRUCT->NAMED_STRUCT, etc.
3. Syntax: QUALIFY->subquery, LATERAL FLATTEN->LATERAL VIEW EXPLODE, CONNECT BY->recursive CTE
4. DDL: Add USING DELTA, CLUSTER BY->ZORDER BY, IDENTITY->GENERATED ALWAYS AS IDENTITY
5. Multi-statement procedures need DECLARE/BEGIN/END/EXCEPTION mapping to Databricks scripting
6. JavaScript UDFs/procedures are NOT supported — must be rewritten in Python or Scala
7. CLONE has no Databricks equivalent — use SHALLOW CLONE or CREATE TABLE AS SELECT

Return a JSON object with exactly these keys:
- "review_notes": list of strings noting issues found (empty list if clean)
- "suggested_fixes": list of specific fix descriptions (empty list if none needed)
- "improved_sql": the full corrected Databricks SQL string, or null if no changes needed
- "confidence_adjustment": a number between -0.5 and 0.0 (0.0 if already correct, negative if issues remain)

Example response:
{
  "review_notes": ["VARCHAR(255) should be STRING", "IFF should use CASE WHEN"],
  "suggested_fixes": ["Replace VARCHAR(255) with STRING", "Rewrite IFF(...) as CASE WHEN ... END"],
  "improved_sql": "SELECT CASE WHEN ... END AS col FROM t",
  "confidence_adjustment": -0.2
}
"""


@dataclass
class LLMReviewResult:
    obj_name: str
    object_type: str
    original_sql: str = ""
    converted_sql: str = ""
    status: str = ""
    confidence_before: float = 0.0
    confidence_after: float = 0.0
    review_notes: list[str] = field(default_factory=list)
    suggested_fixes: list[str] = field(default_factory=list)
    improved_sql: Optional[str] = None
    needs_attention: bool = True
    reviewed_at: Optional[str] = None
    error: Optional[str] = None


def _should_review(obj: ParsedObject, result: Optional[ValidationResult]) -> bool:
    if not result:
        return False
    if result.status == "ERROR":
        return False
    low_confidence = result.confidence < 0.7
    is_architectural = result.status == "ARCHITECTURAL CHANGE"
    has_warnings = bool(result.warnings)
    if low_confidence or is_architectural or has_warnings:
        return True
    if "LANGUAGE JAVASCRIPT" in obj.raw_sql.upper():
        return True
    if obj.object_type == "procedure":
        return True
    return False


def _build_review_prompt(
    obj: ParsedObject,
    result: ValidationResult,
) -> str:
    lines = [
        f"Object: {obj.name} ({obj.object_type})",
        f"Status: {result.status}",
        f"Confidence: {result.confidence:.2f}",
        "",
        "--- Original Snowflake SQL ---",
        obj.raw_sql or "(empty)",
        "",
        "--- Current Converted Databricks SQL ---",
        obj.converted_sql or "(empty)",
        "",
        "--- Validation Issues ---",
    ]
    if result.errors:
        lines.append("Errors:")
        for e in result.errors:
            lines.append(f"  - {e}")
    if result.issues:
        lines.append("Issues:")
        for i in result.issues:
            lines.append(f"  - {i}")
    if result.warnings:
        lines.append("Warnings:")
        for w in result.warnings:
            lines.append(f"  - {w}")
    if not result.errors and not result.issues and not result.warnings:
        lines.append("  (none)")

    lines.append("")
    lines.append("Review the conversion and return a JSON object with review_notes, suggested_fixes, improved_sql, and confidence_adjustment.")
    return "\n".join(lines)


def _call_review_llm(prompt: str) -> Optional[dict]:
    global _LLM_AVAILABLE
    provider = os.environ.get("LLM_PROVIDER", "").lower()
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("LLM_API_KEY", "")

    if not provider:
        if _LLM_AVAILABLE is None:
            logger.info("LLM review: no provider configured, skipping")
            _LLM_AVAILABLE = False
        return None
    if provider != "gemini":
        if _LLM_AVAILABLE is None:
            logger.info(f"LLM review: provider '{provider}' not supported, skipping")
            _LLM_AVAILABLE = False
        return None
    if not api_key:
        if _LLM_AVAILABLE is None:
            logger.warning("LLM review: no API key found (set GEMINI_API_KEY or LLM_API_KEY)")
            _LLM_AVAILABLE = False
        return None

    try:
        import google.generativeai as genai
        genai.configure(api_key=api_key)
        model_name = os.environ.get("LLM_MODEL", "gemini-2.0-flash")
        model = genai.GenerativeModel(
            model_name=model_name,
            system_instruction=REVIEW_SYSTEM_PROMPT,
        )
        resp = model.generate_content(prompt)
        text = resp.text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[-1]
            text = text.rsplit("```", 1)[0]
        return json.loads(text)
    except json.JSONDecodeError as e:
        logger.warning(f"LLM review: failed to parse JSON response: {e}\nResponse was: {resp.text if 'resp' in dir() else 'N/A'}")
        return None
    except Exception as e:
        logger.warning(f"LLM review call failed: {e}")
        return None


def review_object(
    obj: ParsedObject,
    result: ValidationResult,
) -> LLMReviewResult:
    review = LLMReviewResult(
        obj_name=obj.name,
        object_type=obj.object_type,
        original_sql=obj.raw_sql or "",
        converted_sql=obj.converted_sql or "",
        status=result.status,
        confidence_before=result.confidence,
        confidence_after=result.confidence,
    )

    if not obj.converted_sql:
        review.needs_attention = False
        return review

    prompt = _build_review_prompt(obj, result)
    response = _call_review_llm(prompt)

    if response is None:
        review.error = "LLM review failed or no response"
        return review

    review.review_notes = response.get("review_notes", [])
    review.suggested_fixes = response.get("suggested_fixes", [])
    review.improved_sql = response.get("improved_sql")
    adj = response.get("confidence_adjustment", 0.0)
    review.confidence_after = max(0.0, min(1.0, review.confidence_before + adj))
    review.reviewed_at = datetime.now().isoformat(timespec="seconds")

    if review.improved_sql and review.improved_sql != obj.converted_sql:
        # Post-process LLM output to fix known regressions
        fixed = review.improved_sql
        import re
        fixed = re.sub(r"(?i)\bSQL\s+SECURITY\s+DEFINER\b", "SQL SECURITY INVOKER", fixed)
        fixed = re.sub(r"(?i)\bRETURN\s+'", "SELECT '", fixed)
        fixed = re.sub(r"(?i)\bRETURN\s+\"", 'SELECT "', fixed)
        fixed = re.sub(
            r'(CREATE\s+OR\s+REPLACE\s+(?:PROCEDURE|FUNCTION)\s+)(\"[^\"]+\")',
            lambda m: m.group(1) + "`" + m.group(2).strip('"') + "`",
            fixed,
        )
        obj.converted_sql = fixed

    return review


def review_inventory(
    inventory: ProjectInventory,
    validation_results: dict[str, ValidationResult],
) -> dict[str, LLMReviewResult]:
    results: dict[str, LLMReviewResult] = {}
    for obj in inventory.all_objects:
        val = validation_results.get(obj.name)
        if not val:
            continue
        review = review_object(obj, val)
        results[obj.name] = review
    reviewed = sum(1 for r in results.values() if r.needs_attention)
    fixed = sum(1 for r in results.values() if r.improved_sql and r.improved_sql != r.converted_sql)
    print(f"  LLM reviewed {reviewed} object(s)")
    if fixed:
        print(f"  LLM applied {fixed} fix(es)")
    return results


def generate_review_summary(
    review_results: dict[str, LLMReviewResult],
) -> list[str]:
    lines = [
        "",
        "--- LLM Review Summary ---",
    ]
    reviewed = [r for r in review_results.values() if r.needs_attention]
    if not reviewed:
        lines.append("  No objects required LLM review.")
        return lines

    lines.append(f"  Objects reviewed:  {len(reviewed)}")
    lines.append("")

    needs_attention = [r for r in reviewed if r.suggested_fixes or r.error]
    if needs_attention:
        lines.append("  Objects needing attention:")
        for r in needs_attention:
            notes = "; ".join(r.review_notes[:3])
            if r.error:
                lines.append(f"    - {r.obj_name} ({r.object_type}) — ERROR: {r.error}")
            else:
                confidence_delta = r.confidence_after - r.confidence_before
                lines.append(
                    f"    - {r.obj_name} ({r.object_type}) "
                    f"[confidence: {r.confidence_before:.2f} → {r.confidence_after:.2f}]"
                )
                if notes:
                    lines.append(f"      Notes: {notes}")
    else:
        lines.append("  All reviewed objects look good.")

    return lines
