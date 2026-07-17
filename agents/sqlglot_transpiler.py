import logging
from typing import Optional

from sqlglot import transpile, errors

logger = logging.getLogger(__name__)


def transpile_snowflake(sql: str, pretty: bool = True) -> Optional[str]:
    """Transpile Snowflake SQL to Databricks SQL using SQLGlot (AST → AST)."""
    try:
        result = transpile(
            sql,
            read="snowflake",
            write="databricks",
            pretty=pretty,
            error_level=errors.ErrorLevel.WARN,
        )
        if result:
            return result[0]
        return None
    except Exception as e:
        logger.warning(f"SQLGlot transpilation failed: {e}")
        return None


def transpile_all(sql: str, fallback: bool = True) -> str:
    """Transpile and return result, or fall back to original SQL."""
    result = transpile_snowflake(sql)
    if result is not None:
        return result
    if fallback:
        return sql
    raise RuntimeError("SQLGlot transpilation failed and no fallback provided")
