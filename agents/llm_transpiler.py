import json
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

LLM_SYSTEM_PROMPT = """You are an expert SQL migration specialist.
Convert the given Snowflake SQL to Databricks SQL (Spark SQL dialect).

Rules:
1. STRING and STRING(N) -> STRING (no length qualifier)
2. VARCHAR(N), CHAR(N), TEXT(N), NVARCHAR(N) -> STRING
3. VARIANT -> STRING
4. OBJECT -> STRING
5. NUMBER(P,S), DECIMAL(P,S), NUMERIC(P,S) -> DECIMAL(P,S)
6. FLOAT, FLOAT4, FLOAT8, REAL, DOUBLE PRECISION -> DOUBLE
7. BYTEINT -> TINYINT
8. BINARY(N), VARBINARY(N) -> BINARY
9. DATETIME, TIME -> TIMESTAMP
10. TIMESTAMP_NTZ, TIMESTAMP_LTZ, TIMESTAMP_TZ -> TIMESTAMP
11. ARRAY -> ARRAY<STRING>
12. GEOGRAPHY -> STRING
13. Add USING DELTA to CREATE TABLE
14. CLUSTER BY -> ZORDER BY
15. IFF(cond,t,f) -> CASE WHEN cond THEN t ELSE f END
16. QUALIFY -> subquery with WHERE rn = N
17. LATERAL FLATTEN -> LATERAL VIEW EXPLODE
18. ARRAY_AGG -> COLLECT_LIST
19. OBJECT_CONSTRUCT -> NAMED_STRUCT
20. LISTAGG -> CONCAT_WS with COLLECT_LIST
21. ZEROIFNULL -> COALESCE(..., 0)
22. NULLIFZERO -> IF(... = 0, NULL, ...)
23. TO_VARCHAR -> CAST(... AS STRING)
24. TO_NUMBER -> CAST(... AS DECIMAL)
25. MONTHNAME -> DATE_FORMAT(..., 'MMM')
26. DAYNAME -> DATE_FORMAT(..., 'EEE')
27. ARRAY_SIZE -> SIZE
28. GET(obj, key) -> obj[key]
29. RANDOM() -> RAND()
30. SEQ{1,2,4,8}() -> ROW_NUMBER() OVER (ORDER BY 1)

Return ONLY the converted SQL with no explanation."""


def _get_llm_config() -> dict:
    config = {
        "provider": os.environ.get("LLM_PROVIDER", "").lower(),
        "api_key": os.environ.get("LLM_API_KEY", ""),
        "model": os.environ.get("LLM_MODEL", ""),
        "api_base": os.environ.get("LLM_API_BASE", ""),
    }
    config_file = os.environ.get("LLM_CONFIG", "")
    if config_file and os.path.exists(config_file):
        with open(config_file) as f:
            file_config = json.load(f)
            for k, v in file_config.items():
                if not config[k]:
                    config[k] = v
    return config


def _call_openai(system: str, user: str, config: dict) -> Optional[str]:
    try:
        from openai import OpenAI
        client = OpenAI(
            api_key=config.get("api_key"),
            base_url=config.get("api_base") or None,
        )
        model = config.get("model") or "gpt-4o-mini"
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0,
        )
        return resp.choices[0].message.content
    except Exception as e:
        logger.warning(f"OpenAI LLM call failed: {e}")
        return None


def _call_anthropic(system: str, user: str, config: dict) -> Optional[str]:
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=config.get("api_key"))
        model = config.get("model") or "claude-3-haiku-20240307"
        resp = client.messages.create(
            model=model,
            system=system,
            messages=[{"role": "user", "content": user}],
            temperature=0,
            max_tokens=4096,
        )
        return resp.content[0].text
    except Exception as e:
        logger.warning(f"Anthropic LLM call failed: {e}")
        return None


def _call_gemini(system: str, user: str, config: dict) -> Optional[str]:
    try:
        import google.generativeai as genai
        api_key = config.get("api_key") or os.environ.get("GEMINI_API_KEY")
        if not api_key:
            logger.warning("Gemini: no API key found (set GEMINI_API_KEY or LLM_API_KEY)")
            return None
        genai.configure(api_key=api_key)
        model_name = config.get("model") or "gemini-2.0-flash"
        model = genai.GenerativeModel(
            model_name=model_name,
            system_instruction=system,
        )
        resp = model.generate_content(user)
        return resp.text
    except Exception as e:
        logger.warning(f"Gemini LLM call failed: {e}")
        return None


def llm_transpile(sql: str, config: Optional[dict] = None) -> Optional[str]:
    """Transpile Snowflake SQL to Databricks using an LLM overlay."""
    cfg = config or _get_llm_config()
    provider = cfg.get("provider") or os.environ.get("LLM_PROVIDER", "")
    if not provider:
        return None
    prompt = f"Convert this Snowflake SQL to Databricks SQL:\n\n{sql}"
    if provider == "openai":
        return _call_openai(LLM_SYSTEM_PROMPT, prompt, cfg)
    elif provider == "anthropic":
        return _call_anthropic(LLM_SYSTEM_PROMPT, prompt, cfg)
    elif provider == "gemini":
        return _call_gemini(LLM_SYSTEM_PROMPT, prompt, cfg)
    else:
        logger.warning(f"Unsupported LLM provider: {provider}")
        return None
