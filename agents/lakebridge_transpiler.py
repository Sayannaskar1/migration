import asyncio
import logging
import sys
import tempfile
from pathlib import Path
from typing import Optional

from sqlglot import transpile, errors

logger = logging.getLogger(__name__)

# ── LakeBridge custom Snowflake parser + Databricks generator ──
_LAKEBRIDGE_SRC = Path.home() / "Desktop" / "forked" / "lakebridge" / "src"
_HAS_LAKEBRIDGE_DIALECTS = False
if _LAKEBRIDGE_SRC.exists():
    sys.path.insert(0, str(_LAKEBRIDGE_SRC.resolve()))
    try:
        from databricks.labs.lakebridge.transpiler.sqlglot.dialect_utils import SQLGLOT_DIALECTS
        _HAS_LAKEBRIDGE_DIALECTS = True
    except Exception as e:
        logger.warning(f"Could not load LakeBridge dialects: {e}")


def transpile_with_lakebridge(sql: str, pretty: bool = True) -> Optional[str]:
    """Transpile using LakeBridge's custom Snowflake parser + Databricks generator."""
    if not _HAS_LAKEBRIDGE_DIALECTS:
        return _transpile_raw_sqlglot(sql, pretty)
    try:
        read_cls = SQLGLOT_DIALECTS.get("snowflake")
        write_cls = SQLGLOT_DIALECTS.get("databricks")
        if read_cls and write_cls:
            result = transpile(
                sql,
                read=read_cls,
                write=write_cls,
                pretty=pretty,
                error_level=errors.ErrorLevel.WARN,
            )
            if result and result[0]:
                # LakeBridge may comment out the statement with "--" when it cannot
                # fully parse Snowflake-specific syntax (WITH MASKING POLICY, etc.).
                # Fall back to raw SQLGlot which passes unsupported syntax through.
                if result[0].strip().startswith("--"):
                    return _transpile_raw_sqlglot(sql, pretty)
                return result[0]
        return _transpile_raw_sqlglot(sql, pretty)
    except Exception as e:
        logger.warning(f"LakeBridge transpilation failed: {e}")
        return _transpile_raw_sqlglot(sql, pretty)


def _transpile_raw_sqlglot(sql: str, pretty: bool = True) -> Optional[str]:
    try:
        result = transpile(
            sql,
            read="snowflake",
            write="databricks",
            pretty=pretty,
            error_level=errors.ErrorLevel.WARN,
        )
        if result and result[0]:
            out = result[0]
            # SQLGlot may comment out the entire CREATE statement with "--" prefix
            # when it encounters unsupported syntax (LakeBridge monkey-patches sqlglot
            # globally, so even standard dialect calls are affected). Strip the "--"
            # prefix from the first line to recover the DDL structure.
            if out.strip().startswith("--"):
                lines = out.split('\n')
                if lines:
                    lines[0] = lines[0].lstrip('- ')
                out = '\n'.join(lines)
            return out
        return None
    except Exception as e:
        logger.warning(f"Raw SQLGlot transpilation failed: {e}")
        return None


def transpile_all(sql: str, fallback: bool = True) -> str:
    """Try LakeBridge dialacts, fall back to raw SQLGlot, then original SQL."""
    result = transpile_with_lakebridge(sql)
    if result is not None:
        return result
    if fallback:
        return sql
    raise RuntimeError("LakeBridge transpilation failed and no fallback provided")


# ── Morpheus LSP transpiler ──

_MORPHEUS_CONFIG = Path("/tmp/morpheus_config.yml")
_MORPHEUS_JAR = (
    Path.home() / ".databricks" / "labs" / "remorph-transpilers"
    / "databricks-morph-plugin" / "lib" / "databricks-morph-plugin.jar"
)


class MorpheusTranspiler:
    """Direct LSP client for Morpheus JAR (bypasses LSPEngine complexity)."""

    def __init__(self):
        self._client = None
        self._process = None

    def is_available(self) -> bool:
        return _MORPHEUS_JAR.exists()

    def transpile(self, sql: str) -> str:
        try:
            return asyncio.run(self._transpile_async(sql))
        except Exception as e:
            logger.warning(f"Morpheus transpilation failed: {e}")
            return sql

    async def _transpile_async(self, sql: str) -> str:
        from pygls.lsp.client import LanguageClient
        from lsprotocol.types import (
            InitializeParams, ClientCapabilities, TextDocumentItem,
            DidOpenTextDocumentParams, DidCloseTextDocumentParams,
        )
        from uuid import uuid4

        java = "/opt/homebrew/opt/openjdk@21/bin/java"
        jar = str(_MORPHEUS_JAR)

        self._client = LanguageClient("morpheus-client", "0.1")
        await self._client.start_io(java, "-jar", jar)

        try:
            params = InitializeParams(
                process_id=None,
                capabilities=ClientCapabilities(),
                root_uri=None,
                initialization_options={
                    "remorph": {"source-dialect": "snowflake"},
                    "options": {},
                    "custom": {},
                },
            )
            init_result = await self._client.initialize_async(params)
            logger.debug(f"Morpheus init result: {init_result}")

            file_uri = f"file:///inline.sql"
            text_document = TextDocumentItem(
                uri=file_uri,
                language_id="sql",
                version=1,
                text=sql,
            )
            self._client.text_document_did_open(DidOpenTextDocumentParams(text_document))

            from databricks.labs.lakebridge.transpiler.lsp.lsp_engine import (
                TRANSPILE_TO_DATABRICKS_METHOD,
                TranspileDocumentParams,
                TranspileDocumentResult,
            )
            transpile_params = TranspileDocumentParams(uri=file_uri, language_id="sql")
            response = await self._client.protocol.send_request_async(
                TRANSPILE_TO_DATABRICKS_METHOD, transpile_params
            )

            self._client.text_document_did_close(DidCloseTextDocumentParams(text_document))

            diagnostics = getattr(response, 'diagnostics', []) or []
            has_errors = any(
                getattr(d, 'severity', 4) == 1 for d in diagnostics
            )
            if has_errors:
                logger.warning(f"Morpheus returned {len(diagnostics)} error(s), using original SQL")
                return sql

            changes = getattr(response, 'changes', []) or []
            if not changes:
                return sql

            from databricks.labs.lakebridge.transpiler.lsp.lsp_engine import ChangeManager
            from pathlib import Path
            result = ChangeManager.apply(sql, changes, diagnostics, Path("inline.sql"))
            output = result.transpiled_code
            # Reject if Morpheus prepended error comments instead of actual transpilation
            if output.strip().startswith("--") and not output.strip().startswith("CREATE") and not output.strip().startswith("SELECT"):
                logger.warning("Morpheus returned error comments, using original SQL")
                return sql
            return output

        finally:
            await self._client.shutdown_async(None)
            self._client.exit(None)
            await self._client.stop()
            self._client = None


_morpheus = MorpheusTranspiler()


def transpile_with_morpheus(sql: str, fallback: bool = True) -> str:
    if not _morpheus.is_available():
        if fallback:
            return sql
        raise RuntimeError("Morpheus not available")
    return _morpheus.transpile(sql)
