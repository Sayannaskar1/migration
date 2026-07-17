from dataclasses import dataclass


@dataclass
class HealingResult:
    success: bool
    attempt: int
    strategy: str
    error: str | None = None


class SelfHealingEngine:
    def __init__(self, max_retries: int = 3):
        self.max_retries = max_retries

    def heal(self, obj, attempt: int = 1) -> HealingResult:
        if attempt > self.max_retries:
            return HealingResult(
                success=False,
                attempt=attempt,
                strategy="exhausted",
                error="Max retries exceeded",
            )

        strategy = self._select_strategy(obj, attempt)
        if not strategy:
            return HealingResult(
                success=False,
                attempt=attempt,
                strategy="none",
                error="No healing strategy available",
            )

        try:
            healed = self._apply_strategy(strategy, obj)
            if healed:
                obj.converted_sql = healed
                return HealingResult(
                    success=True, attempt=attempt, strategy=strategy
                )
        except Exception:
            pass

        if attempt < self.max_retries:
            return self.heal(obj, attempt + 1)
        return HealingResult(
            success=False,
            attempt=attempt,
            strategy=strategy,
            error=f"All {self.max_retries} retries exhausted",
        )

    def _select_strategy(self, obj, attempt: int) -> str | None:
        sql = obj.converted_sql or obj.raw_sql or ""
        if not obj.converted_sql:
            return "llm_fallback"
        if "MANUAL REVIEW" in sql.upper() and attempt <= 2:
            return "llm_assisted"
        if attempt >= 2:
            return "llm_fallback"
        return None

    def _apply_strategy(self, strategy: str, obj) -> str | None:
        if strategy == "llm_fallback":
            return self._llm_fallback(obj)
        if strategy == "regex_cleanup":
            return self._regex_cleanup(obj)
        if strategy == "llm_assisted":
            return self._llm_assisted(obj)
        return None

    def _llm_fallback(self, obj) -> str | None:
        from agents.llm_transpiler import llm_transpile
        from orchestrator import _get_llm_config
        cfg = _get_llm_config()
        if not cfg.get("provider"):
            return None
        try:
            return llm_transpile(obj.raw_sql or obj.converted_sql or "", cfg)
        except Exception:
            return None

    def _regex_cleanup(self, obj) -> str | None:
        from agents.rule_engine import apply_rules
        try:
            return apply_rules(obj.converted_sql or "", obj.object_type)
        except Exception:
            return None

    def _llm_assisted(self, obj) -> str | None:
        from orchestrator import _get_llm_config
        cfg = _get_llm_config()
        if not cfg.get("provider"):
            return None
        from agents.llm_transpiler import llm_transpile
        try:
            return llm_transpile(obj.raw_sql or "", cfg)
        except Exception:
            return None
