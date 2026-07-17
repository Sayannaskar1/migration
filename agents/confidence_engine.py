from dataclasses import dataclass


@dataclass
class ConfidenceScore:
    score: float
    label: str
    reason: str


class ConfidenceEngine:
    HIGH_THRESHOLD = 0.95
    MEDIUM_THRESHOLD = 0.80

    def score_object(self, obj) -> ConfidenceScore:
        obj_type = obj.object_type
        sql = obj.converted_sql or obj.raw_sql or ""

        score = 1.0
        deductions = []

        if not obj.converted_sql:
            score -= 0.5
            deductions.append("conversion failed")
        elif obj_type in ("procedure",):
            score -= 0.15
            deductions.append("procedure body may need adjustments")

        if hasattr(obj, 'features') and obj.features:
            for feat in obj.features:
                cap = feat.get("capability", "")
                if cap == "not_supported":
                    score -= 0.3
                    deductions.append(f"unsupported feature: {feat.get('feature')}")
                elif cap == "architectural_change":
                    score -= 0.2
                    deductions.append(f"architectural change: {feat.get('feature')}")

        if "MANUAL REVIEW REQUIRED" in sql.upper():
            score -= 0.4
            deductions.append("manual review marker present")

        score = max(0.0, score)

        if score >= self.HIGH_THRESHOLD:
            label = "Automatic"
            reason = "No issues detected"
        elif score >= self.MEDIUM_THRESHOLD:
            label = "LLM Assisted"
            reason = "; ".join(deductions) if deductions else "Minor concerns"
        else:
            label = "Manual Review"
            reason = "; ".join(deductions) if deductions else "Multiple issues"

        return ConfidenceScore(score=round(score, 2), label=label, reason=reason)

    def score_batch(self, inventory) -> list[dict]:
        results = []
        for obj in inventory.all_objects:
            score = self.score_object(obj)
            results.append({
                "name": obj.name,
                "object_type": obj.object_type,
                "confidence": score.score,
                "confidence_label": score.label,
                "confidence_reason": score.reason,
            })
        return results
