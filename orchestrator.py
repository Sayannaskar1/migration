import os
import re
import sys
import time
import hashlib
import concurrent.futures
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
load_dotenv()

from agents.project_loader import load_project, load_project_from_tree, ProjectInventory
from agents.dependency_agent import analyze_dependencies, DependencyGraph
from agents.schema_agent import convert_schema
from agents.sql_translation_agent import translate_inventory
from agents.sqlglot_transpiler import transpile_all as sqlglot_transpile
from agents.lakebridge_transpiler import (
    transpile_all as lakebridge_transpile,
    transpile_with_morpheus,
)
from agents.rule_engine import apply_rules
from agents.capability_checker import (
    check_inventory_capabilities,
    generate_capability_summary,
)
from agents.validation_agent import (
    validate_inventory,
    validate_object,
    generate_validation_summary,
    ValidationResult,
)
from agents.documentation_agent import (
    generate_migration_report,
    generate_object_inventory_csv,
    generate_dependency_diagram,
)
from agents.llm_transpiler import llm_transpile
from agents.llm_review_agent import review_inventory as llm_review
from agents.planner_agent import PlannerAgent, MigrationPlan
from agents.confidence_engine import ConfidenceEngine, ConfidenceScore
from agents.self_healing_engine import SelfHealingEngine, HealingResult
from agents.storage_discovery_agent import StorageDiscoveryAgent, StorageReport
from agents.deployment_agent import DeploymentAgent, DeployResult
from agents.data_migration.data_migration_manager import DataMigrationManager
from agents.migration_state import MigrationState
from agents.assessment_agent import MigrationAssessmentAgent, AssessmentReport
from agents.manifest_agent import ManifestGenerator, MigrationManifest
from agents.performance_optimizer import PerformanceOptimizer, OptimizationSuggestion
from agents.semi_structured_agent import SemiStructuredAgent, SemiStructuredResult
from agents.js_to_python_udf_agent import JSPythonUDFAgent, JSConversionResult


def preprocess_raw(sql: str) -> str:
    """Pre-process Snowflake SQL before SQLGlot to preserve convertible constructs."""
    sql = re.sub(
        r"(?i)(\bNUMBER\s*(?:\([^)]*\))?)\s+AUTOINCREMENT\b",
        r"BIGINT GENERATED ALWAYS AS IDENTITY",
        sql,
    )
    sql = re.sub(
        r"(?i)(\bINT\s*)\s+AUTOINCREMENT\b",
        r"BIGINT GENERATED ALWAYS AS IDENTITY",
        sql,
    )
    sql = re.sub(
        r"(?i)(\bINTEGER\s*)\s+AUTOINCREMENT\b",
        r"BIGINT GENERATED ALWAYS AS IDENTITY",
        sql,
    )
    sql = re.sub(
        r"(?i)(\bBIGINT GENERATED ALWAYS AS IDENTITY)\s+START\s+\d+\s+INCREMENT\s+\d+",
        r"\1",
        sql,
    )
    sql = re.sub(
        r"(?i)\bTO_NUMBER\s*\(([^)]+)\)",
        r"CAST(\1 AS DECIMAL)",
        sql,
    )
    sql = re.sub(
        r"(?i)\bTRY_PARSE_JSON\s*\(",
        "__TRY_PARSE_JSON__(",
        sql,
    )
    return sql


class MigrationOrchestrator:
    PIPELINE_STEPS = [
        "project_loader",
        "dependency_analysis",
        "assessment",
        "storage_discovery",
        "capability_check",
        "plan",
        "sqlglot_transpile",
        "lakebridge_transpile",
        "rule_engine",
        "semi_structured",
        "js_conversion",
        "regex_cleanup",
        "confidence_scoring",
        "llm_verify",
        "validation",
        "llm_review",
        "self_healing",
        "deployment_approval",
        "deployment",
        "performance_optimizer",
        "manifest",
        "documentation",
    ]

    def __init__(self, project_path: str = "", output_dir: str = "output", project_tree: dict[str, str] = None):
        self.project_path = Path(project_path)
        self.project_tree = project_tree
        self.output_dir = Path(output_dir)
        self.inventory: Optional[ProjectInventory] = None
        self.dep_graph: Optional[DependencyGraph] = None
        self.capability_results: dict[str, list[dict]] = {}
        self.validation_results: dict[str, ValidationResult] = {}
        self.migration_plan: Optional[MigrationPlan] = None
        self.confidence_scores: list[dict] = []
        self.healing_results: list[HealingResult] = []
        self.semi_structured_results: list[SemiStructuredResult] = []
        self.js_conversion_results: list[JSConversionResult] = []
        self.storage_report: Optional[StorageReport] = None
        self.assessment_report: Optional[AssessmentReport] = None
        self.manifest: Optional[MigrationManifest] = None
        self.optimization_suggestions: list[OptimizationSuggestion] = []
        self.step_timings: dict[str, float] = {}
        self.dry_run: bool = False
        self.target_version: str = ""
        self.catalog_strategy: str = "preserve"
        self.catalog_merge_target: str = ""
        self.catalog_rename_map: dict[str, str] | None = None
        self.catalog_custom_mappings: list | None = None
        self.source_hashes: dict[str, str] = {}
        self.changed_objects: list[str] = []
        self._sf_connector: Optional[object] = None

    def set_snowflake_connector(self, connector):
        self._sf_connector = connector

    def compute_source_hashes(self) -> dict[str, str]:
        if not self.inventory:
            return {}
        hashes = {}
        for obj in self.inventory.all_objects:
            h = hashlib.sha256((obj.raw_sql or "").encode()).hexdigest()[:16]
            hashes[obj.name] = h
        self.source_hashes = hashes
        return hashes

    def compute_changed_objects(self, previous_hashes: dict[str, str]) -> list[str]:
        current = self.compute_source_hashes()
        changed = []
        for name, h in current.items():
            if previous_hashes.get(name) != h:
                changed.append(name)
        self.changed_objects = changed
        return changed

    def step_incremental_check(self, previous_state: dict | None = None) -> list[str]:
        if not previous_state:
            print("  No previous state — full migration required")
            self.changed_objects = []
            return []
        prev_hashes = previous_state.get("source_hashes", {})
        changed = self.compute_changed_objects(prev_hashes)
        if changed:
            print(f"  Found {len(changed)} changed object(s): {', '.join(changed[:5])}{'...' if len(changed) > 5 else ''}")
        else:
            print("  All objects unchanged — skipping migration")
        return changed

    def to_state(self) -> MigrationState:
        return MigrationState(
            migration_id=str(hash(self.project_path)),
            project_path=str(self.project_path),
            output_dir=str(self.output_dir),
            inventory=self.inventory,
            dep_graph=self.dep_graph,
            storage_report=self.storage_report,
            migration_plan=self.migration_plan,
            capability_results=self.capability_results,
            validation_results=self.validation_results,
            confidence_scores=self.confidence_scores,
            healing_results=self.healing_results,
            assessment_report=self.assessment_report,
            manifest=self.manifest,
            optimization_suggestions=self.optimization_suggestions,
            step_timings=self.step_timings,
            dry_run=self.dry_run,
            target_version=self.target_version,
            catalog_map=(self.migration_plan.catalog_mapping.mapping_table
                         if self.migration_plan and self.migration_plan.catalog_mapping else {}),
            source_hashes=self.source_hashes,
        )

    def step_project_loader(self) -> ProjectInventory:
        if self.inventory:
            return self.inventory
        print("[1/14] Loading project...")
        if self.project_tree is not None:
            self.inventory = load_project_from_tree(self.project_path.name or "project", self.project_tree)
        else:
            self.inventory = load_project(str(self.project_path))
        summary = self.inventory.summary()
        print(f"  Loaded {summary['total_objects']} objects "
              f"({summary['tables']} tables, {summary['views']} views, "
              f"{summary['procedures']} procedures, {summary['functions']} functions)")
        return self.inventory

    def step_dependency_analysis(self) -> DependencyGraph:
        if self.dep_graph:
            return self.dep_graph
        if not self.inventory:
            raise RuntimeError("Project must be loaded first")
        print("[2/14] Analyzing dependencies...")
        self.dep_graph = analyze_dependencies(self.inventory)
        ordered = self.dep_graph.get_deployment_order()
        print(f"  Determined deployment order for {len(ordered)} objects")
        return self.dep_graph

    def step_storage_discovery(self) -> StorageReport | None:
        if self.storage_report:
            return self.storage_report
        if not self._sf_connector:
            print("  No Snowflake connector available — skipping storage discovery")
            return None
        print("[3/14] Storage discovery (analyzing data sources)...")
        agent = StorageDiscoveryAgent()
        self.storage_report = agent.discover(self._sf_connector)
        print(f"  Total tables:     {self.storage_report.total_tables}")
        print(f"  Internal tables:  {len(self.storage_report.internal_tables)}")
        print(f"  External tables:  {len(self.storage_report.external_tables)}")
        print(f"  Iceberg tables:   {len(self.storage_report.iceberg_tables)}")
        print(f"  Cloud providers:  {', '.join(self.storage_report.cloud_providers) if self.storage_report.cloud_providers else 'none detected'}")
        print(f"  Needs export:     {self.storage_report.needs_export}")
        return self.storage_report

    def step_capability_check(self) -> dict[str, list[dict]]:
        if self.capability_results:
            return self.capability_results
        if not self.inventory:
            raise RuntimeError("Project must be loaded first")
        print("[4/14] Capability check (feature detection)...")
        results = check_inventory_capabilities(self.inventory)
        summary = generate_capability_summary(results)
        buckets = summary["buckets"]
        print(f"  Objects with findings: {summary['total_objects_with_findings']}")
        print(f"  Features detected:     {summary['total_features_detected']}")
        for cap, count in buckets.items():
            if count:
                print(f"    {cap}: {count}")
        self.capability_results = results
        return results

    def step_plan(self) -> MigrationPlan:
        if self.migration_plan:
            return self.migration_plan
        if not self.inventory or not self.dep_graph:
            raise RuntimeError("Project must be loaded and dependencies analyzed")
        print("[5/14] Planner agent (execution plan)...")
        planner = PlannerAgent()
        self.migration_plan = planner.build_plan(
            self.inventory,
            storage_report=self.storage_report,
            capability_results=self.capability_results,
            target_version=self.target_version,
            catalog_strategy=self.catalog_strategy,
            catalog_merge_target=self.catalog_merge_target,
            catalog_rename_map=self.catalog_rename_map,
            catalog_custom_mappings=self.catalog_custom_mappings,
        )
        print(f"  Total objects:  {self.migration_plan.total_objects}")
        print(f"  Complexity:     {self.migration_plan.estimated_complexity}")
        print(f"  Export strategy: {self.migration_plan.export_strategy}")
        if self.migration_plan.blockers:
            print(f"  Blockers:       {len(self.migration_plan.blockers)}")
            for b in self.migration_plan.blockers:
                print(f"    - {b}")
        if self.storage_report:
            print(f"  Storage:        {self.migration_plan.storage_summary}")
        return self.migration_plan

    def _preprocess_raw(self, sql: str) -> str:
        return preprocess_raw(sql)

    def _parallel_group(self, items: list, worker_fn, max_workers: int = 4) -> list:
        if len(items) <= 1 or max_workers <= 1:
            return [worker_fn(item) for item in items]
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(worker_fn, item): item for item in items}
            for future in concurrent.futures.as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as e:
                    results.append(e)
        return results

    def step_sqlglot_transpile(self) -> None:
        if not self.inventory:
            raise RuntimeError("Project must be loaded first")
        print("[6/14] SQLGlot transpilation (AST, with LakeBridge custom dialects)...")
        count = 0
        for obj in self.inventory.all_objects:
            if "LANGUAGE JAVASCRIPT" in obj.raw_sql.upper():
                continue
            if obj.raw_sql:
                cleaned = self._preprocess_raw(obj.raw_sql)
                result = lakebridge_transpile(cleaned, fallback=True)
                if result:
                    obj.converted_sql = result
                    count += 1
        print(f"  SQLGlot + LakeBridge transpiled {count} object(s)")

    def step_lakebridge_transpile(self) -> None:
        if not self.inventory:
            raise RuntimeError("Project must be loaded first")
        morpheus_jar = Path.home() / ".databricks" / "labs" / "remorph-transpilers" / "databricks-morph-plugin" / "lib" / "databricks-morph-plugin.jar"
        if not morpheus_jar.exists():
            print("  Morpheus JAR not found, skipping")
            return
        print("[7/14] Morpheus transpilation (LSP)...")
        count = 0
        for obj in self.inventory.all_objects:
            if "LANGUAGE JAVASCRIPT" in obj.raw_sql.upper():
                continue
            if obj.raw_sql:
                morpheus_result = transpile_with_morpheus(obj.raw_sql)
                if morpheus_result and morpheus_result != obj.raw_sql:
                    obj.converted_sql = morpheus_result
                    count += 1
        print(f"  Morpheus transpiled {count} object(s)")

    def step_rule_engine(self) -> None:
        if not self.inventory:
            raise RuntimeError("Project must be loaded first")
        print("[8/14] Rule engine (deterministic mappings)...")
        for obj in self.inventory.all_objects:
            sql = obj.converted_sql if obj.converted_sql else obj.raw_sql
            if sql:
                obj.converted_sql = apply_rules(sql, obj.object_type)
        print(f"  Applied rules to {len(self.inventory.all_objects)} objects")

    def step_semi_structured(self) -> list[SemiStructuredResult]:
        if self.semi_structured_results:
            return self.semi_structured_results
        if not self.inventory:
            raise RuntimeError("Project must be loaded first")
        print("[7b/14] Semi-structured data conversion...")
        agent = SemiStructuredAgent()
        self.semi_structured_results = agent.convert(self.inventory, strategy="native")
        funcs = set()
        for r in self.semi_structured_results:
            funcs.update(r.functions_converted)
        if funcs:
            print(f"  Converted semi-structured functions: {', '.join(sorted(funcs))}")
        print(f"  Processed {len(self.semi_structured_results)} object(s) with semi-structured data")
        return self.semi_structured_results

    def step_js_conversion(self) -> list[JSConversionResult]:
        if self.js_conversion_results:
            return self.js_conversion_results
        if not self.inventory:
            raise RuntimeError("Project must be loaded first")
        print("[7c/14] JavaScript→Python UDF conversion...")
        agent = JSPythonUDFAgent()
        self.js_conversion_results = agent.convert(self.inventory)
        if self.js_conversion_results:
            converted = sum(1 for r in self.js_conversion_results if r.success)
            failed = sum(1 for r in self.js_conversion_results if not r.success)
            print(f"  JS→Python: {converted} converted, {failed} need manual review")
        else:
            print("  No JavaScript objects found")
        return self.js_conversion_results

    def step_regex_cleanup(self) -> None:
        if not self.inventory:
            raise RuntimeError("Project must be loaded first")
        print("[8/14] Regex cleanup & final checks...")
        for obj in self.inventory.all_objects:
            obj.converted_sql = convert_schema(obj)
        translate_inventory(self.inventory)
        print(f"  Cleaned up {len(self.inventory.all_objects)} objects")

    def step_confidence_scoring(self) -> list[dict]:
        if self.confidence_scores:
            return self.confidence_scores
        if not self.inventory:
            raise RuntimeError("Project must be loaded first")
        print("[9/14] Confidence scoring...")
        engine = ConfidenceEngine()
        self.confidence_scores = engine.score_batch(self.inventory)
        buckets = {"Automatic": 0, "LLM Assisted": 0, "Manual Review": 0}
        for s in self.confidence_scores:
            label = s["confidence_label"]
            buckets[label] = buckets.get(label, 0) + 1
        for label, count in buckets.items():
            if count:
                print(f"  {label}: {count}")
        return self.confidence_scores

    def step_llm_verify(self) -> None:
        if not self.inventory:
            raise RuntimeError("Project must be loaded first")
        print("[10/14] LLM verification (confidence-gated)...")

        cfg = _get_llm_config()
        if not cfg.get("provider"):
            print("  LLM: no provider configured, skipping")
            return

        target_count = 0
        for obj in self.inventory.all_objects:
            if not obj.converted_sql:
                continue
            result = validate_object(obj, self.inventory)
            if result.errors:
                fixed = llm_transpile(obj.converted_sql, cfg)
                if fixed and fixed != obj.converted_sql:
                    obj.converted_sql = fixed
                    target_count += 1

        if target_count:
            print(f"  LLM re-transpiled {target_count} object(s)")
        else:
            print("  LLM: no objects needed remediation")

    def step_validation(self) -> dict[str, ValidationResult]:
        if self.validation_results:
            return self.validation_results
        if not self.inventory:
            raise RuntimeError("Project must be loaded first")
        print("[11/14] Validating conversions...")
        self.validation_results = validate_inventory(self.inventory)
        summary = generate_validation_summary(self.validation_results)
        parts = [f"{summary['passed']} passed"]
        if summary['warnings']:
            parts.append(f"{summary['warnings']} warnings")
        if summary['issues']:
            parts.append(f"{summary['issues']} issues")
        if summary['architectural']:
            parts.append(f"{summary['architectural']} architectural")
        if summary['errors']:
            parts.append(f"{summary['errors']} errors")
        print(f"  Validated: {', '.join(parts)}")
        return self.validation_results

    def step_llm_review(self) -> None:
        if not self.inventory:
            raise RuntimeError("Project must be loaded first")
        print("[11.5/14] LLM review (low-confidence / architectural changes)...")
        cfg = _get_llm_config()
        provider = cfg.get("provider") or os.environ.get("LLM_PROVIDER", "")
        api_key = (
            cfg.get("api_key")
            or os.environ.get("LLM_API_KEY")
            or os.environ.get("GEMINI_API_KEY")
        )
        if not provider:
            print("  LLM: no provider configured, skipping review")
            return
        if not api_key:
            print(f"  LLM: provider '{provider}' configured but no API key found — skipping review")
            print("  (set GEMINI_API_KEY or LLM_API_KEY environment variable)")
            return
        llm_review(self.inventory, self.validation_results)

    def step_self_healing(self) -> list[HealingResult]:
        if self.healing_results:
            return self.healing_results
        if not self.inventory:
            raise RuntimeError("Project must be loaded first")
        print("[12/14] Self-healing (retrying failed conversions)...")
        engine = SelfHealingEngine(max_retries=3)
        self.healing_results = []
        for obj in self.inventory.all_objects:
            if not obj.converted_sql:
                result = engine.heal(obj)
                self.healing_results.append(result)
                if result.success:
                    print(f"  Healed: {obj.name} (strategy={result.strategy})")
        healed = sum(1 for r in self.healing_results if r.success)
        failed = sum(1 for r in self.healing_results if not r.success)
        print(f"  Healed: {healed}, Failed: {failed}")
        return self.healing_results

    def step_deployment_approval(self) -> bool:
        print("[13/14] Deployment approval...")
        blockers = []
        if self.migration_plan and self.migration_plan.blockers:
            blockers = self.migration_plan.blockers
        if blockers:
            for b in blockers:
                print(f"  BLOCKER: {b}")
            print("  Approval required: manual review needed")
            return False
        auto_approve = os.environ.get("AUTO_DEPLOY_APPROVE", "").lower() in ("1", "true", "yes")
        if auto_approve:
            print("  Auto-approval enabled — deployment approved")
            return True
        print("  No blockers — deployment marked as pending approval")
        print("  (set AUTO_DEPLOY_APPROVE=1 to auto-approve)")
        return auto_approve

    def step_deployment(self, objects: list[dict], creds: dict, dry_run: bool = False) -> list[DeployResult]:
        print(f"[13.5/14] Deployment (dry_run={dry_run})...")
        catalog_ddl = None
        schema_ddl = None
        if self.migration_plan and self.migration_plan.catalog_mapping:
            cm = self.migration_plan.catalog_mapping
            catalog_ddl = cm.catalog_create_sql
            schema_ddl = cm.schema_create_sql
            if catalog_ddl or schema_ddl:
                print(f"  Pre-deploy: {len(catalog_ddl or [])} catalog(s), {len(schema_ddl or [])} schema(s)")
        agent = DeploymentAgent()
        results = agent.deploy(objects, creds, dry_run=dry_run, catalog_ddl=catalog_ddl, schema_ddl=schema_ddl)
        for r in results:
            status = "OK" if r.success else "FAIL"
            print(f"  {status:4s} {r.object_name} ({r.duration_ms}ms)")
            if r.error:
                print(f"       Error: {r.error}")
        return results

    def step_rollback(self, deployed: list[DeployResult], creds: dict) -> list[DeployResult]:
        print("[13.5/14] Rollback...")
        agent = DeploymentAgent()
        results = agent.rollback(deployed, creds)
        for r in results:
            status = "OK" if r.success else "FAIL"
            print(f"  {status:4s} Rollback {r.object_name}")
        return results

    def step_assessment(self) -> AssessmentReport:
        if self.assessment_report:
            return self.assessment_report
        if not self.inventory:
            raise RuntimeError("Project must be loaded first")
        print("[3/19] Migration assessment...")
        agent = MigrationAssessmentAgent()
        self.assessment_report = agent.assess(self.inventory, self.storage_report, self.capability_results)
        for line in self.assessment_report.summary_lines():
            print(line)
        return self.assessment_report

    def step_performance_optimizer(self) -> list[OptimizationSuggestion]:
        if self.optimization_suggestions:
            return self.optimization_suggestions
        if not self.inventory:
            raise RuntimeError("Project must be loaded first")
        print("[17/19] Performance optimization analysis...")
        optimizer = PerformanceOptimizer()
        self.optimization_suggestions = optimizer.analyze(self.inventory)
        print(f"  Generated {len(self.optimization_suggestions)} optimization suggestion(s)")
        for s in self.optimization_suggestions[:3]:
            print(f"    {s.suggestion_type:10s} {s.object_name} ({s.priority})")
        if len(self.optimization_suggestions) > 3:
            print(f"    ... and {len(self.optimization_suggestions) - 3} more")
        return self.optimization_suggestions

    def step_manifest(self) -> MigrationManifest:
        print("[18/19] Migration manifest...")
        gen = ManifestGenerator()
        state = self.to_state()
        self.manifest = gen.generate(state)
        manifest_path = self.output_dir / "reports" / "migration_manifest.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        self.manifest.save(manifest_path)
        print(f"  Manifest saved: {manifest_path}")
        return self.manifest

    def step_documentation(self) -> dict[str, Path]:
        if not self.inventory or not self.dep_graph:
            raise RuntimeError("Project must be loaded and dependencies analyzed")
        print("[14/14] Generating documentation...")
        converted_dir = self.output_dir / "converted_sql"
        reports_dir = self.output_dir / "reports"
        converted_dir.mkdir(parents=True, exist_ok=True)
        reports_dir.mkdir(parents=True, exist_ok=True)

        for obj in self.inventory.all_objects:
            if obj.converted_sql:
                safe_name = obj.name.replace("/", "_").replace("\\", "_")
                obj_dir = converted_dir / obj.object_type
                obj_dir.mkdir(parents=True, exist_ok=True)
                obj_dir / f"{safe_name}.sql"
                (obj_dir / f"{safe_name}.sql").write_text(
                    f"-- Converted from: {obj.file_path}\n"
                    f"-- Original type:  {obj.object_type}\n"
                    f"-- Databricks SQL\n"
                    f"{'=' * 60}\n\n{obj.converted_sql}",
                    encoding="utf-8",
                )

        report = generate_migration_report(
            self.inventory, self.dep_graph, self.validation_results, reports_dir
        )
        csv = generate_object_inventory_csv(self.inventory, reports_dir)
        diagram = generate_dependency_diagram(self.dep_graph, reports_dir)

        print(f"  Report:         {report}")
        print(f"  Inventory CSV:  {csv}")
        print(f"  Dependency diagram: {diagram}")
        print(f"  Converted SQL:  {converted_dir}")

        print("\n[14/14] Migration complete!")
        return {"report": report, "csv": csv, "diagram": diagram}

    def run(
        self,
        full: bool = True,
        resume_from: str | None = None,
        dry_run: bool = False,
        target_version: str = "current",
        catalog_strategy: str = "preserve",
        catalog_merge_target: str = "",
        catalog_rename_map: dict[str, str] | None = None,
        catalog_custom_mappings: list | None = None,
    ) -> dict:
        self.dry_run = dry_run
        self.target_version = target_version
        self.catalog_strategy = catalog_strategy
        self.catalog_merge_target = catalog_merge_target
        self.catalog_rename_map = catalog_rename_map
        self.catalog_custom_mappings = catalog_custom_mappings
        print("=" * 60)
        print("  Snowflake to Databricks Migration Agent")
        print("=" * 60)
        if dry_run:
            print("  MODE: Dry Run (no deployment)")
        if target_version and target_version != "current":
            print(f"  Target: Databricks {target_version}")
        if catalog_strategy != "preserve":
            print(f"  Catalog Strategy: {catalog_strategy}")
        print()

        resume = resume_from is not None
        started = resume_from is None
        steps = self.PIPELINE_STEPS
        if dry_run:
            steps = [s for s in steps if s != "deployment_approval" and s != "deployment"]

        for i, step_name in enumerate(steps):
            if resume and step_name == resume_from:
                started = True
            if resume and not started:
                print(f"  [resume] Skipping {step_name} (completed)")
                continue
            self._run_step(step_name)

        print()
        print("=" * 60)
        print("  Migration Complete")
        print("=" * 60)
        if self.step_timings:
            print()
            print("  Step Timings")
            max_w = max(len(s) for s in self.step_timings)
            for s, t in self.step_timings.items():
                bar = "#" * max(1, int(t * 5))
                print(f"    {s:{max_w}s}  {t:6.2f}s  {bar}")
        print()

        return {
            "inventory": self.inventory,
            "dep_graph": self.dep_graph,
            "migration_plan": self.migration_plan,
            "validation_results": self.validation_results,
            "confidence_scores": self.confidence_scores,
            "storage_report": self.storage_report,
            "assessment_report": self.assessment_report,
            "manifest": self.manifest,
            "optimization_suggestions": self.optimization_suggestions,
            "step_timings": self.step_timings,
        }

    def _run_step(self, step_name: str):
        step_map = {
            "project_loader": self.step_project_loader,
            "dependency_analysis": self.step_dependency_analysis,
            "assessment": self.step_assessment,
            "storage_discovery": self.step_storage_discovery,
            "capability_check": self.step_capability_check,
            "plan": self.step_plan,
            "sqlglot_transpile": self.step_sqlglot_transpile,
            "lakebridge_transpile": self.step_lakebridge_transpile,
            "rule_engine": self.step_rule_engine,
            "semi_structured": self.step_semi_structured,
            "js_conversion": self.step_js_conversion,
            "regex_cleanup": self.step_regex_cleanup,
            "confidence_scoring": self.step_confidence_scoring,
            "llm_verify": self.step_llm_verify,
            "validation": self.step_validation,
            "llm_review": self.step_llm_review,
            "self_healing": self.step_self_healing,
            "deployment_approval": self.step_deployment_approval,
            "deployment": lambda: None,
            "performance_optimizer": self.step_performance_optimizer,
            "manifest": self.step_manifest,
            "documentation": self.step_documentation,
        }
        fn = step_map.get(step_name)
        if fn:
            start = time.time()
            fn()
            elapsed = round(time.time() - start, 3)
            self.step_timings[step_name] = elapsed

    def run_with_state(self, state: MigrationState) -> MigrationState:
        state.start_time = time.time()
        steps = self.PIPELINE_STEPS

        for step_name in steps:
            if state.has_completed(step_name):
                print(f"  [resume] Skipping {step_name} (completed)")
                continue

            state.current_step = step_name
            state.log(step_name, f"Starting {step_name}")

            try:
                self._run_step(step_name)
                state.completed_steps.append(step_name)
                state.log(step_name, "Completed")
            except Exception as e:
                state.error = str(e)
                state.log(step_name, f"Error: {e}")
                state.end_time = time.time()
                return state

        state.current_step = None
        state.end_time = time.time()
        return state


def _get_llm_config() -> dict:
    import os
    config = {
        "provider": os.environ.get("LLM_PROVIDER", "").lower(),
        "api_key": os.environ.get("LLM_API_KEY", ""),
        "model": os.environ.get("LLM_MODEL", ""),
        "api_base": os.environ.get("LLM_API_BASE", ""),
    }
    config_file = os.environ.get("LLM_CONFIG", "")
    if config_file and Path(config_file).exists():
        import json
        with open(config_file) as f:
            file_config = json.load(f)
            for k, v in file_config.items():
                if not config[k]:
                    config[k] = v
    return config
