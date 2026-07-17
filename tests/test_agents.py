import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from agents.catalog_mapping_engine import (
    CatalogMappingEngine, CatalogMapping, CatalogMapResult,
    CATALOG_STRATEGY_PRESERVE, CATALOG_STRATEGY_MERGE,
    CATALOG_STRATEGY_RENAME, CATALOG_STRATEGY_CUSTOM,
)
from agents.assessment_agent import MigrationAssessmentAgent, AssessmentReport
from agents.manifest_agent import ManifestGenerator, MigrationManifest
from agents.performance_optimizer import PerformanceOptimizer, OptimizationSuggestion
from agents.migration_state import MigrationState, AgentInterface
from agents.deployment_agent import DeploymentAgent, DeployResult
from agents.planner_agent import PlannerAgent, MigrationPlan, PlanStep
from agents.data_migration.mover_base import DataMover, MigrateResult
from agents.data_migration.data_migration_manager import DataMigrationManager
from agents.data_migration.external_data_mover import ExternalDataMover
from agents.data_migration.internal_data_mover import InternalDataMover
from agents.data_migration.iceberg_data_mover import IcebergDataMover
import time
import json
from dataclasses import dataclass


class FakeObject:
    def __init__(self, name="test_obj", object_type="table", raw_sql="SELECT 1", converted_sql=""):
        self.name = name
        self.object_type = object_type
        self.raw_sql = raw_sql
        self.converted_sql = converted_sql
        self.schema_name = ""


class FakeInventory:
    def __init__(self, objects=None):
        self.all_objects = objects or []
        self.project_path = Path(".")
        self.schemas = [o for o in (objects or []) if o.object_type == "schema"]

    def summary(self):
        objs = self.all_objects
        return {
            "total_objects": len(objs),
            "tables": sum(1 for o in objs if o.object_type == "table"),
            "views": sum(1 for o in objs if o.object_type == "view"),
            "procedures": sum(1 for o in objs if o.object_type == "procedure"),
            "functions": sum(1 for o in objs if o.object_type == "function"),
        }


# ═══════════════════════════════════════════════
# MigrationState
# ═══════════════════════════════════════════════

def test_migration_state_create():
    state = MigrationState(migration_id="test-1")
    assert state.migration_id == "test-1"
    assert state.completed_steps == []
    assert state.current_step is None
    assert state.error is None
    assert state.is_complete is True


def test_migration_state_tracking():
    state = MigrationState(migration_id="test-2")
    assert not state.has_completed("project_loader")
    state.completed_steps.append("project_loader")
    assert state.has_completed("project_loader")
    state.log("project_loader", "Loaded")
    assert "Loaded" in state.step_logs["project_loader"]


def test_migration_state_is_complete():
    state = MigrationState(migration_id="test-3")
    assert state.is_complete is True
    state.current_step = "project_loader"
    assert state.is_complete is False
    state.error = "Something broke"
    assert state.is_complete is True


def test_agent_interface_protocol():
    class MyAgent:
        def run(self, state):
            state.completed_steps.append("done")
            return state

    agent = MyAgent()
    state = MigrationState(migration_id="proto-test")
    result = agent.run(state)
    assert result.has_completed("done")


# ═══════════════════════════════════════════════
# CatalogMappingEngine
# ═══════════════════════════════════════════════

def test_catalog_preserve_strategy():
    objs = [
        FakeObject(name="DB1.SCH1.TABLE1", object_type="table"),
        FakeObject(name="DB1.SCH1.VIEW1", object_type="view"),
        FakeObject(name="DB2.SCH2.TABLE2", object_type="table"),
    ]
    inv = FakeInventory(objs)
    engine = CatalogMappingEngine()
    result = engine.build_mapping(inv, strategy=CATALOG_STRATEGY_PRESERVE)

    assert result.strategy == CATALOG_STRATEGY_PRESERVE
    assert len(result.mappings) >= 2
    assert result.catalog_create_sql
    assert all("CREATE CATALOG IF NOT EXISTS" in sql for sql in result.catalog_create_sql)
    mapping_table = result.mapping_table
    assert len(mapping_table) >= 2


def test_catalog_merge_strategy():
    objs = [
        FakeObject(name="DB1.SCH1.TABLE1", object_type="table"),
        FakeObject(name="DB2.SCH1.TABLE2", object_type="table"),
    ]
    inv = FakeInventory(objs)
    engine = CatalogMappingEngine()
    result = engine.build_mapping(inv, strategy=CATALOG_STRATEGY_MERGE, merge_target_catalog="TARGET")

    assert result.strategy == CATALOG_STRATEGY_MERGE
    for m in result.mappings:
        assert m.databricks_catalog == "TARGET"


def test_catalog_rename_strategy():
    objs = [
        FakeObject(name="DB1.SCH1.TABLE1", object_type="table"),
    ]
    inv = FakeInventory(objs)
    engine = CatalogMappingEngine()
    result = engine.build_mapping(inv, strategy=CATALOG_STRATEGY_RENAME, rename_map={"DB1": "NEW_DB"})

    for m in result.mappings:
        if m.snowflake_database == "DB1":
            assert m.databricks_catalog == "NEW_DB"


def test_catalog_custom_strategy():
    objs = [
        FakeObject(name="DB1.SCH1.TABLE1", object_type="table"),
    ]
    inv = FakeInventory(objs)
    engine = CatalogMappingEngine()
    custom = [CatalogMapping(snowflake_database="DB1", databricks_catalog="CUSTOM_DB")]
    result = engine.build_mapping(inv, strategy=CATALOG_STRATEGY_CUSTOM, custom_mappings=custom)

    for m in result.mappings:
        if m.snowflake_database == "DB1":
            assert m.databricks_catalog == "CUSTOM_DB"


def test_catalog_ddl_generation():
    objs = [
        FakeObject(name="DB1.SCH1.TABLE1", object_type="table"),
    ]
    inv = FakeInventory(objs)
    engine = CatalogMappingEngine()
    result = engine.build_mapping(inv, strategy=CATALOG_STRATEGY_PRESERVE)

    assert any("CREATE CATALOG IF NOT EXISTS" in sql for sql in result.catalog_create_sql)
    assert any("CREATE SCHEMA IF NOT EXISTS" in sql for sql in result.schema_create_sql)


def test_catalog_map_result_mapping_table():
    m1 = CatalogMapping(snowflake_database="DB1", databricks_catalog="DB1_NEW",
                         snowflake_schema="SCH1", databricks_schema="SCH1_NEW")
    result = CatalogMapResult(mappings=[m1])
    table = result.mapping_table
    assert len(table) == 1
    assert table[0]["snowflake_database"] == "DB1"
    assert table[0]["databricks_catalog"] == "DB1_NEW"
    assert table[0]["snowflake_schema"] == "SCH1"
    assert table[0]["databricks_schema"] == "SCH1_NEW"


# ═══════════════════════════════════════════════
# AssessmentAgent
# ═══════════════════════════════════════════════

def test_assessment_counts():
    objs = [
        FakeObject(name="DB1.SCH1.T1", object_type="table"),
        FakeObject(name="DB1.SCH1.V1", object_type="view"),
        FakeObject(name="DB1.SCH1.P1", object_type="procedure"),
        FakeObject(name="DB1.SCH1.F1", object_type="function"),
    ]
    inv = FakeInventory(objs)
    agent = MigrationAssessmentAgent()
    report = agent.assess(inv)

    assert report.tables == 1
    assert report.views == 1
    assert report.procedures == 1
    assert report.functions == 1
    assert report.databases >= 1
    assert report.schemas >= 1


def test_assessment_summary_lines():
    report = AssessmentReport(tables=5, views=3, overall_confidence=85.0)
    lines = report.summary_lines()
    assert len(lines) > 10
    assert any("5" in l for l in lines)
    assert any("85" in l for l in lines)


def test_assessment_confidence_computation():
    objs = [FakeObject(name="DB1.SCH1.T1", object_type="table")]
    inv = FakeInventory(objs)
    agent = MigrationAssessmentAgent()
    report = agent.assess(inv)

    assert 0 <= report.overall_confidence <= 100
    assert report.migration_strategy in ("Automatic Migration", "LLM Assisted Migration")


def test_assessment_with_storage_and_capabilities():
    objs = [FakeObject(name="DB1.SCH1.T1", object_type="table")]
    inv = FakeInventory(objs)

    @dataclass
    class FakeStorageReport:
        total_tables: int = 1
        internal_tables: list = None
        external_tables: list = None
        iceberg_tables: list = None
        stages: list = None
        storage_integrations: list = None
        cloud_providers: set = None
        needs_export: bool = False
        needs_s3_credentials: bool = False
        summary: str = ""
        def __init__(self):
            self.internal_tables = [{"name": "t1"}]
            self.external_tables = []
            self.iceberg_tables = []
            self.stages = []
            self.storage_integrations = []
            self.cloud_providers = set()

    agent = MigrationAssessmentAgent()
    report = agent.assess(inv, storage_report=FakeStorageReport())
    assert report.internal_tables == 1


# ═══════════════════════════════════════════════
# ManifestGenerator
# ═══════════════════════════════════════════════

def test_manifest_generation():
    state = MigrationState(migration_id="manifest-test")
    state.step_timings = {"project_loader": 0.5}
    state.assessment_report = AssessmentReport(tables=10, overall_confidence=95.0, migration_strategy="Automatic")
    state.confidence_scores = [{"confidence_label": "Automatic"}]

    generator = ManifestGenerator()
    manifest = generator.generate(state)

    assert manifest.migration_id == "manifest-test"
    assert manifest.assessment["tables"] == 10
    assert manifest.assessment["overall_confidence"] == 95.0
    assert manifest.step_timings["project_loader"] == 0.5


def test_manifest_to_json():
    manifest = MigrationManifest(migration_id="json-test", total_objects=5)
    output = manifest.to_json()
    assert '"migration_id": "json-test"' in output
    assert '"total_objects": 5' in output
    parsed = json.loads(output)
    assert parsed["migration_id"] == "json-test"


def test_manifest_save(tmp_path):
    manifest = MigrationManifest(migration_id="save-test")
    p = tmp_path / "manifest.json"
    manifest.save(p)
    assert p.exists()
    loaded = json.loads(p.read_text())
    assert loaded["migration_id"] == "save-test"


def test_manifest_confidence_buckets():
    scores = [
        {"confidence_label": "Automatic"},
        {"confidence_label": "Automatic"},
        {"confidence_label": "LLM Assisted"},
    ]
    state = MigrationState(migration_id="bucket-test")
    state.confidence_scores = scores
    generator = ManifestGenerator()
    manifest = generator.generate(state)
    assert manifest.confidence_buckets["Automatic"] == 2
    assert manifest.confidence_buckets["LLM Assisted"] == 1


# ═══════════════════════════════════════════════
# PerformanceOptimizer
# ═══════════════════════════════════════════════

def test_performance_optimizer_table():
    obj = FakeObject(name="DB.SCH.TABLE1", object_type="table",
                     converted_sql="CREATE TABLE t1 (id INT)")
    inv = FakeInventory([obj])
    opt = PerformanceOptimizer()
    suggestions = opt.analyze(inv)

    types = {s.suggestion_type for s in suggestions}
    assert "optimize" in types
    assert "vacuum" in types


def test_performance_optimizer_view():
    obj = FakeObject(name="DB.SCH.VIEW1", object_type="view",
                     converted_sql="SELECT * FROM t1 WHERE EXISTS (SELECT 1)")
    inv = FakeInventory([obj])
    opt = PerformanceOptimizer()
    suggestions = opt.analyze(inv)

    assert any(s.suggestion_type == "incremental" for s in suggestions)


def test_performance_optimizer_zorder():
    obj = FakeObject(name="DB.SCH.TABLE1", object_type="table",
                     converted_sql="CREATE TABLE t1 (id INT) PARTITION BY id")
    inv = FakeInventory([obj])
    opt = PerformanceOptimizer()
    suggestions = opt.analyze(inv)

    assert any(s.suggestion_type == "zorder" for s in suggestions)


def test_performance_optimizer_report():
    suggestions = [
        OptimizationSuggestion(
            object_name="DB.SCH.T1", object_type="table",
            suggestion_type="optimize", sql="OPTIMIZE DB.SCH.T1",
            priority="high", reason="Performance",
        )
    ]
    opt = PerformanceOptimizer()
    report = opt.generate_report(suggestions)
    assert "OPTIMIZE" in report
    assert "high" in report


def test_performance_optimizer_empty_report():
    opt = PerformanceOptimizer()
    report = opt.generate_report([])
    assert "No suggestions" in report


# ═══════════════════════════════════════════════
# DeploymentAgent
# ═══════════════════════════════════════════════

def test_deploy_ordering():
    objects = [
        {"name": "F1", "object_type": "function"},
        {"name": "T1", "object_type": "table"},
        {"name": "V1", "object_type": "view"},
    ]
    agent = DeploymentAgent()
    results = agent.deploy(objects, creds={"db_hostname": "x", "db_http_path": "y", "db_token": "z"},
                           dry_run=True)

    assert len(results) == 3
    names = [r.object_name for r in results]
    tables = [n for n in names if n == "T1"]
    assert len(tables) == 1


def test_deploy_catalog_schema_ddl():
    objects = [{"name": "T1", "object_type": "table", "converted_sql": "CREATE TABLE t1 (id INT)"}]
    agent = DeploymentAgent()
    results = agent.deploy(
        objects,
        creds={"db_hostname": "x", "db_http_path": "y", "db_token": "z"},
        dry_run=True,
        catalog_ddl=["CREATE CATALOG IF NOT EXISTS test_catalog"],
        schema_ddl=["CREATE SCHEMA IF NOT EXISTS test_catalog.test_schema"],
    )
    assert len(results) == 3
    types = [r.object_type for r in results]
    assert "catalog" in types
    assert "schema" in types


def test_deploy_result_dataclass():
    r = DeployResult(object_name="T1", object_type="table", success=True, duration_ms=42)
    assert r.object_name == "T1"
    assert r.success is True
    assert r.duration_ms == 42


def test_rollback_sql():
    agent = DeploymentAgent()
    sql = agent._build_rollback_sql("T1", "table")
    assert sql == "DROP TABLE IF EXISTS T1"
    sql = agent._build_rollback_sql("V1", "view")
    assert sql == "DROP VIEW IF EXISTS V1"


def test_build_rollback_sql():
    agent = DeploymentAgent()
    sql = agent._build_rollback_sql("my_catalog", "catalog")
    assert sql == "DROP CATALOG IF EXISTS my_catalog"
    sql = agent._build_rollback_sql("my_schema", "schema")
    assert sql == "DROP SCHEMA IF EXISTS my_schema"
    sql = agent._build_rollback_sql("my_table", "table")
    assert sql == "DROP TABLE IF EXISTS my_table"
    sql = agent._build_rollback_sql("unknown", "unknown_type")
    assert sql is None


def test_dependency_checking():
    obj = {"name": "T1", "object_type": "table", "raw_sql": "CREATE TABLE T1 AS SELECT * FROM DEP1",
           "converted_sql": "CREATE TABLE T1 AS SELECT * FROM DEP1"}
    other = {"name": "DEP1", "object_type": "table", "raw_sql": "CREATE TABLE DEP1 (id INT)",
             "converted_sql": "CREATE TABLE DEP1 (id INT)"}
    agent = DeploymentAgent()
    results = agent.deploy([obj, other], creds={"db_hostname": "x", "db_http_path": "y", "db_token": "z"},
                           dry_run=True)
    assert len(results) == 2
    assert results[0].success is True


def test_dependency_three_part_name():
    obj = {"name": "DB1.SCH1.VIEW1", "object_type": "view",
           "raw_sql": "CREATE VIEW DB1.SCH1.VIEW1 AS SELECT * FROM DB1.SCH1.TABLE1",
           "converted_sql": "SELECT * FROM DB1.SCH1.TABLE1"}
    other = {"name": "DB1.SCH1.TABLE1", "object_type": "table", "raw_sql": "CREATE TABLE ...",
             "converted_sql": "CREATE TABLE ..."}
    agent = DeploymentAgent()
    results = agent.deploy([obj, other], creds={"db_hostname": "x", "db_http_path": "y", "db_token": "z"},
                           dry_run=True)
    assert len(results) == 2


# ═══════════════════════════════════════════════
# PlannerAgent
# ═══════════════════════════════════════════════

def test_planner_build_plan():
    objs = [
        FakeObject(name="DB1.SCH1.TABLE1", object_type="table", raw_sql="CREATE TABLE TABLE1 (id INT)"),
        FakeObject(name="DB1.SCH1.VIEW1", object_type="view", raw_sql="CREATE VIEW VIEW1 AS SELECT * FROM TABLE1"),
    ]
    inv = FakeInventory(objs)
    planner = PlannerAgent()
    plan = planner.build_plan(inv)

    assert isinstance(plan, MigrationPlan)
    assert plan.total_objects == 2
    assert plan.estimated_complexity in ("low", "medium", "high")
    assert len(plan.execution_order) == 2
    assert plan.catalog_mapping is not None


def test_planner_with_target_version():
    objs = [FakeObject(name="DB1.SCH1.T1", object_type="table")]
    inv = FakeInventory(objs)
    planner = PlannerAgent()
    plan = planner.build_plan(inv, target_version="latest")
    assert plan.target_version == "latest"


def test_planner_finds_blockers():
    objs = [FakeObject(name="DB1.SCH1.T1", object_type="table",
                        raw_sql="CREATE TABLE T1 CLONE T0")]
    inv = FakeInventory(objs)
    planner = PlannerAgent()
    plan = planner.build_plan(inv)
    assert len(plan.blockers) >= 1
    assert "CLONE" in plan.blockers[0]


def test_planner_complexity_estimation():
    simple = FakeObject(name="T1", object_type="table", raw_sql="SELECT 1")
    medium = FakeObject(name="T2", object_type="table", raw_sql="SELECT 1\n" * 40)
    complex = FakeObject(name="T3", object_type="table", raw_sql="SELECT 1\n" * 120)
    planner = PlannerAgent()
    assert planner._estimate_complexity(simple) == "low"
    assert planner._estimate_complexity(medium) == "medium"
    assert planner._estimate_complexity(complex) == "high"


def test_planner_sort_by_dependency():
    step_a = PlanStep(object_name="A", object_type="table")
    step_b = PlanStep(object_name="B", object_type="table", depends_on=["A"])
    steps = [step_b, step_a]
    planner = PlannerAgent()
    ordered = planner._sort_by_dependency(list(steps))
    assert ordered[0].object_name == "A"
    assert ordered[1].object_name == "B"


# ═══════════════════════════════════════════════
# Data Migration
# ═══════════════════════════════════════════════

class FakeMover(DataMover):
    def migrate(self, table_info, sf_creds, db_creds, storage_creds=None):
        return MigrateResult(table="test", storage_type="internal", strategy="copy",
                             rows=100, duration_ms=50, success=True)


def test_data_mover_base():
    mover = FakeMover()
    result = mover.migrate({}, {}, {})
    assert result.success is True
    assert result.rows == 100


def test_data_migration_manager():
    manager = DataMigrationManager()
    from dataclasses import dataclass

    @dataclass
    class FakeStorageReport:
        total_tables: int = 0
        internal_tables: list = None
        external_tables: list = None
        iceberg_tables: list = None
        needs_export: bool = False
        summary: str = ""
        def __post_init__(self):
            if self.internal_tables is None:
                self.internal_tables = []
            if self.external_tables is None:
                self.external_tables = []
            if self.iceberg_tables is None:
                self.iceberg_tables = []

    source_creds = {"sf_account": "test", "sf_user": "u", "sf_password": "p", "sf_warehouse": "w"}
    target_creds = {"db_hostname": "h", "db_http_path": "/sql", "db_token": "t"}
    results = manager.migrate(FakeStorageReport(), source_creds, target_creds)
    assert isinstance(results, list)
    assert len(results) == 0


def test_external_data_mover_no_location():
    mover = ExternalDataMover()
    from dataclasses import dataclass

    @dataclass
    class FakeTable:
        name: str = "T1"
        database: str = "DB1"
        schema: str = "SCH1"
        storage_location: str = ""
        cloud_provider: str = "aws"

    result = mover.migrate(FakeTable(), {}, {})
    assert isinstance(result, MigrateResult)
    assert not result.success


def test_external_data_mover_ddl():
    mover = ExternalDataMover()
    from dataclasses import dataclass

    @dataclass
    class FakeTable:
        name: str = "T1"
        database: str = "DB1"
        schema: str = "SCH1"

    ddl = mover._build_external_table_ddl(FakeTable(), "s3://bucket/path", "AWS")
    assert "AWS" in ddl
    assert "s3://bucket/path" in ddl


def test_internal_data_mover():
    mover = InternalDataMover()
    from dataclasses import dataclass

    @dataclass
    class FakeTable:
        name: str = "T1"
        database: str = "DB1"
        schema: str = "SCH1"
        storage_location: str = ""
        cloud_provider: str = "aws"

    result = mover.migrate(FakeTable(), {}, {})
    assert isinstance(result, MigrateResult)


def test_iceberg_data_mover():
    mover = IcebergDataMover()
    from dataclasses import dataclass

    @dataclass
    class FakeTable:
        name: str = "T1"
        database: str = "DB1"
        schema: str = "SCH1"
        storage_location: str = "s3://bucket/path"
        cloud_provider: str = "aws"

    from unittest.mock import patch
    with patch("connectors.databricks_connector.DatabricksConnector.deploy") as mock_deploy:
        result = mover.migrate(FakeTable(), {}, {})
        assert isinstance(result, MigrateResult)


# ═══════════════════════════════════════════════
# Orchestrator incremental + parallel
# ═══════════════════════════════════════════════

def test_compute_source_hashes():
    objs = [FakeObject(name="T1", raw_sql="SELECT 1")]
    inv = FakeInventory(objs)
    from orchestrator import MigrationOrchestrator
    orch = MigrationOrchestrator(project_path=".", output_dir="/tmp/test_orch")
    orch.inventory = inv
    hashes = orch.compute_source_hashes()
    assert "T1" in hashes
    assert len(hashes["T1"]) == 16


def test_changed_objects():
    objs = [FakeObject(name="T1", raw_sql="SELECT 1")]
    inv = FakeInventory(objs)
    from orchestrator import MigrationOrchestrator
    orch = MigrationOrchestrator(project_path=".", output_dir="/tmp/test_orch")
    orch.inventory = inv
    changed = orch.compute_changed_objects({"T1": "different_hash"})
    assert "T1" in changed


def test_unchanged_objects():
    objs = [FakeObject(name="T1", raw_sql="SELECT 1")]
    inv = FakeInventory(objs)
    from orchestrator import MigrationOrchestrator
    orch = MigrationOrchestrator(project_path=".", output_dir="/tmp/test_orch")
    orch.inventory = inv
    hashes = orch.compute_source_hashes()
    changed = orch.compute_changed_objects(hashes)
    assert len(changed) == 0


def test_incremental_check_no_previous():
    from orchestrator import MigrationOrchestrator
    orch = MigrationOrchestrator(project_path=".", output_dir="/tmp/test_orch")
    changed = orch.step_incremental_check(previous_state=None)
    assert changed == []


# ═══════════════════════════════════════════════
# Connectors base
# ═══════════════════════════════════════════════

def test_source_connector_abstract():
    from connectors.base import SourceConnector
    import inspect
    assert inspect.isabstract(SourceConnector)


def test_target_connector_abstract():
    from connectors.base import TargetConnector
    import inspect
    assert inspect.isabstract(TargetConnector)


def test_translator_abstract():
    from connectors.base import Translator
    import inspect
    assert inspect.isabstract(Translator)


def test_snowflake_connector_is_source():
    from connectors.snowflake_connector import SnowflakeConnector
    from connectors.base import SourceConnector
    assert issubclass(SnowflakeConnector, SourceConnector)


def test_databricks_connector_is_target():
    from connectors.databricks_connector import DatabricksConnector
    from connectors.base import TargetConnector
    assert issubclass(DatabricksConnector, TargetConnector)


# ═══════════════════════════════════════════════
# RunState resume helpers
# ═══════════════════════════════════════════════

def test_run_with_state():
    from orchestrator import MigrationOrchestrator
    state = MigrationState(migration_id="run-state-test")
    state.config["test_mode"] = True
    orch = MigrationOrchestrator(project_path=".", output_dir="/tmp/test_orch")
    assert orch is not None


def test_to_state():
    from orchestrator import MigrationOrchestrator
    orch = MigrationOrchestrator(project_path=".", output_dir="/tmp/test_orch")
    orch.source_hashes = {"T1": "abcdef1234567890"}
    state = orch.to_state()
    assert state.source_hashes == {"T1": "abcdef1234567890"}


def test_semi_structured_json_typeof():
    from agents.semi_structured_agent import SemiStructuredAgent
    from parser.sql_parser import ParsedObject as PO
    from pathlib import Path

    obj = PO(
        object_type="view",
        name="test_view",
        schema_name=None,
        raw_sql="SELECT JSON_TYPEOF(data) AS type FROM docs",
        file_path=Path("test.sql"),
        dependencies=[],
        cte_names=[],
    )
    obj.converted_sql = obj.raw_sql

    class FakeInventory:
        all_objects = [obj]

    agent = SemiStructuredAgent()
    results = agent.convert(FakeInventory(), strategy="native")
    assert len(results) == 1
    assert "JSON_TYPEOF" in results[0].functions_converted
    assert "LOWER(TYPEOF" in obj.converted_sql
    assert "JSON_TYPEOF" not in obj.converted_sql


def test_semi_structured_check_json():
    from agents.semi_structured_agent import SemiStructuredAgent
    from parser.sql_parser import ParsedObject as PO
    from pathlib import Path

    obj = PO(
        object_type="view",
        name="test_view",
        schema_name=None,
        raw_sql="SELECT CHECK_JSON(data) AS is_valid FROM docs",
        file_path=Path("test.sql"),
        dependencies=[],
        cte_names=[],
    )
    obj.converted_sql = obj.raw_sql

    class FakeInventory:
        all_objects = [obj]

    agent = SemiStructuredAgent()
    results = agent.convert(FakeInventory(), strategy="native")
    assert len(results) == 1
    assert "CHECK_JSON" in results[0].functions_converted
    assert "TRY_PARSE_JSON" in obj.converted_sql


def test_semi_structured_strip_null_value():
    from agents.semi_structured_agent import SemiStructuredAgent
    from parser.sql_parser import ParsedObject as PO
    from pathlib import Path

    obj = PO(
        object_type="view",
        name="test_view",
        schema_name=None,
        raw_sql="SELECT STRIP_NULL_VALUE(data) AS cleaned FROM docs",
        file_path=Path("test.sql"),
        dependencies=[],
        cte_names=[],
    )
    obj.converted_sql = obj.raw_sql

    class FakeInventory:
        all_objects = [obj]

    agent = SemiStructuredAgent()
    results = agent.convert(FakeInventory(), strategy="native")
    assert len(results) == 1
    assert "STRIP_NULL_VALUE" in results[0].functions_converted
    assert "REGEXP_REPLACE" in obj.converted_sql


def test_semi_structured_to_array():
    from agents.semi_structured_agent import SemiStructuredAgent
    from parser.sql_parser import ParsedObject as PO
    from pathlib import Path

    obj = PO(
        object_type="view",
        name="test_view",
        schema_name=None,
        raw_sql="SELECT TO_ARRAY(data) AS arr FROM docs",
        file_path=Path("test.sql"),
        dependencies=[],
        cte_names=[],
    )
    obj.converted_sql = obj.raw_sql

    class FakeInventory:
        all_objects = [obj]

    agent = SemiStructuredAgent()
    results = agent.convert(FakeInventory(), strategy="native")
    assert len(results) == 1
    assert "TO_ARRAY" in results[0].functions_converted
    assert "ARRAY(" in obj.converted_sql


def test_semi_structured_object_pick():
    from agents.semi_structured_agent import SemiStructuredAgent
    from parser.sql_parser import ParsedObject as PO
    from pathlib import Path

    obj = PO(
        object_type="view",
        name="test_view",
        schema_name=None,
        raw_sql="SELECT OBJECT_PICK(data, 'name', 'age') AS sub FROM docs",
        file_path=Path("test.sql"),
        dependencies=[],
        cte_names=[],
    )
    obj.converted_sql = obj.raw_sql

    class FakeInventory:
        all_objects = [obj]

    agent = SemiStructuredAgent()
    results = agent.convert(FakeInventory(), strategy="native")
    assert len(results) == 1
    assert "OBJECT_PICK" in results[0].functions_converted
    assert "NAMED_STRUCT" in obj.converted_sql


def test_semi_structured_object_delete():
    from agents.semi_structured_agent import SemiStructuredAgent
    from parser.sql_parser import ParsedObject as PO
    from pathlib import Path

    obj = PO(
        object_type="view",
        name="test_view",
        schema_name=None,
        raw_sql="SELECT OBJECT_DELETE(data, 'secret') AS cleaned FROM docs",
        file_path=Path("test.sql"),
        dependencies=[],
        cte_names=[],
    )
    obj.converted_sql = obj.raw_sql

    class FakeInventory:
        all_objects = [obj]

    agent = SemiStructuredAgent()
    results = agent.convert(FakeInventory(), strategy="native")
    assert len(results) == 1
    assert "OBJECT_DELETE" in results[0].functions_converted
    assert "DROP(" in obj.converted_sql


def test_semi_structured_noop():
    from agents.semi_structured_agent import SemiStructuredAgent
    from parser.sql_parser import ParsedObject as PO
    from pathlib import Path

    obj = PO(
        object_type="view",
        name="test_view",
        schema_name=None,
        raw_sql="SELECT id, name FROM users WHERE id = 1",
        file_path=Path("test.sql"),
        dependencies=[],
        cte_names=[],
    )
    obj.converted_sql = obj.raw_sql

    class FakeInventory:
        all_objects = [obj]

    agent = SemiStructuredAgent()
    results = agent.convert(FakeInventory(), strategy="native")
    assert len(results) == 0
    assert obj.converted_sql == obj.raw_sql


def test_rule_engine_json_typeof():
    from agents.rule_engine import apply_rules
    sql = "SELECT JSON_TYPEOF(data) AS type FROM docs"
    result = apply_rules(sql, "view")
    assert "LOWER(TYPEOF" in result
    assert "JSON_TYPEOF" not in result


def test_rule_engine_check_json():
    from agents.rule_engine import apply_rules
    sql = "SELECT CHECK_JSON(data) AS is_valid FROM docs"
    result = apply_rules(sql, "view")
    assert "TRY_PARSE_JSON" in result
    assert "CHECK_JSON" not in result


def test_rule_engine_strip_null_value():
    from agents.rule_engine import apply_rules
    sql = "SELECT STRIP_NULL_VALUE(data) AS cleaned FROM docs"
    result = apply_rules(sql, "view")
    assert "REGEXP_REPLACE" in result
    assert "STRIP_NULL_VALUE" not in result


def test_rule_engine_to_array():
    from agents.rule_engine import apply_rules
    sql = "SELECT TO_ARRAY(data) AS arr FROM docs"
    result = apply_rules(sql, "view")
    assert "ARRAY(" in result
    assert "TO_ARRAY" not in result


def test_js_agent_detects_javascript():
    from agents.js_to_python_udf_agent import JSPythonUDFAgent
    from parser.sql_parser import ParsedObject as PO
    from pathlib import Path

    js_obj = PO(
        object_type="function",
        name="my_func",
        schema_name=None,
        raw_sql="CREATE FUNCTION my_func(x VARCHAR) RETURNS VARCHAR LANGUAGE JAVASCRIPT AS $$ return x; $$",
        file_path=Path("test.sql"),
        dependencies=[],
        cte_names=[],
    )
    js_obj.converted_sql = None

    sql_obj = PO(
        object_type="function",
        name="my_sql_func",
        schema_name=None,
        raw_sql="CREATE FUNCTION my_sql_func(x VARCHAR) RETURNS VARCHAR LANGUAGE SQL AS $$ return x; $$",
        file_path=Path("test.sql"),
        dependencies=[],
        cte_names=[],
    )
    sql_obj.converted_sql = None

    class FakeInventory:
        all_objects = [js_obj, sql_obj]

    agent = JSPythonUDFAgent()
    assert agent._is_javascript(js_obj) is True
    assert agent._is_javascript(sql_obj) is False


def test_js_agent_extract_body():
    from agents.js_to_python_udf_agent import JSPythonUDFAgent

    agent = JSPythonUDFAgent()
    sql = "CREATE FUNCTION my_func() RETURNS VARCHAR LANGUAGE JAVASCRIPT AS $$\n  return 'hello';\n$$"
    body = agent._extract_js_body(sql)
    assert body is not None
    assert "return 'hello'" in body

    sql2 = "CREATE FUNCTION my_func() RETURNS VARCHAR LANGUAGE JAVASCRIPT AS $$ return x; $$"
    body2 = agent._extract_js_body(sql2)
    assert body2 is not None
    assert "return x" in body2


def test_js_agent_extract_params():
    from agents.js_to_python_udf_agent import JSPythonUDFAgent

    agent = JSPythonUDFAgent()
    sql = "CREATE FUNCTION my_func(name VARCHAR, age NUMBER, active BOOLEAN) RETURNS VARCHAR LANGUAGE JAVASCRIPT AS $$ return name; $$"
    params = agent._extract_params(sql)
    assert "name STRING" in params
    assert "age DOUBLE" in params
    assert "active BOOLEAN" in params


def test_js_agent_extract_returns():
    from agents.js_to_python_udf_agent import JSPythonUDFAgent

    agent = JSPythonUDFAgent()
    assert agent._extract_returns("CREATE FUNCTION f() RETURNS VARCHAR LANGUAGE JAVASCRIPT AS $$ return x; $$") == "STRING"
    assert agent._extract_returns("CREATE FUNCTION f() RETURNS NUMBER LANGUAGE JAVASCRIPT AS $$ return x; $$") == "DOUBLE"
    assert agent._extract_returns("CREATE FUNCTION f() RETURNS INT LANGUAGE JAVASCRIPT AS $$ return x; $$") == "INT"
    assert agent._extract_returns("CREATE FUNCTION f() LANGUAGE JAVASCRIPT AS $$ return x; $$") == ""


def test_js_agent_fallback():
    from agents.js_to_python_udf_agent import JSPythonUDFAgent

    agent = JSPythonUDFAgent()
    sql = "CREATE FUNCTION my_func(x VARCHAR) RETURNS VARCHAR LANGUAGE JAVASCRIPT AS $$ return x; $$"
    fallback = agent._generate_fallback(sql, "function")
    assert "CREATE OR REPLACE FUNCTION my_func()" in fallback
    assert "LANGUAGE PYTHON" in fallback
    assert "TODO" in fallback
    assert "MANUAL REVIEW REQUIRED" in fallback


def test_js_agent_clean_llm_output():
    from agents.js_to_python_udf_agent import JSPythonUDFAgent

    agent = JSPythonUDFAgent()
    assert agent._clean_llm_output("```python\nreturn x\n```") == "return x"
    assert agent._clean_llm_output("```\nreturn x\n```") == "return x"
    assert agent._clean_llm_output("return x") == "return x"
    assert agent._clean_llm_output("  return x  ") == "return x"


def test_js_agent_generate_function_ddl():
    from agents.js_to_python_udf_agent import JSPythonUDFAgent

    agent = JSPythonUDFAgent()
    sql = "CREATE OR REPLACE FUNCTION my_func(x VARCHAR, y NUMBER) RETURNS VARCHAR LANGUAGE JAVASCRIPT AS $$ return x; $$"
    ddl = agent._generate_function_ddl(sql, "return x.upper()")
    assert "CREATE OR REPLACE FUNCTION my_func(x STRING, y DOUBLE)" in ddl
    assert "RETURNS STRING" in ddl
    assert "LANGUAGE PYTHON" in ddl
    assert "return x.upper()" in ddl


def test_js_agent_generate_procedure_ddl():
    from agents.js_to_python_udf_agent import JSPythonUDFAgent

    agent = JSPythonUDFAgent()
    sql = "CREATE OR REPLACE PROCEDURE my_proc() LANGUAGE JAVASCRIPT AS $$ spark.sql('SELECT 1'); $$"
    ddl = agent._generate_procedure_ddl(sql, "spark.sql('SELECT 1')")
    assert "CREATE OR REPLACE PROCEDURE my_proc()" in ddl
    assert "LANGUAGE PYTHON" in ddl
    assert "SQL SECURITY INVOKER" in ddl
    assert "spark.sql('SELECT 1')" in ddl


def test_js_agent_no_llm_still_generates_fallback():
    from agents.js_to_python_udf_agent import JSPythonUDFAgent
    from parser.sql_parser import ParsedObject as PO
    from pathlib import Path

    obj = PO(
        object_type="function",
        name="my_func",
        schema_name=None,
        raw_sql="CREATE FUNCTION my_func(x VARCHAR) RETURNS VARCHAR LANGUAGE JAVASCRIPT AS $$ return x; $$",
        file_path=Path("test.sql"),
        dependencies=[],
        cte_names=[],
    )
    obj.converted_sql = None

    class FakeInventory:
        all_objects = [obj]

    agent = JSPythonUDFAgent()
    results = agent.convert(FakeInventory(), llm_config={"provider": "", "api_key": ""})
    assert len(results) == 1
    assert results[0].success is False
    assert results[0].error is not None
    assert "TODO" in obj.converted_sql
    assert "MANUAL REVIEW REQUIRED" in obj.converted_sql
