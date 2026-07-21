# Snowflake → Databricks Migration Agent v2.1

## Overview

Enterprise-grade migration platform that automates end-to-end migration of Snowflake databases to Databricks. The system discovers Snowflake objects via live connection or SQL file upload, transpiles DDL/DML to Databricks-compatible SQL through a 3-layer transpilation engine, validates correctness, runs LLM-based review, deploys objects to Databricks via the Statement Execution API, and migrates data through cloud storage (S3/Azure Blob/GCS).

**Repository**: [github.com/Sayannaskar1/migration](https://github.com/Sayannaskar1/migration)

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Web Framework | FastAPI (Python 3.14) |
| ASGI Server | Uvicorn |
| Templating | Jinja2 |
| SQL Parsing | SQLGlot (AST-based transpiler) |
| LLM Integration | Google Gemini 2.0+ (also OpenAI/Anthropic) |
| Encryption | Fernet (symmetric AES) |
| Database | SQLite (local persistence) |
| Snowflake Connector | snowflake-connector-python |
| Databricks Connector | databricks-sdk (Statement Execution API) |
| S3 Access | boto3 |
| Data Processing | Pandas, PyArrow, NumPy |
| Frontend | HTML/CSS (no JS framework), Inter font, responsive design |

**Requirements** (from `requirements.txt`):
- sqlglot>=25.0.0
- fastapi>=0.110.0, uvicorn>=0.29.0, python-multipart>=0.0.9
- python-dotenv>=1.0.0
- snowflake-connector-python>=3.12.0
- databricks-sql-connector>=3.5.0, databricks-sdk>=0.28.0
- google-generativeai>=0.8.0

---

## Project Structure

```
Migration-Agent/
  app.py                  # FastAPI web app: all routes, endpoints, run orchestration
  main.py                 # CLI entry point for headless migration
  orchestrator.py         # MigrationOrchestrator — 21-step pipeline manager
  database.py             # SQLite persistence layer (projects, runs, ddl_cache)
  Dockerfile & .dockerignore
  requirements.txt
  run.sh & terminate.sh
  queue.db                # Job queue SQLite (auto-created)
  .key                    # Fernet encryption key (auto-generated)
  .env                    # LLM configuration (LLM_PROVIDER, LLM_MODEL, LLM_API_KEY)

  agents/                          # === AI Agent Modules ===
    project_loader.py               # Load SQL project from file system or in-memory tree
    dependency_agent.py             # Dependency analysis, topological sort, cycle detection
    schema_agent.py                 # DDL conversion: 16 object types, UPDATE→MERGE transpiler
    sql_translation_agent.py        # Schema/object translation utilities
    sqlglot_transpiler.py           # SQLGlot AST-based batch transpilation
    lakebridge_transpiler.py        # LakeBridge custom SQLGlot dialects + Morpheus LSP transpiler
    rule_engine.py                  # Deterministic regex-based SQL conversion (100+ rules)
    capability_checker.py           # Feature detection: Snowflake-specific capabilities
    validation_agent.py             # Syntax validation, schema comparison, type checking
    confidence_engine.py            # Confidence scoring per object (Automatic/LLM/Manual)
    llm_transpiler.py               # LLM-based SQL transpilation (Gemini/OpenAI/Anthropic)
    llm_review_agent.py             # LLM review of all converted objects with post-processing
    assessment_agent.py             # Migration assessment report generation
    planner_agent.py                # Execution plan builder, catalog mapping
    catalog_mapping_engine.py       # Catalog/schema mapping strategies (preserve/merge/rename)
    storage_discovery_agent.py      # Snowflake storage analysis (internal/external/Iceberg tables)
    deployment_agent.py             # Deploy objects to Databricks with dependency resolution
    documentation_agent.py          # Migration report, inventory CSV, dependency diagram
    manifest_agent.py               # JSON migration manifest generator
    performance_optimizer.py        # OPTIMIZE/VACUUM/ZORDER recommendations
    self_healing_engine.py          # Retry failed conversions via regex/LLM fallback
    semi_structured_agent.py        # Semi-structured data (VARIANT/JSON) conversion
    js_to_python_udf_agent.py       # JavaScript UDF → Python UDF conversion (rule-based + LLM fallback)
    migration_state.py              # State management for resumable migrations
    data_migration_engine.py        # Top-level orchestrator for data migration

    platform/                       # === Platform Migration Strategies ===
      mapping_catalog.py            # Object type → target mapping catalog
      strategy_base.py              # Abstract base for strategy analyses
      strategies/                   # Individual strategy implementations
        policy_strategy.py          # Masking/row access policy strategies
        role_strategy.py            # Role migration strategy
        task_strategy.py            # Task → Job strategy
        warehouse_strategy.py       # Warehouse → SQL Warehouse strategy

    data_migration/                 # === Data Migration Subsystem ===
      mover_base.py                 # Abstract DataMover base class + MigrateResult dataclass
      data_migration_manager.py     # Routes tables to correct mover (internal/external/iceberg)
      internal_data_mover.py        # Export Snowflake tables → S3/Azure/GCS → load to Databricks
      external_data_mover.py        # Register Snowflake external tables as Databricks tables
      iceberg_data_mover.py         # Sync Snowflake Iceberg tables to Databricks

  connectors/                      # === Database Connectors ===
    base.py                        # Abstract SourceConnector/TargetConnector/Translator
    snowflake_connector.py          # Snowflake connection, DDL extraction, object listing
    databricks_connector.py         # Databricks SQL Warehouse Statement Execution API

  parser/                          # === SQL Parsing ===
    sql_parser.py                   # Regex-based SQL parser: split statements, identify object types
    ast_parser.py                   # SQLGlot AST parsing: syntax validation, feature detection, type mapping

  templates/                       # === Jinja2 Templates ===
    base.html                      # Base layout with sidebar, navigation, toast system
    index.html                     # Dashboard: project list, upload ZIP, Snowflake/DB connection form
    project.html                   # Project detail: edit credentials, view run history, run migration
    results.html                   # Migration results: conversion tables, SQL diff, deploy, data migration
    progress.html                  # Real-time progress tracking with polling
    history.html                   # All runs across all projects

  knowledge/                       # === Domain Knowledge ===
    databricks_rules.json           # Data type mappings, conversion rules, Delta Lake features
    snowflake_rules.json            # Snowflake-specific rules and unsupported features

  prompts/                         # === LLM Prompts ===
    translation_prompt.txt          # System prompt for LLM transpilation
    validation_prompt.txt           # System prompt for LLM validation

  tests/                           # === Test Suite (84 tests) ===
    test_agents.py                  # 72 tests for all agents
    test_transpiler.py              # 12 tests for transpilation rules
```

---

## Core Architecture & Data Flow

### 3-Layer Transpilation Engine

SQL conversion operates in three layers, applied sequentially:

```
Layer 1: SQLGlot (AST)        — 25% of conversions
  Parses SQL into AST, converts Snowflake dialect → Databricks dialect
  Handles: basic type mapping, JOIN/FROM syntax, basic DDL

Layer 2: Rule Engine (Regex)  — 70% of conversions
  ~100+ deterministic regex patterns applied in sequence
  Handles: type replacement, function mapping, DDL enhancement
  Covered: IFF→CASE, QUALIFY→subquery, LATERAL FLATTEN→VIEW EXPLODE,
           ARRAY_AGG→COLLECT_LIST, OBJECT_CONSTRUCT→NAMED_STRUCT,
           LISTAGG→CONCAT_WS, DATEDIFF rewrites, NVL2→CASE, DECODE→CASE,
           colon accessor→GET_JSON_OBJECT, PIVOT cleanup, UNIFORM→FLOOR(RAND())

Layer 3: LLM (Gemini/OpenAI/Anthropic) — 5% of conversions
  Used for: failed conversions (self-healing), low-confidence objects (review),
            complex procedures/functions that need semantic understanding
```

### 23-Step Migration Pipeline

The `MigrationOrchestrator.run()` method executes these steps in order:

```
 1. project_loader        — Load SQL objects from file system or in-memory tree
 2. dependency_analysis   — Build dependency graph, topological sort, detect cycles
 3. assessment            — MigrationAssessmentAgent: size, complexity, cost estimates
 4. storage_discovery     — StorageDiscoveryAgent: classify tables (internal/external/Iceberg)
 5. capability_check      — Detect Snowflake-specific capabilities/features per object
 6. plan                  — PlannerAgent: build execution plan with catalog mapping
 7. platform_analysis     — PlatformMigrationEngine: strategy analysis for platform objects (streams, tasks, pipes, stages, etc.)
 8. sqlglot_transpile     — Layer 1: SQLGlot AST transpilation + LakeBridge custom dialects
 9. lakebridge_transpile  — Layer 1b: Morpheus LSP transpiler (JAR-based, fallback when available)
10. rule_engine           — Layer 2: deterministic regex rules
11. semi_structured       — SemiStructuredAgent: handle VARIANT/JSON/XML
12. js_conversion         — JSPythonUDFAgent: JS→Python UDF conversion (rule-based + LLM fallback)
13. regex_cleanup         — SchemaAgent cleanup pass + SQL translation agent
14. confidence_scoring    — ConfidenceEngine: score each object (0.0–1.0)
15. llm_verify            — Layer 3a: LLM re-transpile objects with validation errors
16. validation            — ValidationAgent: syntax check, schema compare, Snowflake feature residual
17. llm_review            — Layer 3b: LLM review ALL objects (unconditional)
18. self_healing          — SelfHealingEngine: retry failed conversions (3 attempts)
19. deployment_approval   — Check blockers, auto-approve if set
20. deployment            — DeploymentAgent: deploy to Databricks with dep resolution
21. performance_optimizer — PerformanceOptimizer: OPTIMIZE/VACUUM suggestions
22. manifest              — ManifestGenerator: JSON migration manifest
23. documentation         — Generate report, inventory CSV, dependency diagram
```

### Web UI Pipeline (FastAPI)

The FastAPI app (`app.py`) uses a simplified 9-phase pipeline tracked in real-time:

```
Phase 1: Connect to Snowflake   — Authenticate, validate credentials, test connection
Phase 2: Extract DDL             — Discover schemas, extract tables/views/procedures/functions/
                                    streams/tasks/stages/pipes/file_formats/policies/sequences
Phase 3: Dependency Analysis     — Parse SQL, build AST, resolve references, detect cycles,
                                    determine execution order, generate dependency graph
Phase 4: Capability Analysis     — Detect unsupported features, JavaScript procedures,
                                    external functions, dynamic SQL, generate capability report
Phase 5: Translation             — Translate schemas/tables/views/functions/procedures,
                                    apply rule engine, convert semi-structured & JavaScript,
                                    regex cleanup, score confidence
Phase 6: AI Verification         — Prepare prompt, send to LLM, receive response,
                                    apply suggestions, generate confidence
Phase 7: Validation              — Validate SQL syntax, Databricks compatibility,
                                    dependencies, architecture, semantic validation
Phase 8: Self Healing            — Detect errors, classify, choose repair strategy,
                                    retry translation, validate repair
Phase 9: Generate Report         — Generate inventory, dependency graph, statistics,
                                    recommendations, summary, save report
```

Each step reports progress via a shared `progress` dict. The UI polls `/api/status/{run_id}` every 2 seconds.

---

## All Agents — Detailed Reference

### 1. ProjectLoader (`agents/project_loader.py`)
**Classes**: `ProjectInventory`
- Loads `.sql` files from a directory tree or in-memory dict
- Parses each file into `ParsedObject` instances (splits multi-statement files)
- Categorizes objects: schemas, tables, views, procedures, functions, external_tables, stages, materialized_views, sequences
- Supports loading from ZIP upload (in-memory tree mode)

### 2. DependencyAgent (`agents/dependency_agent.py`)
**Classes**: `DependencyGraph`
- Builds directed graph of object dependencies (FROM/JOIN/REFERENCES)
- Topological sort for deployment order
- Cycle detection
- Object-type-aware deployment ordering

### 3. SchemaAgent (`agents/schema_agent.py`)
**Functions**: `convert_schema`, `_convert_create_table_sql`, `_convert_create_schema_sql`, `_convert_create_view_sql`, `_convert_create_procedure_sql`, `_convert_create_function_sql`, `_convert_create_external_table_sql`, `_convert_create_stage_sql`, `_convert_create_materialized_view_sql`, `_convert_create_sequence_sql`, `_convert_create_masking_policy_sql`, `_convert_create_row_access_policy_sql`, `_convert_create_role_sql`, `_convert_create_stream_sql`, `_convert_create_pipe_sql`, `_convert_create_file_format_sql`, `_convert_create_task_sql`, `_convert_update_from_to_merge`, `_convert_procedure_body`
- Deterministic DDL-to-DDL conversion for 16 object types
- Key transpilations:
  - **Procedure**: `CREATE PROCEDURE` → Databricks SQL with `SQL SECURITY INVOKER`, `RETURN` → `SELECT` (migration note block), `SQLROWCOUNT` → `ROW_COUNT` comment, `LET` → `DECLARE`/`SET`, `UPDATE...FROM` → `MERGE INTO`, `TEMPORARY TABLE` → `TEMPORARY VIEW` (with migration note), triple-quote dynamic SQL concatenation fix (`''''''` → `'''`). `EXECUTE IMMEDIATE` does NOT trigger manual review. `EXECUTE AS OWNER` preserved as migration note only.
  - **Function**: `RETURNS NUMBER` → `RETURNS DECIMAL`, `VARCHAR(20)` → `STRING`, `AS 'body'` → `RETURN body`, JavaScript UDF → Python UDF (via rule-based translator or LLM)
  - **Table**: `AUTOINCREMENT` → IDENTITY, add `USING DELTA` with `TBLPROPERTIES(allowColumnDefaults)=true`, `CLUSTER BY` → `ZORDER BY`, `IDENTITY(start, inc)` → `GENERATED BY DEFAULT AS IDENTITY`, type conversion safety net (`VARCHAR`→`STRING`, `VARIANT`→`STRING`, `NUMBER`→`DECIMAL`, `TIMESTAMP_NTZ/LTZ/TZ`→`TIMESTAMP`, `FLOAT4/8/DOUBLE PRECISION/REAL`→`DOUBLE`), trailing content truncation, `NEXT VALUE FOR seq` → `DEFAULT seq.NEXTVAL` fix
  - **View**: `SECURE VIEW` → structured `Security Architecture Change` block, aggregate view optimization recommendations
  - **Stage**: Internal stages → managed `CREATE VOLUME`; external stages → cloud-specific `CREATE STORAGE CREDENTIAL` + `CREATE EXTERNAL LOCATION` + `CREATE EXTERNAL VOLUME` with `target_cloud` selector (AWS IAM_ROLE / Azure AZURE_MANAGED_IDENTITY / GCP GCP_SERVICE_ACCOUNT)
  - **Sequence**: `OR REPLACE` → `DROP SEQUENCE IF EXISTS` + `CREATE`, `START WITH` / `INCREMENT BY` preserved, `NOORDER` / `ORDER` → migration notes, `NEXTVAL` → `NEXT VALUE FOR`
  - **Masking Policy**: Metadata block (original name, return type, expression), `ALTER TABLE ... SET MASK` template, `Status: Successfully Converted` + `ACTION REQUIRED`
  - **Row Access Policy**: Same pattern as masking policy — metadata + `ALTER TABLE ... SET ROW FILTER`
  - **Stream**: Metadata block + `delta.enableChangeDataFeed = true` + `table_changes()` and Structured Streaming templates
  - **Pipe**: Metadata block + Auto Loader (`cloudFiles`) template; no `CREATE PIPE` DDL; `Status + Action Required` section
  - **Task**: Migration artifact (all comments), extracts schedule/warehouse/AFTER dependencies/WHEN condition; confidence labels (80% CRON, 50% stream-triggered)
  - **File Format**: Parses all properties into `_file_format_registry` dict, generates migration note with property mapping table, `FORMAT_OPTIONS` example, Spark options block; no blanket manual review
  - **External table**: → managed Delta table with architectural change note
  - **Schema**: Preserved as-is with catalog mapping prefix
  - **Role**: Preserved as-is

### 4. SQLTranslationAgent (`agents/sql_translation_agent.py`)
- Pipeline cleanup: schema name remapping, table reference rewriting across catalogs
- Integration with catalog mapping engine for database→catalog translation

### 5. SQLGlotTranspiler & LakeBridge (`agents/sqlglot_transpiler.py`, `agents/lakebridge_transpiler.py`)
- **SQLGlotTranspiler**: Batch transpilation via standard SQLGlot Snowflake→Databricks dialect; fallback preserves original SQL on parse failure
- **LakeBridge Transpiler**: Custom SQLGlot dialects from Databricks Labs LakeBridge project (loaded from `~/Desktop/forked/lakebridge/src` when available); globally monkey-patches sqlglot dialect registry
- **Morpheus LSP**: JAR-based transpiler (`databricks-morph-plugin.jar`) with LSP client support; used as optional secondary transpilation pass
- **`--` prefix handling**: LakeBridge may comment out CREATE statements with `--` when it encounters unsupported syntax — fallback strips `--` prefix from first line to recover DDL structure
- Pre-processing: `NUMBER AUTOINCREMENT` → `BIGINT GENERATED ALWAYS AS IDENTITY`, `TO_NUMBER` → `CAST(... AS DECIMAL)`, `TRY_PARSE_JSON` replacement

### 6. RuleEngine (`agents/rule_engine.py`)
- 619 lines of deterministic regex replacement rules
- Type mapping: 40+ Snowflake→Databricks type conversions (NUMBER→DECIMAL, VARCHAR→STRING, etc.)
- Function mapping: IFF, NVL2, DECODE, ARRAY_AGG, OBJECT_CONSTRUCT, LISTAGG, ZEROIFNULL, NULLIFZERO, TO_VARCHAR, TO_NUMBER, MONTHNAME, DAYNAME, ARRAY_SIZE, GET, FLATTEN, RANDOM, SEQUENCE, DATEDIFF, RATIO_TO_REPORT, NVL, UNIFORM, MINUS→EXCEPT
- DDL rules: add USING DELTA, CLUSTER BY→ZORDER BY, dummy column fallback
- Semi-structured: colon accessor `:` → `GET_JSON_OBJECT`, ARRAY access
- QUALIFY → windowed subquery wrapper
- LATERAL FLATTEN → LATERAL VIEW EXPLODE

### 7. CapabilityChecker (`agents/capability_checker.py`)
- Feature detection per object: flag Snowflake-specific features (MATCH_RECOGNIZE, PIVOT, CLONE, etc.)
- Capability summary: automatic/needs-review/unsupported buckets

### 8. ValidationAgent (`agents/validation_agent.py`)
**Classes**: `ValidationResult`
- Syntax validation via SQLGlot Databricks dialect parsing (skipped for procedural SQL with BEGIN/DECLARE/EXCEPTION and for architectural types)
- Schema match: column count comparison between source and target (with backtick retry)
- Feature residual detection: detect unconverted Snowflake features (filtered against false positive keywords)
- Type validation: catch `STRING(N)` or `BINARY(N)` (invalid in Databricks)
- Constraint warnings: PRIMARY KEY/FOREIGN KEY are informational in Unity Catalog
- JavaScript UDF detection: checks `converted_sql` (not `raw_sql`) to avoid false positives on successfully converted JS
- Architectural change detection: pipes, tasks, file_formats, streams, stages, masking_policies, row_access_policies, sequences skip SQL validation entirely
- Sequence-specific validation: checks START WITH/INCREMENT BY preservation, OR REPLACE adaptation, NOORDER/ORDER documentation
- Confidence computation: base 1.0, -0.1 per warning, -0.2 per issue, -0.3 per error

### 9. ConfidenceEngine (`agents/confidence_engine.py`)
**Classes**: `ConfidenceScore`
- Scores each converted object 0.0–1.0
- Thresholds: ≥0.95 = "Automatic", ≥0.80 = "LLM Assisted", <0.80 = "Manual Review"
- Deductions: no converted_sql (-0.5), procedure type (-0.15), unsupported features (-0.3), architectural changes (-0.2), MANUAL REVIEW markers (-0.4)

### 10. LLMTranspiler (`agents/llm_transpiler.py`)
- Supports three providers: Gemini, OpenAI, Anthropic
- Configurable via `LLM_PROVIDER`, `LLM_API_KEY`, `LLM_MODEL`, `LLM_API_BASE` env vars
- 43-rule system prompt for Snowflake→Databricks conversion
- Fallback for failed SQLGlot/rule conversions

### 11. LLMReviewAgent (`agents/llm_review_agent.py`)
**Classes**: `LLMReviewResult`
- Reviews ALL converted objects (unconditional, no filter)
- Structured JSON response: review_notes, suggested_fixes, improved_sql, confidence_adjustment
- Post-processing safety net: fix LLM regressions (SQL SECURITY DEFINER→INVOKER, RETURN→SELECT, double-quoted names→backtick)
- Runs on every object with converted_sql

### 12. AssessmentAgent (`agents/assessment_agent.py`)
**Classes**: `AssessmentReport`, `MigrationAssessmentAgent`
- Object inventory analysis (database/schema counts, object breakdown)
- Storage assessment (internal/external/Iceberg)
- Capability assessment (feature compatibility)
- Runtime and cost estimation
- Migration strategy recommendation (Automatic / LLM Assisted / Manual Review)

### 13. PlannerAgent (`agents/planner_agent.py`)
**Classes**: `PlanStep`, `MigrationPlan`, `PlannerAgent`
- Builds ordered execution plan per deployment order
- Catalog mapping integration (preserve/merge/rename strategies)
- Blocker detection (CLONE, JavaScript UDFs)
- Complexity estimation per object (lines of SQL)

### 14. CatalogMappingEngine (`agents/catalog_mapping_engine.py`)
- Strategies: preserve (1:1 mapping), merge (multiple Snowflake DBs → single catalog), rename (explicit mapping)
- Generates `CREATE CATALOG IF NOT EXISTS` and `CREATE SCHEMA IF NOT EXISTS` DDL

### 15. StorageDiscoveryAgent (`agents/storage_discovery_agent.py`)
**Classes**: `TableStorageInfo`, `StageInfo`, `StorageReport`, `StorageDiscoveryAgent`
- Queries Snowflake SHOW TABLES / SHOW STAGES / SHOW STORAGE INTEGRATIONS
- Classifies tables: internal (needs export), external (direct register), Iceberg (sync format)
- Detects cloud provider from storage location (AWS/Azure/GCS)

### 16. DeploymentAgent (`agents/deployment_agent.py`)
**Classes**: `DeployResult`, `DeploymentAgent`
- Deploys to Databricks SQL Warehouse via Statement Execution API
- Object ordering: catalog → schema → table → view → function → procedure (up to 20 passes)
- Multi-pass dependency resolution: objects wait 3 retries for dependencies
- Dry-run mode: validates without executing
- Rollback: generates DROP statements in reverse order
- String literal stripping in dependency extraction to avoid false positives

### 17. DocumentationAgent (`agents/documentation_agent.py`)
- Generates comprehensive text report with per-object details
- Inventory CSV export
- Dependency diagram (text-based)
- Stat box with 5 mutually exclusive categories: Auto Converted, Manual Review, Architectural, Issues, Failed
- Bug fix: `manual_review_count` reference corrected to `manual_review` (variable name)

### 18. ManifestAgent (`agents/manifest_agent.py`)
**Classes**: `MigrationManifest`, `ManifestGenerator`
- JSON manifest with full migration metadata
- Contains: assessment, storage_summary, plan, validation, confidence buckets, timings

### 19. PerformanceOptimizer (`agents/performance_optimizer.py`)
**Classes**: `OptimizationSuggestion`, `PerformanceOptimizer`
- Suggests: OPTIMIZE, VACUUM, ZORDER BY, auto-optimize, materialized views
- Priority-based recommendations (high/medium/low)

### 20. SelfHealingEngine (`agents/self_healing_engine.py`)
**Classes**: `HealingResult`, `SelfHealingEngine`
- 3 attempts max, progressive strategies: LLM assisted → regex cleanup → LLM fallback
- Only activates for objects without converted_sql or with MANUAL REVIEW markers

### 21. SemiStructuredAgent (`agents/semi_structured_agent.py`)
- Converts VARIANT/OBJECT handling to Databricks native JSON support
- Strategy options: "native" (use Databricks JSON functions), "struct" (use STRUCT type)

### 22. JSPythonUDFAgent (`agents/js_to_python_udf_agent.py`)
- Converts JavaScript UDFs to Python UDFs for Databricks using a two-phase approach:
  1. **Rule-based translator** (`_rule_based_translate`): Attempts deterministic JS→Python conversion first. Handles: null checks, `===`/`!==`→`==`/`!=`, `&&`/`||`→`and`/`or`, string methods (`.toLowerCase()`/`.trim()`/`.toUpperCase()`/`.length`/`.indexOf()`/`.includes()`/`.slice()`/`.replace()`/`.split()`/`.join()`/`.push()`), Math methods, parseInt/parseFloat, ternary `x ? y : z`, if/else if/else, object/array literal syntax
  2. **LLM fallback**: Complex patterns (snowflake APIs, eval, classes, async, prototypes, etc.) fall back to Gemini/OpenAI/Anthropic
- `_extract_params()` fallback regex uses `[^\s(]+` to prevent gobbling the opening parenthesis
- `_ensure_imports()` auto-injects required Python modules (math, json, etc.)
- Status tracking: converted/failed per object

### 23. DataMigrationEngine (`agents/data_migration_engine.py`)
- Top-level orchestrator: passes storage report + credentials to DataMigrationManager

### 24. DataMigrationManager (`agents/data_migration/data_migration_manager.py`)
- Routes tables: external → ExternalDataMover, iceberg → IcebergDataMover, internal → InternalDataMover
- Passes `cloud_provider` override for internal tables

### 25. InternalDataMover (`agents/data_migration/internal_data_mover.py`)
**Classes**: `InternalDataMover`
- 3 cloud providers: AWS (S3), Azure (ABFSS), GCS
- AWS flow: `CREATE STAGE` on Snowflake → `COPY INTO` unloads Parquet to S3 → Python fallback reads S3 Parquet → writes to Databricks via pandas 1000-row batches
- Azure flow: similar with ABFSS SAS token credentials
- GCS flow: similar with GCS storage integration
- Pre-check: verify Databricks target table exists before exporting
- Row count verification post-load
- Batch INSERT with 3 retries, 1000 rows per batch

### 26. ExternalDataMover (`agents/data_migration/external_data_mover.py`)
- Registers Snowflake external tables in Databricks via `CREATE TABLE ... USING ... LOCATION '...'`

### 27. IcebergDataMover (`agents/data_migration/iceberg_data_mover.py`)
- Syncs Iceberg tables: `SYNC FROM ICEBERG TABLE`

### 28. PlatformMigrationEngine (`agents/platform/`)
- Strategy-based analysis for platform-level objects (streams, tasks, pipes, stages, warehouses, masking/row access policies, roles, file formats, security integrations)
- Each object type has a strategy class that recommends target service, generates deployment SQL, and estimates automation percentage
- Integration with planner agent and documentation report
- Used in pipeline step 7 (`platform_analysis`) before transpilation

---

## FastAPI Web App — API Endpoints

### UI Routes (HTML)
| Route | Method | Description |
|-------|--------|-------------|
| `/` | GET | Dashboard — project list + create form |
| `/project/{id}` | GET | Project detail page |
| `/results/{run_id}` | GET | Migration results with review/approval/deploy UI |
| `/progress/{run_id}` | GET | Real-time progress page |
| `/history` | GET | All runs across all projects |

### REST API Endpoints
| Route | Method | Description |
|-------|--------|-------------|
| `/health` | GET | Health check (status, version, service name) |
| `/api/projects` | GET | List all projects (decrypted) |
| `/api/projects/save` | POST | Create project (encrypts secrets before save) |
| `/api/projects/update` | POST | Update project (decrypts existing, merges, re-encrypts) |
| `/api/projects/{id}` | GET | Get single project |
| `/api/projects/{id}/credentials` | GET | Get decrypted credentials only |
| `/api/projects/{id}` | DELETE | Delete project |
| `/api/projects/start-migration` | POST | Start migration → enqueue + background thread |
| `/api/test-snowflake` | POST | Test Snowflake connection |
| `/api/test-databricks` | POST | Test Databricks connection |
| `/api/status/{run_id}` | GET | Get run status (progress, steps, conversions) |
| `/api/runs` | GET | List all runs |
| `/api/run/{run_id}/conversions` | GET | Get conversions for a run |
| `/api/cancel/{run_id}` | POST | Cancel running migration |
| `/api/deploy/{run_id}` | POST | Deploy approved conversions to Databricks |
| `/api/deploy-results/{run_id}` | GET | Get deployment results |
| `/api/discover-storage/{run_id}` | POST | Run storage discovery on Snowflake for run |
| `/api/save-s3-creds/{run_id}` | POST | Save cloud storage credentials for data migration |
| `/api/data-migrate/{run_id}` | POST | Run data migration (triggers export + load) |
| `/api/data-migration-results/{run_id}` | GET | Get data migration results |
| `/api/review/approve/{run_id}` | POST | Approve a conversion object |
| `/api/review/reject/{run_id}` | POST | Reject a conversion object |
| `/api/review/update-sql/{run_id}` | POST | Manually edit approved SQL for an object |
| `/api/rollback/{run_id}` | POST | Rollback deployed objects |
| `/api/migration-summary/{run_id}` | GET | Get migration summary |
| `/api/project-runs/{project_id}` | GET | Get all runs for a project |
| `/api/progress/eta/{run_id}` | GET | Get ETA for running migration |

### Key App Configuration (app.py)
- `app.title`: "Snowflake to Databricks Migration Agent"
- `app.version`: "2.1.0"
- `_SECRET_FIELDS`: `{sf_password, db_token, s3_access_key, s3_secret_key, azure_sas_token, gcs_service_account}`
- `PROGRESS_TOTAL`: 9 (phases in UI migration)
- `_RUNS_TTL`: 6 hours (in-memory cache TTL)
- `_RUNS_MAX`: 200 (max cached runs)

### Credential Security Model
1. Fernet key auto-generated at `Migration-Agent/.key` on first run
2. `_encrypt()` / `_decrypt()` wrap Fernet operations
3. `_secure_creds()` encrypts all `_SECRET_FIELDS` values in a dict
4. `_restore_creds()` decrypts them (skips non-encrypted values like `dapi*` tokens)
5. Projects encrypted on `save_project` and `update_project`
6. Runs encrypted via `_store_run()` (in-memory cache keeps plaintext, DB stores encrypted)
7. `_load_projects()` decrypts on load via `_restore_creds()`
8. Runs decrypted via `_get_run()` → `_restore_creds()`

---

## Database Schema

### `projects` table (SQLite)
Stores project configurations with encrypted credentials.

| Column | Type | Description |
|--------|------|-------------|
| id | TEXT PK | Auto-increment integer as string |
| name | TEXT | Project name |
| description | TEXT | Project description |
| sf_account | TEXT | Snowflake account identifier |
| sf_user | TEXT | Snowflake username |
| sf_password | TEXT | Encrypted password |
| sf_warehouse | TEXT | Snowflake warehouse |
| sf_role | TEXT | Snowflake role |
| sf_database | TEXT | Snowflake database (optional filter) |
| sf_schema | TEXT | Snowflake schema (optional filter) |
| db_hostname | TEXT | Databricks server hostname |
| db_http_path | TEXT | SQL Warehouse HTTP path |
| db_token | TEXT | Encrypted PAT token |
| db_catalog | TEXT | Databricks catalog |
| db_schema | TEXT | Databricks schema |
| target_cloud | TEXT | Target cloud provider (aws/azure/gcp) for stage credential generation |
| created_at | REAL | Unix timestamp |
| updated_at | REAL | Unix timestamp |

### `runs` table (SQLite)
Stores migration run state with full results.

| Column | Type | Description |
|--------|------|-------------|
| id | TEXT PK | Run ID (12-char hex) |
| project_id | TEXT | Foreign key to projects |
| sf_account/user/password/warehouse/role/database/schema | TEXT | Snapshot of Snowflake creds at run time |
| db_hostname/http_path/token/catalog/schema | TEXT | Snapshot of Databricks creds |
| s3_bucket/region/access_key/secret_key/iam_role/storage_integration | TEXT | Cloud storage credentials |
| done | INTEGER | Boolean: migration complete |
| error | TEXT | Error message if failed |
| progress | TEXT | JSON progress dict |
| cancel | INTEGER | Boolean: cancel requested |
| completed_steps | TEXT | JSON list of completed step names |
| conversions | TEXT | JSON array of conversion objects |
| summary | TEXT | JSON migration summary |
| report | TEXT | Full text report |
| deploy_allowed | INTEGER | Boolean: ready for deploy |
| refresh | INTEGER | Boolean: re-extract DDL |
| plan | TEXT | JSON migration plan |
| confidence_scores | TEXT | JSON confidence scores |
| storage_report | TEXT | JSON storage report |
| catalog_ddl | TEXT | JSON catalog DDL list |
| schema_ddl | TEXT | JSON schema DDL list |
| deploy_results | TEXT | JSON deployment results |
| data_migration_results | TEXT | JSON data migration results |
| tmp_dir | TEXT | Temp working directory path |
| created_at / updated_at | REAL | Timestamps |

### `ddl_cache` table
| Column | Type | Description |
|--------|------|-------------|
| creds_hash | TEXT PK | SHA-256 hash of Snowflake creds |
| project_tree | TEXT | JSON object tree (DDL by path) |
| cached_at | REAL | Cached timestamp |

### `migration_queue` table (separate queue.db)
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment |
| run_id | TEXT UNIQUE | Foreign key to runs |
| status | TEXT | pending / running / completed / failed |
| created_at / started_at / finished_at | REAL | Timestamps |

---

## Connectors

### SnowflakeConnector (`connectors/snowflake_connector.py`)
- Connection: snowflake.connector with configurable account, warehouse, role, database, schema
- Session settings: `STATEMENT_TIMEOUT_IN_SECONDS = 600`
- DDL extraction via `GET_DDL()` for each object type
- Object listing: `SHOW DATABASES`, `SHOW SCHEMAS`, `SHOW TABLES`, `SHOW VIEWS`, `SHOW PROCEDURES`, `SHOW FUNCTIONS`, `SHOW EXTERNAL TABLES`, `SHOW STAGES`, `SHOW MATERIALIZED VIEWS`, `SHOW SEQUENCES`
- `extract_project()`: full DDL extraction into `{path: ddl_string}` tree dict
- Safe name handling: strips quotes, replaces special chars

### DatabricksConnector (`connectors/databricks_connector.py`)
- Connection: databricks-sdk `WorkspaceClient` + Statement Execution API
- Hostname cleaning: strips protocol and path components
- Warehouse ID extraction from HTTP path via regex
- `execute_sql()`: runs SQL through Statement Execution, returns structured result dict
- `deploy()`: executes list of SQL statements, respects `on_error="stop"`, skips `MANUAL REVIEW` objects
- `test_connection()`: validates hostname format, HTTP path, token length; runs `SELECT 1`
- Catalog/schema extraction from SQL DDL for context-awareness

---

## Data Migration System

### Flow
```
Snowflake Table
    │
    ▼
Snowflake COPY INTO Stage (Parquet format)
    │
    ▼
Cloud Storage (S3 / Azure Blob / GCS)
    │
    ▼
Python Fallback: boto3 → PyArrow → pandas → Databricks INSERT (1000-row batches)
    │
    ▼
Databricks Delta Table
```

### Cloud Provider Support
| Provider | URL Scheme | Credentials |
|----------|-----------|-------------|
| AWS S3 | `s3://bucket/...` | Access Key + Secret Key, or IAM Role, or Storage Integration |
| Azure Blob | `abfss://container@account.dfs.core.windows.net/...` | SAS Token |
| Google GCS | `gcs://bucket/...` | Storage Integration (service account) |

### Key Implementation Details
- `_sanitize()`: strips brackets, quotes, whitespace from all credential values
- `_sql_literal()`: converts Python values to SQL literals (handles NaN, Inf, datetime, Decimal, numpy types)
- `_verify_row_count()`: `SELECT COUNT(*)` post-load validation
- Batch loading: 1000 rows per INSERT, 3 retries per batch, exponential backoff
- Target table creation: inferred schema from PyArrow Parquet schema

---

## Migration States & Review/Approval Workflow

### Conversion Object State
Each converted object has these review fields:
- `review_status`: `"pending_review"` | `"approved"` | `"rejected"`
- `approved_sql`: manually editable approved SQL (defaults to converted_sql)
- `reviewer_notes`: human or LLM notes

### Deployment Gate
- Schemas are auto-approved
- Only objects with `review_status == "approved"` are deployed
- Deploy button only active when at least one object is approved
- `deploy_allowed` flag on run must be true

### Deployment Flow
1. Create catalog DDL → execute
2. Create schema DDL → execute
3. Iterate objects in order: table → view → function → procedure
4. Multi-pass dependency resolution (up to 20 passes)
5. 3 retries per object
6. `MANUAL REVIEW` markers skip deployment

---

## Configuration & Environment

### `.env` File
```
LLM_PROVIDER=gemini
LLM_MODEL=gemini-2.5-flash
LLM_API_KEY=your_key_here
```

Alternative env vars: `GEMINI_API_KEY`, `LLM_CONFIG` (path to JSON config file), `AUTO_DEPLOY_APPROVE=1` (auto-approve deployment), `DATABRICKS_HOST`, `DATABRICKS_TOKEN`

### Encryption Key
- Auto-generated at `Migration-Agent/.key`
- Falls back to plaintext if `cryptography` is not installed
- Used for all credential fields across projects and runs

### Startup
```bash
cd Migration-Agent
uvicorn app:app --reload --host 0.0.0.0 --port 8001
```
Or via `run.sh` which activates the virtual environment and starts the server on port 8001.

---

## Testing (152 tests)

### `tests/test_agents.py` (72 tests)
Tests for: orchestrator pipeline, project loader, SQL parser, schema agent, dependency analysis, rule engine, validation, confidence scoring, LLM transpiler, self-healing, storage discovery, deployment, data migration (internal/external), capability checker, planner, catalog mapping, performance optimizer, semi-structured agent, JS→Python conversion, manifest generation, data movers (internal/external/iceberg)

### `tests/test_transpiler.py` (80 tests)
Tests for: DDL conversion (all object types), UPDATE→MERGE transpiler, procedure body conversion, function conversion, EXECUTE AS → SQL SECURITY INVOKER, TEMPORARY TABLE → TEMPORARY VIEW, SQLGlot transpile, rule engine function mapping, DATEDIFF conversion, IFF→CASE, QUALIFY→subquery, sequence conversion (OR REPLACE, START WITH, INCREMENT BY, NOORDER), nextval/currval, identity detection, constraint validation, architecture change classification

Run with: `python -m pytest tests/ -v`

---

## Key Design Decisions

1. **3-layer transpilation**: SQLGlot handles 25%, Rule Engine handles 70%, LLM handles 5% — balances speed vs accuracy
2. **In-memory plaintext cache**: `_runs` dict keeps decrypted credentials to avoid double-encryption; DB stores encrypted
3. **Multi-pass deployment**: enables circular dependency resolution without deadlock (max 20 passes, 3 retries per object)
4. **Python fallback for S3→Databricks**: SQL Warehouse COPY INTO requires Unity Catalog external locations (not available), so pandas-based INSERT is used
5. **Procedure post-processing safety net**: after LLM review, regex fixes known regressions (SQL SECURITY DEFINER, RETURN '', double-quoted names)
6. **Deterministic UPDATE→MERGE transpiler**: Regex-based, handles nested parens in subqueries, alias extraction
7. **Unconditional LLM review**: removed conditional filtering — reviews every object with converted_sql
8. **9-step UI progress**: simplified from 23-step pipeline for real-time web tracking
9. **Thread-local SQLite connections**: prevents threading issues in FastAPI async context
10. **Job queue with queue.db**: serializes migrations to prevent concurrent Snowflake/Databricks conflicts
11. **LakeBridge global monkey-patch**: LakeBridge import globally patches sqlglot dialect registry — `--` prefix stripping added to recover DDL structure from unsupported syntax
12. **Target Cloud parameter**: `target_cloud` (aws/azure/gcp) passed through creds → orchestrator → `convert_schema()` for cloud-specific stage credential generation
13. **Sequence skip in SQLGlot transpilation**: sequences added to `_PLATFORM_SKIP_TYPES` to prevent LakeBridge from stripping `START WITH`/`INCREMENT BY`; custom converter handles them instead
14. **Mutually exclusive status categories**: `Auto Converted`, `Manual Review`, `Architectural`, `Issues`, `Failed` — each counted once in report summary; old-run backward compatibility via `_patch_summary`

---

## UI Features

- **Dashboard**: Saved project list with quick-load, ZIP upload form, Snowflake & Databricks credential forms
- **Project Detail**: Edit credentials, enable/disable Databricks, run migration, view run history
- **Progress Page**: Real-time step tracking with polling, cancel button
- **Results Page**: Tabular conversion list, SQL diff viewer, review/approve/reject per object, deploy controls, cloud storage credential tabs (AWS/Azure/GCS), data migration controls
- **History Page**: Cross-project run history with filtering
- **Templates restored to original state**: UI changes (dark mode, expand/collapse, download report) were reverted per user request; templates are at original commit state

### JavaScript Functions
- Tab system for cloud providers (AWS/Azure/GCS)
- Real-time progress polling (`setInterval`, 2s)
- Copy-to-clipboard for SQL code blocks
- Toast notification system
- Modal for project save
- Responsive sidebar toggle
- Dynamic Databricks form show/hide

---

## Error Handling

- All API endpoints return JSON with `ok: true/false` and descriptive error messages
- Migration errors captured per step, stored in run state
- Database operations use WAL mode with busy timeout for concurrent safety
- LLM calls wrapped in try/except with fallback to current state
- Deployment retries 3x per object, dependency retries across 20 passes
- Connection testing before any migration/discovery operation
- Snowflake session timeout set to 600 seconds
- Cache invalidation via DDL cache hash based on Snowflake credentials
