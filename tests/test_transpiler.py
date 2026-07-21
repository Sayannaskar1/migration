"""
QA test suite for Snowflake→Databricks transpilation pipeline.
Tests every feature pattern, DDL variant, and edge case.
"""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from agents.rule_engine import apply_rules
from agents.schema_agent import convert_schema
from agents.sql_translation_agent import translate_sql
from agents.sqlglot_transpiler import transpile_snowflake
from parser.ast_parser import detect_snowflake_features, validate_sql_syntax
from parser.sql_parser import ParsedObject
from agents.project_loader import ProjectInventory
from agents.validation_agent import validate_object
from orchestrator import MigrationOrchestrator, preprocess_raw

PASS = 0
FAIL = 0
TOTAL = 0
FAILURES = []


def _make_obj(raw_sql, obj_type="table", name="test_obj"):
    obj = ParsedObject(
        object_type=obj_type,
        name=name,
        schema_name=None,
        raw_sql=raw_sql,
        file_path=Path("test.sql"),
        dependencies=[],
        cte_names=[],
    )
    obj.converted_sql = raw_sql
    return obj


def run_pipeline(sql, obj_type="table"):
    """Run the full transpilation pipeline on a single SQL statement."""
    obj = _make_obj(sql, obj_type)
    if obj_type not in ("procedure", "sequence") and "LANGUAGE JAVASCRIPT" not in (obj.raw_sql or "").upper():
        cleaned = preprocess_raw(sql)
        result = transpile_snowflake(cleaned)
        if result:
            obj.converted_sql = result
    obj.converted_sql = apply_rules(obj.converted_sql, obj.object_type)
    obj.converted_sql = convert_schema(obj)
    return obj.converted_sql


def do_validate(sql, obj_type="table"):
    """Run full pipeline + validation, return (converted_sql, validation_result)."""
    obj = _make_obj(sql, obj_type)
    if obj_type not in ("procedure", "sequence") and "LANGUAGE JAVASCRIPT" not in (obj.raw_sql or "").upper():
        cleaned = preprocess_raw(sql)
        result = transpile_snowflake(cleaned)
        if result:
            obj.converted_sql = result
    obj.converted_sql = apply_rules(obj.converted_sql, obj.object_type)
    obj.converted_sql = convert_schema(obj)
    inv = ProjectInventory(Path("/tmp"))
    inv.all_objects.append(obj)
    val = validate_object(obj, inv)
    return obj.converted_sql, val


def check(name, snowflake_sql, expected_contains=None, expected_not_contains=None,
          obj_type="table", expected_status=None):
    global PASS, FAIL, TOTAL
    TOTAL += 1
    try:
        result = run_pipeline(snowflake_sql, obj_type)
    except Exception as e:
        FAIL += 1
        msg = f"  FAIL [{name}] Exception: {e}"
        FAILURES.append(msg)
        print(msg)
        return

    issues = []
    if result is None:
        issues.append("Result is None")
        FAIL += 1
        FAILURES.append(f"  FAIL [{name}] Result is None")
        print(f"  FAIL [{name}] Result is None")
        return

    if expected_contains:
        for exp in expected_contains:
            if not re.search(exp, result, re.IGNORECASE):
                issues.append(f"Missing: /{exp}/")

    if expected_not_contains:
        for exp in expected_not_contains:
            if re.search(exp, result, re.IGNORECASE):
                issues.append(f"Found unexpected: /{exp}/")

    if expected_status:
        _, val = do_validate(snowflake_sql, obj_type)
        if val.status != expected_status:
            issues.append(f"Expected status {expected_status}, got {val.status}")

    if issues:
        FAIL += 1
        msg = f"  FAIL [{name}]"
        for iss in issues:
            msg += f"\n         {iss}"
        FAILURES.append(msg)
        print(msg)
        print(f"         Result:\n{result[:500]}")
    else:
        PASS += 1
        print(f"  PASS [{name}]")


def section(title):
    print(f"\n{'='*70}\n  {title}\n{'='*70}")


def test_ddl():
    section("DDL: TABLES")

    check("basic_table",
        "CREATE TABLE customers (id NUMBER, name VARCHAR(100), active BOOLEAN)",
        [r"USING DELTA", r"DECIMAL", r"STRING", r"BOOLEAN"])

    check("create_or_replace_table",
        "CREATE OR REPLACE TABLE orders (id NUMBER, total DECIMAL(10,2))",
        [r"USING DELTA", r"DECIMAL"])

    check("table_with_identity",
        "CREATE TABLE users (id NUMBER AUTOINCREMENT, email VARCHAR(255))",
        [r"IDENTITY", r"STRING"],
        [r"AUTOINCREMENT"], "table")

    check("table_with_int_autoinc",
        "CREATE TABLE items (id INT AUTOINCREMENT, name VARCHAR(200))",
        [r"IDENTITY", r"STRING"],
        [r"AUTOINCREMENT"], "table")

    check("table_with_primary_key",
        "CREATE TABLE dept (id NUMBER PRIMARY KEY, name VARCHAR(100))",
        [r"USING DELTA", r"PRIMARY KEY"])

    check("table_with_foreign_key",
        "CREATE TABLE emp (id NUMBER, dept_id NUMBER, FOREIGN KEY (dept_id) REFERENCES dept(id))",
        [r"USING DELTA", r"FOREIGN KEY"])

    check("table_with_cluster_by",
        "CREATE TABLE events (ts TIMESTAMP, event STRING) CLUSTER BY (ts)",
        [r"ZORDER BY", r"USING DELTA"],
        [r"CLUSTER\s+BY\s*\("])

    check("table_variant_column",
        "CREATE TABLE logs (data VARIANT, metadata OBJECT)",
        [r"STRING", r"USING DELTA"])

    check("table_geography_column",
        "CREATE TABLE locations (lat FLOAT, lng FLOAT, geo GEOGRAPHY)",
        [r"DOUBLE", r"STRING", r"USING DELTA"])

    check("table_all_types",
        """CREATE TABLE all_types (
            c1 NUMBER(10,2), c2 VARCHAR(100), c3 CHAR(20), c4 TEXT,
            c5 BINARY(16), c6 VARBINARY(200), c7 BOOLEAN, c8 DATE,
            c9 DATETIME, c10 TIME, c11 TIMESTAMP_NTZ, c12 TIMESTAMP_LTZ,
            c13 TIMESTAMP_TZ, c14 FLOAT, c15 DOUBLE, c16 REAL,
            c17 BYTEINT, c18 TINYINT, c19 INTEGER, c20 BIGINT
        )""",
        [r"DECIMAL", r"STRING", r"BINARY", r"BOOLEAN", r"DATE", r"TIMESTAMP",
         r"DOUBLE", r"TINYINT", r"INT", r"BIGINT", r"USING DELTA"],
        [r"VARCHAR\(\d+\)", r"CHAR\(\d+\)", r"BINARY\(\d+\)", r"TEXT\b"])

    check("clone_table",
        "CREATE TABLE orders_backup CLONE orders",
        obj_type="table",
        expected_status="ARCHITECTURAL CHANGE")

    check("table_data_retention",
        "CREATE TABLE audit (id NUMBER, action STRING) DATA_RETENTION_TIME_IN_DAYS=90",
        [r"USING DELTA"],
        [r"DATA_RETENTION_TIME_IN_DAYS"])


def test_views():
    section("DDL: VIEWS")

    check("basic_view",
        "CREATE VIEW vw_active_users AS SELECT id, name FROM users WHERE active = TRUE",
        [r"CREATE OR REPLACE VIEW"],
        obj_type="view")

    check("secure_view",
        "CREATE SECURE VIEW vw_salaries AS SELECT id, salary FROM employees",
        [r"CREATE OR REPLACE VIEW"],
        [r"SECURE\s+VIEW"], "view")


def test_functions():
    section("DDL: FUNCTIONS")

    check("sql_udf_simple",
        """CREATE FUNCTION add(a NUMBER, b NUMBER)
        RETURNS NUMBER
        AS $$ SELECT a + b $$""",
        [r"CREATE OR REPLACE FUNCTION", r"RETURN"],
        [r"\$\$"], "function")

    check("sql_udf_multi_line",
        """CREATE FUNCTION get_discount(price NUMBER, rate NUMBER)
        RETURNS NUMBER
        AS $$
          price * COALESCE(rate, 0.1)
        $$""",
        [r"RETURN"],
        [r"\$\$"], "function")

    check("js_udf",
        """CREATE OR REPLACE FUNCTION score_risk(amt DOUBLE)
        RETURNS DOUBLE
        LANGUAGE JAVASCRIPT
        AS $$ return Math.min(amt * 1.5, 100); $$""",
        [r"LANGUAGE JAVASCRIPT", r"MANUAL REVIEW REQUIRED"],
        None, "function")


def test_procedures():
    section("DDL: PROCEDURES")

    check("sql_procedure",
        """CREATE PROCEDURE process_orders()
        RETURNS STRING
        LANGUAGE SQL
        AS $$
          INSERT INTO orders_processed SELECT * FROM orders WHERE status = 'PENDING';
          RETURN 'OK';
        $$""",
        [r"CREATE OR REPLACE PROCEDURE", r"BEGIN", r"END;"],
        [r"\$\$", r"MANUAL REVIEW", r"ARCHITECTURAL CHANGE"], "procedure")

    check("js_procedure",
        """CREATE PROCEDURE calculate_metrics()
        RETURNS STRING
        LANGUAGE JAVASCRIPT
        AS $$
          var result = snowflake.execute({ sqlText: 'SELECT COUNT(*) FROM events' });
          return 'OK';
        $$""",
        [r"LANGUAGE JAVASCRIPT", r"MANUAL REVIEW REQUIRED"],
        None, "procedure")


def test_dml_functions():
    section("DML: FUNCTION CONVERSIONS")

    check("iff_function",
        "SELECT IFF(status = 'A', 'Active', 'Inactive') AS status_label FROM users",
        [r"IF\s*\("],
        [r"IFF\s*\("])

    check("nested_iff",
        "SELECT IFF(score > 90, 'A', IFF(score > 80, 'B', 'C')) AS grade FROM results",
        [r"IF\s*\("],
        [r"IFF\s*\("])

    check("array_agg",
        "SELECT ARRAY_AGG(product_name) AS products FROM orders",
        [r"COLLECT_LIST"],
        [r"ARRAY_AGG"])

    check("object_construct",
        "SELECT OBJECT_CONSTRUCT('name', name, 'age', age) AS obj FROM people",
        [r"STRUCT"],
        [r"OBJECT_CONSTRUCT"])

    check("listagg_basic",
        "SELECT LISTAGG(product, ', ') WITHIN GROUP (ORDER BY product) AS products FROM items",
        [r"CONCAT_WS", r"COLLECT_LIST"],
        [r"LISTAGG"])

    check("zeroifnull",
        "SELECT ZEROIFNULL(amount) AS safe_amount FROM payments",
        [r"IF\s*\(.*IS NULL.*0"],
        [r"ZEROIFNULL"])

    check("nullifzero",
        "SELECT NULLIFZERO(discount) AS real_discount FROM deals",
        [r"IF\(.*= 0, NULL,"],
        [r"NULLIFZERO"])

    check("to_varchar",
        "SELECT TO_VARCHAR(amount) AS amount_str FROM payments",
        [r"TO_CHAR|CAST\(.*AS STRING\)"],
        [r"TO_VARCHAR"])

    check("to_number",
        "SELECT TO_NUMBER(amount_str) AS amount FROM raw_data",
        [r"CAST\(.*DECIMAL"],
        [r"TO_NUMBER"])

    check("monthname",
        "SELECT MONTHNAME(ts) AS month FROM events",
        [r"DATE_FORMAT"],
        [r"MONTHNAME"])

    check("dayname",
        "SELECT DAYNAME(ts) AS day FROM events",
        [r"DATE_FORMAT"],
        [r"DAYNAME"])

    check("array_size",
        "SELECT ARRAY_SIZE(arr) AS cnt FROM data",
        [r"SIZE\("],
        [r"ARRAY_SIZE"])

    check("get_function",
        "SELECT GET(data, 'key') AS val FROM docs",
        [r"data\[|GET_JSON_OBJECT"],
        [r"\bGET\s*\("])

    check("random_func",
        "SELECT RANDOM() AS r FROM numbers",
        [r"RAND\(\)"],
        [r"RANDOM"])

    check("seq_functions",
        "SELECT SEQ1() AS rn FROM table_generator",
        [r"ROW_NUMBER\(\) OVER \(ORDER BY 1\)"],
        [r"SEQ[1248]"])

    check("parse_json",
        "SELECT PARSE_JSON('{\"a\":1}') AS obj",
        [r"PARSE_JSON"],
        None)

    check("nvl2_func",
        "SELECT NVL2(a, b, c) AS val FROM t",
        [r"CASE WHEN.*IS NOT NULL THEN.*ELSE.*END"],
        [r"NVL2"])

    check("decode_func",
        """SELECT DECODE(status, 'A', 'Active', 'B', 'Backup', 'Unknown') AS label FROM t""",
        [r"CASE\s+status\s+WHEN\s+'A'\s+THEN\s+'Active'\s+WHEN\s+'B'\s+THEN\s+'Backup'\s+ELSE\s+'Unknown'\s+END"],
        [r"DECODE"])

    check("flatten_solo",
        "SELECT FLATTEN(arr) AS val FROM data",
        [r"EXPLODE|LATERAL VIEW"],
        [r"FLATTEN\s*\("])

    check("pivot_basic",
        "SELECT * FROM sales PIVOT (SUM(amount) FOR region IN ('North', 'South')) AS p",
        [r"PIVOT"],
        None)

    check("unpivot_basic",
        "SELECT * FROM sales UNPIVOT (amount FOR region IN (q1, q2, q3, q4)) AS u",
        [r"UNPIVOT"],
        None)

    check("group_by_all",
        "SELECT department, SUM(salary) FROM employees GROUP BY ALL",
        [r"GROUP BY ALL"],
        [r"GROUP\s+BY\s+\d+"])

    check("create_sequence",
        "CREATE SEQUENCE my_seq START WITH 100 INCREMENT BY 1",
        [r"CREATE SEQUENCE", r"START WITH 100", r"INCREMENT BY 1"],
        None,
        obj_type="sequence")

    check("create_sequence_or_replace",
        "CREATE OR REPLACE SEQUENCE SEQ_CUSTOMER_KEY START WITH 1 INCREMENT BY 1 NOORDER",
        [r"DROP SEQUENCE IF EXISTS", r"CREATE SEQUENCE", r"START WITH 1", r"INCREMENT BY 1", r"NOORDER"],
        None,
        obj_type="sequence")

    check("nextval_colon",
        "SELECT my_seq.NEXTVAL AS id",
        [r"my_seq\.NEXTVAL"],
        None)

    check("currval_colon",
        "SELECT my_seq.CURRVAL AS id",
        [r"my_seq\.CURRVAL"],
        None)

    check("nextval_function",
        "SELECT NEXTVAL('my_seq') AS id",
        [r"my_seq\.NEXTVAL"],
        [r"NEXTVAL\("])

    check("currval_function",
        "SELECT CURRVAL('my_seq') AS id",
        [r"my_seq\.CURRVAL"],
        [r"CURRVAL\("])

    check("nvl_func",
        "SELECT NVL(commission, 0) AS comm FROM sales",
        [r"COALESCE"],
        [r"\bNVL\b"])

    check("ratio_to_report",
        "SELECT name, salary, RATIO_TO_REPORT(salary) OVER (PARTITION BY dept) AS ratio FROM emp",
        [r"/ SUM\(salary\) OVER"],
        [r"RATIO_TO_REPORT"])

    check("minus_conversion",
        "SELECT id FROM table_a MINUS SELECT id FROM table_b",
        [r"EXCEPT"],
        [r"MINUS"])

    check("materialized_view",
        "CREATE MATERIALIZED VIEW mv AS SELECT * FROM source",
        None, None, "table")


def test_dml_qualify():
    section("DML: QUALIFY")

    check("qualify_simple",
        "SELECT id, name, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM emp QUALIFY rn = 1",
        [r"WHERE\s+rn\s*=\s*1"],
        [r"QUALIFY"])


def test_dml_lateral():
    section("DML: LATERAL FLATTEN")

    check("lateral_flatten_basic",
        "SELECT e.id, f.value FROM events e, LATERAL FLATTEN(INPUT => e.tags) f",
        [r"LATERAL VIEW EXPLODE"],
        [r"FLATTEN"])


def test_features():
    section("FEATURES: EDGE CASES")

    check("desc_nulls_first",
        "SELECT * FROM users ORDER BY name DESC NULLS FIRST",
        [r"DESC\s+NULLS\s+LAST"],
        [r"NULLS\s+FIRST"])

    check("time_travel_at",
        "SELECT * FROM orders AT(TIMESTAMP => '2024-01-01'::TIMESTAMP)",
        None, None)

    check("convert_timezone",
        "SELECT CONVERT_TIMEZONE('UTC', 'America/New_York', ts) AS local_ts FROM events",
        None, None)

    check("randstr",
        "SELECT RANDSTR(10, 'abcdef') AS r FROM data",
        None, None)

    check("to_array",
        "SELECT TO_ARRAY('a') AS arr",
        None, None)

    check("to_boolean",
        "SELECT TO_BOOLEAN('true') AS flag",
        None, None)


def test_variant_access():
    section("VARIANT ACCESS")

    check("colon_accessor_cast",
        "SELECT CAST(data:field AS VARCHAR) AS val FROM docs",
        [r"GET_JSON_OBJECT"],
        [r"data:field"])

    check("colon_accessor_bare",
        "SELECT data:field AS val FROM docs",
        [r"GET_JSON_OBJECT"],
        [r"data:field"])

    check("colon_accessor_nested",
        "SELECT data:metadata:field AS val FROM t",
        [r"GET_JSON_OBJECT\(data,\s*'\$\.metadata\.field'\)"],
        [])

    check("colon_accessor_bracket",
        """SELECT data:"field-name" AS val FROM t""",
        [r"""GET_JSON_OBJECT\(data,\s*'\$\["field-name"\]'\)"""],
        [])

    check("colon_accessor_single_quoted",
        """SELECT data:'field name' AS val FROM t""",
        [r"""GET_JSON_OBJECT\(data,\s*'\$\["field name"\]'\)"""],
        [])

    check("colon_accessor_cast_bracket",
        """SELECT CAST(data:"field-name" AS VARCHAR) AS val FROM t""",
        [r"""CAST\(GET_JSON_OBJECT\(data,\s*'\$\["field-name"\]'\)"""],
        [])

    check("try_parse_json",
        "SELECT TRY_PARSE_JSON('{\"a\":1}') AS obj",
        [r"TRY_PARSE_JSON"],
        [r"\bPARSE_JSON\("])
    section("VALIDATION")

    check("valid_table_syntax",
        "CREATE TABLE t (id DECIMAL, name STRING) USING DELTA",
        expected_status="PASS")

    check("valid_select_syntax",
        "SELECT id, name FROM t WHERE active = TRUE",
        expected_status="PASS")

    check("clone_architectural_status",
        "CREATE TABLE backup CLONE source",
        obj_type="table",
        expected_status="ARCHITECTURAL CHANGE")

    check("js_udf_unsupported",
        "CREATE FUNCTION f() RETURNS INT LANGUAGE JAVASCRIPT AS $$ return 1 $$",
        obj_type="function",
        expected_status="ERROR")


def test_regression():
    section("REGRESSION: ENTERPRISE PATTERNS")

    check("int_autoinc_generates_identity",
        "CREATE TABLE t (id INT AUTOINCREMENT START 1 INCREMENT 1, val VARCHAR(50))",
        [r"IDENTITY"],
        [r"AUTOINCREMENT"], "table")

    check("desc_nulls_first_dml",
        "SELECT * FROM t ORDER BY created_at DESC NULLS FIRST",
        [r"DESC\s+NULLS\s+LAST"],
        [r"NULLS\s+FIRST"])

    check("js_udf_has_manual_review",
        """CREATE FUNCTION score(x DOUBLE)
        RETURNS DOUBLE
        LANGUAGE JAVASCRIPT
        AS $$ return Math.min(x, 100); $$""",
        [r"MANUAL REVIEW REQUIRED"],
        None, "function")

    check("procedure_strips_dollar_dollar",
        """CREATE PROCEDURE p()
        LANGUAGE SQL
        AS $$
          INSERT INTO t SELECT * FROM s;
        $$""",
        [r"BEGIN", r"END;"],
        [r"\$\$", r"ARCHITECTURAL CHANGE", r"MANUAL REVIEW"], "procedure")

    check("procedure_with_while_loop",
        """CREATE PROCEDURE process_batch()
        RETURNS STRING
        LANGUAGE SQL
        AS $$
          DECLARE i INT DEFAULT 0;
          BEGIN
            WHILE (i < 10) DO
              INSERT INTO log VALUES (i);
              i := i + 1;
            END WHILE;
            FOR row IN (SELECT id FROM source) DO
              INSERT INTO target VALUES (row.id);
            END FOR;
            LOOP
              BREAK;
            END LOOP;
            RETURN 'OK';
          END;
        $$""",
        [r"DECLARE", r"WHILE", r"FOR ", r"LOOP", r"END;"],
        [r"\$\$", r"MANUAL REVIEW", r"ARCHITECTURAL CHANGE"],
        "procedure")

    check("procedure_with_declare_if",
        """CREATE PROCEDURE calc_bonus(emp_id NUMBER, rating NUMBER)
        RETURNS NUMBER
        LANGUAGE SQL
        AS $$
          DECLARE
            base NUMBER;
            mult NUMBER DEFAULT 0.1;
          BEGIN
            SELECT salary INTO base FROM employees WHERE id = emp_id;
            LET mult := 0.2;
            IF (rating >= 4) THEN
              mult := 0.3;
            ELSEIF (rating >= 3) THEN
              mult := 0.2;
            ELSE
              mult := 0.05;
            END IF;
            RETURN base * mult;
          END;
        $$""",
        [r"DECLARE", r"BEGIN", r"END;", r"ELSEIF", r"SET "],
        [r"\$\$", r"ARCHITECTURAL CHANGE", r"MANUAL REVIEW", r"LET "],
        "procedure")


def test_update_from_to_merge():
    section("UPDATE FROM → MERGE INTO CONVERSION")

    check("update_from_subquery",
        """CREATE PROCEDURE sp_refresh()
        RETURNS STRING
        LANGUAGE SQL
        AS $$
          UPDATE AD_SALES.AD_CAMPAIGNS C
          SET PACING_STATUS = 'ON_PACE',
              LAST_UPDATED_TS = CURRENT_TIMESTAMP()
          FROM (
              SELECT CAMPAIGN_ID, SUM(IMPRESSIONS_DELIVERED) AS TOTAL_DELIVERED
              FROM AD_SALES.AD_IMPRESSIONS
              GROUP BY CAMPAIGN_ID
          ) I
          WHERE C.CAMPAIGN_ID = I.CAMPAIGN_ID;
          RETURN 'OK';
        $$""",
        [r"MERGE INTO", r"USING", r"WHEN MATCHED THEN UPDATE SET"],
        [r"UPDATE\s+\S+\s+\w+\s+SET.*FROM"], "procedure")

    check("update_from_simple_table",
        """CREATE PROCEDURE sp_update()
        RETURNS STRING
        LANGUAGE SQL
        AS $$
          UPDATE target_table T
          SET col1 = S.col1
          FROM source_table S
          WHERE T.id = S.id;
          RETURN 'OK';
        $$""",
        [r"MERGE INTO", r"USING source_table", r"ON T.id = S.id"],
        [r"UPDATE\s+\S+\s+\w+\s+SET.*FROM"], "procedure")

    check("update_from_multiple_set",
        """CREATE PROCEDURE sp_multi()
        RETURNS STRING
        LANGUAGE SQL
        AS $$
          UPDATE my_schema.my_table T
          SET status = A.status_label,
              last_updated = CURRENT_TIMESTAMP()
          FROM (
              SELECT id, fn_get_status(id) AS status_label
              FROM my_schema.source_table
              GROUP BY id
          ) A
          WHERE T.id = A.id;
          RETURN 'Done';
        $$""",
        [r"MERGE INTO my_schema.my_table T", r"WHEN MATCHED THEN UPDATE SET"],
        [r"UPDATE\s+\S+\s+\w+\s+SET.*FROM"], "procedure")

    check("update_from_no_manual_review",
        """CREATE PROCEDURE sp_clean()
        RETURNS STRING
        LANGUAGE SQL
        AS $$
          UPDATE schema1.table1 T
          SET val = S.val
          FROM schema2.table2 S
          WHERE T.key = S.key;
          RETURN 'OK';
        $$""",
        [r"MERGE INTO"],
        [r"MANUAL REVIEW REQUIRED.*UPDATE FROM"], "procedure")


def test_detection():
    section("FEATURE DETECTION")

    for name, sql, expected_features in [
        ("detect_clone", "CREATE TABLE t CLONE s", ["CLONE"]),
        ("detect_stream", "CREATE STREAM s ON TABLE t", ["STREAMS"]),
        ("detect_task", "CREATE TASK t SCHEDULE = '1 MINUTE'", ["TASKS"]),
        ("detect_identity", "CREATE TABLE t (id NUMBER IDENTITY(1,1))", ["IDENTITY"]),
        ("detect_autoincrement", "CREATE TABLE t (id INT AUTOINCREMENT)", ["AUTOINCREMENT"]),
        ("detect_js", "CREATE FUNCTION f() RETURNS INT LANGUAGE JAVASCRIPT AS $$ return 1 $$", ["LANGUAGE JAVASCRIPT"]),
        ("detect_nvl2", "SELECT NVL2(x, y, z) FROM t", ["NVL2"]),
        ("detect_decode", "SELECT DECODE(x, 1, 'a', 2, 'b') FROM t", ["DECODE"]),
        ("detect_group_by_all", "SELECT department, SUM(s) FROM t GROUP BY ALL", ["GROUP BY ALL"]),
        ("detect_create_sequence", "CREATE SEQUENCE my_seq START 100", ["CREATE SEQUENCE"]),
        ("detect_nextval", "SELECT my_seq.NEXTVAL", ["NEXTVAL"]),
        ("detect_currval", "SELECT my_seq.CURRVAL", ["CURRVAL"]),
    ]:
        features = detect_snowflake_features(sql)
        missing = [ef for ef in expected_features if ef not in features]
        if missing:
            FAILURES.append(f"  FAIL [{name}] Missing features: {missing}")
            print(f"  FAIL [{name}] Missing: {missing}")
            global FAIL
            FAIL += 1
        else:
            global PASS
            PASS += 1
            print(f"  PASS [{name}]")
        global TOTAL
        TOTAL += 1


def print_summary():
    global PASS, FAIL, TOTAL
    print(f"\n{'='*70}")
    print(f"  QA TEST SUMMARY")
    print(f"{'='*70}")
    print(f"  Total:  {TOTAL}")
    print(f"  Passed: {PASS}")
    print(f"  Failed: {FAIL}")
    rate = PASS/TOTAL*100 if TOTAL > 0 else 0
    print(f"  Rate:   {rate:.1f}%")
    if FAILURES:
        print(f"\n  FAILURES:")
        for f in FAILURES:
            print(f"    {f}")
    print()
    return rate


if __name__ == "__main__":
    test_ddl()
    test_views()
    test_functions()
    test_procedures()
    test_update_from_to_merge()
    test_dml_functions()
    test_dml_qualify()
    test_dml_lateral()
    test_features()
    test_variant_access()
    test_regression()
    test_detection()
    rate = print_summary()
    sys.exit(0 if rate >= 90 else 1)
