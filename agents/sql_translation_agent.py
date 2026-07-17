import re
from typing import Optional
from agents.project_loader import ParsedObject, ProjectInventory
from parser.ast_parser import detect_snowflake_features


def translate_sql(obj: ParsedObject, inventory: ProjectInventory) -> str:
    sql = obj.converted_sql if obj.converted_sql else obj.raw_sql

    features = detect_snowflake_features(sql)

    has_manual = any(
        feat in features
        for feat in [
            "MATCH_RECOGNIZE",
            "CONNECT BY",
            "SAMPLE",
            "TABLESAMPLE",
        ]
    )
    if has_manual:
        sql += "\n\n-- MANUAL REVIEW REQUIRED: Complex Snowflake feature detected"

    return sql


def translate_inventory(inventory: ProjectInventory) -> None:
    for obj in inventory.all_objects:
        obj.converted_sql = translate_sql(obj, inventory)
