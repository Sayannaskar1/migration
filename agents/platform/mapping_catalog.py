OBJECT_MAPPING: dict[str, dict] = {
    "stream": {
        "target": "Delta Change Data Feed",
        "deployment": "cdf",
        "description": "Snowflake STREAM → Enable Delta CDF + Structured Streaming",
        "automation": 30,
    },
    "task": {
        "target": "Databricks Job",
        "deployment": "job",
        "description": "Snowflake TASK → Databricks Workflows Job",
        "automation": 40,
    },
    "warehouse": {
        "target": "SQL Warehouse",
        "deployment": "warehouse",
        "description": "Snowflake WAREHOUSE → Databricks SQL Warehouse",
        "automation": 30,
    },
    "pipe": {
        "target": "Auto Loader",
        "deployment": "autoloader",
        "description": "Snowflake PIPE → Auto Loader (cloudFiles)",
        "automation": 25,
    },
    "stage": {
        "target": "Unity Catalog Volume",
        "deployment": "volume",
        "description": "Snowflake STAGE → Unity Catalog Volume or External Location",
        "automation": 20,
    },
    "resource_monitor": {
        "target": "Unity Catalog Budget + Cluster Policy",
        "deployment": "budget",
        "description": "Snowflake RESOURCE MONITOR → Budgets & Policies",
        "automation": 10,
    },
    "role": {
        "target": "Unity Catalog Role",
        "deployment": "role",
        "description": "Snowflake ROLE → Unity Catalog CREATE ROLE",
        "automation": 100,
    },
    "security_integration": {
        "target": "OAuth / SCIM / IDP configuration",
        "deployment": "manual",
        "description": "Snowflake SECURITY INTEGRATION → Manual Account Console setup",
        "automation": 5,
    },
    "storage_integration": {
        "target": "Storage credential in Unity Catalog",
        "deployment": "credential",
        "description": "Snowflake STORAGE INTEGRATION → UC Storage Credential",
        "automation": 10,
    },
    "notification_integration": {
        "target": "Notification destination in Databricks",
        "deployment": "manual",
        "description": "Snowflake NOTIFICATION INTEGRATION → Manual setup",
        "automation": 5,
    },
    "api_integration": {
        "target": "External gateway / API proxy",
        "deployment": "manual",
        "description": "Snowflake API INTEGRATION → Manual configuration",
        "automation": 5,
    },
    "network_policy": {
        "target": "IP ACL / Private Link / VPC peering",
        "deployment": "manual",
        "description": "Snowflake NETWORK POLICY → Databricks Network configuration",
        "automation": 10,
    },
    "file_format": {
        "target": "Inline file format in COPY INTO or Auto Loader",
        "deployment": "inline",
        "description": "Snowflake FILE FORMAT → Inline format specification",
        "automation": 5,
    },
    "masking_policy": {
        "target": "Function-based column mask",
        "deployment": "mask",
        "description": "Snowflake MASKING POLICY → CREATE FUNCTION + ALTER TABLE SET MASK",
        "automation": 60,
    },
    "row_access_policy": {
        "target": "Function-based row filter",
        "deployment": "row_filter",
        "description": "Snowflake ROW ACCESS POLICY → CREATE FUNCTION + ALTER TABLE SET ROW FILTER",
        "automation": 60,
    },
    "user": {
        "target": "SCIM provisioning / Account Console",
        "deployment": "scim",
        "description": "Snowflake USER → SCIM sync or Account Console user management",
        "automation": 5,
    },
    "alert": {
        "target": "Databricks Alert / Dashboard",
        "deployment": "alert",
        "description": "Snowflake ALERT → Databricks SQL Alert or Lakeview Dashboard",
        "automation": 20,
    },
    "share": {
        "target": "Delta Sharing",
        "deployment": "delta_sharing",
        "description": "Snowflake SHARE → Delta Sharing",
        "automation": 30,
    },
    "dynamic_table": {
        "target": "Databricks Dynamic View / Materialized View",
        "deployment": "dynamic_view",
        "description": "Snowflake DYNAMIC TABLE → Databricks Materialized View or Streaming Table",
        "automation": 60,
    },
    "tag": {
        "target": "Unity Catalog Tag",
        "deployment": "tag",
        "description": "Snowflake TAG → Unity Catalog ALTER ... SET TAGS",
        "automation": 80,
    },
}


PLATFORM_OBJECT_TYPES: set[str] = set(OBJECT_MAPPING.keys())


def get_mapping(object_type: str) -> dict | None:
    return OBJECT_MAPPING.get(object_type)


def is_platform_object(object_type: str) -> bool:
    return object_type in PLATFORM_OBJECT_TYPES
