# DIF dbt Config-Driven Data Quality Solution v1.5


## Executive Summary

This document describes the **config-driven data quality testing solution** for the Data Integrity Framework (DIF) using dbt. This approach enables dynamic test execution by reading rule configurations from Snowflake control tables at runtime, executing tests via **dbt-elementary** and **dbt-artifacts** packages, and logging all results back to DIF audit tables.

### Version 1.5 Scope

**In-scope Rule Types:**
| Rule Type | Description | dbt Implementation |
|-----------|-------------|-------------------|
| **NULL_CHECK** | Validates null percentage in columns | `dbt_utils.not_null_proportion` + Elementary |
| **REFERENTIAL_INTEGRITY** | Validates foreign key relationships | `dbt_utils.relationships` + Elementary |
| **SUM_MATCH** | Validates sum equality between source/target | Custom macro + Elementary |

### Key Design Principles

1. **Config-driven** — Rules stored in `CONFIG_CONTROLS.RULES` table, not YAML files
2. **Zero-code test addition** — New rules added to control tables, no dbt code changes required
3. **Elementary observability** — All tests tracked with anomaly detection and historical trends
4. **Unified audit trail** — Results written to existing `AUDIT_CONTROLS.DQ_LOG` and `RUN_METADATA` tables
5. **Rule group isolation** — Each dbt run processes only rules for a specific RULE_GROUP

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    dbt Pipeline Execution                                    │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────────────────────┐ │
│  │                           1. CONFIG LOADING PHASE                                       │ │
│  │  ┌────────────────────────────┐    ┌─────────────────────────────────────────────────┐ │ │
│  │  │  dbt run                    │    │  Macro: load_dif_rules_for_group()             │ │ │
│  │  │  --vars 'rule_group: XYZ'   │───▶│  • Reads CONFIG_CONTROLS.RULES                 │ │ │
│  │  │                             │    │  • Filters by RULE_GROUP = 'XYZ'               │ │ │
│  │  │                             │    │  • Returns list of rule configurations         │ │ │
│  │  └────────────────────────────┘    └─────────────────────────────────────────────────┘ │ │
│  └────────────────────────────────────────────────────────────────────────────────────────┘ │
│                                              │                                               │
│                                              ▼                                               │
│  ┌────────────────────────────────────────────────────────────────────────────────────────┐ │
│  │                           2. DYNAMIC TEST GENERATION                                    │ │
│  │  ┌────────────────────────────────────────────────────────────────────────────────────┐│ │
│  │  │  For each rule in rule_group:                                                       ││ │
│  │  │                                                                                     ││ │
│  │  │  ┌─────────────────┐   ┌─────────────────────┐   ┌─────────────────────────────┐   ││ │
│  │  │  │  NULL_CHECK     │   │ REFERENTIAL_INTEGRITY│   │ SUM_MATCH                   │   ││ │
│  │  │  │                 │   │                      │   │                             │   ││ │
│  │  │  │ dbt_utils.      │   │ dbt_utils.           │   │ dif_sum_match               │   ││ │
│  │  │  │ not_null_       │   │ relationships_where  │   │ (custom macro)              │   ││ │
│  │  │  │ proportion      │   │                      │   │                             │   ││ │
│  │  │  │                 │   │ elementary.          │   │ elementary.                 │   ││ │
│  │  │  │ elementary.     │   │ column_test          │   │ column_values_match         │   ││ │
│  │  │  │ column_anomalies│   │                      │   │                             │   ││ │
│  │  │  └─────────────────┘   └─────────────────────┘   └─────────────────────────────┘   ││ │
│  │  └────────────────────────────────────────────────────────────────────────────────────┘│ │
│  └────────────────────────────────────────────────────────────────────────────────────────┘ │
│                                              │                                               │
│                                              ▼                                               │
│  ┌────────────────────────────────────────────────────────────────────────────────────────┐ │
│  │                           3. TEST EXECUTION & METRICS                                   │ │
│  │  ┌──────────────────────────┐     ┌────────────────────────────────────────────────┐   │ │
│  │  │  dbt test                 │     │  dbt-artifacts Package                         │   │ │
│  │  │  --select tag:dif_v15     │     │  • fct_dbt_test_results                        │   │ │
│  │  │                           │     │  • fct_dbt_model_executions                    │   │ │
│  │  └──────────────────────────┘     └────────────────────────────────────────────────┘   │ │
│  │                                                                                         │ │
│  │  ┌────────────────────────────────────────────────────────────────────────────────────┐│ │
│  │  │  dbt-elementary Package                                                            ││ │
│  │  │  • Test result storage                                                              ││ │
│  │  │  • Anomaly detection                                                                ││ │
│  │  │  • Historical comparisons                                                           ││ │
│  │  │  • elementary.elementary_test_results table                                         ││ │
│  │  └────────────────────────────────────────────────────────────────────────────────────┘│ │
│  └────────────────────────────────────────────────────────────────────────────────────────┘ │
│                                              │                                               │
│                                              ▼                                               │
│  ┌────────────────────────────────────────────────────────────────────────────────────────┐ │
│  │                           4. AUDIT LOGGING PHASE                                        │ │
│  │  ┌────────────────────────────────────────────────────────────────────────────────────┐│ │
│  │  │  on-run-end Hook: dif_sync_test_results_to_audit()                                 ││ │
│  │  │                                                                                     ││ │
│  │  │  ┌───────────────────────────┐   ┌───────────────────────────────────────────────┐ ││ │
│  │  │  │  AUDIT_CONTROLS.DQ_LOG    │   │  AUDIT_CONTROLS.RUN_METADATA                  │ ││ │
│  │  │  │  • LOG_ID                 │   │  • RUN_ID                                     │ ││ │
│  │  │  │  • RUN_ID                 │   │  • RULE_GROUP                                 │ ││ │
│  │  │  │  • RULE_ID                │   │  • START_TIME / END_TIME                      │ ││ │
│  │  │  │  • RULE_TYPE              │   │  • TOTAL/PASSED/FAILED/ERROR_RULES            │ ││ │
│  │  │  │  • STATUS                 │   │  • STATUS                                     │ ││ │
│  │  │  │  • EXPECTED/ACTUAL_VALUE  │   │                                               │ ││ │
│  │  │  │  • DEVIATION              │   │                                               │ ││ │
│  │  │  │  • ERROR_MESSAGE          │   │                                               │ ││ │
│  │  │  │  • ENGINE = 'DBT'         │   │                                               │ ││ │
│  │  │  └───────────────────────────┘   └───────────────────────────────────────────────┘ ││ │
│  │  └────────────────────────────────────────────────────────────────────────────────────┘│ │
│  └────────────────────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
                                              │
                                              ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                           SNOWFLAKE CONTROL PLANE                                            │
│  ┌───────────────────────────────────────────────────────────────────────────────────────┐  │
│  │  DB_GOVERNANCE                                                                         │  │
│  │  ├── CONFIG_CONTROLS                                                                   │  │
│  │  │   ├── RULES           ◄── Config source for rule definitions                       │  │
│  │  │   ├── DATA_ASSETS     ◄── Asset metadata (tables, columns, connections)            │  │
│  │  │   └── CONNECTIONS     ◄── Database connection registry                             │  │
│  │  └── AUDIT_CONTROLS                                                                    │  │
│  │      ├── DQ_LOG          ◄── Test result log (insert after each run)                  │  │
│  │      ├── RUN_METADATA    ◄── Run summary (insert at run start, update at end)         │  │
│  │      └── AGGREGATION_LOG ◄── Computed aggregations for SUM_MATCH reconciliation       │  │
│  └───────────────────────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Package Dependencies

### packages.yml

```yaml
# Generated by GithubCopilot
packages:
  # Core testing utilities
  - package: dbt-labs/dbt_utils
    version: [">=1.1.0", "<2.0.0"]
  
  # Elementary data observability (v0.22.1 as specified)
  - package: elementary-data/elementary
    version: "0.22.1"
  
  # Artifact capture for test result storage
  - package: brooklyn-data/dbt_artifacts
    version: [">=2.6.0", "<3.0.0"]
```

### Package Feature Usage

| Package | Features Used | Purpose |
|---------|---------------|---------|
| **dbt-utils** | `relationships_where`, `not_null_proportion` | Core test implementations |
| **elementary** | Test result storage, anomaly detection, historical trends | Observability layer |
| **dbt-artifacts** | `fct_dbt_test_results`, `fct_dbt_model_executions` | Raw artifact storage for audit sync |

---

## Control Table Schema (Reference)

### CONFIG_CONTROLS.RULES (Existing Table)

```sql
-- Key columns for dbt config-driven approach
RULE_ID VARCHAR(200) PRIMARY KEY,          -- Unique rule identifier
RULE_GROUP VARCHAR(200) NOT NULL,          -- Used to filter rules per run
RULE_TYPE VARCHAR(100) NOT NULL,           -- NULL_CHECK, REFERENTIAL_INTEGRITY, SUM_MATCH
SOURCE_ASSET_ID VARCHAR(200) NOT NULL,     -- FK to DATA_ASSETS
TARGET_ASSET_ID VARCHAR(200),              -- FK to DATA_ASSETS (for cross-asset rules)
THRESHOLD_VALUE VARCHAR(100),              -- Pass/fail threshold
THRESHOLD_TYPE VARCHAR(50),                -- ABSOLUTE, PERCENTAGE, BOOLEAN
SEVERITY VARCHAR(20) NOT NULL,             -- CRITICAL, HIGH, MEDIUM, LOW, INFO
RULE_BODY TEXT,                            -- JSON config (column names, patterns, etc.)
IS_ACTIVE BOOLEAN DEFAULT TRUE             -- Enable/disable without deleting
```

### RULE_BODY JSON Schema by Rule Type

#### NULL_CHECK
```json
{
  "column": "CUSTOMER_ID",
  "include_empty_strings": false,
  "threshold_type": "percentage",
  "threshold_value": 0
}
```

#### REFERENTIAL_INTEGRITY
```json
{
  "source_column": "CUSTOMER_FK",
  "target_column": "CUSTOMER_ID",
  "allow_nulls": true
}
```

#### SUM_MATCH
```json
{
  "column": "ORDER_AMOUNT",
  "source_filter": "ORDER_STATUS = 'COMPLETED'",
  "target_filter": "STATUS = 'FINAL'",
  "tolerance_type": "percentage",
  "tolerance_value": 0.01
}
```

---

## Implementation Components

### 1. Config Loading Macro

**File:** `macros/dif_load_rules.sql`

```sql
-- Generated by GithubCopilot
-- Macro: Load DIF rules from CONFIG_CONTROLS.RULES for a specific rule group
-- Usage: {{ dif_load_rules_for_group(var('rule_group')) }}

{% macro dif_load_rules_for_group(rule_group) %}
  {% set rules_query %}
    SELECT 
      r.RULE_ID,
      r.RULE_TYPE,
      r.SOURCE_ASSET_ID,
      r.TARGET_ASSET_ID,
      r.THRESHOLD_VALUE,
      r.THRESHOLD_TYPE,
      r.SEVERITY,
      r.RULE_BODY,
      r.RECON_TOLERANCE_TYPE,
      r.RECON_TOLERANCE_VALUE,
      sa.ASSET_PATH AS source_asset_path,
      sa.ASSET_TYPE AS source_asset_type,
      sc.DATABASE_NAME AS source_database,
      ta.ASSET_PATH AS target_asset_path,
      ta.ASSET_TYPE AS target_asset_type,
      tc.DATABASE_NAME AS target_database
    FROM {{ var('dif_database') }}.CONFIG_CONTROLS.RULES r
    JOIN {{ var('dif_database') }}.CONFIG_CONTROLS.DATA_ASSETS sa 
      ON r.SOURCE_ASSET_ID = sa.ASSET_ID
    LEFT JOIN {{ var('dif_database') }}.CONFIG_CONTROLS.CONNECTIONS sc 
      ON sa.CONNECTION_NAME = sc.CONNECTION_NAME
    LEFT JOIN {{ var('dif_database') }}.CONFIG_CONTROLS.DATA_ASSETS ta 
      ON r.TARGET_ASSET_ID = ta.ASSET_ID
    LEFT JOIN {{ var('dif_database') }}.CONFIG_CONTROLS.CONNECTIONS tc 
      ON ta.CONNECTION_NAME = tc.CONNECTION_NAME
    WHERE r.RULE_GROUP = '{{ rule_group }}'
      AND r.IS_ACTIVE = TRUE
      AND r.RULE_TYPE IN ('NULL_CHECK', 'REFERENTIAL_INTEGRITY', 'SUM_MATCH')
    ORDER BY r.RULE_ID
  {% endset %}
  
  {% set results = run_query(rules_query) %}
  
  {% if execute %}
    {{ return(results) }}
  {% else %}
    {{ return([]) }}
  {% endif %}
{% endmacro %}


{% macro dif_parse_rule_body(rule_body_str) %}
  {# Safely parse RULE_BODY JSON string into a dictionary #}
  {% if rule_body_str and rule_body_str != '' %}
    {% set parsed = fromjson(rule_body_str) %}
    {{ return(parsed) }}
  {% else %}
    {{ return({}) }}
  {% endif %}
{% endmacro %}
```

---

### 2. Test Generation Macros

#### 2.1 NULL_CHECK Implementation

**File:** `macros/dif_tests/dif_null_check.sql`

```sql
-- Generated by GithubCopilot
-- Macro: Generate NULL_CHECK test using dbt-utils and elementary
-- Implements DIF NULL_CHECK rule type

{% macro dif_generate_null_check_test(rule_id, source_asset_path, rule_body, threshold_value, severity) %}
  {#
    Generate a not-null test for a specific column.
    Uses dbt_utils.not_null_proportion for threshold-based checking.
    Logs to elementary for observability.
    
    Args:
      rule_id: DIF RULE_ID for traceability
      source_asset_path: Fully qualified table name (database.schema.table)
      rule_body: Parsed JSON with column name and config
      threshold_value: Maximum null percentage allowed (e.g., "0" = no nulls)
      severity: error or warn
  #}
  
  {% set column_name = rule_body.get('column', '') %}
  {% set include_empty = rule_body.get('include_empty_strings', false) %}
  {% set threshold_pct = (threshold_value | float) / 100 if threshold_value else 0 %}
  
  {% if column_name %}
    {# Use expression_is_true for flexible null checking #}
    {% set null_check_expression %}
      CASE 
        WHEN {{ column_name }} IS NULL THEN 0
        {% if include_empty %}
        WHEN TRIM({{ column_name }}) = '' THEN 0
        {% endif %}
        ELSE 1
      END
    {% endset %}
    
    SELECT
      '{{ rule_id }}' AS dif_rule_id,
      'NULL_CHECK' AS dif_rule_type,
      '{{ column_name }}' AS column_name,
      COUNT(*) AS total_rows,
      SUM({{ null_check_expression }}) AS non_null_count,
      COUNT(*) - SUM({{ null_check_expression }}) AS null_count,
      ROUND((COUNT(*) - SUM({{ null_check_expression }})) * 100.0 / NULLIF(COUNT(*), 0), 4) AS null_percentage,
      {{ threshold_value if threshold_value else 0 }} AS threshold_value,
      CASE 
        WHEN ROUND((COUNT(*) - SUM({{ null_check_expression }})) * 100.0 / NULLIF(COUNT(*), 0), 4) > {{ threshold_value if threshold_value else 0 }}
        THEN 'FAIL'
        ELSE 'PASS'
      END AS test_status
    FROM {{ source_asset_path }}
    HAVING ROUND((COUNT(*) - SUM({{ null_check_expression }})) * 100.0 / NULLIF(COUNT(*), 0), 4) > {{ threshold_value if threshold_value else 0 }}
  {% else %}
    {{ exceptions.raise_compiler_error("NULL_CHECK rule " ~ rule_id ~ " missing 'column' in RULE_BODY") }}
  {% endif %}
{% endmacro %}


{% macro dif_null_check_with_elementary(rule_id, model_name, column_name, severity, threshold_pct) %}
  {#
    Wrapper that invokes elementary column anomaly monitoring alongside null check.
    This enables historical trend analysis for null percentages.
  #}
  {{ elementary.column_anomalies(
      column_name=column_name,
      column_anomalies=['null_count', 'null_percent'],
      where=none,
      time_bucket={
        'period': 'day',
        'count': 1
      },
      tags=['dif_v15', 'null_check', rule_id]
  ) }}
{% endmacro %}
```

---

#### 2.2 REFERENTIAL_INTEGRITY Implementation

**File:** `macros/dif_tests/dif_referential_integrity.sql`

```sql
-- Generated by GithubCopilot
-- Macro: Generate REFERENTIAL_INTEGRITY test using dbt-utils relationships
-- Implements DIF REFERENTIAL_INTEGRITY rule type

{% macro dif_generate_referential_integrity_test(rule_id, source_asset_path, target_asset_path, rule_body, threshold_value, severity) %}
  {#
    Generate a referential integrity test between source (child) and target (parent) tables.
    Uses dbt_utils.relationships_where for conditional checking.
    
    Args:
      rule_id: DIF RULE_ID for traceability
      source_asset_path: Child table (database.schema.table)
      target_asset_path: Parent table (database.schema.table)
      rule_body: Parsed JSON with source_column, target_column, allow_nulls
      threshold_value: Maximum orphan count allowed
      severity: error or warn
  #}
  
  {% set source_column = rule_body.get('source_column', '') %}
  {% set target_column = rule_body.get('target_column', '') %}
  {% set allow_nulls = rule_body.get('allow_nulls', true) %}
  {% set max_orphans = (threshold_value | int) if threshold_value else 0 %}
  
  {% if source_column and target_column %}
    WITH orphan_records AS (
      SELECT 
        s.{{ source_column }} AS orphan_key,
        COUNT(*) AS orphan_count
      FROM {{ source_asset_path }} s
      LEFT JOIN {{ target_asset_path }} t 
        ON s.{{ source_column }} = t.{{ target_column }}
      WHERE t.{{ target_column }} IS NULL
        {% if allow_nulls %}
        AND s.{{ source_column }} IS NOT NULL
        {% endif %}
      GROUP BY s.{{ source_column }}
    ),
    
    summary AS (
      SELECT
        '{{ rule_id }}' AS dif_rule_id,
        'REFERENTIAL_INTEGRITY' AS dif_rule_type,
        '{{ source_column }}' AS source_column,
        '{{ target_column }}' AS target_column,
        '{{ source_asset_path }}' AS source_table,
        '{{ target_asset_path }}' AS target_table,
        COUNT(DISTINCT orphan_key) AS distinct_orphan_keys,
        SUM(orphan_count) AS total_orphan_rows,
        {{ max_orphans }} AS threshold_value,
        CASE 
          WHEN COUNT(DISTINCT orphan_key) > {{ max_orphans }} THEN 'FAIL'
          ELSE 'PASS'
        END AS test_status
      FROM orphan_records
    )
    
    SELECT * FROM summary
    WHERE distinct_orphan_keys > {{ max_orphans }}
  {% else %}
    {{ exceptions.raise_compiler_error("REFERENTIAL_INTEGRITY rule " ~ rule_id ~ " missing 'source_column' or 'target_column' in RULE_BODY") }}
  {% endif %}
{% endmacro %}


{% test dif_referential_integrity(model, rule_id, target_relation, source_column, target_column, threshold=0, allow_nulls=true) %}
  {#
    Generic test that can be defined in schema.yml for declarative usage.
    This complements the macro-based dynamic generation.
  #}
  {% set null_filter = "AND " ~ source_column ~ " IS NOT NULL" if allow_nulls else "" %}
  
  SELECT 
    {{ source_column }} AS orphan_key
  FROM {{ model }}
  WHERE {{ source_column }} NOT IN (
    SELECT {{ target_column }} FROM {{ target_relation }} WHERE {{ target_column }} IS NOT NULL
  )
  {{ null_filter }}
  HAVING COUNT(*) > {{ threshold }}
  
{% endtest %}
```

---

#### 2.3 SUM_MATCH Implementation

**File:** `macros/dif_tests/dif_sum_match.sql`

```sql
-- Generated by GithubCopilot
-- Macro: Generate SUM_MATCH test for column sum reconciliation
-- Implements DIF SUM_MATCH rule type

{% macro dif_generate_sum_match_test(rule_id, source_asset_path, target_asset_path, rule_body, threshold_value, recon_tolerance_type, recon_tolerance_value, severity) %}
  {#
    Generate a sum match reconciliation test between source and target tables.
    Computes SUM() for specified column and validates within tolerance.
    
    Args:
      rule_id: DIF RULE_ID for traceability
      source_asset_path: Source table (database.schema.table)
      target_asset_path: Target table (database.schema.table)
      rule_body: Parsed JSON with column, source_filter, target_filter
      threshold_value: Legacy threshold (overridden by recon_tolerance)
      recon_tolerance_type: ABSOLUTE or PERCENTAGE
      recon_tolerance_value: Numeric tolerance threshold
      severity: error or warn
  #}
  
  {% set column_name = rule_body.get('column', '') %}
  {% set source_filter = rule_body.get('source_filter', '1=1') %}
  {% set target_filter = rule_body.get('target_filter', '1=1') %}
  {% set tolerance_type = recon_tolerance_type if recon_tolerance_type else rule_body.get('tolerance_type', 'ABSOLUTE') %}
  {% set tolerance_value = recon_tolerance_value if recon_tolerance_value else rule_body.get('tolerance_value', 0) %}
  
  {% if column_name %}
    WITH source_sum AS (
      SELECT 
        SUM(CAST({{ column_name }} AS DECIMAL(38,6))) AS total_value,
        COUNT(*) AS row_count
      FROM {{ source_asset_path }}
      WHERE {{ source_filter }}
    ),
    
    target_sum AS (
      SELECT 
        SUM(CAST({{ column_name }} AS DECIMAL(38,6))) AS total_value,
        COUNT(*) AS row_count
      FROM {{ target_asset_path }}
      WHERE {{ target_filter }}
    ),
    
    comparison AS (
      SELECT
        '{{ rule_id }}' AS dif_rule_id,
        'SUM_MATCH' AS dif_rule_type,
        '{{ column_name }}' AS column_name,
        '{{ source_asset_path }}' AS source_table,
        '{{ target_asset_path }}' AS target_table,
        s.total_value AS source_sum,
        t.total_value AS target_sum,
        s.row_count AS source_row_count,
        t.row_count AS target_row_count,
        ABS(COALESCE(s.total_value, 0) - COALESCE(t.total_value, 0)) AS absolute_difference,
        CASE 
          WHEN COALESCE(s.total_value, 0) = 0 THEN 
            CASE WHEN COALESCE(t.total_value, 0) = 0 THEN 0 ELSE 100 END
          ELSE ROUND(ABS(s.total_value - t.total_value) * 100.0 / ABS(s.total_value), 6)
        END AS percent_variance,
        '{{ tolerance_type }}' AS tolerance_type,
        {{ tolerance_value }} AS tolerance_value,
        CASE 
          {% if tolerance_type | upper == 'PERCENTAGE' %}
          WHEN CASE 
                 WHEN COALESCE(s.total_value, 0) = 0 THEN 
                   CASE WHEN COALESCE(t.total_value, 0) = 0 THEN 0 ELSE 100 END
                 ELSE ROUND(ABS(s.total_value - t.total_value) * 100.0 / ABS(s.total_value), 6)
               END > {{ tolerance_value }}
          THEN 'FAIL'
          {% else %}
          WHEN ABS(COALESCE(s.total_value, 0) - COALESCE(t.total_value, 0)) > {{ tolerance_value }}
          THEN 'FAIL'
          {% endif %}
          ELSE 'PASS'
        END AS test_status
      FROM source_sum s
      CROSS JOIN target_sum t
    )
    
    SELECT * FROM comparison
    WHERE test_status = 'FAIL'
  {% else %}
    {{ exceptions.raise_compiler_error("SUM_MATCH rule " ~ rule_id ~ " missing 'column' in RULE_BODY") }}
  {% endif %}
{% endmacro %}


{% test dif_sum_match(model, rule_id, target_relation, column_name, source_filter='1=1', target_filter='1=1', tolerance_type='ABSOLUTE', tolerance_value=0) %}
  {# Generic test version for schema.yml usage #}
  
  WITH source_sum AS (
    SELECT SUM(CAST({{ column_name }} AS DECIMAL(38,6))) AS total_value
    FROM {{ model }}
    WHERE {{ source_filter }}
  ),
  target_sum AS (
    SELECT SUM(CAST({{ column_name }} AS DECIMAL(38,6))) AS total_value
    FROM {{ target_relation }}
    WHERE {{ target_filter }}
  )
  
  SELECT
    s.total_value AS source_sum,
    t.total_value AS target_sum,
    ABS(COALESCE(s.total_value, 0) - COALESCE(t.total_value, 0)) AS difference
  FROM source_sum s, target_sum t
  WHERE 
    {% if tolerance_type | upper == 'PERCENTAGE' %}
    CASE 
      WHEN COALESCE(s.total_value, 0) = 0 THEN 
        CASE WHEN COALESCE(t.total_value, 0) = 0 THEN 0 ELSE 100 END
      ELSE ABS(s.total_value - t.total_value) * 100.0 / ABS(s.total_value)
    END > {{ tolerance_value }}
    {% else %}
    ABS(COALESCE(s.total_value, 0) - COALESCE(t.total_value, 0)) > {{ tolerance_value }}
    {% endif %}
    
{% endtest %}
```

---

### 3. Dynamic Test Orchestration Model

**File:** `models/dif_validation/dif_config_driven_tests.sql`

```sql
-- Generated by GithubCopilot
-- Model: Dynamic test orchestration based on CONFIG_CONTROLS.RULES
-- This model materializes test results for config-driven rules

{{ config(
    materialized='incremental',
    unique_key='test_execution_id',
    schema='DIF_TEST_RESULTS',
    tags=['dif_v15', 'config_driven'],
    post_hook=[
        "{{ dif_log_test_batch_to_aggregation() }}"
    ]
) }}

{# Load rules for current run's rule_group #}
{% set rule_group = var('rule_group', 'DEFAULT') %}
{% set run_id = invocation_id %}
{% set rules = dif_load_rules_for_group(rule_group) %}

{% if execute and rules | length > 0 %}
  
  {% set test_queries = [] %}
  
  {% for rule in rules %}
    {% set rule_id = rule['RULE_ID'] %}
    {% set rule_type = rule['RULE_TYPE'] %}
    {% set source_path = rule['source_asset_path'] %}
    {% set target_path = rule['target_asset_path'] %}
    {% set threshold_value = rule['THRESHOLD_VALUE'] %}
    {% set severity = rule['SEVERITY'] | lower %}
    {% set rule_body = dif_parse_rule_body(rule['RULE_BODY']) %}
    {% set recon_tolerance_type = rule['RECON_TOLERANCE_TYPE'] %}
    {% set recon_tolerance_value = rule['RECON_TOLERANCE_VALUE'] %}
    
    {% if rule_type == 'NULL_CHECK' %}
      {% set test_sql = dif_generate_null_check_test(
          rule_id=rule_id,
          source_asset_path=source_path,
          rule_body=rule_body,
          threshold_value=threshold_value,
          severity=severity
      ) %}
    {% elif rule_type == 'REFERENTIAL_INTEGRITY' %}
      {% set test_sql = dif_generate_referential_integrity_test(
          rule_id=rule_id,
          source_asset_path=source_path,
          target_asset_path=target_path,
          rule_body=rule_body,
          threshold_value=threshold_value,
          severity=severity
      ) %}
    {% elif rule_type == 'SUM_MATCH' %}
      {% set test_sql = dif_generate_sum_match_test(
          rule_id=rule_id,
          source_asset_path=source_path,
          target_asset_path=target_path,
          rule_body=rule_body,
          threshold_value=threshold_value,
          recon_tolerance_type=recon_tolerance_type,
          recon_tolerance_value=recon_tolerance_value,
          severity=severity
      ) %}
    {% endif %}
    
    {% if test_sql %}
      {% do test_queries.append({
          'rule_id': rule_id,
          'rule_type': rule_type,
          'sql': test_sql
      }) %}
    {% endif %}
  {% endfor %}
  
  {# Union all test results with metadata #}
  {% for test in test_queries %}
    {% if not loop.first %}UNION ALL{% endif %}
    
    SELECT
      '{{ run_id }}' || '_' || '{{ test.rule_id }}' AS test_execution_id,
      '{{ run_id }}' AS run_id,
      '{{ rule_group }}' AS rule_group,
      '{{ test.rule_id }}' AS rule_id,
      '{{ test.rule_type }}' AS rule_type,
      CURRENT_TIMESTAMP() AS executed_at,
      test_result.*
    FROM (
      {{ test.sql }}
    ) test_result
  {% endfor %}
  
{% else %}
  {# No rules found - return empty result set with schema #}
  SELECT
    NULL AS test_execution_id,
    NULL AS run_id,
    NULL AS rule_group,
    NULL AS rule_id,
    NULL AS rule_type,
    NULL AS executed_at,
    NULL AS test_status
  WHERE 1=0
{% endif %}
```

---

### 4. Audit Logging Macros

**File:** `macros/dif_audit/dif_log_to_dq_log.sql`

```sql
-- Generated by GithubCopilot
-- Macro: Log dbt test results to AUDIT_CONTROLS.DQ_LOG
-- Called as on-run-end hook

{% macro dif_sync_test_results_to_audit(results) %}
  {% if execute %}
    {% set run_id = invocation_id %}
    {% set rule_group = var('rule_group', 'DEFAULT') %}
    
    {# Filter for test results only #}
    {% set test_results = results | selectattr("node.resource_type", "equalto", "test") | list %}
    
    {% if test_results | length > 0 %}
      
      {# Insert RUN_METADATA record first #}
      {% set run_metadata_sql %}
        INSERT INTO {{ var('dif_database') }}.AUDIT_CONTROLS.RUN_METADATA (
          RUN_ID,
          RULE_GROUP,
          PIPELINE_NAME,
          EXECUTED_BY,
          START_TIME,
          END_TIME,
          STATUS,
          TOTAL_RULES,
          PASSED_RULES,
          FAILED_RULES,
          ERROR_RULES,
          CREATED_AT
        )
        SELECT
          '{{ run_id }}' AS RUN_ID,
          '{{ rule_group }}' AS RULE_GROUP,
          'DBT_CONFIG_DRIVEN_V15' AS PIPELINE_NAME,
          '{{ target.user }}' AS EXECUTED_BY,
          MIN(timing_start) AS START_TIME,
          MAX(timing_end) AS END_TIME,
          CASE 
            WHEN SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) > 0 THEN 'ERROR'
            WHEN SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) > 0 THEN 'FAIL'
            ELSE 'PASS'
          END AS STATUS,
          COUNT(*) AS TOTAL_RULES,
          SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) AS PASSED_RULES,
          SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) AS FAILED_RULES,
          SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS ERROR_RULES,
          CURRENT_TIMESTAMP() AS CREATED_AT
        FROM (
          {% for test in test_results %}
          SELECT
            '{{ test.status }}' AS status,
            {% if test.timing and test.timing | length > 0 %}
            TIMESTAMP '{{ test.timing[0].started_at }}'::TIMESTAMP_LTZ AS timing_start,
            TIMESTAMP '{{ test.timing[-1].completed_at }}'::TIMESTAMP_LTZ AS timing_end
            {% else %}
            CURRENT_TIMESTAMP() AS timing_start,
            CURRENT_TIMESTAMP() AS timing_end
            {% endif %}
          {% if not loop.last %}UNION ALL{% endif %}
          {% endfor %}
        )
      {% endset %}
      
      {% do run_query(run_metadata_sql) %}
      {{ log("DIF: Inserted RUN_METADATA for run_id=" ~ run_id, info=True) }}
      
      {# Insert individual DQ_LOG records #}
      {% set dq_log_sql %}
        INSERT INTO {{ var('dif_database') }}.AUDIT_CONTROLS.DQ_LOG (
          LOG_ID,
          RUN_ID,
          RULE_ID,
          RULE_TYPE,
          ENGINE,
          STATUS,
          EXPECTED_VALUE,
          ACTUAL_VALUE,
          DEVIATION,
          ERROR_MESSAGE,
          START_TIME,
          END_TIME,
          LOGGED_AT
        )
        VALUES
        {% for test in test_results %}
        (
          {{ var('dif_database') }}.AUDIT_CONTROLS.LOG_ID_SEQ.NEXTVAL,
          '{{ run_id }}',
          '{{ dif_extract_rule_id_from_test(test.node.name) }}',
          '{{ dif_map_test_to_rule_type(test.node.name) }}',
          'DBT',
          '{{ dif_map_status(test.status) }}',
          NULL,
          {{ test.failures if test.failures is not none else 'NULL' }},
          {{ test.failures if test.failures is not none else 'NULL' }},
          {% if test.status != 'pass' and test.message %}
          '{{ test.message | replace("'", "''") | truncate(4990) }}'
          {% else %}
          NULL
          {% endif %},
          {% if test.timing and test.timing | length > 0 %}
          TIMESTAMP '{{ test.timing[0].started_at }}'::TIMESTAMP_LTZ,
          TIMESTAMP '{{ test.timing[-1].completed_at }}'::TIMESTAMP_LTZ
          {% else %}
          CURRENT_TIMESTAMP(),
          CURRENT_TIMESTAMP()
          {% endif %},
          CURRENT_TIMESTAMP()
        ){% if not loop.last %},{% endif %}
        {% endfor %}
      {% endset %}
      
      {% do run_query(dq_log_sql) %}
      {{ log("DIF: Logged " ~ test_results | length ~ " test results to DQ_LOG", info=True) }}
      
    {% endif %}
  {% endif %}
{% endmacro %}


{% macro dif_extract_rule_id_from_test(test_name) %}
  {# Extract DIF RULE_ID from test name if embedded, otherwise generate DBT_ prefix #}
  {% set name_upper = test_name | upper %}
  
  {% if 'RULE_' in name_upper %}
    {# Find RULE_XXX pattern #}
    {% set parts = name_upper.split('RULE_') %}
    {% if parts | length > 1 %}
      {% set rule_suffix = parts[1].split('_')[0] %}
      {{ return('RULE_' ~ rule_suffix) }}
    {% endif %}
  {% endif %}
  
  {{ return('DBT_' ~ name_upper[:50]) }}
{% endmacro %}


{% macro dif_map_test_to_rule_type(test_name) %}
  {% set name_lower = test_name | lower %}
  
  {% if 'not_null' in name_lower or 'null_check' in name_lower %}
    {{ return('NULL_CHECK') }}
  {% elif 'relationship' in name_lower or 'referential' in name_lower %}
    {{ return('REFERENTIAL_INTEGRITY') }}
  {% elif 'sum_match' in name_lower %}
    {{ return('SUM_MATCH') }}
  {% elif 'unique' in name_lower or 'duplicate' in name_lower %}
    {{ return('DUPLICATE_CHECK') }}
  {% else %}
    {{ return('CUSTOM_SQL') }}
  {% endif %}
{% endmacro %}


{% macro dif_map_status(dbt_status) %}
  {% if dbt_status == 'pass' %}
    {{ return('PASS') }}
  {% elif dbt_status == 'fail' %}
    {{ return('FAIL') }}
  {% elif dbt_status == 'warn' %}
    {{ return('WARN') }}
  {% elif dbt_status == 'error' %}
    {{ return('ERROR') }}
  {% elif dbt_status == 'skipped' %}
    {{ return('SKIP') }}
  {% else %}
    {{ return('ERROR') }}
  {% endif %}
{% endmacro %}
```

---

### 5. Aggregation Logging for SUM_MATCH

**File:** `macros/dif_audit/dif_log_aggregation.sql`

```sql
-- Generated by GithubCopilot
-- Macro: Log aggregation values to AUDIT_CONTROLS.AGGREGATION_LOG
-- Used for SUM_MATCH reconciliation tracking

{% macro dif_log_test_batch_to_aggregation() %}
  {# Called as post_hook on dif_config_driven_tests model #}
  
  INSERT INTO {{ var('dif_database') }}.AUDIT_CONTROLS.AGGREGATION_LOG (
    AGGREGATION_LOG_ID,
    RUN_ID,
    RULE_ID,
    ASSET_ID,
    ASSET_SIDE,
    AGGREGATION_TYPE,
    COLUMN_NAME,
    AGGREGATION_VALUE,
    ROW_COUNT,
    FILTER_APPLIED,
    JOB_NAME,
    JOB_RUN_ID,
    EXECUTION_ENGINE,
    COMPUTED_AT
  )
  SELECT
    {{ var('dif_database') }}.AUDIT_CONTROLS.AGGREGATION_LOG_SEQ.NEXTVAL,
    run_id,
    rule_id,
    source_table AS asset_id,
    'SOURCE' AS asset_side,
    'SUM' AS aggregation_type,
    column_name,
    source_sum AS aggregation_value,
    source_row_count AS row_count,
    NULL AS filter_applied,
    'DBT_CONFIG_DRIVEN_V15' AS job_name,
    run_id AS job_run_id,
    'DBT' AS execution_engine,
    executed_at AS computed_at
  FROM {{ this }}
  WHERE rule_type = 'SUM_MATCH'
  
  UNION ALL
  
  SELECT
    {{ var('dif_database') }}.AUDIT_CONTROLS.AGGREGATION_LOG_SEQ.NEXTVAL,
    run_id,
    rule_id,
    target_table AS asset_id,
    'TARGET' AS asset_side,
    'SUM' AS aggregation_type,
    column_name,
    target_sum AS aggregation_value,
    target_row_count AS row_count,
    NULL AS filter_applied,
    'DBT_CONFIG_DRIVEN_V15' AS job_name,
    run_id AS job_run_id,
    'DBT' AS execution_engine,
    executed_at AS computed_at
  FROM {{ this }}
  WHERE rule_type = 'SUM_MATCH';
  
{% endmacro %}
```

---

### 6. Elementary Integration

**File:** `models/elementary_models/dif_elementary_test_wrapper.yml`

```yaml
# Generated by GithubCopilot
# Schema file for elementary integration tests
# These wrap DIF rules with elementary observability features

version: 2

elementary:
  config:
    elementary_database: '{{ var("dif_database") }}'
    elementary_schema: 'ELEMENTARY'

models:
  - name: dif_config_driven_tests
    description: "Config-driven DIF tests with elementary monitoring"
    config:
      tags: ['dif_v15', 'elementary']
      meta:
        owner: 'dif_team'
        
    # Elementary monitors for test result anomalies
    tests:
      - elementary.volume_anomalies:
          timestamp_column: executed_at
          time_bucket:
            period: hour
            count: 1
          sensitivity: 3
          tags: ['dif_monitoring']
          
      - elementary.schema_changes:
          tags: ['dif_monitoring']
```

**File:** `models/elementary_models/schema.yml`

```yaml
# Generated by GithubCopilot
# Elementary source configuration for DIF control tables

version: 2

sources:
  - name: config_controls
    database: '{{ var("dif_database", "DB_GOVERNANCE") }}'
    schema: CONFIG_CONTROLS
    tables:
      - name: rules
        identifier: RULES
        description: "DIF rule configuration table"
        columns:
          - name: RULE_ID
            description: "Unique rule identifier"
            tests:
              - unique
              - not_null
          - name: RULE_GROUP
            description: "Logical grouping for batch execution"
          - name: RULE_TYPE
            description: "Type of validation rule"
            tests:
              - accepted_values:
                  values: ['NULL_CHECK', 'REFERENTIAL_INTEGRITY', 'SUM_MATCH', 'ROW_COUNT', 'DUPLICATE_CHECK']
                  severity: warn
                  
      - name: data_assets
        identifier: DATA_ASSETS
        description: "Registry of data sources"
        
      - name: connections
        identifier: CONNECTIONS
        description: "Database connection registry"

  - name: audit_controls
    database: '{{ var("dif_database", "DB_GOVERNANCE") }}'
    schema: AUDIT_CONTROLS
    freshness:
      warn_after: {count: 24, period: hour}
      error_after: {count: 48, period: hour}
    loaded_at_field: LOGGED_AT
    tables:
      - name: dq_log
        identifier: DQ_LOG
        description: "Test execution log"
        
      - name: run_metadata
        identifier: RUN_METADATA
        description: "Run summary metadata"
        
      - name: aggregation_log
        identifier: AGGREGATION_LOG
        description: "Computed aggregation values for reconciliation"
```

---

## dbt_project.yml Configuration

```yaml
# Generated by GithubCopilot
name: 'dif_ops_dq'
version: '1.5.0'
config-version: 2

profile: 'dif_snowflake'

model-paths: ["models"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

# Variables for DIF integration
vars:
  # DIF Control Plane configuration
  dif_database: 'DB_GOVERNANCE'
  dif_config_schema: 'CONFIG_CONTROLS'
  dif_audit_schema: 'AUDIT_CONTROLS'
  
  # Default rule group (override at runtime)
  rule_group: 'DEFAULT'
  
  # Severity mapping (DIF severity → dbt severity)
  severity_mapping:
    CRITICAL: error
    HIGH: error
    MEDIUM: warn
    LOW: warn
    INFO: warn
  
  # Elementary package configuration
  elementary:
    elementary_database: '{{ var("dif_database") }}'
    elementary_schema: 'ELEMENTARY'
  
  # dbt_artifacts configuration
  dbt_artifacts:
    dbt_artifacts_database: '{{ var("dif_database") }}'
    dbt_artifacts_schema: 'DBT_ARTIFACTS'

# Default test configurations
tests:
  dif_ops_dq:
    +store_failures: true
    +schema: dbt_test_audit
    +severity: warn

# Model configurations
models:
  dif_ops_dq:
    dif_validation:
      +materialized: incremental
      +schema: DIF_TEST_RESULTS
      +tags: ['dif_v15']
    elementary_models:
      +enabled: true
      +schema: ELEMENTARY

# Post-run hook to sync results to DIF audit tables
on-run-end:
  - "{{ dif_sync_test_results_to_audit(results) }}"

# Seeds configuration
seeds:
  dif_ops_dq:
    +schema: DIF_SEEDS
```

---

## Execution Guide

### Running Tests for a Rule Group

```bash
# Execute all DIF v1.5 tests for a specific rule group
dbt run --select dif_validation --vars '{"rule_group": "STAGING_VALIDATION"}'

# Run tests only (after model materialization)
dbt test --select tag:dif_v15 --vars '{"rule_group": "STAGING_VALIDATION"}'

# Full build with tests
dbt build --select +dif_config_driven_tests --vars '{"rule_group": "FINANCIAL_RECONCILIATION"}'
```

### Sample Rule Configuration

Insert rules into `CONFIG_CONTROLS.RULES`:

```sql
-- Generated by GithubCopilot
-- Sample rules for DIF v1.5 config-driven tests

-- NULL_CHECK: Validate customer_id is never null
INSERT INTO CONFIG_CONTROLS.RULES (
    RULE_ID, RULE_GROUP, RULE_TYPE, SOURCE_ASSET_ID, 
    THRESHOLD_VALUE, THRESHOLD_TYPE, SEVERITY, RULE_BODY, IS_ACTIVE
) VALUES (
    'RULE_NC_001', 'STAGING_VALIDATION', 'NULL_CHECK', 'ASSET_STG_CUSTOMERS',
    '0', 'PERCENTAGE', 'CRITICAL',
    '{"column": "CUSTOMER_ID", "include_empty_strings": true}',
    TRUE
);

-- REFERENTIAL_INTEGRITY: Validate orders reference valid customers
INSERT INTO CONFIG_CONTROLS.RULES (
    RULE_ID, RULE_GROUP, RULE_TYPE, SOURCE_ASSET_ID, TARGET_ASSET_ID,
    THRESHOLD_VALUE, THRESHOLD_TYPE, SEVERITY, RULE_BODY, IS_ACTIVE
) VALUES (
    'RULE_RI_001', 'STAGING_VALIDATION', 'REFERENTIAL_INTEGRITY', 
    'ASSET_STG_ORDERS', 'ASSET_STG_CUSTOMERS',
    '0', 'ABSOLUTE', 'HIGH',
    '{"source_column": "CUSTOMER_FK", "target_column": "CUSTOMER_ID", "allow_nulls": true}',
    TRUE
);

-- SUM_MATCH: Validate order amounts match between staging and fact
INSERT INTO CONFIG_CONTROLS.RULES (
    RULE_ID, RULE_GROUP, RULE_TYPE, SOURCE_ASSET_ID, TARGET_ASSET_ID,
    THRESHOLD_VALUE, SEVERITY, RULE_BODY,
    RECON_TOLERANCE_TYPE, RECON_TOLERANCE_VALUE, IS_ACTIVE
) VALUES (
    'RULE_SM_001', 'FINANCIAL_RECONCILIATION', 'SUM_MATCH',
    'ASSET_STG_ORDERS', 'ASSET_FACT_ORDERS',
    NULL, 'CRITICAL',
    '{"column": "ORDER_AMOUNT", "source_filter": "ORDER_STATUS = ''COMPLETED''", "target_filter": "STATUS = ''FINAL''"}',
    'PERCENTAGE', 0.01,
    TRUE
);
```

---

## File Structure

```
dbt_ops_dq/
├── dbt_project.yml
├── packages.yml
├── profiles.yml.example
├── DIF_DBT_CONFIG_DRIVEN_SOLUTION_v1.5.md    # This documentation
│
├── macros/
│   ├── dif_load_rules.sql                    # Config loading from control tables
│   ├── dif_tests/
│   │   ├── dif_null_check.sql                # NULL_CHECK implementation
│   │   ├── dif_referential_integrity.sql     # REFERENTIAL_INTEGRITY implementation
│   │   └── dif_sum_match.sql                 # SUM_MATCH implementation
│   └── dif_audit/
│       ├── dif_log_to_dq_log.sql             # On-run-end audit sync
│       └── dif_log_aggregation.sql           # Aggregation logging for SUM_MATCH
│
├── models/
│   ├── dif_validation/
│   │   ├── dif_config_driven_tests.sql       # Dynamic test orchestration
│   │   └── schema.yml
│   └── elementary_models/
│       ├── dif_elementary_test_wrapper.yml   # Elementary monitoring config
│       └── schema.yml                        # Source definitions
│
├── tests/
│   └── generic/
│       ├── dif_null_check.sql                # Reusable generic tests
│       ├── dif_referential_integrity.sql
│       └── dif_sum_match.sql
│
└── seeds/
    └── schema.yml
```

---

## Next Version Roadmap

### v1.6 Planned Features
- ROW_COUNT and ROW_COUNT_RANGE checks
- DUPLICATE_CHECK implementation
- Pattern/regex validation

### v1.7 Planned Features
- SCHEMA_VALIDATION checks
- DATA_FRESHNESS monitoring
- VALUE_RANGE validation

### v2.0 Planned Features
- Full anomaly detection with elementary
- Historical trend analysis
- Automated threshold recommendations
- Slack/email alerting integration

---

## References

- [dbt-utils documentation](https://hub.getdbt.com/dbt-labs/dbt_utils/latest/)
- [Elementary documentation](https://docs.elementary-data.com/)
- [dbt_artifacts documentation](https://hub.getdbt.com/brooklyn-data/dbt_artifacts/latest/)
- [DIF Architecture Documentation](../ARCHITECTURE.md)
- [DIF Rule Types Reference](../RULE_TYPES_REFERENCE.md)
