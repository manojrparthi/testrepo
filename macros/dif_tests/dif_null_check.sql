
{# Macro: Generate NULL_CHECK test using dbt-utils and elementary #}
{# Implements DIF NULL_CHECK rule type for v1.5 #}

{% macro dif_generate_null_check_test(rule_id, source_asset_path, rule_body, threshold_value, severity) %}
  {#
    Generate a not-null test query for a specific column.
    Uses SQL aggregation for flexible threshold-based checking.
    
    Args:
      rule_id: DIF RULE_ID for traceability
      source_asset_path: Fully qualified table name (database.schema.table)
      rule_body: Parsed JSON dict with 'column' and optional 'include_empty_strings'
      threshold_value: Maximum null percentage allowed (e.g., "0" = no nulls, "5" = 5% allowed)
      severity: DIF severity (mapped to dbt error/warn)
      
    Returns:
      SQL query string that returns failing rows when threshold exceeded
      
    RULE_BODY Schema:
      {
        "column": "COLUMN_NAME",           // Required: column to check
        "include_empty_strings": false     // Optional: treat empty strings as null
      }
  #}
  
  {% set column_name = rule_body.get('column', '') %}
  {% set include_empty = rule_body.get('include_empty_strings', false) %}
  {% set threshold_pct = (threshold_value | float) if threshold_value else 0 %}
  
  {% if column_name %}
    {# Build null check expression based on configuration #}
    {% set null_condition %}
      {{ column_name }} IS NULL
      {% if include_empty %}
      OR TRIM(CAST({{ column_name }} AS VARCHAR)) = ''
      {% endif %}
    {% endset %}
    
    {# -- Unified output schema for UNION ALL compatibility -- #}
    WITH null_stats AS (
      SELECT
        COUNT(*) AS total_rows,
        SUM(CASE WHEN {{ null_condition }} THEN 1 ELSE 0 END) AS null_count
      FROM {{ source_asset_path }}
    )

    SELECT
      '{{ rule_id }}' AS dif_rule_id,
      'NULL_CHECK' AS dif_rule_type,
      '{{ column_name }}' AS column_name,
      '{{ source_asset_path }}' AS source_table,
      CAST(NULL AS VARCHAR(1000)) AS target_table,
      total_rows,
      null_count,
      total_rows - null_count AS non_null_count,
      ROUND(null_count * 100.0 / NULLIF(total_rows, 0), 4) AS null_percentage,
      CAST(NULL AS NUMBER(38,0)) AS distinct_orphan_keys,
      CAST(NULL AS NUMBER(38,0)) AS total_orphan_rows,
      CAST(NULL AS DECIMAL(38,6)) AS source_sum,
      CAST(NULL AS DECIMAL(38,6)) AS target_sum,
      CAST(NULL AS NUMBER(38,0)) AS source_row_count,
      CAST(NULL AS NUMBER(38,0)) AS target_row_count,
      CAST(NULL AS DECIMAL(38,6)) AS absolute_difference,
      CAST(NULL AS DECIMAL(10,6)) AS percent_variance,
      CAST({{ threshold_pct }} AS VARCHAR(100)) AS threshold_value,
      'PERCENTAGE' AS threshold_type,
      CAST(NULL AS VARCHAR(50)) AS tolerance_type,
      CAST(NULL AS DECIMAL(20,6)) AS tolerance_value,
      CASE 
        WHEN ROUND(null_count * 100.0 / NULLIF(total_rows, 0), 4) > {{ threshold_pct }}
        THEN 'FAIL'
        ELSE 'PASS'
      END AS test_status,
      '{{ severity }}' AS severity,
      CAST(NULL AS VARCHAR(2000)) AS source_filter_applied,
      CAST(NULL AS VARCHAR(2000)) AS target_filter_applied
    FROM null_stats
  {% else %}
    {{ exceptions.raise_compiler_error("NULL_CHECK rule " ~ rule_id ~ " missing required 'column' in RULE_BODY") }}
  {% endif %}
{% endmacro %}


{% test dif_null_check(model, rule_id, column_name, threshold_pct=0, include_empty_strings=false) %}
  {#
    Generic test version for schema.yml usage.
    Can be used for declarative test definition alongside config-driven tests.
    
    Usage in schema.yml:
      columns:
        - name: customer_id
          tests:
            - dif_null_check:
                rule_id: RULE_NC_MANUAL_001
                threshold_pct: 0
                include_empty_strings: true
  #}
  
  {% set null_condition %}
    {{ column_name }} IS NULL
    {% if include_empty_strings %}
    OR TRIM(CAST({{ column_name }} AS VARCHAR)) = ''
    {% endif %}
  {% endset %}
  
  SELECT
    '{{ rule_id }}' AS dif_rule_id,
    {{ column_name }} AS failing_value
  FROM {{ model }}
  WHERE {{ null_condition }}
  {% if threshold_pct == 0 %}
  -- Zero tolerance: any null is a failure
  LIMIT 1
  {% else %}
  -- Threshold-based: only return rows if percentage exceeds limit
  AND (
    SELECT ROUND(
      SUM(CASE WHEN {{ null_condition }} THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 
      4
    )
    FROM {{ model }}
  ) > {{ threshold_pct }}
  {% endif %}
  
{% endtest %}


{% macro dif_null_check_with_elementary(model_name, column_name, rule_id) %}
  {#
    Wrapper that registers column with elementary for null monitoring.
    Enables historical trend analysis for null percentages.
    
    Note: This is called separately from the test to set up monitoring.
  #}
  {{ elementary.column_anomalies(
      model=model_name,
      column_name=column_name,
      column_anomalies=['null_count', 'null_percent'],
      timestamp_column=none,
      where=none,
      time_bucket={
        'period': 'day',
        'count': 1
      },
      tags=['dif_v15', 'null_check', rule_id]
  ) }}
{% endmacro %}
