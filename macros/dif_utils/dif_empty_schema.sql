
{# Macro: Return empty test result schema for no-op scenarios #}

{% macro dif_empty_test_result_schema() %}
  {#
    Returns an empty result set with the expected schema.
    Used when no rules exist for a rule_group or no tests are generated.
    
    Ensures consistent schema regardless of rule execution.
  #}
  
  SELECT
    CAST(NULL AS VARCHAR(500)) AS test_execution_id,
    CAST(NULL AS VARCHAR(200)) AS run_id,
    CAST(NULL AS VARCHAR(200)) AS rule_group,
    CAST(NULL AS TIMESTAMP_LTZ) AS executed_at,
    CAST(NULL AS VARCHAR(200)) AS dif_rule_id,
    CAST(NULL AS VARCHAR(100)) AS dif_rule_type,
    CAST(NULL AS VARCHAR(200)) AS column_name,
    CAST(NULL AS VARCHAR(1000)) AS source_table,
    CAST(NULL AS VARCHAR(1000)) AS target_table,
    CAST(NULL AS NUMBER(38,0)) AS total_rows,
    CAST(NULL AS NUMBER(38,0)) AS null_count,
    CAST(NULL AS NUMBER(38,0)) AS non_null_count,
    CAST(NULL AS DECIMAL(10,4)) AS null_percentage,
    CAST(NULL AS NUMBER(38,0)) AS distinct_orphan_keys,
    CAST(NULL AS NUMBER(38,0)) AS total_orphan_rows,
    CAST(NULL AS DECIMAL(38,6)) AS source_sum,
    CAST(NULL AS DECIMAL(38,6)) AS target_sum,
    CAST(NULL AS NUMBER(38,0)) AS source_row_count,
    CAST(NULL AS NUMBER(38,0)) AS target_row_count,
    CAST(NULL AS DECIMAL(38,6)) AS absolute_difference,
    CAST(NULL AS DECIMAL(10,6)) AS percent_variance,
    CAST(NULL AS VARCHAR(100)) AS threshold_value,
    CAST(NULL AS VARCHAR(50)) AS threshold_type,
    CAST(NULL AS VARCHAR(50)) AS tolerance_type,
    CAST(NULL AS DECIMAL(20,6)) AS tolerance_value,
    CAST(NULL AS VARCHAR(20)) AS test_status,
    CAST(NULL AS VARCHAR(20)) AS severity,
    CAST(NULL AS VARCHAR(2000)) AS source_filter_applied,
    CAST(NULL AS VARCHAR(2000)) AS target_filter_applied
  WHERE 1=0
{% endmacro %}
