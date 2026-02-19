
{# Macro: Log aggregation values to AUDIT_CONTROLS.AGGREGATION_LOG #}
{# Used for SUM_MATCH reconciliation tracking and historical analysis #}

{% macro dif_log_test_batch_to_aggregation() %}
  {#
    Log aggregation values computed during SUM_MATCH tests.
    Called as post_hook on dif_config_driven_tests model.
    
    Populates AGGREGATION_LOG for:
    - Historical trend analysis
    - Cross-run reconciliation comparison
    - Audit trail of computed values
    
    Side Effects:
      - Inserts rows into AUDIT_CONTROLS.AGGREGATION_LOG
  #}
  
  {% set dif_db = var('dif_database', 'DB_GOVERNANCE') %}
  
  INSERT INTO {{ dif_db }}.AUDIT_CONTROLS.AGGREGATION_LOG (
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
  -- Log SOURCE side aggregations
  SELECT
    {{ dif_db }}.AUDIT_CONTROLS.AGGREGATION_LOG_SEQ.NEXTVAL,
    run_id,
    dif_rule_id,
    source_table AS asset_id,
    'SOURCE' AS asset_side,
    'SUM' AS aggregation_type,
    column_name,
    source_sum AS aggregation_value,
    source_row_count AS row_count,
    source_filter_applied AS filter_applied,
    'DBT_CONFIG_DRIVEN_V15' AS job_name,
    run_id AS job_run_id,
    'DBT' AS execution_engine,
    executed_at AS computed_at
  FROM {{ this }}
  WHERE dif_rule_type = 'SUM_MATCH'
  
  UNION ALL
  
  -- Log TARGET side aggregations
  SELECT
    {{ dif_db }}.AUDIT_CONTROLS.AGGREGATION_LOG_SEQ.NEXTVAL,
    run_id,
    dif_rule_id,
    target_table AS asset_id,
    'TARGET' AS asset_side,
    'SUM' AS aggregation_type,
    column_name,
    target_sum AS aggregation_value,
    target_row_count AS row_count,
    target_filter_applied AS filter_applied,
    'DBT_CONFIG_DRIVEN_V15' AS job_name,
    run_id AS job_run_id,
    'DBT' AS execution_engine,
    executed_at AS computed_at
  FROM {{ this }}
  WHERE dif_rule_type = 'SUM_MATCH';
  
{% endmacro %}


{% macro dif_log_null_check_metrics() %}
  {#
    Log null check metrics for historical tracking.
    Can be used to populate profiling tables or anomaly detection.
    
    Called as optional post_hook for detailed null tracking.
  #}
  
  {% set dif_db = var('dif_database', 'DB_GOVERNANCE') %}
  
  INSERT INTO {{ dif_db }}.AUDIT_CONTROLS.AGGREGATION_LOG (
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
    {{ dif_db }}.AUDIT_CONTROLS.AGGREGATION_LOG_SEQ.NEXTVAL,
    run_id,
    dif_rule_id,
    source_table AS asset_id,
    'SOURCE' AS asset_side,
    'NULL_COUNT' AS aggregation_type,
    column_name,
    null_count AS aggregation_value,
    total_rows AS row_count,
    NULL AS filter_applied,
    'DBT_CONFIG_DRIVEN_V15' AS job_name,
    run_id AS job_run_id,
    'DBT' AS execution_engine,
    executed_at AS computed_at
  FROM {{ this }}
  WHERE dif_rule_type = 'NULL_CHECK';
  
{% endmacro %}


{% macro dif_create_aggregation_log_sequence() %}
  {#
    Ensure AGGREGATION_LOG_SEQ sequence exists.
    Run this once during setup or include in pre-hook.
  #}
  
  {% set dif_db = var('dif_database', 'DB_GOVERNANCE') %}
  
  CREATE SEQUENCE IF NOT EXISTS {{ dif_db }}.AUDIT_CONTROLS.AGGREGATION_LOG_SEQ
    START WITH 1
    INCREMENT BY 1;
    
{% endmacro %}
