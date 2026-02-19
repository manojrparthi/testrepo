
{# Macro: Log dbt test results to AUDIT_CONTROLS.DQ_LOG #}
{# Called as on-run-end hook to persist all test outcomes #}

{% macro dif_sync_test_results_to_audit(results) %}
  {#
    Sync test results from dbt run to DIF audit tables.
    Handles both RUN_METADATA (summary) and DQ_LOG (individual tests).
    
    Called automatically via on-run-end hook in dbt_project.yml.
    
    Args:
      results: dbt results object containing all execution outcomes
      
    Side Effects:
      - Inserts/updates RUN_METADATA record
      - Inserts DQ_LOG records for each test
  #}
  
  {% if execute %}
    {% set run_id = invocation_id %}
    {% set rule_group = var('rule_group', 'DEFAULT') %}
    {% set dif_db = var('dif_database', 'DB_GOVERNANCE') %}
    
    {# Filter for test results only #}
    {% set test_results = results | selectattr("node.resource_type", "equalto", "test") | list %}
    
    {{ log("DIF: Processing " ~ test_results | length ~ " test results for audit logging", info=True) }}
    
    {% if test_results | length > 0 %}
      
      {# Calculate summary statistics #}
      {% set pass_count = test_results | selectattr("status", "equalto", "pass") | list | length %}
      {% set fail_count = test_results | selectattr("status", "equalto", "fail") | list | length %}
      {% set error_count = test_results | selectattr("status", "equalto", "error") | list | length %}
      {% set warn_count = test_results | selectattr("status", "equalto", "warn") | list | length %}
      
      {# Determine overall status #}
      {% set overall_status = 'PASS' %}
      {% if error_count > 0 %}
        {% set overall_status = 'ERROR' %}
      {% elif fail_count > 0 %}
        {% set overall_status = 'FAIL' %}
      {% elif warn_count > 0 %}
        {% set overall_status = 'WARN' %}
      {% endif %}
      
      {# Check if RUN_METADATA already exists (idempotency) #}
      {% set check_run_exists %}
        SELECT COUNT(*) AS cnt FROM {{ dif_db }}.AUDIT_CONTROLS.RUN_METADATA WHERE RUN_ID = '{{ run_id }}'
      {% endset %}
      {% set run_exists_result = run_query(check_run_exists) %}
      {% set run_exists = run_exists_result.columns[0].values()[0] > 0 %}
      
      {% if run_exists %}
        {# Update existing RUN_METADATA record #}
        {% set update_run_metadata_sql %}
          UPDATE {{ dif_db }}.AUDIT_CONTROLS.RUN_METADATA
          SET 
            END_TIME = CURRENT_TIMESTAMP(),
            STATUS = '{{ overall_status }}',
            TOTAL_RULES = {{ test_results | length }},
            PASSED_RULES = {{ pass_count }},
            FAILED_RULES = {{ fail_count }},
            ERROR_RULES = {{ error_count }}
          WHERE RUN_ID = '{{ run_id }}'
        {% endset %}
        {% do run_query(update_run_metadata_sql) %}
        {{ log("DIF: Updated RUN_METADATA for run_id=" ~ run_id, info=True) }}
      {% else %}
        {# Insert new RUN_METADATA record #}
        {% set insert_run_metadata_sql %}
          INSERT INTO {{ dif_db }}.AUDIT_CONTROLS.RUN_METADATA (
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
          VALUES (
            '{{ run_id }}',
            '{{ rule_group }}',
            'DBT_CONFIG_DRIVEN_V15',
            '{{ target.user }}',
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(),
            '{{ overall_status }}',
            {{ test_results | length }},
            {{ pass_count }},
            {{ fail_count }},
            {{ error_count }},
            CURRENT_TIMESTAMP()
          )
        {% endset %}
        {% do run_query(insert_run_metadata_sql) %}
        {{ log("DIF: Inserted RUN_METADATA for run_id=" ~ run_id, info=True) }}
      {% endif %}
      
      {# Insert individual DQ_LOG records #}
      {% set dq_log_sql %}
        INSERT INTO {{ dif_db }}.AUDIT_CONTROLS.DQ_LOG (
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
          {{ dif_db }}.AUDIT_CONTROLS.LOG_ID_SEQ.NEXTVAL,
          '{{ run_id }}',
          '{{ dif_extract_rule_id_from_test(test.node.name) }}',
          '{{ dif_map_test_to_rule_type(test.node.name) }}',
          'DBT',
          '{{ dif_map_status(test.status) }}',
          NULL,
          {% if test.failures is not none %}{{ test.failures }}{% else %}NULL{% endif %},
          {% if test.failures is not none %}{{ test.failures }}{% else %}NULL{% endif %},
          {% if test.status != 'pass' and test.message %}
          '{{ test.message | replace("'", "''") | replace("\n", " ") | truncate(4990) }}'
          {% else %}
          NULL
          {% endif %},
          {% if test.timing and test.timing | length > 0 %}
          TRY_TO_TIMESTAMP_LTZ('{{ test.timing[0].started_at }}'),
          TRY_TO_TIMESTAMP_LTZ('{{ test.timing[-1].completed_at }}')
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
      
    {% else %}
      {{ log("DIF: No test results to log", info=True) }}
    {% endif %}
  {% endif %}
{% endmacro %}


{% macro dif_extract_rule_id_from_test(test_name) %}
  {#
    Extract DIF RULE_ID from test name if embedded.
    Pattern: Tests may include RULE_XXX in their name from config-driven generation.
    
    Args:
      test_name: dbt test node name
      
    Returns:
      Extracted RULE_ID or generated DBT_<name> identifier
  #}
  {% set name_upper = test_name | upper %}
  
  {# Look for RULE_ pattern (e.g., dif_null_check_RULE_NC_001) #}
  {% if 'RULE_' in name_upper %}
    {% set parts = name_upper.split('RULE_') %}
    {% if parts | length > 1 %}
      {# Extract rule ID (e.g., NC_001 from "NC_001_COLUMN") #}
      {% set rule_suffix = parts[1].split('_') %}
      {% if rule_suffix | length >= 2 %}
        {{ return('RULE_' ~ rule_suffix[0] ~ '_' ~ rule_suffix[1]) }}
      {% else %}
        {{ return('RULE_' ~ rule_suffix[0]) }}
      {% endif %}
    {% endif %}
  {% endif %}
  
  {# No RULE_ pattern found - generate DBT_ prefix identifier #}
  {{ return('DBT_' ~ name_upper[:100]) }}
{% endmacro %}


{% macro dif_map_test_to_rule_type(test_name) %}
  {#
    Map dbt test name to DIF rule type.
    Uses keyword matching to infer rule type.
    
    Args:
      test_name: dbt test node name
      
    Returns:
      DIF rule type string
  #}
  {% set name_lower = test_name | lower %}
  
  {# Match against known patterns #}
  {% if 'not_null' in name_lower or 'null_check' in name_lower %}
    {{ return('NULL_CHECK') }}
  {% elif 'relationship' in name_lower or 'referential' in name_lower %}
    {{ return('REFERENTIAL_INTEGRITY') }}
  {% elif 'sum_match' in name_lower %}
    {{ return('SUM_MATCH') }}
  {% elif 'unique' in name_lower or 'duplicate' in name_lower %}
    {{ return('DUPLICATE_CHECK') }}
  {% elif 'accepted_values' in name_lower or 'value_range' in name_lower %}
    {{ return('VALUE_RANGE') }}
  {% elif 'row_count' in name_lower %}
    {{ return('ROW_COUNT') }}
  {% elif 'freshness' in name_lower %}
    {{ return('DATA_FRESHNESS') }}
  {% else %}
    {{ return('CUSTOM_SQL') }}
  {% endif %}
{% endmacro %}


{% macro dif_map_status(dbt_status) %}
  {#
    Map dbt test status to DIF status enum.
    
    Args:
      dbt_status: dbt status string (pass, fail, warn, error, skipped)
      
    Returns:
      DIF status string (PASS, FAIL, WARN, ERROR, SKIP)
  #}
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
