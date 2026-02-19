
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
  
  {# Collect all test SQL queries #}
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
    
    {# Generate test SQL based on rule type #}
    {% if rule_type == 'NULL_CHECK' %}
      {% set test_sql = dif_generate_null_check_test(
          rule_id=rule_id,
          source_asset_path=source_path,
          rule_body=rule_body,
          threshold_value=threshold_value,
          severity=severity
      ) %}
      {% if test_sql %}
        {% do test_queries.append({
            'rule_id': rule_id,
            'rule_type': rule_type,
            'sql': test_sql
        }) %}
      {% endif %}
      
    {% elif rule_type == 'REFERENTIAL_INTEGRITY' %}
      {% set test_sql = dif_generate_referential_integrity_test(
          rule_id=rule_id,
          source_asset_path=source_path,
          target_asset_path=target_path,
          rule_body=rule_body,
          threshold_value=threshold_value,
          severity=severity
      ) %}
      {% if test_sql %}
        {% do test_queries.append({
            'rule_id': rule_id,
            'rule_type': rule_type,
            'sql': test_sql
        }) %}
      {% endif %}
      
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
      {% if test_sql %}
        {% do test_queries.append({
            'rule_id': rule_id,
            'rule_type': rule_type,
            'sql': test_sql
        }) %}
      {% endif %}
    {% endif %}
  {% endfor %}
  
  {# Union all test results with execution metadata #}
  {% if test_queries | length > 0 %}
    {% for test in test_queries %}
      {% if not loop.first %}
      UNION ALL
      {% endif %}
      
      SELECT
        '{{ run_id }}' || '_' || '{{ test.rule_id }}' AS test_execution_id,
        '{{ run_id }}' AS run_id,
        '{{ rule_group }}' AS rule_group,
        CURRENT_TIMESTAMP() AS executed_at,
        test_result.*
      FROM (
        {{ test.sql }}
      ) test_result
    {% endfor %}
  {% else %}
    {# No rules generated - return empty result set #}
    {{ dif_empty_test_result_schema() }}
  {% endif %}
  
{% else %}
  {# No rules found for rule_group - return empty result set with schema #}
  {{ dif_empty_test_result_schema() }}
{% endif %}
