
{# Macro: Generate SUM_MATCH test for column sum reconciliation #}
{# Implements DIF SUM_MATCH rule type for v1.5 #}

{% macro dif_generate_sum_match_test(rule_id, source_asset_path, target_asset_path, rule_body, threshold_value, recon_tolerance_type, recon_tolerance_value, severity) %}
  {#
    Generate a sum match reconciliation test between source and target tables.
    Computes SUM() for specified column and validates within tolerance.
    
    Args:
      rule_id: DIF RULE_ID for traceability
      source_asset_path: Source table (database.schema.table)
      target_asset_path: Target table (database.schema.table)
      rule_body: Parsed JSON with column, source_filter, target_filter, tolerance settings
      threshold_value: Legacy threshold (overridden by recon_tolerance fields)
      recon_tolerance_type: ABSOLUTE or PERCENTAGE (from RULES.RECON_TOLERANCE_TYPE)
      recon_tolerance_value: Numeric tolerance threshold (from RULES.RECON_TOLERANCE_VALUE)
      severity: DIF severity (mapped to dbt error/warn)
      
    Returns:
      SQL query string that returns comparison when variance exceeds tolerance
      
    RULE_BODY Schema:
      {
        "column": "AMOUNT",                        // Required: numeric column to sum
        "source_filter": "STATUS = 'ACTIVE'",     // Optional: WHERE clause for source
        "target_filter": "STATUS = 'FINAL'",      // Optional: WHERE clause for target
        "tolerance_type": "PERCENTAGE",            // Optional: overrides RECON_TOLERANCE_TYPE
        "tolerance_value": 0.01                    // Optional: overrides RECON_TOLERANCE_VALUE
      }
  #}
  
  {% set column_name = rule_body.get('column', '') %}
  {% set source_filter = rule_body.get('source_filter', '1=1') %}
  {% set target_filter = rule_body.get('target_filter', '1=1') %}
  
  {# Tolerance precedence: rule_body > recon fields > threshold_value > default 0 #}
  {% set tolerance_type = rule_body.get('tolerance_type') or recon_tolerance_type or 'ABSOLUTE' %}
  {% set tolerance_value = rule_body.get('tolerance_value') or recon_tolerance_value or threshold_value or 0 %}
  {% set tolerance_value_float = (tolerance_value | float) if tolerance_value else 0 %}
  
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
    
    {# -- Unified output schema for UNION ALL compatibility -- #}
    comparison AS (
      SELECT
        '{{ rule_id }}' AS dif_rule_id,
        'SUM_MATCH' AS dif_rule_type,
        '{{ column_name }}' AS column_name,
        '{{ source_asset_path }}' AS source_table,
        '{{ target_asset_path }}' AS target_table,
        CAST(NULL AS NUMBER(38,0)) AS total_rows,
        CAST(NULL AS NUMBER(38,0)) AS null_count,
        CAST(NULL AS NUMBER(38,0)) AS non_null_count,
        CAST(NULL AS DECIMAL(10,4)) AS null_percentage,
        CAST(NULL AS NUMBER(38,0)) AS distinct_orphan_keys,
        CAST(NULL AS NUMBER(38,0)) AS total_orphan_rows,
        s.total_value AS source_sum,
        t.total_value AS target_sum,
        s.row_count AS source_row_count,
        t.row_count AS target_row_count,
        ABS(COALESCE(s.total_value, 0) - COALESCE(t.total_value, 0)) AS absolute_difference,
        -- Calculate percent variance (handle divide by zero)
        CASE 
          WHEN COALESCE(s.total_value, 0) = 0 THEN 
            CASE WHEN COALESCE(t.total_value, 0) = 0 THEN 0.0 ELSE 100.0 END
          ELSE ROUND(ABS(s.total_value - t.total_value) * 100.0 / ABS(s.total_value), 6)
        END AS percent_variance,
        CAST(NULL AS VARCHAR(100)) AS threshold_value,
        CAST(NULL AS VARCHAR(50)) AS threshold_type,
        '{{ tolerance_type | upper }}' AS tolerance_type,
        {{ tolerance_value_float }} AS tolerance_value,
        -- Determine pass/fail based on tolerance type
        CASE 
          {% if tolerance_type | upper == 'PERCENTAGE' %}
          WHEN CASE 
                 WHEN COALESCE(s.total_value, 0) = 0 THEN 
                   CASE WHEN COALESCE(t.total_value, 0) = 0 THEN 0.0 ELSE 100.0 END
                 ELSE ROUND(ABS(s.total_value - t.total_value) * 100.0 / ABS(s.total_value), 6)
               END > {{ tolerance_value_float }}
          THEN 'FAIL'
          {% else %}
          WHEN ABS(COALESCE(s.total_value, 0) - COALESCE(t.total_value, 0)) > {{ tolerance_value_float }}
          THEN 'FAIL'
          {% endif %}
          ELSE 'PASS'
        END AS test_status,
        '{{ severity }}' AS severity,
        '{{ source_filter }}' AS source_filter_applied,
        '{{ target_filter }}' AS target_filter_applied
      FROM source_sum s
      CROSS JOIN target_sum t
    )
    
    SELECT * FROM comparison
  {% else %}
    {{ exceptions.raise_compiler_error("SUM_MATCH rule " ~ rule_id ~ " missing required 'column' in RULE_BODY") }}
  {% endif %}
{% endmacro %}


{% test dif_sum_match(model, rule_id, target_relation, column_name, source_filter='1=1', target_filter='1=1', tolerance_type='ABSOLUTE', tolerance_value=0) %}
  {#
    Generic test version for schema.yml usage.
    Enables declarative sum match tests.
    
    Usage in schema.yml:
      models:
        - name: stg_orders
          tests:
            - dif_sum_match:
                rule_id: RULE_SM_MANUAL_001
                target_relation: ref('fact_orders')
                column_name: order_amount
                source_filter: "order_status = 'COMPLETED'"
                target_filter: "status = 'FINAL'"
                tolerance_type: PERCENTAGE
                tolerance_value: 0.01
  #}
  
  {% set tolerance_value_float = (tolerance_value | float) if tolerance_value else 0 %}
  
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
    '{{ rule_id }}' AS dif_rule_id,
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
    END > {{ tolerance_value_float }}
    {% else %}
    ABS(COALESCE(s.total_value, 0) - COALESCE(t.total_value, 0)) > {{ tolerance_value_float }}
    {% endif %}
    
{% endtest %}


{% macro dif_sum_match_by_group(source_asset_path, target_asset_path, sum_column, group_columns, source_filter='1=1', target_filter='1=1', tolerance_type='ABSOLUTE', tolerance_value=0) %}
  {#
    Generate sum match comparison grouped by specified columns.
    Useful for partitioned reconciliation (e.g., by date, region).
    
    Args:
      source_asset_path: Source table
      target_asset_path: Target table
      sum_column: Column to sum
      group_columns: List of columns to group by
      source_filter: Optional WHERE for source
      target_filter: Optional WHERE for target
      tolerance_type: ABSOLUTE or PERCENTAGE
      tolerance_value: Threshold value
      
    Returns:
      SQL query comparing sums per group
  #}
  {% set group_cols = group_columns | join(', ') %}
  {% set tolerance_value_float = (tolerance_value | float) if tolerance_value else 0 %}
  
  WITH source_sums AS (
    SELECT 
      {{ group_cols }},
      SUM(CAST({{ sum_column }} AS DECIMAL(38,6))) AS total_value,
      COUNT(*) AS row_count
    FROM {{ source_asset_path }}
    WHERE {{ source_filter }}
    GROUP BY {{ group_cols }}
  ),
  
  target_sums AS (
    SELECT 
      {{ group_cols }},
      SUM(CAST({{ sum_column }} AS DECIMAL(38,6))) AS total_value,
      COUNT(*) AS row_count
    FROM {{ target_asset_path }}
    WHERE {{ target_filter }}
    GROUP BY {{ group_cols }}
  )
  
  SELECT
    {% for col in group_columns %}
    COALESCE(s.{{ col }}, t.{{ col }}) AS {{ col }},
    {% endfor %}
    s.total_value AS source_sum,
    t.total_value AS target_sum,
    ABS(COALESCE(s.total_value, 0) - COALESCE(t.total_value, 0)) AS absolute_difference,
    CASE 
      WHEN COALESCE(s.total_value, 0) = 0 THEN 
        CASE WHEN COALESCE(t.total_value, 0) = 0 THEN 0 ELSE 100 END
      ELSE ROUND(ABS(s.total_value - t.total_value) * 100.0 / ABS(s.total_value), 6)
    END AS percent_variance,
    CASE 
      {% if tolerance_type | upper == 'PERCENTAGE' %}
      WHEN CASE 
             WHEN COALESCE(s.total_value, 0) = 0 THEN 
               CASE WHEN COALESCE(t.total_value, 0) = 0 THEN 0 ELSE 100 END
             ELSE ROUND(ABS(s.total_value - t.total_value) * 100.0 / ABS(s.total_value), 6)
           END > {{ tolerance_value_float }}
      THEN 'FAIL'
      {% else %}
      WHEN ABS(COALESCE(s.total_value, 0) - COALESCE(t.total_value, 0)) > {{ tolerance_value_float }}
      THEN 'FAIL'
      {% endif %}
      ELSE 'PASS'
    END AS status
  FROM source_sums s
  FULL OUTER JOIN target_sums t 
    ON {% for col in group_columns %}s.{{ col }} = t.{{ col }}{% if not loop.last %} AND {% endif %}{% endfor %}
{% endmacro %}
