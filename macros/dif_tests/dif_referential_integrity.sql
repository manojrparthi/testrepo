
{# Macro: Generate REFERENTIAL_INTEGRITY test using dbt-utils relationships #}
{# Implements DIF REFERENTIAL_INTEGRITY rule type for v1.5 #}

{% macro dif_generate_referential_integrity_test(rule_id, source_asset_path, target_asset_path, rule_body, threshold_value, severity) %}
  {#
    Generate a referential integrity test between source (child) and target (parent) tables.
    Identifies orphan records where foreign key values don't exist in parent table.
    
    Args:
      rule_id: DIF RULE_ID for traceability
      source_asset_path: Child table (database.schema.table)
      target_asset_path: Parent table (database.schema.table)
      rule_body: Parsed JSON dict with source_column, target_column, allow_nulls
      threshold_value: Maximum orphan count allowed (0 = no orphans permitted)
      severity: DIF severity (mapped to dbt error/warn)
      
    Returns:
      SQL query string that returns summary when orphan count exceeds threshold
      
    RULE_BODY Schema:
      {
        "source_column": "FK_COLUMN",      // Required: FK column in child table
        "target_column": "PK_COLUMN",      // Required: PK column in parent table
        "allow_nulls": true                // Optional: exclude null FKs from check (default: true)
      }
  #}
  
  {% set source_column = rule_body.get('source_column', '') %}
  {% set target_column = rule_body.get('target_column', '') %}
  {% set allow_nulls = rule_body.get('allow_nulls', true) %}
  {% set max_orphans = (threshold_value | int) if threshold_value else 0 %}
  
  {% if source_column and target_column %}
    WITH orphan_records AS (
      -- Find all records in source that don't have matching parent
      SELECT 
        s.{{ source_column }} AS orphan_key
      FROM {{ source_asset_path }} s
      LEFT JOIN {{ target_asset_path }} t 
        ON s.{{ source_column }} = t.{{ target_column }}
      WHERE t.{{ target_column }} IS NULL
        {% if allow_nulls %}
        -- Exclude null foreign keys from orphan check
        AND s.{{ source_column }} IS NOT NULL
        {% endif %}
    ),
    
    orphan_summary AS (
      SELECT
        COUNT(DISTINCT orphan_key) AS distinct_orphan_keys,
        COUNT(*) AS total_orphan_rows
      FROM orphan_records
    )
    
    {# -- Unified output schema for UNION ALL compatibility -- #}
    SELECT
      '{{ rule_id }}' AS dif_rule_id,
      'REFERENTIAL_INTEGRITY' AS dif_rule_type,
      '{{ source_column }}' AS column_name,
      '{{ source_asset_path }}' AS source_table,
      '{{ target_asset_path }}' AS target_table,
      CAST(NULL AS NUMBER(38,0)) AS total_rows,
      CAST(NULL AS NUMBER(38,0)) AS null_count,
      CAST(NULL AS NUMBER(38,0)) AS non_null_count,
      CAST(NULL AS DECIMAL(10,4)) AS null_percentage,
      os.distinct_orphan_keys,
      os.total_orphan_rows,
      CAST(NULL AS DECIMAL(38,6)) AS source_sum,
      CAST(NULL AS DECIMAL(38,6)) AS target_sum,
      CAST(NULL AS NUMBER(38,0)) AS source_row_count,
      CAST(NULL AS NUMBER(38,0)) AS target_row_count,
      CAST(NULL AS DECIMAL(38,6)) AS absolute_difference,
      CAST(NULL AS DECIMAL(10,6)) AS percent_variance,
      CAST({{ max_orphans }} AS VARCHAR(100)) AS threshold_value,
      'ABSOLUTE' AS threshold_type,
      CAST(NULL AS VARCHAR(50)) AS tolerance_type,
      CAST(NULL AS DECIMAL(20,6)) AS tolerance_value,
      CASE 
        WHEN os.distinct_orphan_keys > {{ max_orphans }} THEN 'FAIL'
        ELSE 'PASS'
      END AS test_status,
      '{{ severity }}' AS severity,
      CAST(NULL AS VARCHAR(2000)) AS source_filter_applied,
      CAST(NULL AS VARCHAR(2000)) AS target_filter_applied
    FROM orphan_summary os
  {% else %}
    {{ exceptions.raise_compiler_error("REFERENTIAL_INTEGRITY rule " ~ rule_id ~ " missing required 'source_column' or 'target_column' in RULE_BODY") }}
  {% endif %}
{% endmacro %}


{% test dif_referential_integrity(model, rule_id, target_relation, source_column, target_column, threshold=0, allow_nulls=true) %}
  {#
    Generic test version for schema.yml usage.
    Compatible with dbt_utils.relationships but with DIF-specific metadata.
    
    Usage in schema.yml:
      columns:
        - name: customer_fk
          tests:
            - dif_referential_integrity:
                rule_id: RULE_RI_MANUAL_001
                target_relation: ref('dim_customer')
                source_column: customer_fk
                target_column: customer_id
                threshold: 0
                allow_nulls: true
  #}
  
  SELECT 
    '{{ rule_id }}' AS dif_rule_id,
    {{ source_column }} AS orphan_key
  FROM {{ model }}
  WHERE {{ source_column }} NOT IN (
    SELECT {{ target_column }} 
    FROM {{ target_relation }} 
    WHERE {{ target_column }} IS NOT NULL
  )
  {% if allow_nulls %}
  AND {{ source_column }} IS NOT NULL
  {% endif %}
  
{% endtest %}


{% macro dif_get_orphan_sample(source_asset_path, target_asset_path, source_column, target_column, allow_nulls, sample_size=10) %}
  {#
    Get sample of orphan records for debugging/alerting.
    Useful for understanding which specific records are failing.
    
    Args:
      source_asset_path: Child table
      target_asset_path: Parent table
      source_column: FK column in child
      target_column: PK column in parent
      allow_nulls: Whether to exclude null FKs
      sample_size: Number of orphan records to return
      
    Returns:
      SQL query for sample orphan records
  #}
  SELECT 
    s.{{ source_column }} AS orphan_key,
    s.*
  FROM {{ source_asset_path }} s
  LEFT JOIN {{ target_asset_path }} t 
    ON s.{{ source_column }} = t.{{ target_column }}
  WHERE t.{{ target_column }} IS NULL
    {% if allow_nulls %}
    AND s.{{ source_column }} IS NOT NULL
    {% endif %}
  LIMIT {{ sample_size }}
{% endmacro %}
