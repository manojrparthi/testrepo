
{# Macro: Load DIF rules from CONFIG_CONTROLS.RULES for a specific rule group #}
{# This is the core config-driven loading mechanism for DIF v1.5 #}

{% macro dif_load_rules_for_group(rule_group) %}
  {#
    Load all active rules for a given rule group from CONFIG_CONTROLS.RULES.
    Joins DATA_ASSETS and CONNECTIONS to resolve full asset paths.
    
    Args:
      rule_group: The RULE_GROUP value to filter rules (e.g., 'STAGING_VALIDATION')
      
    Returns:
      Agate table with rule configurations including resolved asset paths
      
    Usage:
      {% set rules = dif_load_rules_for_group(var('rule_group')) %}
      {% for rule in rules %}
        {{ rule['RULE_ID'] }}
      {% endfor %}
  #}
  
  {% set rules_query %}
    SELECT 
      r.RULE_ID,
      r.RULE_GROUP,
      r.RULE_TYPE,
      r.SOURCE_ASSET_ID,
      r.TARGET_ASSET_ID,
      r.THRESHOLD_VALUE,
      r.THRESHOLD_TYPE,
      r.SEVERITY,
      r.RULE_BODY,
      r.RULE_DESCRIPTION,
      r.RECON_TOLERANCE_TYPE,
      r.RECON_TOLERANCE_VALUE,
      -- Source asset resolution
      sa.ASSET_PATH AS source_asset_path,
      sa.ASSET_TYPE AS source_asset_type,
      sc.DATABASE_NAME AS source_database,
      sc.CONNECTION_NAME AS source_connection,
      -- Target asset resolution (nullable for single-asset rules)
      ta.ASSET_PATH AS target_asset_path,
      ta.ASSET_TYPE AS target_asset_type,
      tc.DATABASE_NAME AS target_database,
      tc.CONNECTION_NAME AS target_connection
    FROM {{ var('dif_database') }}.CONFIG_CONTROLS.RULES r
    -- Join source asset
    JOIN {{ var('dif_database') }}.CONFIG_CONTROLS.DATA_ASSETS sa 
      ON r.SOURCE_ASSET_ID = sa.ASSET_ID
    LEFT JOIN {{ var('dif_database') }}.CONFIG_CONTROLS.CONNECTIONS sc 
      ON sa.CONNECTION_NAME = sc.CONNECTION_NAME
    -- Join target asset (optional)
    LEFT JOIN {{ var('dif_database') }}.CONFIG_CONTROLS.DATA_ASSETS ta 
      ON r.TARGET_ASSET_ID = ta.ASSET_ID
    LEFT JOIN {{ var('dif_database') }}.CONFIG_CONTROLS.CONNECTIONS tc 
      ON ta.CONNECTION_NAME = tc.CONNECTION_NAME
    WHERE r.RULE_GROUP = '{{ rule_group }}'
      AND r.IS_ACTIVE = TRUE
      -- v1.5 scope: NULL_CHECK, REFERENTIAL_INTEGRITY, SUM_MATCH only
      AND r.RULE_TYPE IN ('NULL_CHECK', 'REFERENTIAL_INTEGRITY', 'SUM_MATCH')
    ORDER BY 
      CASE r.SEVERITY 
        WHEN 'CRITICAL' THEN 1 
        WHEN 'HIGH' THEN 2 
        WHEN 'MEDIUM' THEN 3 
        WHEN 'LOW' THEN 4 
        ELSE 5 
      END,
      r.RULE_ID
  {% endset %}
  
  {% set results = run_query(rules_query) %}
  
  {% if execute %}
    {{ log("DIF: Loaded " ~ results | length ~ " rules for group '" ~ rule_group ~ "'", info=True) }}
    {{ return(results) }}
  {% else %}
    {{ return([]) }}
  {% endif %}
{% endmacro %}


{% macro dif_parse_rule_body(rule_body_str) %}
  {#
    Safely parse RULE_BODY JSON string into a dictionary.
    Returns empty dict if null or invalid JSON.
    
    Args:
      rule_body_str: JSON string from RULES.RULE_BODY column
      
    Returns:
      Dictionary with parsed configuration
      
    Security: Input is from control table, trusted source
  #}
  {% if rule_body_str and rule_body_str != '' and rule_body_str != 'None' %}
    {% set parsed = fromjson(rule_body_str) %}
    {{ return(parsed) }}
  {% else %}
    {{ return({}) }}
  {% endif %}
{% endmacro %}


{% macro dif_get_full_table_ref(database, asset_path) %}
  {#
    Build fully qualified table reference from database and asset path.
    
    Args:
      database: Database name (may be null)
      asset_path: Asset path (schema.table or database.schema.table)
      
    Returns:
      Fully qualified table reference
  #}
  {% if database and '.' not in asset_path.split('.')[0] %}
    {{ return(database ~ '.' ~ asset_path) }}
  {% else %}
    {{ return(asset_path) }}
  {% endif %}
{% endmacro %}


{% macro dif_map_severity_to_dbt(dif_severity) %}
  {#
    Map DIF severity levels to dbt test severity.
    
    Args:
      dif_severity: DIF severity (CRITICAL, HIGH, MEDIUM, LOW, INFO)
      
    Returns:
      dbt severity (error or warn)
  #}
  {% set severity_map = var('severity_mapping', {
    'CRITICAL': 'error',
    'HIGH': 'error',
    'MEDIUM': 'warn',
    'LOW': 'warn',
    'INFO': 'warn'
  }) %}
  
  {{ return(severity_map.get(dif_severity | upper, 'warn')) }}
{% endmacro %}
