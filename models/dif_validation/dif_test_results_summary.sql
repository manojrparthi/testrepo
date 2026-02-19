
-- Model: Aggregated view of test results by rule group and run
-- Provides summary statistics for monitoring and dashboards

{{ config(
    materialized='view',
    schema='DIF_OBSERVABILITY',
    tags=['dif_v15', 'observability']
) }}

WITH test_runs AS (
  SELECT
    run_id,
    rule_group,
    MIN(executed_at) AS run_start,
    MAX(executed_at) AS run_end,
    COUNT(*) AS total_tests,
    SUM(CASE WHEN test_status = 'PASS' THEN 1 ELSE 0 END) AS passed_tests,
    SUM(CASE WHEN test_status = 'FAIL' THEN 1 ELSE 0 END) AS failed_tests,
    -- Breakdown by rule type
    SUM(CASE WHEN dif_rule_type = 'NULL_CHECK' THEN 1 ELSE 0 END) AS null_check_count,
    SUM(CASE WHEN dif_rule_type = 'REFERENTIAL_INTEGRITY' THEN 1 ELSE 0 END) AS ri_check_count,
    SUM(CASE WHEN dif_rule_type = 'SUM_MATCH' THEN 1 ELSE 0 END) AS sum_match_count,
    -- Pass rates by rule type
    SUM(CASE WHEN dif_rule_type = 'NULL_CHECK' AND test_status = 'PASS' THEN 1 ELSE 0 END) AS null_check_passed,
    SUM(CASE WHEN dif_rule_type = 'REFERENTIAL_INTEGRITY' AND test_status = 'PASS' THEN 1 ELSE 0 END) AS ri_check_passed,
    SUM(CASE WHEN dif_rule_type = 'SUM_MATCH' AND test_status = 'PASS' THEN 1 ELSE 0 END) AS sum_match_passed
  FROM {{ ref('dif_config_driven_tests') }}
  GROUP BY run_id, rule_group
)

SELECT
  run_id,
  rule_group,
  run_start,
  run_end,
  DATEDIFF('second', run_start, run_end) AS run_duration_seconds,
  total_tests,
  passed_tests,
  failed_tests,
  ROUND(passed_tests * 100.0 / NULLIF(total_tests, 0), 2) AS overall_pass_rate,
  -- Rule type breakdown
  null_check_count,
  ri_check_count,
  sum_match_count,
  -- Pass rates by type
  ROUND(null_check_passed * 100.0 / NULLIF(null_check_count, 0), 2) AS null_check_pass_rate,
  ROUND(ri_check_passed * 100.0 / NULLIF(ri_check_count, 0), 2) AS ri_check_pass_rate,
  ROUND(sum_match_passed * 100.0 / NULLIF(sum_match_count, 0), 2) AS sum_match_pass_rate,
  -- Overall status
  CASE 
    WHEN failed_tests = 0 THEN 'PASS'
    ELSE 'FAIL'
  END AS overall_status
FROM test_runs
ORDER BY run_start DESC
