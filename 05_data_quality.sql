WITH checks AS (
  SELECT 'cleaned_row_count_matches_fact' AS check_name,
    (SELECT COUNT(*) FROM CURATED.VW_LAUNCH_CLEAN) AS expected_count,
    (SELECT COUNT(*) FROM CURATED.FACT_LAUNCH) AS observed_count
  UNION ALL
  SELECT 'null_fact_source_keys', 0,
    (SELECT COUNT(*) FROM CURATED.FACT_LAUNCH WHERE source_name IS NULL OR source_id IS NULL)
  UNION ALL
  SELECT 'null_fact_dimension_keys', 0,
    (SELECT COUNT(*) FROM CURATED.FACT_LAUNCH
     WHERE rocket_key IS NULL OR launch_site_key IS NULL OR agency_key IS NULL
        OR mission_type_key IS NULL OR orbit_key IS NULL OR date_key IS NULL)
  UNION ALL
  SELECT 'orphan_rocket_keys', 0,
    (SELECT COUNT(*) FROM CURATED.FACT_LAUNCH f LEFT JOIN CURATED.DIM_ROCKET d
      ON f.rocket_key = d.rocket_key WHERE d.rocket_key IS NULL)
  UNION ALL
  SELECT 'orphan_site_keys', 0,
    (SELECT COUNT(*) FROM CURATED.FACT_LAUNCH f LEFT JOIN CURATED.DIM_LAUNCH_SITE d
      ON f.launch_site_key = d.launch_site_key WHERE d.launch_site_key IS NULL)
  UNION ALL
  SELECT 'orphan_agency_keys', 0,
    (SELECT COUNT(*) FROM CURATED.FACT_LAUNCH f LEFT JOIN CURATED.DIM_AGENCY d
      ON f.agency_key = d.agency_key WHERE d.agency_key IS NULL)
  UNION ALL
  SELECT 'orphan_mission_type_keys', 0,
    (SELECT COUNT(*) FROM CURATED.FACT_LAUNCH f LEFT JOIN CURATED.DIM_MISSION_TYPE d
      ON f.mission_type_key = d.mission_type_key WHERE d.mission_type_key IS NULL)
  UNION ALL
  SELECT 'orphan_orbit_keys', 0,
    (SELECT COUNT(*) FROM CURATED.FACT_LAUNCH f LEFT JOIN CURATED.DIM_ORBIT d
      ON f.orbit_key = d.orbit_key WHERE d.orbit_key IS NULL)
  UNION ALL
  SELECT 'orphan_date_keys', 0,
    (SELECT COUNT(*) FROM CURATED.FACT_LAUNCH f LEFT JOIN CURATED.DIM_DATE d
      ON f.date_key = d.date_key WHERE d.date_key IS NULL)
  UNION ALL
  SELECT 'duplicate_fact_source_keys', 0,
    (SELECT COUNT(*) FROM (
       SELECT source_name, source_id FROM CURATED.FACT_LAUNCH
       GROUP BY source_name, source_id HAVING COUNT(*) > 1
     ))
  UNION ALL
  SELECT 'duplicate_current_rocket_names', 0,
    (SELECT COUNT(*) FROM (
       SELECT rocket_name FROM CURATED.DIM_ROCKET WHERE is_current
       GROUP BY rocket_name HAVING COUNT(*) > 1
     ))
  UNION ALL
  SELECT 'duplicate_current_agencies', 0,
    (SELECT COUNT(*) FROM (
       SELECT agency_name FROM CURATED.DIM_AGENCY WHERE is_current
       GROUP BY agency_name HAVING COUNT(*) > 1
     ))
)
SELECT check_name, expected_count, observed_count,
  CASE WHEN expected_count = observed_count THEN 'PASS' ELSE 'FAIL' END AS result
FROM checks
ORDER BY check_name;
