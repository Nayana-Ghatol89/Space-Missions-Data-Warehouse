-- Yearly rocket launch success rates with year-over-year and running-average windows.
WITH yearly AS (
  SELECT rocket.rocket_key, rocket.rocket_name, date_dim.calendar_year,
    COUNT(*) FILTER (WHERE fact.success_flag IS NOT NULL) AS classified_launches,
    COUNT(*) FILTER (WHERE fact.success_flag) AS successful_launches,
    COUNT(*) FILTER (WHERE fact.success_flag)::DOUBLE
      / NULLIF(COUNT(*) FILTER (WHERE fact.success_flag IS NOT NULL), 0) AS success_rate
  FROM CURATED.FACT_LAUNCH fact
  JOIN CURATED.DIM_ROCKET rocket ON fact.rocket_key = rocket.rocket_key
  JOIN CURATED.DIM_DATE date_dim ON fact.date_key = date_dim.date_key
  WHERE date_dim.date_key <> 0
  GROUP BY rocket.rocket_key, rocket.rocket_name, date_dim.calendar_year
)
SELECT rocket_name, calendar_year, classified_launches, successful_launches,
  ROUND(success_rate * 100, 2) AS success_rate_pct,
  ROUND(LAG(success_rate) OVER (PARTITION BY rocket_key ORDER BY calendar_year) * 100, 2)
    AS prior_year_success_rate_pct,
  ROUND(AVG(success_rate) OVER (
    PARTITION BY rocket_key ORDER BY calendar_year
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) * 100, 2) AS running_avg_success_rate_pct
FROM yearly
ORDER BY rocket_name, calendar_year;

-- Running launch totals per agency by year.
WITH agency_year AS (
  SELECT agency.agency_key, agency.agency_name, date_dim.calendar_year, COUNT(*) AS launches
  FROM CURATED.FACT_LAUNCH fact
  JOIN CURATED.DIM_AGENCY agency ON fact.agency_key = agency.agency_key
  JOIN CURATED.DIM_DATE date_dim ON fact.date_key = date_dim.date_key
  WHERE date_dim.date_key <> 0
  GROUP BY agency.agency_key, agency.agency_name, date_dim.calendar_year
)
SELECT agency_name, calendar_year, launches,
  SUM(launches) OVER (
    PARTITION BY agency_key ORDER BY calendar_year
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_launch_total
FROM agency_year
ORDER BY agency_name, calendar_year;

-- Recursive rocket lineage traversal, guarded against cycles.
WITH RECURSIVE rocket_tree (root_key, rocket_key, rocket_name, depth, key_path) AS (
  SELECT rocket_key, rocket_key, rocket_name, 0,
    '|' || CAST(rocket_key AS VARCHAR) || '|'
  FROM CURATED.DIM_ROCKET
  WHERE is_current AND rocket_key <> 0 AND parent_rocket_key IS NULL
  UNION ALL
  SELECT tree.root_key, child.rocket_key, child.rocket_name, tree.depth + 1,
    tree.key_path || CAST(child.rocket_key AS VARCHAR) || '|'
  FROM rocket_tree tree
  JOIN CURATED.DIM_ROCKET child
    ON child.parent_rocket_key = tree.rocket_key AND child.is_current
  WHERE POSITION('|' || CAST(child.rocket_key AS VARCHAR) || '|' IN tree.key_path) = 0
)
SELECT root.rocket_name AS root_rocket, tree.rocket_name AS rocket_variant, tree.depth
FROM rocket_tree tree
JOIN CURATED.DIM_ROCKET root ON root.rocket_key = tree.root_key
ORDER BY root_rocket, depth, rocket_variant;

-- Agency/year/rocket subtotals and grand total.
SELECT
  CASE WHEN GROUPING(agency.agency_name) = 1 THEN 'ALL AGENCIES' ELSE agency.agency_name END AS agency,
  CASE WHEN GROUPING(date_dim.calendar_year) = 1 THEN NULL ELSE date_dim.calendar_year END AS launch_year,
  CASE WHEN GROUPING(rocket.rocket_name) = 1 THEN 'ALL ROCKETS' ELSE rocket.rocket_name END AS rocket,
  COUNT(*) AS launches,
  COUNT(*) FILTER (WHERE fact.success_flag) AS successful_launches
FROM CURATED.FACT_LAUNCH fact
JOIN CURATED.DIM_AGENCY agency ON fact.agency_key = agency.agency_key
JOIN CURATED.DIM_ROCKET rocket ON fact.rocket_key = rocket.rocket_key
JOIN CURATED.DIM_DATE date_dim ON fact.date_key = date_dim.date_key
WHERE date_dim.date_key <> 0
GROUP BY ROLLUP(agency.agency_name, date_dim.calendar_year, rocket.rocket_name)
ORDER BY agency, launch_year, rocket;

-- Agency/year cross-tab with all subtotal combinations.
SELECT
  CASE WHEN GROUPING(agency.agency_name) = 1 THEN 'ALL AGENCIES' ELSE agency.agency_name END AS agency,
  CASE WHEN GROUPING(date_dim.calendar_year) = 1 THEN NULL ELSE date_dim.calendar_year END AS launch_year,
  COUNT(*) AS launches,
  COUNT(*) FILTER (WHERE fact.success_flag) AS successful_launches
FROM CURATED.FACT_LAUNCH fact
JOIN CURATED.DIM_AGENCY agency ON fact.agency_key = agency.agency_key
JOIN CURATED.DIM_DATE date_dim ON fact.date_key = date_dim.date_key
WHERE date_dim.date_key <> 0
GROUP BY CUBE(agency.agency_name, date_dim.calendar_year)
ORDER BY agency, launch_year;
