CREATE TABLE inspection_staging (
    CAMIS TEXT,
    DBA TEXT,
    BORO TEXT,
    BUILDING TEXT,
    STREET TEXT,
    ZIPCODE TEXT,
    PHONE TEXT,
    CUISINE_DESCRIPTION TEXT,
    INSPECTION_DATE TEXT,
    ACTION TEXT,
    VIOLATION_CODE TEXT,
    VIOLATION_DESCRIPTION TEXT,
    CRITICAL_FLAG TEXT,
    SCORE TEXT,
    GRADE TEXT,
    GRADE_DATE TEXT,
    RECORD_DATE TEXT,
    INSPECTION_TYPE TEXT,
    LATITUDE TEXT,
    LONGITUDE TEXT,
    COMMUNITY_BOARD TEXT,
    COUNCIL_DISTRICT TEXT,
    CENSUS_TRACT TEXT,
    BIN TEXT,
    BBL TEXT,
    NTA TEXT,
    LOCATION_POINT1 TEXT
);


COPY inspection_staging
FROM 'I:/Data-analytics projects/NYC Restaurant/Data/Clean/Cleaned_restaurant_inspection.csv'
DELIMITER ','
CSV HEADER
ENCODING 'LATIN1'
QUOTE '"';


ALTER TABLE inspection_staging RENAME TO inspections;



/* =========================
   BOROUGH INSPECTIONS
========================= */

CREATE VIEW inspections_by_borough AS
SELECT
    boro,
    COUNT(*) AS total_inspection_by_boro
FROM inspections
GROUP BY boro;



/* =========================
   GRADE DISTRIBUTION
========================= */

CREATE VIEW grade_distribution AS
SELECT
    COALESCE(grade, 'Not Graded') AS grade,
    COUNT(*) AS count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM inspections
GROUP BY COALESCE(grade, 'Not Graded');



/* =========================
   INSPECTION TYPES
========================= */

CREATE VIEW common_inspections AS
SELECT
    inspection_type,
    COUNT(*) AS inspection_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM inspections
GROUP BY inspection_type
ORDER BY inspection_count DESC
LIMIT 10;



/* =========================
   TOP VIOLATIONS
========================= */

CREATE VIEW top_violations AS
SELECT
    violation_description,
    COUNT(*) AS total
FROM inspections
GROUP BY violation_description
ORDER BY total DESC
LIMIT 10;



/* =========================
   CRITICAL SPLIT
========================= */

CREATE VIEW critical_vs_noncritical AS
SELECT
    critical_flag,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM inspections
GROUP BY critical_flag;



/* =========================
   VIOLATION CLASSIFICATION
========================= */

CREATE VIEW violation_classification AS
SELECT
    boro,
    CASE
        WHEN violation_description ILIKE '%surface%'
          OR violation_description ILIKE '%equipment%'
          OR violation_description ILIKE '%clean%'
        THEN 'Equipment & Cleanliness'

        WHEN violation_description ILIKE '%mice%'
          OR violation_description ILIKE '%rat%'
          OR violation_description ILIKE '%roach%'
          OR violation_description ILIKE '%fly%'
          OR violation_description ILIKE '%harborage%'
        THEN 'Pest & Sanitation Conditions'

        WHEN violation_description ILIKE '%temperature%'
          OR violation_description ILIKE '%41%'
          OR violation_description ILIKE '%140%'
        THEN 'Food Temperature Control'

        WHEN violation_description ILIKE '%plumbing%'
          OR violation_description ILIKE '%drain%'
          OR violation_description ILIKE '%sewage%'
          OR violation_description ILIKE '%back-flow%'
        THEN 'Infrastructure'

        WHEN violation_description ILIKE '%certificate%'
          OR violation_description ILIKE '%FPC%'
          OR violation_description ILIKE '%permit%'
        THEN 'Compliance'

        ELSE 'Other'
    END AS violation_group,
    COUNT(*) AS total
FROM inspections
GROUP BY boro, violation_description;



/* =========================
   BOROUGH CRITICAL RATE
========================= */

CREATE VIEW borough_critical_rate AS
SELECT
    boro,
    COUNT(*) FILTER (WHERE critical_flag = 'Critical') AS critical_violations,
    COUNT(*) AS total_inspections,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE critical_flag = 'Critical') / COUNT(*),
        2
    ) AS critical_rate_pct
FROM inspections
GROUP BY boro;



/* =========================
   CUISINE GROUPING
========================= */

CREATE VIEW cuisine_grouped AS
SELECT
    cuisine_description,
    grade,
    critical_flag,
    CASE
        WHEN cuisine_description ILIKE '%american%'
          OR cuisine_description ILIKE '%new american%'
          OR cuisine_description ILIKE '%californian%'
          OR cuisine_description ILIKE '%continental%'
          OR cuisine_description ILIKE '%english%'
          OR cuisine_description ILIKE '%australian%'
        THEN 'American'

        WHEN cuisine_description ILIKE '%italian%'
          OR cuisine_description ILIKE '%french%'
          OR cuisine_description ILIKE '%german%'
          OR cuisine_description ILIKE '%greek%'
          OR cuisine_description ILIKE '%spanish%'
          OR cuisine_description ILIKE '%portuguese%'
          OR cuisine_description ILIKE '%scandinavian%'
          OR cuisine_description ILIKE '%polish%'
          OR cuisine_description ILIKE '%russian%'
          OR cuisine_description ILIKE '%czech%'
          OR cuisine_description ILIKE '%eastern european%'
          OR cuisine_description ILIKE '%basque%'
        THEN 'European'

        WHEN cuisine_description ILIKE '%chinese%'
          OR cuisine_description ILIKE '%japanese%'
          OR cuisine_description ILIKE '%korean%'
          OR cuisine_description ILIKE '%thai%'
          OR cuisine_description ILIKE '%indian%'
          OR cuisine_description ILIKE '%bangladeshi%'
          OR cuisine_description ILIKE '%pakistani%'
          OR cuisine_description ILIKE '%indonesian%'
          OR cuisine_description ILIKE '%filipino%'
          OR cuisine_description ILIKE '%southeast asian%'
          OR cuisine_description ILIKE '%asian%'
          OR cuisine_description ILIKE '%fusion%'
          OR cuisine_description ILIKE '%middle eastern%'
        THEN 'Asian'

        WHEN cuisine_description ILIKE '%mexican%'
          OR cuisine_description ILIKE '%tex-mex%'
          OR cuisine_description ILIKE '%latin american%'
          OR cuisine_description ILIKE '%caribbean%'
          OR cuisine_description ILIKE '%brazilian%'
          OR cuisine_description ILIKE '%peruvian%'
          OR cuisine_description ILIKE '%creole%'
          OR cuisine_description ILIKE '%cajun%'
        THEN 'Latin & Caribbean'

        WHEN cuisine_description ILIKE '%pizza%'
          OR cuisine_description ILIKE '%hamburger%'
          OR cuisine_description ILIKE '%hotdog%'
          OR cuisine_description ILIKE '%sandwich%'
          OR cuisine_description ILIKE '%bagel%'
          OR cuisine_description ILIKE '%donut%'
          OR cuisine_description ILIKE '%chicken%'
          OR cuisine_description ILIKE '%seafood%'
          OR cuisine_description ILIKE '%salads%'
        THEN 'Fast / Casual'

        WHEN cuisine_description ILIKE '%coffee%'
          OR cuisine_description ILIKE '%tea%'
          OR cuisine_description ILIKE '%juice%'
          OR cuisine_description ILIKE '%smoothie%'
          OR cuisine_description ILIKE '%dessert%'
          OR cuisine_description ILIKE '%frozen%'
          OR cuisine_description ILIKE '%fruit%'
        THEN 'Beverages & Snacks'

        WHEN cuisine_description ILIKE '%vegan%'
          OR cuisine_description ILIKE '%vegetarian%'
        THEN 'Health / Dietary'

        ELSE 'Other / Misc'
    END AS cuisine_group
FROM inspections;



/* =========================
   CUISINE ANALYSIS
========================= */

SELECT
    cuisine_group,
    COUNT(*) AS total_count
FROM cuisine_grouped
GROUP BY cuisine_group;



SELECT
    cuisine_group,
    grade,
    COUNT(*) AS count,
    ROUND(
        100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY cuisine_group),
        2
    ) AS pct_within_group
FROM cuisine_grouped
WHERE grade IN ('A','B','C')
GROUP BY cuisine_group, grade;



/* =========================
   AVG SCORE CUISINE
========================= */

SELECT
    cuisine_description,
    ROUND(
        AVG(
            CASE grade
                WHEN 'A' THEN 1
                WHEN 'B' THEN 2
                WHEN 'C' THEN 3
            END
        ), 2
    ) AS avg_score,
    COUNT(*) AS total_inspections
FROM inspections
WHERE grade IN ('A','B','C')
GROUP BY cuisine_description
HAVING COUNT(*) >= 30
ORDER BY avg_score DESC
LIMIT 5;



/* =========================
   CRITICAL RATE CUISINE
========================= */

CREATE VIEW critical_violation_propotion AS
SELECT
    cuisine_description,
    COUNT(*) AS total_violation_count,
    COUNT(*) FILTER (WHERE critical_flag = 'Critical') AS critical_violation_count,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE critical_flag = 'Critical') / COUNT(*),
        2
    ) AS critical_rate_pct
FROM inspections
GROUP BY cuisine_description
HAVING COUNT(*) >= 500;



/* =========================
   TIME TRENDS
========================= */

CREATE VIEW grade_trend_yearly AS
SELECT
    EXTRACT(YEAR FROM TO_DATE(inspection_date, 'MM/DD/YYYY')) AS year,
    grade,
    COUNT(*) AS count,
    ROUND(
        100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY EXTRACT(YEAR FROM TO_DATE(inspection_date, 'MM/DD/YYYY'))),
        2
    ) AS pct_within_year
FROM inspections
WHERE grade IN ('A','B','C')
GROUP BY year, grade;



CREATE VIEW critical_violation_trend AS
SELECT
    EXTRACT(YEAR FROM TO_DATE(inspection_date, 'MM/DD/YYYY')) AS year,
    COUNT(*) FILTER (WHERE critical_flag = 'Critical') AS critical_violations,
    COUNT(*) AS total_violations,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE critical_flag = 'Critical') / COUNT(*),
        2
    ) AS critical_rate_pct
FROM inspections
GROUP BY year;



CREATE VIEW borough_critical_trend AS
SELECT
    boro,
    EXTRACT(YEAR FROM TO_DATE(inspection_date, 'MM/DD/YYYY')) AS year,
    COUNT(*) FILTER (WHERE critical_flag = 'Critical') AS critical_violations,
    COUNT(*) AS total_violations,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE critical_flag = 'Critical') / COUNT(*),
        2
    ) AS critical_rate_pct
FROM inspections
GROUP BY boro, year;



/* =========================
   DRIVER ANALYSIS
========================= */

CREATE VIEW bronx_brooklyn_drivers AS
SELECT
    violation_group,
    COUNT(*) AS total_violations,
    COUNT(*) FILTER (WHERE boro IN ('BRONX','BROOKLYN')) AS bx_bk_violations,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE boro IN ('BRONX','BROOKLYN')) / COUNT(*),
        2
    ) AS bx_bk_share_pct
FROM violation_classification
GROUP BY violation_group;



CREATE OR REPLACE VIEW inspections_clean AS
SELECT
    CAMIS,
    DBA,
    UPPER(BORO) AS borough,
    CUISINE_DESCRIPTION,
    TO_DATE(INSPECTION_DATE, 'MM/DD/YYYY') AS inspection_date,
    INSPECTION_TYPE,
    NULLIF(GRADE, '') AS grade,
    NULLIF(CRITICAL_FLAG, '') AS critical_flag,
    VIOLATION_DESCRIPTION
FROM inspections;


CREATE OR REPLACE VIEW v_borough_inspections AS
SELECT
    borough,
    COUNT(*) AS total_inspections
FROM inspections_clean
GROUP BY borough;



CREATE OR REPLACE VIEW v_borough_critical_rate AS
SELECT
    borough,
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE critical_flag = 'Critical') AS critical,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE critical_flag = 'Critical') / COUNT(*),
        2
    ) AS critical_rate_pct
FROM inspections_clean
GROUP BY borough;

select * from v_borough_critical_rate;


CREATE OR REPLACE VIEW v_grade_distribution AS
SELECT
    COALESCE(grade, 'Not Graded') AS grade,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM inspections_clean
GROUP BY COALESCE(grade, 'Not Graded');


CREATE OR REPLACE VIEW v_grade_trend AS
SELECT
    EXTRACT(YEAR FROM inspection_date) AS year,
    grade,
    COUNT(*) AS total,
    ROUND(
        100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY EXTRACT(YEAR FROM inspection_date)),
        2
    ) AS pct
FROM inspections_clean
WHERE grade IN ('A','B','C')
GROUP BY year, grade;

select * from v_grade_trend;


CREATE OR REPLACE VIEW v_inspection_types AS
SELECT
    inspection_type,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM inspections_clean
GROUP BY inspection_type;



CREATE OR REPLACE VIEW v_top_violations AS
SELECT
    violation_description,
    COUNT(*) AS total
FROM inspections_clean
GROUP BY violation_description
ORDER BY total DESC
LIMIT 10;



CREATE OR REPLACE VIEW v_critical_split AS
SELECT
    critical_flag,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM inspections_clean
GROUP BY critical_flag;



CREATE OR REPLACE VIEW v_cuisine_grade AS
SELECT
    cuisine_description,
    grade,
    COUNT(*) AS total,
    ROUND(
        100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY cuisine_description),
        2
    ) AS pct_within_cuisine
FROM inspections_clean
WHERE grade IN ('A','B','C')
GROUP BY cuisine_description, grade;



CREATE OR REPLACE VIEW v_cuisine_group_avg_score AS
SELECT
    cuisine_group,
    ROUND(
        AVG(
            CASE grade
                WHEN 'A' THEN 1
                WHEN 'B' THEN 2
                WHEN 'C' THEN 3
            END
        ), 2
    ) AS avg_score,
    COUNT(*) AS total_inspections
FROM cuisine_grouped
WHERE grade IN ('A','B','C')
GROUP BY cuisine_group;

select * from v_cuisine_group_avg_score;





CREATE OR REPLACE VIEW v_cuisine_critical_rate AS
SELECT
    cuisine_description,
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE critical_flag = 'Critical') AS critical,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE critical_flag = 'Critical') / COUNT(*),
        2
    ) AS critical_rate_pct
FROM inspections_clean
GROUP BY cuisine_description
HAVING COUNT(*) >= 500;



CREATE OR REPLACE VIEW v_critical_trend AS
SELECT
    EXTRACT(YEAR FROM inspection_date) AS year,
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE critical_flag = 'Critical') AS critical,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE critical_flag = 'Critical') / COUNT(*),
        2
    ) AS critical_rate_pct
FROM inspections_clean
GROUP BY year;

select * from v_critical_trend;

CREATE OR REPLACE VIEW v_borough_critical_trend AS
SELECT
    borough,
    EXTRACT(YEAR FROM inspection_date) AS year,
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE critical_flag = 'Critical') AS critical,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE critical_flag = 'Critical') / COUNT(*),
        2
    ) AS critical_rate_pct
FROM inspections_clean
GROUP BY borough, year;

select * from v_borough_critical_trend;

CREATE OR REPLACE VIEW v_violation_drivers AS
SELECT
    violation_description,
    COUNT(*) AS total_violations,
    COUNT(*) FILTER (WHERE critical_flag = 'Critical') AS critical_violations,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE critical_flag = 'Critical') / COUNT(*),
        2
    ) AS critical_rate_pct
FROM inspections_clean
GROUP BY violation_description;

select * from v_borough_critical_trend;

CREATE OR REPLACE VIEW v_violation_category_risk AS
SELECT
    violation_group,
    COUNT(*) AS total_violations,
    COUNT(*) FILTER (WHERE critical_flag = 'Critical') AS critical_violations,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE critical_flag = 'Critical') / COUNT(*),
        2
    ) AS critical_rate_pct
FROM violation_classification
GROUP BY violation_group;

create or replace view boro_grade as
select
	boro, 
	grade,
	count(*) as grade_count
from inspections
where grade not ilike 'Not Graded'
group by boro, grade;

select * from boro_grade

create view grades_by_cuisine_types as
SELECT
    cuisine_group,
    grade,
    COUNT(*) AS total,
    ROUND(
        100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY cuisine_group),
        2
    ) AS pct_within_group
FROM cuisine_grouped
WHERE grade IN ('A','B','C')
GROUP BY cuisine_group, grade
ORDER BY cuisine_group, grade;

