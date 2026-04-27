

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



CREATE OR REPLACE VIEW v_cuisine_avg_score AS
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
FROM inspections_clean
WHERE grade IN ('A','B','C')
GROUP BY cuisine_description
HAVING COUNT(*) >= 30;



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