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


create view inspections_by_borough as
select
	boro,
	count(*) as total_inspection_by_boro
from inspections
group by boro;

alter table inspection_staging rename to inspections;

select * from inspections_by_borough;

create view grade_distribution as
select
	coalesce(grade, 'Not Graded') as grade,
	count(*) as count,
	round(100.0 * count(*) / sum(count(*)) over (), 2) as pct
from inspections
group by coalesce(grade, 'Not Graded')
order by count desc;

select * from grade_distribution;

create view common_inspections as
select
	inspection_type,
	count(*) as inspection_count,
	round(100 * count(*) / sum(count(*)) over(),2) as pct
from inspections
group by inspection_type
order by inspection_count desc
limit 10;

select * from common_inspections;

CREATE OR REPLACE VIEW top_violations AS
SELECT
    violation_description,
    COUNT(*) AS total
FROM inspections
GROUP BY violation_description
ORDER BY total DESC
LIMIT 10;

select * from top_violations;

CREATE OR REPLACE VIEW critical_vs_noncritical AS
SELECT
    critical_flag,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM inspections
GROUP BY critical_flag;

select * from critical_vs_noncritical;

CREATE OR REPLACE VIEW violation_classification AS
SELECT
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
GROUP BY 1
ORDER BY total DESC;

select * from violation_classification;


CREATE OR REPLACE VIEW borough_critical_rate AS
SELECT
    boro,
    COUNT(*) FILTER (WHERE critical_flag = 'Critical') AS critical_violations,
    COUNT(*) AS total_inspections,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE critical_flag = 'Critical') / COUNT(*),
        2
    ) AS critical_rate_pct
FROM inspections
GROUP BY boro
ORDER BY critical_rate_pct DESC;

select * from borough_critical_rate;


CREATE OR REPLACE VIEW cuisine_grouped AS
SELECT
    cuisine_description,
    CASE
        -- AMERICAN
        WHEN cuisine_description ILIKE '%american%'
          OR cuisine_description ILIKE '%new american%'
          OR cuisine_description ILIKE '%californian%'
          OR cuisine_description ILIKE '%continental%'
          OR cuisine_description ILIKE '%english%'
          OR cuisine_description ILIKE '%australian%'
        THEN 'American'
        -- EUROPEAN
        WHEN cuisine_description ILIKE '%italian%'
          OR cuisine_description ILIKE '%french%'
          OR cuisine_description ILIKE '%new french%'
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
        -- ASIAN
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
          OR cuisine_description ILIKE '%iranian%'
          OR cuisine_description ILIKE '%turkish%'
          OR cuisine_description ILIKE '%middle eastern%'
          OR cuisine_description ILIKE '%lebanese%'
          OR cuisine_description ILIKE '%moroccan%'
        THEN 'Asian'
        -- LATIN & CARIBBEAN
        WHEN cuisine_description ILIKE '%mexican%'
          OR cuisine_description ILIKE '%tex-mex%'
          OR cuisine_description ILIKE '%latin american%'
          OR cuisine_description ILIKE '%caribbean%'
          OR cuisine_description ILIKE '%brazilian%'
          OR cuisine_description ILIKE '%peruvian%'
          OR cuisine_description ILIKE '%chilean%'
          OR cuisine_description ILIKE '%creole%'
          OR cuisine_description ILIKE '%cajun%'
        THEN 'Latin & Caribbean'
        -- FAST / CASUAL
        WHEN cuisine_description ILIKE '%pizza%'
          OR cuisine_description ILIKE '%hamburger%'
          OR cuisine_description ILIKE '%hotdog%'
          OR cuisine_description ILIKE '%sandwich%'
          OR cuisine_description ILIKE '%bagel%'
          OR cuisine_description ILIKE '%donut%'
          OR cuisine_description ILIKE '%chicken%'
          OR cuisine_description ILIKE '%seafood%'
          OR cuisine_description ILIKE '%soups%'
          OR cuisine_description ILIKE '%salads%'
        THEN 'Fast / Casual'
        -- BEVERAGES & SNACKS
        WHEN cuisine_description ILIKE '%coffee%'
          OR cuisine_description ILIKE '%tea%'
          OR cuisine_description ILIKE '%juice%'
          OR cuisine_description ILIKE '%smoothie%'
          OR cuisine_description ILIKE '%bottled%'
          OR cuisine_description ILIKE '%nuts%'
          OR cuisine_description ILIKE '%confection%'
          OR cuisine_description ILIKE '%dessert%'
          OR cuisine_description ILIKE '%frozen%'
          OR cuisine_description ILIKE '%fruit%'
        THEN 'Beverages & Snacks'
        -- HEALTH / DIETARY
        WHEN cuisine_description ILIKE '%vegan%'
          OR cuisine_description ILIKE '%vegetarian%'
        THEN 'Health / Dietary'
        -- OTHER
        WHEN cuisine_description ILIKE '%other%'
          OR cuisine_description ILIKE '%not listed%'
          OR cuisine_description ILIKE '%unknown%'
        THEN 'Other / Misc'
        ELSE 'Other / Misc'
    END AS cuisine_group
FROM inspections;


select 
cuisine_group,
count(*) as total_count
from cuisine_grouped
group by cuisine_group;

