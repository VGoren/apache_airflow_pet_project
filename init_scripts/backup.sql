CREATE SCHEMA stg;	-- (Staging Layer) 
CREATE SCHEMA ods;	-- (Operational Data Store)
CREATE SCHEMA dm;   -- (Data Mart)


CREATE TABLE ods.projects
(
	id 		     int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	project_name varchar,
	UNIQUE(project_name)
);

CREATE TABLE ods.pages
(
	id 		     int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	project_id   int,
	page_name    varchar,
	CONSTRAINT fk_project FOREIGN KEY (project_id) REFERENCES ods.projects (id) ON DELETE CASCADE ON UPDATE CASCADE,
	UNIQUE (project_id, page_name)
);

CREATE TABLE ods.fct_views
(
	timestamp        date,
	page_id			 int,
	desktop_views 	 int, 
	mobile_web_views int,
	mobile_app_views int,
	all_access_views int,
	CONSTRAINT fk_page    FOREIGN KEY (page_id)    REFERENCES ods.pages    (id) ON DELETE CASCADE ON UPDATE CASCADE
);

/*
 SELECT ods.upsert_page('ods', 'en.wikipedia.org', 'Coca-Cola')
*/
CREATE OR REPLACE FUNCTION ods.upsert_page
(
	SCHEMA 		 varchar,
	PROJECT_NAME varchar,
	PAGE_NAME    varchar
)
RETURNS int AS $sql$

DECLARE 
	result_id int;
	sql_query text;
BEGIN
	sql_query := $$
				 WITH 
				 inserted_project AS
				 (
				     INSERT INTO $$||SCHEMA||$$.projects (project_name)
				     SELECT '$$||PROJECT_NAME||$$' AS project_name          
				     ON CONFLICT (project_name)          DO UPDATE SET project_name = EXCLUDED.project_name 
				     RETURNING id
				 ),
				 inserted_page AS
				 (
				     INSERT INTO $$||SCHEMA||$$.pages    (project_id, page_name) 
				     SELECT id                  AS project_id,       
				            '$$||PAGE_NAME||$$' AS page_name
				     FROM inserted_project
				     ON CONFLICT (project_id, page_name) DO UPDATE SET project_id   = EXCLUDED.project_id,
				                                                       page_name    = EXCLUDED.page_name
				     RETURNING id
				 )
				 SELECT id FROM inserted_page
			     $$;

	--RAISE NOTICE '%', sql_query;
    
	EXECUTE sql_query INTO result_id;
    RETURN result_id;
END;
$sql$ 
LANGUAGE plpgsql;

/*
SELECT * FROM dm.get_pages_spearman_corr
(
    /*report_subperiod = */'week',
    /*report_period    = */'quarter',
    /*PROJECT_NAME_1   = */'en.wikipedia.org', /*PAGE_NAME_1 = */'Santa_Claus',
    /*PROJECT_NAME_2   = */'en.wikipedia.org', /*PAGE_NAME_2 = */'Coca-Cola',
    /*start_date       = */'01.01.2020',
    /*end_date         = */'01.01.2026'
)
*/
CREATE OR REPLACE FUNCTION dm.get_pages_spearman_corr
(
    report_subperiod varchar,
    report_period    varchar,
    PROJECT_NAME_1   varchar, PAGE_NAME_1 varchar,
    PROJECT_NAME_2   varchar, PAGE_NAME_2 varchar,
    start_date       varchar,
    end_date         varchar
)
RETURNS TABLE 
(
    period               timestamptz, 
    spearman_correlation float8
) 
AS $sql$
DECLARE 
    sql_query text;
BEGIN
    sql_query := $$
                 WITH 
                 paired_data AS (
                     SELECT DATE_TRUNC('$$||report_subperiod||$$', a.timestamp) AS report_subperiod,
                            DATE_TRUNC('$$||report_period||$$',    a.timestamp) AS report_period,
                            a.all_access_views                 					AS val_a,
                            b.all_access_views                 					AS val_b
                     FROM      ods.fct_views a
                     INNER JOIN ods.pages     apage ON a.page_id        = apage.id
                     INNER JOIN ods.projects  aproj ON apage.project_id = aproj.id
                     INNER JOIN ods.fct_views b     ON a.timestamp      = b.timestamp
                     INNER JOIN ods.pages     bpage ON b.page_id        = bpage.id
                     INNER JOIN ods.projects  bproj ON bpage.project_id = bproj.id
                     WHERE aproj.project_name = '$$||PROJECT_NAME_1||$$' AND apage.page_name = '$$||PAGE_NAME_1||$$'
                       AND bproj.project_name = '$$||PROJECT_NAME_2||$$' AND bpage.page_name = '$$||PAGE_NAME_2||$$'
                       AND a.timestamp BETWEEN '$$||start_date||$$' 
                                           AND '$$||end_date||$$'
                 ),
                 paired_grouped_data AS 
                 (
                     SELECT report_subperiod,
                            report_period,
                            SUM(val_a) AS val_a,
                            SUM(val_b) AS val_b
                     FROM paired_data
                     GROUP BY report_period, report_subperiod
                 ),
                 ranked_data AS (
                     SELECT report_period,
                            RANK() OVER (PARTITION BY report_period ORDER BY val_a) AS rank_a,
                            RANK() OVER (PARTITION BY report_period ORDER BY val_b) AS rank_b
                     FROM paired_grouped_data
                 )
                 SELECT report_period,
                        CORR(rank_a, rank_b) AS spearman_correlation
                 FROM ranked_data
                 GROUP BY report_period
                 ORDER BY report_period
                 $$;

    --RAISE NOTICE '%', sql_query;

    RETURN QUERY EXECUTE sql_query;
END;
$sql$ 
LANGUAGE plpgsql;



--CREATE TABLE dm.fct_count_day_earthquake AS SELECT time::date AS date, count(*)        FROM ods.fct_earthquake GROUP BY 1;
--CREATE TABLE dm.fct_avg_day_earthquake   AS SELECT time::date AS date, avg(mag::float) FROM ods.fct_earthquake GROUP BY 1;