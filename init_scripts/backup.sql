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




--CREATE TABLE dm.fct_count_day_earthquake AS SELECT time::date AS date, count(*)        FROM ods.fct_earthquake GROUP BY 1;
--CREATE TABLE dm.fct_avg_day_earthquake   AS SELECT time::date AS date, avg(mag::float) FROM ods.fct_earthquake GROUP BY 1;