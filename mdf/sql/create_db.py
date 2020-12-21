create = """
CREATE SCHEMA workflow
    AUTHORIZATION postgres;

COMMENT ON SCHEMA workflow
    IS 'OLTP layer for mdf to store meta data about projects';
    
CREATE SCHEMA data
    AUTHORIZATION postgres;

COMMENT ON SCHEMA data
    IS 'OLAP layer for mdf to store analitical data';

/*
##################################################################
CREATE project structure FOR WORKFLOW SCHEMA
##################################################################
*/


CREATE TABLE workflow.projects
(
    project_id bigserial NOT NULL,
    project_name text NOT NULL,
    project_description text,
    project_table_raw_config json NOT NULL,
    created timestamp with time zone NOT NULL DEFAULT NOW(),
    last_updated timestamp with time zone NOT NULL DEFAULT NOW(),
    PRIMARY KEY (project_id)
);

ALTER TABLE workflow.projects
    OWNER to postgres;
   
CREATE OR REPLACE FUNCTION workflow.trigger_last_updated()
RETURNS TRIGGER AS $$
BEGIN
  NEW.last_updated = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER last_updated
BEFORE UPDATE ON workflow.projects
FOR EACH ROW
EXECUTE PROCEDURE workflow.trigger_last_updated();

/*
Trigger below will create table based on the config provided in JSON after inserting new project.
*/

CREATE OR REPLACE FUNCTION workflow.create_project_data_table()
RETURNS trigger as $$
DECLARE 
    n record;
BEGIN 

EXECUTE 'CREATE TABLE data.'|| NEW.project_name || '( dataset_id bigint )';

EXECUTE 'CREATE INDEX '|| NEW.project_name || '_index ON data.'|| NEW.project_name || ' USING btree (dataset_id ASC NULLS LAST) TABLESPACE pg_default';

FOR n IN SELECT 
            REPLACE(json_array_elements(project_table_raw_config -> 'column_names')::text, '"', '') AS cn,
            REPLACE(json_array_elements(project_table_raw_config -> 'column_types')::text, '"', '') AS ct
         FROM workflow.projects WHERE project_id = NEW.project_id
LOOP
    EXECUTE 'ALTER TABLE data.' || NEW.project_name || ' ADD COLUMN ' || n.cn || ' ' || n.ct ;
END LOOP ;
RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER create_project_data_table_trigger
AFTER INSERT
ON workflow.projects
FOR EACH ROW
EXECUTE PROCEDURE workflow.create_project_data_table();


/*
##################################################################
CREATE datafeed  structure FOR WORKFLOW SCHEMA
##################################################################
*/



CREATE TABLE workflow.datafeeds
(
    datafeed_id bigserial NOT NULL,
    project_id bigint NOT NULL,
    datafeed_name text COLLATE pg_catalog."default" NOT NULL,
    datafeed_execution_script text COLLATE pg_catalog."default",
    datafeed_quality_rules json,
	datafeed_enhancement_rules json,
    datafeed_is_running boolean DEFAULT false,
    datafeed_last_executed date,
    datafeed_execution_count bigint DEFAULT 0,
	created date NOT NULL DEFAULT now(),
	last_updated date NOT NULL DEFAULT now(),
    CONSTRAINT datafeed_id PRIMARY KEY (datafeed_id),
    CONSTRAINT project_id FOREIGN KEY (project_id)
        REFERENCES workflow.projects (project_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

TABLESPACE pg_default;

CREATE TRIGGER last_updated
BEFORE UPDATE ON workflow.datafeeds
FOR EACH ROW
EXECUTE PROCEDURE workflow.trigger_last_updated();

/*
##################################################################
CREATE dataset structure FOR WORKFLOW SCHEMA
##################################################################
*/


CREATE TABLE workflow.datasets
(
    dataset_id bigserial NOT NULL,
    datafeed_id integer,
    dataset_quality_status character varying COLLATE pg_catalog."default" NOT NULL,
    dataset_enhancement_status character varying COLLATE pg_catalog."default" NOT NULL,
    dataset_quality_pushed boolean,
    created timestamp without time zone NOT NULL,
    last_updated timestamp without time zone NOT NULL,
    CONSTRAINT datasets_pkey PRIMARY KEY (dataset_id),
    CONSTRAINT datasets_datafeed_id_fkey FOREIGN KEY (datafeed_id)
        REFERENCES workflow.datafeeds (datafeed_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

TABLESPACE pg_default;


CREATE TRIGGER last_updated
BEFORE UPDATE ON workflow.datasets
FOR EACH ROW
EXECUTE PROCEDURE workflow.trigger_last_updated();


CREATE TABLE workflow.dq_checks_results
(
    id bigserial NOT NULL,
    dataset_id integer,
    checks_date timestamp without time zone NOT NULL,
    issues json,
    CONSTRAINT dq_checks_results_pkey PRIMARY KEY (id),
    CONSTRAINT dq_checks_results_dataset_id_fkey FOREIGN KEY (dataset_id)
        REFERENCES workflow.datasets (dataset_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

TABLESPACE pg_default;


CREATE OR REPLACE PROCEDURE workflow.forcepush_quality_status_proc(ds_id bigint)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
cur_dq_status CHAR;
BEGIN

SELECT dataset_quality_status INTO cur_dq_status
FROM workflow.datasets
WHERE dataset_id = ds_id;

ASSERT cur_dq_status = 'F', 'Quality Status Is Not F! Impossible to forcepush!';

UPDATE workflow.datasets
SET dataset_quality_pushed = 'TRUE',
    dataset_quality_status = 'S'
WHERE dataset_id = ds_id;
END;
$BODY$;


CREATE OR REPLACE PROCEDURE workflow.rollback_quality_status_proc(ds_id bigint)
LANGUAGE 'plpgsql'
AS $BODY$
BEGIN

UPDATE workflow.datasets
SET dataset_quality_pushed = 'FALSE',
    dataset_quality_status = 'P'
WHERE dataset_id = ds_id;

DELETE FROM workflow.dq_checks_results
WHERE dataset_id = ds_id;

END;
$BODY$;


CREATE OR REPLACE VIEW workflow.last_dataset_on_success_v AS
SELECT 
    a.dataset_id AS dataset_id_now,
    CASE 
        WHEN max(b.dataset_id) IS NOT NULL THEN max(b.dataset_id)
        ELSE a.dataset_id
    END AS dataset_id_last_success
FROM workflow.datasets a
LEFT JOIN workflow.datasets b
ON a.datafeed_id = b.datafeed_id 
AND a.dataset_id > b.dataset_id
AND a.created > b.created
AND b.dataset_quality_status = 'S'
GROUP BY a.dataset_id

"""