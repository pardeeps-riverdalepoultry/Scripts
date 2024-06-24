Select max(_fivetran_synced) as max_time from time_off_event;

ALTER STAGE S3_TIME_OFF_EVENT SET DIRECTORY = (ENABLE = TRUE);

alter pipe time_off_incremental_load set pipe_execution_paused = false;


create or replace pipe RPE_APOLLO.NAMELY.INCREMENTAL_LOAD_TIME_OFF_EVENT auto_ingest=true as COPY INTO RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_TIME_OFF_EVENT
    FROM @RPE_APOLLO.NAMELY.S3_NAMELY_TIME_OFF_EVENT
    file_format =  (FORMAT_NAME = RPE_APOLLO.NAMELY.PARQUET_FILE_FORMAT)
    match_by_column_name = case_insensitive;


CREATE OR REPLACE TABLE RPE_APOLLO.NAMELY.TIME_OFF_EVENT CLONE RPE_APOLLO.NAMELY_FIVETRAN.TIME_OFF_EVENT;
CREATE OR REPLACE TABLE RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_TIME_OFF_EVENT CLONE RPE_APOLLO.NAMELY_FIVETRAN.INCREMENTAL_UPDATE_TIME_OFF_EVENT;



CREATE OR REPLACE STAGE S3_NAMELY_TIME_OFF_EVENT
STORAGE_INTEGRATION = AWS_S3_INTEGRATION
URL = 's3://riverdale-analytics/namely_custom/report_time_off_events/data/'
FILE_FORMAT = RPE_APOLLO.NAMELY.PARQUET_FILE_FORMAT;


create or replace dynamic table RPE_APOLLO.NAMELY.TIME_OFF_EVENT_TO_INSERT(
	_FIVETRAN_BATCH,
	_FIVETRAN_INDEX,
	_FIVETRAN_SYNCED,
	LEGAL_FIRST_NAME,
	APPROVED,
	GUID,
	DATE_UPDATED,
	JOB_TITLE,
	LEAVE_END,
	LAST_NAME,
	DAYS_OFF,
	UNITS,
	RECORD_ID,
	TYPE,
	EMPLOYEE_NOTES,
	LEAVE_START,
	DATE_SUBMITTED,
	MANAGER_NOTES
) lag = '1 hour' refresh_mode = AUTO initialize = ON_CREATE warehouse = COMPUTE_WH
 as
SELECT *
FROM incremental_update_time_off_event
WHERE INCREMENTAL_UPDATE_time_off_event._fivetran_batch = (SELECT MAX(INCREMENTAL_UPDATE_time_off_event._fivetran_batch) FROM INCREMENTAL_UPDATE_time_off_event);


CREATE OR REPLACE TABLE RPE_APOLLO.NAMELY.TIME_OFF_EVENT_FULL_REPORT CLONE RPE_APOLLO.NAMELY_FIVETRAN.TIME_OFF_EVENT_FULL_REPORT;
CREATE OR REPLACE TABLE RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_TIME_OFF_EVENT_FULL_REPORT CLONE RPE_APOLLO.NAMELY_FIVETRAN.INCREMENTAL_UPDATE_TIME_OFF_EVENT_FULL_REPORT;


CREATE OR REPLACE STAGE S3_NAMELY_TIME_OFF_EVENT_FULL_REPORT
STORAGE_INTEGRATION = AWS_S3_INTEGRATION
URL = 's3://riverdale-analytics/namely_custom/report_unfiltered_time_off_events/data/'
FILE_FORMAT = RPE_APOLLO.NAMELY.PARQUET_FILE_FORMAT;

create or replace pipe RPE_APOLLO.NAMELY.INCREMENTAL_LOAD_TIME_OFF_EVENT_FULL_REPORT auto_ingest=true as COPY INTO RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_TIME_OFF_EVENT_FULL_REPORT
    FROM @RPE_APOLLO.NAMELY.S3_NAMELY_TIME_OFF_EVENT_FULL_REPORT
    file_format =  (FORMAT_NAME = RPE_APOLLO.NAMELY.PARQUET_FILE_FORMAT)
    match_by_column_name = case_insensitive;


create or replace dynamic table RPE_APOLLO.NAMELY.TIME_OFF_EVENT_FULL_REPORT_TO_INSERT(
	_FIVETRAN_BATCH,
	_FIVETRAN_INDEX,
	_FIVETRAN_SYNCED,
	LEGAL_FIRST_NAME,
	APPROVED,
	GUID,
	DATE_UPDATED,
	JOB_TITLE,
	LEAVE_END,
	LAST_NAME,
	DAYS_OFF,
	UNITS,
	RECORD_ID,
	TYPE,
	EMPLOYEE_NOTES,
	LEAVE_START,
	DATE_SUBMITTED,
	MANAGER_NOTES
) lag = '4 hours' refresh_mode = AUTO initialize = ON_CREATE warehouse = COMPUTE_WH
 as
SELECT *
FROM incremental_update_time_off_event_full_report
WHERE INCREMENTAL_UPDATE_time_off_event_full_report._fivetran_batch = (SELECT MAX(INCREMENTAL_UPDATE_time_off_event_full_report._fivetran_batch) FROM INCREMENTAL_UPDATE_TIME_OFF_EVENT_FULL_REPORT);



CREATE OR REPLACE PROCEDURE RPE_APOLLO.NAMELY.UPDATETIMEOFFEVENT()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN
  -- Step 1: Update TIME_OFF_EVENT with data from Dynamic table
  MERGE INTO rpe_apollo.namely.time_off_event tgt
  USING rpe_apollo.namely.time_off_event_to_insert src
  ON tgt.record_id = src.record_id
  WHEN MATCHED THEN
    UPDATE SET 
        tgt._FIVETRAN_BATCH = src._FIVETRAN_BATCH,
        tgt._FIVETRAN_INDEX = src._FIVETRAN_INDEX,
        tgt._FIVETRAN_SYNCED = src._FIVETRAN_SYNCED,
        tgt.LEGAL_FIRST_NAME = src.LEGAL_FIRST_NAME,
        tgt.APPROVED = src.APPROVED,
        tgt.GUID = src.GUID,
        tgt.DATE_UPDATED = src.DATE_UPDATED,
        tgt.JOB_TITLE = src.JOB_TITLE,
        tgt.LEAVE_END = src.LEAVE_END,
        tgt.LAST_NAME = src.LAST_NAME,
        tgt.DAYS_OFF = src.DAYS_OFF,
        tgt.UNITS = src.UNITS,
        tgt.RECORD_ID = src.RECORD_ID,
        tgt.TYPE = src.TYPE,
        tgt.EMPLOYEE_NOTES = src.EMPLOYEE_NOTES,
        tgt.LEAVE_START = src.LEAVE_START,
        tgt.DATE_SUBMITTED = src.DATE_SUBMITTED,
        tgt.MANAGER_NOTES = src.MANAGER_NOTES
  WHEN NOT MATCHED THEN
    INSERT (
        _FIVETRAN_BATCH,
        _FIVETRAN_INDEX,
        _FIVETRAN_SYNCED,
        LEGAL_FIRST_NAME,
        APPROVED,
        GUID,
        DATE_UPDATED,
        JOB_TITLE,
        LEAVE_END,
        LAST_NAME,
        DAYS_OFF,
        UNITS,
        RECORD_ID,
        TYPE,
        EMPLOYEE_NOTES,
        LEAVE_START,
        DATE_SUBMITTED,
        MANAGER_NOTES
    )
    VALUES (
        src._FIVETRAN_BATCH,
        src._FIVETRAN_INDEX,
        src._FIVETRAN_SYNCED,
        src.LEGAL_FIRST_NAME,
        src.APPROVED,
        src.GUID,
        src.DATE_UPDATED,
        src.JOB_TITLE,
        src.LEAVE_END,
        src.LAST_NAME,
        src.DAYS_OFF,
        src.UNITS,
        src.RECORD_ID,
        src.TYPE,
        src.EMPLOYEE_NOTES,
        src.LEAVE_START,
        src.DATE_SUBMITTED,
        src.MANAGER_NOTES
    );

    -- Step 2: Updating time_off_event using time_off_event_full_report table
    MERGE INTO rpe_apollo.namely.time_off_event toe
    USING (
        SELECT 
            toe.record_id,
            CASE 
                WHEN tom.record_id IS NULL THEN TRUE
                ELSE FALSE
            END AS record_deleted
        FROM 
            rpe_apollo.namely.time_off_event toe
        LEFT JOIN 
            rpe_apollo.namely.time_off_event_full_report_to_insert tom 
        ON 
            toe.record_id = tom.record_id
    ) src
    ON toe.record_id = src.record_id
    WHEN MATCHED AND toe.DATE_SUBMITTED > ''2024-01-01'' THEN 
        UPDATE SET record_deleted = src.record_deleted;

    -- Step 3: Truncate table incrmental_update_time_off_event records where _fivetran_batch is not equal to max(_fivetran_batch)
    DELETE FROM rpe_apollo.namely.incremental_update_time_off_event 
    WHERE _fivetran_batch NOT IN (
        SELECT MAX(_fivetran_batch) 
        FROM rpe_apollo.namely.incremental_update_time_off_event
    )
    AND (SELECT COUNT(DISTINCT _fivetran_batch) FROM rpe_apollo.namely.incremental_update_time_off_event) > 1;

    -- Step 4: Truncate table incrmental_update_time_off_event_full_report records where _fivetran_batch is not equal to max(_fivetran_batch)
    DELETE FROM rpe_apollo.namely.incremental_update_time_off_event_full_report
    WHERE _fivetran_batch NOT IN (
        SELECT MAX(_fivetran_batch) 
        FROM rpe_apollo.namely.incremental_update_time_off_event_full_report
    )
    AND (SELECT COUNT(DISTINCT _fivetran_batch) FROM rpe_apollo.namely.incremental_update_time_off_event_full_report) > 1;

    RETURN ''Update and Truncate Completed'';
END;
';

CALL RPE_APOLLO.NAMELY.UPDATETIMEOFFEVENT();


create or replace task RPE_APOLLO.NAMELY.UPDATE_TIME_OFF_EVENT
	warehouse=COMPUTE_WH
	schedule='60 minutes'
	as CALL rpe_apollo.namely.updatetimeoffevent();


EXECUTE TASK RPE_APOLLO.NAMELY.UPDATE_TIME_OFF_EVENT;


select * from time_off_event_full_report;

Call rpe_apollo.namely.updatetimeoffevent();

Select max(_fivetran_synced) as max_time from time_off_event;
select

SELECT *
FROM incremental_update_time_off_event
WHERE INCREMENTAL_UPDATE_time_off_event._fivetran_batch = (SELECT MAX(INCREMENTAL_UPDATE_time_off_event._fivetran_batch) FROM INCREMENTAL_UPDATE_time_off_event);

SELECT * from incremental_update_time_off_event;

truncate table incremental_update_time_off_event;

SELECT * from time_off_event_to_insert;

SELECT *
FROM incremental_update_time_off_event
WHERE INCREMENTAL_UPDATE_time_off_event._fivetran_batch = (SELECT MAX(INCREMENTAL_UPDATE_time_off_event._fivetran_batch) FROM INCREMENTAL_UPDATE_time_off_event);

alter task RPE_APOLLO.NAMELY.UPDATE_TIME_OFF_EVENT resume;

call rpe_apollo.namely.updatetimeoffevent();


create or replace pipe RPE_APOLLO.NAMELY.INCREMENTAL_LOAD_PROFILE auto_ingest=true as COPY INTO RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_PROFILE
    FROM @RPE_APOLLO.NAMELY.S3_NAMELY_PROFILE
    file_format =  (FORMAT_NAME = RPE_APOLLO.NAMELY.PARQUET_FILE_FORMAT)
    match_by_column_name = case_insensitive;


COPY INTO RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_PROFILE
FROM @RPE_APOLLO.NAMELY.S3_NAMELY_PROFILE
file_format =  (FORMAT_NAME = RPE_APOLLO.NAMELY.PARQUET_FILE_FORMAT)
    match_by_column_name = case_insensitive;