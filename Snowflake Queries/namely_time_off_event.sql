-- CREATED TABLES USING THE DOWNLOADED PARQUET FILES
SELECT * FROM RPE_APOLLO.NAMELY.TIME_OFF_EVENT;
SELECT* FROM RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_TIME_OFF_EVENT;

-- NEXT CREATE FILE FORMAT, ALREADY CREATED FOR THIS TASK

-- CREATE STAGE
CREATE OR REPLACE STAGE S3_TIME_OFF_EVENT
STORAGE_INTEGRATION = AWS_S3_INTEGRATION
URL = 's3://riverdale-analytics/namely_custom/report_time_off_events/data/'
FILE_FORMAT = RPE_APOLLO.NAMELY.NAMELY_PARQUET_FORMAT;

-- CREATE PIPE
CREATE OR REPLACE PIPE TIME_OFF_INCREMENTAL_LOAD
    AUTO_INGEST = TRUE
    AS COPY INTO RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_TIME_OFF_EVENT
    FROM @RPE_APOLLO.NAMELY.S3_TIME_OFF_EVENT
    file_format =  (FORMAT_NAME = RPE_APOLLO.NAMELY.NAMELY_PARQUET_FORMAT)
    match_by_column_name = case_insensitive;

-- ADD NEW COLUMN NAMED 'DELETE' IN TIME_OFF_EVENT TABLE
ALTER TABLE RPE_APOLLO.NAMELY.TIME_OFF_EVENT ADD COLUMN RECORD_DELETED BOOLEAN;

-- DESCRIBE TABLE TIME_OFF_EVENT
DESCRIBE TABLE RPE_APOLLO.NAMELY.TIME_OFF_EVENT;

-- ADDED A NEW TABLE TO HOLD FULL REPORT FROM TIME OFF EVENT REPORT WITH LOWER UPDATING FREQUENCY
-- CREATE STATE FOR THIS FULL REPORT TABLE
CREATE OR REPLACE STAGE S3_TIME_OFF_EVENT_FULL_REPORT
STORAGE_INTEGRATION = AWS_S3_INTEGRATION
URL = 's3://riverdale-analytics/namely_custom/report_unfiltered_time_off_events/data/'
FILE_FORMAT = RPE_APOLLO.NAMELY.NAMELY_PARQUET_FORMAT;

-- CREATE PIPE FOR FULL REPORT
CREATE OR REPLACE PIPE TIME_OFF_EVENT_FULL_REPORT_INCREMENTAL_LOAD
    AUTO_INGEST = TRUE
    AS COPY INTO RPE_APOLLO.NAMELY.TIME_OFF_EVENT_FULL_REPORT
    FROM @RPE_APOLLO.NAMELY.S3_TIME_OFF_EVENT_FULL_REPORT
    file_format =  (FORMAT_NAME = RPE_APOLLO.NAMELY.NAMELY_PARQUET_FORMAT)
    match_by_column_name = case_insensitive;

-- CREATE A DYNAMIC TABLE FOR TIME_OFF_EVENT_TO_INSERT
CREATE OR REPLACE DYNAMIC TABLE time_off_event_to_insert
TARGET_LAG = '60 minute'
WAREHOUSE = COMPUTE_WH
AS
SELECT *
FROM incremental_update_time_off_event
WHERE INCREMENTAL_UPDATE_time_off_event._fivetran_batch = (SELECT MAX(INCREMENTAL_UPDATE_time_off_event._fivetran_batch) FROM INCREMENTAL_UPDATE_time_off_event);

-- CREATE A DYNAMIC TABLE FOR TIME_OFF_EVENT_FULL_REPORT_TO_INSERT
CREATE OR REPLACE DYNAMIC TABLE time_off_event_full_report_to_insert
TARGET_LAG = '240 minute'
WAREHOUSE = COMPUTE_WH
AS
SELECT *
FROM incremental_update_time_off_event_full_report
WHERE INCREMENTAL_UPDATE_time_off_event_full_report._fivetran_batch = (SELECT MAX(INCREMENTAL_UPDATE_time_off_event_full_report._fivetran_batch) FROM INCREMENTAL_UPDATE_TIME_OFF_EVENT_FULL_REPORT);

-- CREATE A PROCEDURE TO UPDATE TIME_OFF_EVENT TABLE

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
    MERGE INTO time_off_event toe
    USING (
        SELECT 
            toe.record_id,
            CASE 
                WHEN tom.record_id IS NULL THEN TRUE
                ELSE FALSE
            END AS record_deleted
        FROM 
            time_off_event toe
        LEFT JOIN 
            time_off_event_full_report_to_insert tom 
        ON 
            toe.record_id = tom.record_id
    ) src
    ON toe.record_id = src.record_id
    WHEN MATCHED AND toe.DATE_SUBMITTED > ''2024-01-01'' THEN 
        UPDATE SET record_deleted = src.record_deleted;

    -- Step 3: Truncate table incrmental_update_time_off_event records where _fivetran_batch is not equal to max(_fivetran_batch)
    DELETE FROM incremental_update_time_off_event 
    WHERE _fivetran_batch NOT IN (
        SELECT MAX(_fivetran_batch) 
        FROM incremental_update_time_off_event
    )
    AND (SELECT COUNT(DISTINCT _fivetran_batch) FROM incremental_update_time_off_event) > 1;

    -- Step 4: Truncate table incrmental_update_time_off_event_full_report records where _fivetran_batch is not equal to max(_fivetran_batch)
    DELETE FROM incremental_update_time_off_event_full_report
    WHERE _fivetran_batch NOT IN (
        SELECT MAX(_fivetran_batch) 
        FROM incremental_update_time_off_event_full_report
    )
    AND (SELECT COUNT(DISTINCT _fivetran_batch) FROM incremental_update_time_off_event_full_report) > 1;

    RETURN ''Update and Truncate Completed'';
END;
';

-- call the procedure
CALL RPE_APOLLO.NAMELY.UPDATETIMEOFFEVENT();

-- create Task
create or replace task RPE_APOLLO.NAMELY.UPDATE_TIME_OFF_EVENT
	warehouse=COMPUTE_WH
	schedule='60 minutes'
	as CALL updatetimeoffevent();

-- resume the task
ALTER TASK RPE_APOLLO.NAMELY.UPDATE_TIME_OFF_EVENT RESUME;


select * from RPE_APOLLO.NAMELY.incremental_update_time_off_event;