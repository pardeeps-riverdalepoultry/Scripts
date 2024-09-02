select processing_date, count(processing_date) as processingDate from driver group by processing_date order by processing_date desc;


select max(_fivetran_synced) from TIME_OFF_EVENT;


create or replace dynamic table RPE_APOLLO.NAMELY.TIME_OFF_EVENT_TO_INSERT(
	_FIVETRAN_BATCH NUMBER(38,0),
	_FIVETRAN_INDEX NUMBER(38,0),
	_FIVETRAN_SYNCED TIMESTAMP_NTZ(9),
	LEGAL_FIRST_NAME VARCHAR(16777216),
	APPROVED BOOLEAN,
	GUID VARCHAR(16777216),
	DATE_UPDATED VARCHAR(16777216),
	JOB_TITLE VARCHAR(16777216),
	LAST_NAME VARCHAR(16777216),
	DAYS_OFF FLOAT,
	UNITS VARCHAR(16777216),
	RECORD_ID VARCHAR(16777216),
	TYPE VARCHAR(16777216),
	EMPLOYEE_NOTES VARCHAR(16777216),
	DATE_SUBMITTED VARCHAR(16777216),
	MANAGER_NOTES VARCHAR(16777216),
	LEAVE_START DATE,
	LEAVE_END DATE
) lag = '4 hour' refresh_mode = AUTO initialize = ON_CREATE warehouse = COMPUTE_WH
 as
SELECT *
FROM incremental_update_time_off_event
WHERE INCREMENTAL_UPDATE_time_off_event._fivetran_batch = (SELECT MAX(INCREMENTAL_UPDATE_time_off_event._fivetran_batch) FROM INCREMENTAL_UPDATE_time_off_event);

execute task RPE_APOLLO.NAMELY.UPDATE_TIME_OFF_EVENT;


CALL UPDATETIMEOFFEVENT();


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


select processing_date, count(processing_date) as processingDate from driver group by processing_date order by processing_date desc;