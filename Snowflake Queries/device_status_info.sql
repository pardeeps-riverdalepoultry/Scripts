select * from device_status_info where _fivetran_synced in (select max(_fivetran_synced) from device_status_info);
select * from incremental_update_device_status_info;

-- NEXT CREATE FILE FORMAT, ALREADY CREATED FOR THIS TASK

-- CREATE STAGE
CREATE OR REPLACE STAGE S3_DEVICE_STATUS_INFO
STORAGE_INTEGRATION = AWS_S3_INTEGRATION
URL = 's3://riverdale-analytics/geotab_rpe/device_status_info/data/'
FILE_FORMAT = RPE_APOLLO.GEOTAB.PARQUET_GEOTAB_DATA;

-- CREATE PIPE
CREATE OR REPLACE PIPE DEVICE_STATUS_INFO_INCREMENTAL_LOAD
    AUTO_INGEST = TRUE
    AS COPY INTO RPE_APOLLO.GEOTAB.INCREMENTAL_UPDATE_DEVICE_STATUS_INFO
    FROM @RPE_APOLLO.GEOTAB.S3_DEVICE_STATUS_INFO
    file_format =  (FORMAT_NAME = RPE_APOLLO.GEOTAB.PARQUET_GEOTAB_DATA)
    match_by_column_name = case_insensitive;

desc table device_status_info;


-- create procedure
CREATE OR REPLACE PROCEDURE rpe_apollo.geotab.refreshDeviceStatusInfo()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    recordCount NUMBER(38, 2);
BEGIN
    -- Check if incremental_update_device_status_info has any records
    SELECT COUNT(*) INTO recordCount FROM rpe_apollo.geotab.incremental_update_device_status_info;

    IF (recordCount > 0) THEN
        -- Delete all records from device_status_info
        DELETE FROM rpe_apollo.geotab.device_status_info;

        -- Insert records with the maximum _fivetran_batch value to device_status_info
        INSERT INTO rpe_apollo.geotab.device_status_info
        SELECT * 
        FROM rpe_apollo.geotab.incremental_update_device_status_info
        WHERE _fivetran_batch = (SELECT MAX(_fivetran_batch) FROM rpe_apollo.geotab.incremental_update_device_status_info);

        -- Delete records from incremental_update_device_status_info where _fivetran_batch doesn't have the maximum value
        DELETE FROM rpe_apollo.geotab.incremental_update_device_status_info
        WHERE _fivetran_batch <> (SELECT MAX(_fivetran_batch) FROM rpe_apollo.geotab.incremental_update_device_status_info);
    END IF;

    RETURN 'Procedure executed successfully';
END;
$$;

-- create a task

create or replace task RPE_APOLLO.GEOTAB.REFRESH_DEVICE_STATUS_INFO
	warehouse=COMPUTE_WH
	schedule='60 minutes'
	as CALL refreshDeviceStatusInfo();


ALTER TASK RPE_APOLLO.GEOTAB.REFRESH_DEVICE_STATUS_INFO RESUME;

CALL rpe_apollo.geotab.refreshDeviceStatusInfo();

select * from device_status_info;


alter task refresh_device_status_info suspend;
Alter task refresh_device_status_info
set schedule = '30 minutes';

alter task refresh_device_status_info resume;

