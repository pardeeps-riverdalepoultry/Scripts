create table RPE_APOLLO.DAILY_LOAD_BOARD.GRADING_RESULT_MERLIN CLONE RPE_APOLLO_TESTING_ENVIRONMENT.DAILY_LOAD_BOARD.GRADING_RESULT;

create table RPE_APOLLO.NAMELY.NAMELY_GROUP CLONE RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_GROUP;

--create stage

CREATE OR REPLACE STAGE RPE_APOLLO.DAILY_LOAD_BOARD.S3_GRADING_RESULT_MERLIN_STAGE
STORAGE_INTEGRATION = AWS_S3_INTEGRATION
URL = 's3://riverdale-analytics/daily_load_board/Grading_Result_Merlin/'
FILE_FORMAT = RPE_APOLLO.DAILY_LOAD_BOARD.CSV_DAILY_LOAD_BOARD;



create or replace pipe RPE_APOLLO.DAILY_LOAD_BOARD.S3_GRADING_RESULT_MERLIN_PIPE auto_ingest=true as COPY INTO RPE_APOLLO.DAILY_LOAD_BOARD.GRADING_RESULT_MERLIN FROM @RPE_APOLLO.DAILY_LOAD_BOARD.S3_GRADING_RESULT_MERLIN_STAGE FILE_FORMAT = CSV_DAILY_LOAD_BOARD;



-- create a select query that counts number of records in a table grouped by processing date and order by processing date in descending order
SELECT processing_date, count(processing_date) as record_count FROM RPE_APOLLO.DAILY_LOAD_BOARD.load group by processing_date order by processing_date desc;


SELECT kill_date, farm_name, grade_a_percent from RPE_APOLLO.DAILY_LOAD_BOARD.grading_result_merlin where KILL_DATE = '2024-04-29';

desc table RPE_APOLLO.DAILY_LOAD_BOARD.grading_result_merlin;


CREATE OR REPLACE STAGE RPE_APOLLO.NAMELY.S3_NAMELY_GROUP_STAGE
STORAGE_INTEGRATION = AWS_S3_INTEGRATION
URL = 's3://riverdale-analytics/namely_custom/groups/data/'
FILE_FORMAT = RPE_APOLLO.NAMELY.PARQUET_FILE_FORMAT;

create or replace pipe RPE_APOLLO.NAMELY.INCREMENTAL_LOAD_GROUP auto_ingest=true as COPY INTO RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_GROUP
    FROM @RPE_APOLLO.NAMELY.S3_NAMELY_GROUP_STAGE
    file_format =  (FORMAT_NAME = RPE_APOLLO.NAMELY.PARQUET_FILE_FORMAT)
    match_by_column_name = case_insensitive;


CREATE OR REPLACE PROCEDURE RPE_APOLLO.NAMELY.REFRESH_GROUP_TABLE()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    recordCount NUMBER(38, 2);
BEGIN
    -- Check if RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_GROUP has any records
    SELECT COUNT(*) INTO recordCount FROM RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_GROUP;

    IF (recordCount > 0) THEN
        -- Delete all records from GROUP
        DELETE FROM RPE_APOLLO.NAMELY.GROUP;

        -- Insert records with the maximum _fivetran_batch value to GROUP
        INSERT INTO RPE_APOLLO.NAMELY.GROUP
        SELECT * 
        FROM RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_GROUP
        WHERE _fivetran_batch = (SELECT MAX(_fivetran_batch) FROM RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_GROUP);

        -- Delete records from RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_GROUP where _fivetran_batch doesn''t have the maximum value
        DELETE FROM RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_GROUP
        WHERE _fivetran_batch <> (SELECT MAX(_fivetran_batch) FROM RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_GROUP);
    END IF;

    RETURN ''Procedure executed successfully'';
END;
';


DECLARE
    recordCount NUMBER(38, 2);
BEGIN
    SELECT COUNT(*) INTO recordCount 
    FROM RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_GROUP;


    return recordCount;
    -- You can add other logic here to use the recordCount variable

END;

CREATE OR REPLACE PROCEDURE REFRESHGROUPTABLE()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    var result = snowflake.execute(
        `SELECT COUNT(*) AS record_count FROM INCREMENTAL_UPDATE_GROUP`
    );
    
    result.next();
    var recordCount = result.getColumnValue('RECORD_COUNT');

    if (recordCount > 0) {
        // Delete all records from GROUP
        snowflake.execute(
            `DELETE FROM GROUP`
        );

        // Get the maximum _fivetran_batch value
        var maxBatchResult = snowflake.execute(
            `SELECT MAX(_fivetran_batch) AS max_batch FROM INCREMENTAL_UPDATE_GROUP`
        );
        maxBatchResult.next();
        var maxBatch = maxBatchResult.getColumnValue('MAX_BATCH');

        if (maxBatch !== null) {
            // Insert records with max _fivetran_batch value
            snowflake.execute({
                sqlText: `INSERT INTO GROUP
                          SELECT * FROM INCREMENTAL_UPDATE_GROUP
                          WHERE _fivetran_batch = :1`,
                binds: [maxBatch]
            });

            // Delete records where _fivetran_batch is not the max
            snowflake.execute({
                sqlText: `DELETE FROM INCREMENTAL_UPDATE_GROUP
                          WHERE _fivetran_batch <> :1`,
                binds: [maxBatch]
            });

            return 'Records updated successfully.';
        } else {
            return 'Failed to retrieve the maximum _fivetran_batch value.';
        }
    } else {
        return 'No records found in INCREMENTAL_UPDATE_GROUP.';
    }
} catch (err) {
    return 'Failed to update records: ' + err;
}
$$;


create or replace task RPE_APOLLO.NAMELY.REFRESH_GROUP
	warehouse=COMPUTE_WH
	schedule='1440 minutes'
	as CALL RPE_APOLLO.NAMELY.REFRESHGROUPTABLE();


EXECUTE TASK RPE_APOLLO.NAMELY.REFRESH_GROUP;


CALL REFRESHGROUPTABLE();

CREATE OR REPLACE PROCEDURE RPE_APOLLO.NAMELY.REFRESHGROUPTABLE()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    recordCount NUMBER(38, 2);
BEGIN
    -- Check if RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_GROUP has any records
    SELECT COUNT(*) INTO recordCount FROM RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_GROUP;
    -- Delete all records from GROUP
    DELETE FROM RPE_APOLLO.NAMELY.GROUP;

    -- Insert records with the maximum _fivetran_batch value to GROUP
    INSERT INTO RPE_APOLLO.NAMELY.GROUP
    SELECT * 
    FROM RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_GROUP
    WHERE _fivetran_batch = (SELECT MAX(_fivetran_batch) FROM RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_GROUP);

    -- Delete records from RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_GROUP where _fivetran_batch doesn''t have the maximum value
    DELETE FROM RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_GROUP
    WHERE _fivetran_batch <> (SELECT MAX(_fivetran_batch) FROM RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_GROUP);
    END IF;

    RETURN ''Procedure executed successfully'';
END;
';

desc table RPE_APOLLO.NAMELY.GROUP;


CREATE OR REPLACE PROCEDURE RPE_APOLLO.NAMELY.REFRESHGROUPTABLE()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    recordCount NUMBER(38, 2);
BEGIN
    -- Check if RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_GROUP has any records
    SELECT COUNT(*) INTO recordCount FROM RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_GROUP;

    IF (recordCount > 0) THEN
        -- Delete all records from GROUP
        DELETE FROM RPE_APOLLO.NAMELY.NAMELY_GROUP;

        -- Insert records with the maximum _fivetran_batch value to GROUP
        INSERT INTO RPE_APOLLO.NAMELY.NAMELY_GROUP
        SELECT * 
        FROM RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_GROUP
        WHERE _fivetran_batch = (SELECT MAX(_fivetran_batch) FROM RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_GROUP);

        -- Delete records from RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_GROUP where _fivetran_batch doesn''t have the maximum value
        DELETE FROM RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_GROUP
        WHERE _fivetran_batch <> (SELECT MAX(_fivetran_batch) FROM RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_GROUP);
    END IF;

    RETURN ''Procedure executed successfully'';
END;
';