alter table loads_copy_v2
add plant_arrival_time TIMESTAMP_NTZ;

desc table loads_copy_v2;

select * from loads_copy_v2 where processing_date = '2024-05-08';

create or replace table loads_copy_v2 
clone RPE_APOLLO.DAILY_LOAD_BOARD.LOAD;

-- Migrating changes to Load Table in Live Environment
desc table load;

-- adding new columns to the table
alter table load
--add trailer_pickup_time TIMESTAMP_NTZ
add plant_arrival_time TIMESTAMP_NTZ;

-- pause the S3_LOAD_PIPE
alter pipe S3_LOAD_PIPE set pipe_execution_paused = true;
-- creating the pipe again
create or replace pipe RPE_APOLLO.DAILY_LOAD_BOARD.S3_LOAD_PIPE auto_ingest=true as COPY INTO RPE_APOLLO.DAILY_LOAD_BOARD.LOAD FROM @RPE_APOLLO.DAILY_LOAD_BOARD.S3_LOAD_STAGE FILE_FORMAT = CSV_DAILY_LOAD_BOARD;
-- starting the pipe
alter pipe S3_LOAD_PIPE set pipe_execution_paused = false;

-- Test data
select * from event where processing_date = '2024-05-17';

--I want to select the minimum processing date from the event table
select min(processing_date) from event;

-- delete records from load where processing_date = '2024-05-07';
delete from load where processing_date = '2024-05-06';

