-- change leave start data type
ALTER TABLE RPE_APOLLO_TESTING_ENVIRONMENT.DAILY_LOAD_BOARD.GRADING_RESULT ADD COLUMN ACTUAL_TRACTOR_TEMP VARCHAR(16777216);
UPDATE RPE_APOLLO_TESTING_ENVIRONMENT.DAILY_LOAD_BOARD.GRADING_RESULT SET ACTUAL_TRACTOR_TEMP = ACTUAL_TRACTOR ;
ALTER TABLE RPE_APOLLO_TESTING_ENVIRONMENT.DAILY_LOAD_BOARD.GRADING_RESULT DROP COLUMN ACTUAL_TRACTOR;
ALTER TABLE RPE_APOLLO_TESTING_ENVIRONMENT.DAILY_LOAD_BOARD.GRADING_RESULT RENAME COLUMN ACTUAL_TRACTOR_TEMP TO ACTUAL_TRACTOR;