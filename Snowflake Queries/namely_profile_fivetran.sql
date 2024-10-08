-- change table name from profile to profile_fivetran
ALTER TABLE profile_fivetran RENAME TO profile;

-- change table name from incremental_update_profile to incremental_update_profile_fivetran
ALTER TABLE incremental_update_profile_fivetran RENAME TO incremental_update_profile;

--rename stage name from S3_NAMELY_PROFILES TO S3_NAMELY_PROFILE_FIVETRAN
ALTER STAGE S3_NAMELY_PROFILE_FIVETRAN RENAME TO S3_NAMELY_PROFILE;

--rename pipe name from profile_incremental_load to profile_fivetran_incremental_load
create or replace pipe RPE_APOLLO.NAMELY.PROFILE_INCREMENTAL_LOAD auto_ingest=true as COPY INTO RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_PROFILE
    FROM @RPE_APOLLO.NAMELY.S3_NAMELY_PROFILE
    file_format =  (FORMAT_NAME = RPE_APOLLO.NAMELY.NAMELY_PARQUET_FORMAT)
    match_by_column_name = case_insensitive;

CREATE OR REPLACE DYNAMIC TABLE PROFILE_TO_INSERT
TARGET_LAG = '240 minute'
WAREHOUSE = COMPUTE_WH
AS
WITH CTE AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY ID ORDER BY _FIVETRAN_SYNCED DESC) AS rn
    FROM RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_PROFILE
)
SELECT *
FROM CTE
WHERE rn = 1 AND ID NOT IN ('7741803') AND _FIVETRAN_DELETED = FALSE;

CREATE OR REPLACE PROCEDURE RPE_APOLLO.NAMELY.UPDATEPROFILE()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '

    function executeSqlStatement(sqlText) {
        try {
            var stmt = snowflake.createStatement({ sqlText: sqlText });
            var rs = stmt.execute();
            return "Success";
        } catch (err) {
            return "Error: " + err.message;
        }
    }
    
    
    var update_primary_table = `
        MERGE INTO RPE_APOLLO.NAMELY.PROFILE AS t
        USING RPE_APOLLO.NAMELY.PROFILE_TO_INSERT AS s
        ON t.ID = s.ID
        WHEN MATCHED THEN
            UPDATE SET
				t.ID = s.ID,
				t.DIVERSITY_STATS = s.DIVERSITY_STATS,
				t.GENDER_IDENTITY = s.GENDER_IDENTITY,
				t.UPDATED_AT = s.UPDATED_AT,
				t.CREATED_AT = s.CREATED_AT,
				t.TEAM_POSITIONS = s.TEAM_POSITIONS,
				t.SALARY_IS_HOURLY = s.SALARY_IS_HOURLY,
				t.SALARY_YEARLY_AMOUNT = s.SALARY_YEARLY_AMOUNT,
				t._FIVETRAN_DELETED = s._FIVETRAN_DELETED,
				t.HOME_COUNTRY_ID = s.HOME_COUNTRY_ID,
				t.JOB_ID = s.JOB_ID,
				t.OFFICE_COUNTRY_ID = s.OFFICE_COUNTRY_ID,
				t._FIVETRAN_SYNCED = s._FIVETRAN_SYNCED,
				t.COMPANY_UUID = s.COMPANY_UUID,
				t.SALARY_RATE = s.SALARY_RATE,
				t.ACCESS_ROLE = s.ACCESS_ROLE,
				t.DEPARTURE_DATE = s.DEPARTURE_DATE,
				t.BIO = s.BIO,
				t.SSN = s.SSN,
				t.OFFICE_DIRECT_DIAL = s.OFFICE_DIRECT_DIAL,
				t.EMERGENCY_CONTACT_PHONE = s.EMERGENCY_CONTACT_PHONE,
				t.HOME_PHONE = s.HOME_PHONE,
				t.EMERGENCY_CONTACT_RELATIONSHIP = s.EMERGENCY_CONTACT_RELATIONSHIP,
				t.SALARY_AMOUNT_RAW = s.SALARY_AMOUNT_RAW,
				t.SALARY_DATE = s.SALARY_DATE,
				t.EMPLOYEE_TYPE = s.EMPLOYEE_TYPE,
				t.FULL_NAME = s.FULL_NAME,
				t.OFFICE_MAIN_NUMBER = s.OFFICE_MAIN_NUMBER,
				t.SALARY_CURRENCY_TYPE = s.SALARY_CURRENCY_TYPE,
				t.DOB = s.DOB,
				t.EMPLOYEE_ID = s.EMPLOYEE_ID,
				t.LINKEDIN_URL = s.LINKEDIN_URL,
				t.PREFERRED_NAME = s.PREFERRED_NAME,
				t.ETHNICITY = s.ETHNICITY,
				t.GENDER = s.GENDER,
				t.SALARY_PAYROLL_COMPANY_NAME = s.SALARY_PAYROLL_COMPANY_NAME,
				t.HOME_ZIP = s.HOME_ZIP,
				t.SALARY_PAYROLL_JOB_ID = s.SALARY_PAYROLL_JOB_ID,
				t.MOBILE_PHONE = s.MOBILE_PHONE,
				t.OFFICE_STATE_ID = s.OFFICE_STATE_ID,
				t.OFFICE_CITY = s.OFFICE_CITY,
				t.FIRST_NAME = s.FIRST_NAME,
				t.EMAIL = s.EMAIL,
				t.START_DATE = s.START_DATE,
				t.RESUME = s.RESUME,
				t.USER_STATUS = s.USER_STATUS,
				t.IS_NAMELY_ADMIN = s.IS_NAMELY_ADMIN,
				t.LAST_NAME = s.LAST_NAME,
				t.MIDDLE_NAME = s.MIDDLE_NAME,
				t.HOME_ADDRESS_2 = s.HOME_ADDRESS_2,
				t.HOME_ADDRESS_1 = s.HOME_ADDRESS_1,
				t.OFFICE_ZIP = s.OFFICE_ZIP,
				t.OFFICE_ADDRESS_2 = s.OFFICE_ADDRESS_2,
				t.MARITAL_STATUS = s.MARITAL_STATUS,
				t.HOME_CITY = s.HOME_CITY,
				t.SALARY_GUID = s.SALARY_GUID,
				t.OFFICE_ADDRESS_1 = s.OFFICE_ADDRESS_1,
				t.DATE_FORMAT = s.DATE_FORMAT,
				t.HOME_STATE_ID = s.HOME_STATE_ID,
				t.PERSONAL_EMAIL = s.PERSONAL_EMAIL,
				t.EMERGENCY_CONTACT = s.EMERGENCY_CONTACT
		WHEN NOT MATCHED THEN
			INSERT (ID, DIVERSITY_STATS, GENDER_IDENTITY, UPDATED_AT, CREATED_AT, TEAM_POSITIONS, SALARY_IS_HOURLY, SALARY_YEARLY_AMOUNT, _FIVETRAN_DELETED, HOME_COUNTRY_ID, JOB_ID, OFFICE_COUNTRY_ID, _FIVETRAN_SYNCED, COMPANY_UUID, SALARY_RATE, ACCESS_ROLE, DEPARTURE_DATE, BIO, SSN, OFFICE_DIRECT_DIAL, EMERGENCY_CONTACT_PHONE, HOME_PHONE, EMERGENCY_CONTACT_RELATIONSHIP, SALARY_AMOUNT_RAW, SALARY_DATE, EMPLOYEE_TYPE, FULL_NAME, OFFICE_MAIN_NUMBER, SALARY_CURRENCY_TYPE, DOB, EMPLOYEE_ID, LINKEDIN_URL, PREFERRED_NAME, ETHNICITY, GENDER, SALARY_PAYROLL_COMPANY_NAME, HOME_ZIP, SALARY_PAYROLL_JOB_ID, MOBILE_PHONE, OFFICE_STATE_ID, OFFICE_CITY, FIRST_NAME, EMAIL, START_DATE, RESUME, USER_STATUS, IS_NAMELY_ADMIN, LAST_NAME, MIDDLE_NAME, HOME_ADDRESS_2, HOME_ADDRESS_1, OFFICE_ZIP, OFFICE_ADDRESS_2, MARITAL_STATUS, HOME_CITY, SALARY_GUID, OFFICE_ADDRESS_1, DATE_FORMAT, HOME_STATE_ID, PERSONAL_EMAIL, EMERGENCY_CONTACT)
			VALUES (s.ID, s.DIVERSITY_STATS, s.GENDER_IDENTITY, s.UPDATED_AT, s.CREATED_AT, s.TEAM_POSITIONS, s.SALARY_IS_HOURLY, s.SALARY_YEARLY_AMOUNT, s._FIVETRAN_DELETED, s.HOME_COUNTRY_ID, s.JOB_ID, s.OFFICE_COUNTRY_ID, s._FIVETRAN_SYNCED, s.COMPANY_UUID, s.SALARY_RATE, s.ACCESS_ROLE, s.DEPARTURE_DATE, s.BIO, s.SSN, s.OFFICE_DIRECT_DIAL, s.EMERGENCY_CONTACT_PHONE, s.HOME_PHONE, s.EMERGENCY_CONTACT_RELATIONSHIP, s.SALARY_AMOUNT_RAW, s.SALARY_DATE, s.EMPLOYEE_TYPE, s.FULL_NAME, s.OFFICE_MAIN_NUMBER, s.SALARY_CURRENCY_TYPE, s.DOB, s.EMPLOYEE_ID, s.LINKEDIN_URL, s.PREFERRED_NAME, s.ETHNICITY, s.GENDER, s.SALARY_PAYROLL_COMPANY_NAME, s.HOME_ZIP, s.SALARY_PAYROLL_JOB_ID, s.MOBILE_PHONE, s.OFFICE_STATE_ID, s.OFFICE_CITY, s.FIRST_NAME, s.EMAIL, s.START_DATE, s.RESUME, s.USER_STATUS, s.IS_NAMELY_ADMIN, s.LAST_NAME, s.MIDDLE_NAME, s.HOME_ADDRESS_2, s.HOME_ADDRESS_1, s.OFFICE_ZIP, s.OFFICE_ADDRESS_2, s.MARITAL_STATUS, s.HOME_CITY, s.SALARY_GUID, s.OFFICE_ADDRESS_1, s.DATE_FORMAT, s.HOME_STATE_ID, s.PERSONAL_EMAIL, s.EMERGENCY_CONTACT);
          
           
    `;

    var remove_deleted_profiles_from_primary_table = `
        DELETE FROM RPE_APOLLO.NAMELY.PROFILE WHERE ID NOT IN (SELECT ID FROM RPE_APOLLO.NAMELY.PROFILE_TO_INSERT);
    `;
    var remove_historic_data_from_incremental_table = `
        DELETE FROM RPE_APOLLO.NAMELY.INCREMENTAL_UPDATE_PROFILE WHERE _fivetran_synced < (SELECT MIN(_fivetran_synced) FROM RPE_APOLLO.NAMELY.PROFILE_TO_INSERT WHERE rn = 1);
    `;
    
        
    // Execute the SQL statements
    var result1 = executeSqlStatement(update_primary_table);
    var result2 = executeSqlStatement(remove_deleted_profiles_from_primary_table);
    var result3 = executeSqlStatement(remove_historic_data_from_incremental_table);

    return "update_primary_table " + result1 + "\\n" + "remove_deleted_profiles_from_primary_table " + result2 + "\\n" + "remove_historic_data_from_incremental_table " + result3;
';

CALL RPE_APOLLO.NAMELY.UPDATEPROFILE();



create or replace task RPE_APOLLO.NAMELY.INSERT_UPDATE_DELETE_PROFILE
	warehouse=COMPUTE_WH
	schedule='240 minutes'
	as CALL UPDATEPROFILE();

ALTER TASK RPE_APOLLO.NAMELY.INSERT_UPDATE_DELETE_PROFILE RESUME;

-- CALL TASK RPE_APOLLO.NAMELY.INSERT_UPDATE_DELETE_PROFILE_FIVETRAN;
EXECUTE TASK RPE_APOLLO.NAMELY.INSERT_UPDATE_DELETE_PROFILE;

-- update schema name from rpe_apollo.namely to rpe_apollo.namely_fivetran
ALTER SCHEMA rpe_apollo.namely RENAME TO rpe_apollo.namely_fivetran;


CREATE OR REPLACE DYNAMIC TABLE RPE_APOLLO.NAMELY_FIVETRAN.PROFILE_TO_INSERT
TARGET_LAG = '240 minute'
WAREHOUSE = COMPUTE_WH
AS
WITH CTE AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY ID ORDER BY _FIVETRAN_SYNCED DESC) AS rn
    FROM RPE_APOLLO.NAMELY_FIVETRAN.INCREMENTAL_UPDATE_PROFILE
)
SELECT *
FROM CTE
WHERE rn = 1 AND ID NOT IN ('7741803') AND _FIVETRAN_DELETED = FALSE;


CREATE OR REPLACE PROCEDURE RPE_APOLLO.NAMELY_FIVETRAN.UPDATEPROFILE()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '

    function executeSqlStatement(sqlText) {
        try {
            var stmt = snowflake.createStatement({ sqlText: sqlText });
            var rs = stmt.execute();
            return "Success";
        } catch (err) {
            return "Error: " + err.message;
        }
    }
    
    
    var update_primary_table = `
        MERGE INTO RPE_APOLLO.NAMELY_FIVETRAN.PROFILE AS t
        USING RPE_APOLLO.NAMELY_FIVETRAN.PROFILE_TO_INSERT AS s
        ON t.ID = s.ID
        WHEN MATCHED THEN
            UPDATE SET
				t.ID = s.ID,
				t.DIVERSITY_STATS = s.DIVERSITY_STATS,
				t.GENDER_IDENTITY = s.GENDER_IDENTITY,
				t.UPDATED_AT = s.UPDATED_AT,
				t.CREATED_AT = s.CREATED_AT,
				t.TEAM_POSITIONS = s.TEAM_POSITIONS,
				t.SALARY_IS_HOURLY = s.SALARY_IS_HOURLY,
				t.SALARY_YEARLY_AMOUNT = s.SALARY_YEARLY_AMOUNT,
				t._FIVETRAN_DELETED = s._FIVETRAN_DELETED,
				t.HOME_COUNTRY_ID = s.HOME_COUNTRY_ID,
				t.JOB_ID = s.JOB_ID,
				t.OFFICE_COUNTRY_ID = s.OFFICE_COUNTRY_ID,
				t._FIVETRAN_SYNCED = s._FIVETRAN_SYNCED,
				t.COMPANY_UUID = s.COMPANY_UUID,
				t.SALARY_RATE = s.SALARY_RATE,
				t.ACCESS_ROLE = s.ACCESS_ROLE,
				t.DEPARTURE_DATE = s.DEPARTURE_DATE,
				t.BIO = s.BIO,
				t.SSN = s.SSN,
				t.OFFICE_DIRECT_DIAL = s.OFFICE_DIRECT_DIAL,
				t.EMERGENCY_CONTACT_PHONE = s.EMERGENCY_CONTACT_PHONE,
				t.HOME_PHONE = s.HOME_PHONE,
				t.EMERGENCY_CONTACT_RELATIONSHIP = s.EMERGENCY_CONTACT_RELATIONSHIP,
				t.SALARY_AMOUNT_RAW = s.SALARY_AMOUNT_RAW,
				t.SALARY_DATE = s.SALARY_DATE,
				t.EMPLOYEE_TYPE = s.EMPLOYEE_TYPE,
				t.FULL_NAME = s.FULL_NAME,
				t.OFFICE_MAIN_NUMBER = s.OFFICE_MAIN_NUMBER,
				t.SALARY_CURRENCY_TYPE = s.SALARY_CURRENCY_TYPE,
				t.DOB = s.DOB,
				t.EMPLOYEE_ID = s.EMPLOYEE_ID,
				t.LINKEDIN_URL = s.LINKEDIN_URL,
				t.PREFERRED_NAME = s.PREFERRED_NAME,
				t.ETHNICITY = s.ETHNICITY,
				t.GENDER = s.GENDER,
				t.SALARY_PAYROLL_COMPANY_NAME = s.SALARY_PAYROLL_COMPANY_NAME,
				t.HOME_ZIP = s.HOME_ZIP,
				t.SALARY_PAYROLL_JOB_ID = s.SALARY_PAYROLL_JOB_ID,
				t.MOBILE_PHONE = s.MOBILE_PHONE,
				t.OFFICE_STATE_ID = s.OFFICE_STATE_ID,
				t.OFFICE_CITY = s.OFFICE_CITY,
				t.FIRST_NAME = s.FIRST_NAME,
				t.EMAIL = s.EMAIL,
				t.START_DATE = s.START_DATE,
				t.RESUME = s.RESUME,
				t.USER_STATUS = s.USER_STATUS,
				t.IS_NAMELY_ADMIN = s.IS_NAMELY_ADMIN,
				t.LAST_NAME = s.LAST_NAME,
				t.MIDDLE_NAME = s.MIDDLE_NAME,
				t.HOME_ADDRESS_2 = s.HOME_ADDRESS_2,
				t.HOME_ADDRESS_1 = s.HOME_ADDRESS_1,
				t.OFFICE_ZIP = s.OFFICE_ZIP,
				t.OFFICE_ADDRESS_2 = s.OFFICE_ADDRESS_2,
				t.MARITAL_STATUS = s.MARITAL_STATUS,
				t.HOME_CITY = s.HOME_CITY,
				t.SALARY_GUID = s.SALARY_GUID,
				t.OFFICE_ADDRESS_1 = s.OFFICE_ADDRESS_1,
				t.DATE_FORMAT = s.DATE_FORMAT,
				t.HOME_STATE_ID = s.HOME_STATE_ID,
				t.PERSONAL_EMAIL = s.PERSONAL_EMAIL,
				t.EMERGENCY_CONTACT = s.EMERGENCY_CONTACT
		WHEN NOT MATCHED THEN
			INSERT (ID, DIVERSITY_STATS, GENDER_IDENTITY, UPDATED_AT, CREATED_AT, TEAM_POSITIONS, SALARY_IS_HOURLY, SALARY_YEARLY_AMOUNT, _FIVETRAN_DELETED, HOME_COUNTRY_ID, JOB_ID, OFFICE_COUNTRY_ID, _FIVETRAN_SYNCED, COMPANY_UUID, SALARY_RATE, ACCESS_ROLE, DEPARTURE_DATE, BIO, SSN, OFFICE_DIRECT_DIAL, EMERGENCY_CONTACT_PHONE, HOME_PHONE, EMERGENCY_CONTACT_RELATIONSHIP, SALARY_AMOUNT_RAW, SALARY_DATE, EMPLOYEE_TYPE, FULL_NAME, OFFICE_MAIN_NUMBER, SALARY_CURRENCY_TYPE, DOB, EMPLOYEE_ID, LINKEDIN_URL, PREFERRED_NAME, ETHNICITY, GENDER, SALARY_PAYROLL_COMPANY_NAME, HOME_ZIP, SALARY_PAYROLL_JOB_ID, MOBILE_PHONE, OFFICE_STATE_ID, OFFICE_CITY, FIRST_NAME, EMAIL, START_DATE, RESUME, USER_STATUS, IS_NAMELY_ADMIN, LAST_NAME, MIDDLE_NAME, HOME_ADDRESS_2, HOME_ADDRESS_1, OFFICE_ZIP, OFFICE_ADDRESS_2, MARITAL_STATUS, HOME_CITY, SALARY_GUID, OFFICE_ADDRESS_1, DATE_FORMAT, HOME_STATE_ID, PERSONAL_EMAIL, EMERGENCY_CONTACT)
			VALUES (s.ID, s.DIVERSITY_STATS, s.GENDER_IDENTITY, s.UPDATED_AT, s.CREATED_AT, s.TEAM_POSITIONS, s.SALARY_IS_HOURLY, s.SALARY_YEARLY_AMOUNT, s._FIVETRAN_DELETED, s.HOME_COUNTRY_ID, s.JOB_ID, s.OFFICE_COUNTRY_ID, s._FIVETRAN_SYNCED, s.COMPANY_UUID, s.SALARY_RATE, s.ACCESS_ROLE, s.DEPARTURE_DATE, s.BIO, s.SSN, s.OFFICE_DIRECT_DIAL, s.EMERGENCY_CONTACT_PHONE, s.HOME_PHONE, s.EMERGENCY_CONTACT_RELATIONSHIP, s.SALARY_AMOUNT_RAW, s.SALARY_DATE, s.EMPLOYEE_TYPE, s.FULL_NAME, s.OFFICE_MAIN_NUMBER, s.SALARY_CURRENCY_TYPE, s.DOB, s.EMPLOYEE_ID, s.LINKEDIN_URL, s.PREFERRED_NAME, s.ETHNICITY, s.GENDER, s.SALARY_PAYROLL_COMPANY_NAME, s.HOME_ZIP, s.SALARY_PAYROLL_JOB_ID, s.MOBILE_PHONE, s.OFFICE_STATE_ID, s.OFFICE_CITY, s.FIRST_NAME, s.EMAIL, s.START_DATE, s.RESUME, s.USER_STATUS, s.IS_NAMELY_ADMIN, s.LAST_NAME, s.MIDDLE_NAME, s.HOME_ADDRESS_2, s.HOME_ADDRESS_1, s.OFFICE_ZIP, s.OFFICE_ADDRESS_2, s.MARITAL_STATUS, s.HOME_CITY, s.SALARY_GUID, s.OFFICE_ADDRESS_1, s.DATE_FORMAT, s.HOME_STATE_ID, s.PERSONAL_EMAIL, s.EMERGENCY_CONTACT);
          
           
    `;

    var remove_deleted_profiles_from_primary_table = `
        DELETE FROM RPE_APOLLO.NAMELY_FIVETRAN.PROFILE WHERE ID NOT IN (SELECT ID FROM RPE_APOLLO.NAMELY_FIVETRAN.PROFILE_TO_INSERT);
    `;
    var remove_historic_data_from_incremental_table = `
        DELETE FROM RPE_APOLLO.NAMELY_FIVETRAN.INCREMENTAL_UPDATE_PROFILE WHERE _fivetran_synced < (SELECT MIN(_fivetran_synced) FROM RPE_APOLLO.NAMELY_FIVETRAN.PROFILE_TO_INSERT WHERE rn = 1);
    `;
    
        
    // Execute the SQL statements
    var result1 = executeSqlStatement(update_primary_table);
    var result2 = executeSqlStatement(remove_deleted_profiles_from_primary_table);
    var result3 = executeSqlStatement(remove_historic_data_from_incremental_table);

    return "update_primary_table " + result1 + "\\n" + "remove_deleted_profiles_from_primary_table " + result2 + "\\n" + "remove_historic_data_from_incremental_table " + result3;
';


create or replace task RPE_APOLLO.NAMELY_FIVETRAN.INSERT_UPDATE_DELETE_PROFILE
	warehouse=COMPUTE_WH
	schedule='240 minutes'
	as CALL RPE_APOLLO.NAMELY_FIVETRAN.UPDATEPROFILE();


SELECT * FROM INCREMENTAL_UPDATE_GROUP;


CREATE OR REPLACE DYNAMIC TABLE RPE_APOLLO.NAMELY_FIVETRAN.NAMELY_GROUP_TO_INSERT
TARGET_LAG = '240 minute'
WAREHOUSE = COMPUTE_WH
AS
WITH CTE AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY ID ORDER BY _FIVETRAN_SYNCED DESC) AS rn
    FROM RPE_APOLLO.NAMELY_FIVETRAN.INCREMENTAL_UPDATE_GROUP
)
SELECT *
FROM CTE
WHERE rn = 1;


CREATE OR REPLACE PROCEDURE RPE_APOLLO.NAMELY_FIVETRAN.REFRESHPROFILEGROUP()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
try {
  // Step 1: Check if table INCREMENTAL_UPDATE_PROFILE_GROUP has data
  var checkQuery = "SELECT COUNT(*) FROM RPE_APOLLO.NAMELY_FIVETRAN.INCREMENTAL_UPDATE_PROFILE_GROUP";
  var checkStmt = snowflake.createStatement({sqlText: checkQuery});
  var checkResult = checkStmt.execute();
  if (checkResult.next()) {
    var rowCount = checkResult.getColumnValue(1);
    if (rowCount > 0) {
      // Step 2: Clear data from table PROFILE_GROUP
      var clearTable2Query = "DELETE FROM RPE_APOLLO.NAMELY_FIVETRAN.PROFILE_GROUP";
      var clearTable2Stmt = snowflake.createStatement({sqlText: clearTable2Query});
      clearTable2Stmt.execute();

      // Step 3: Copy data from INCREMENTAL_UPDATE_PROFILE_GROUP to PROFILE_GROUP
      var copyDataQuery = "INSERT INTO RPE_APOLLO.NAMELY_FIVETRAN.PROFILE_GROUP SELECT * FROM RPE_APOLLO.NAMELY_FIVETRAN.INCREMENTAL_UPDATE_PROFILE_GROUP";
      var copyDataStmt = snowflake.createStatement({sqlText: copyDataQuery});
      copyDataStmt.execute();

      // Step 4: Clear data from INCREMENTAL_UPDATE_PROFILE_GROUP (truncate)
      var truncateTable1Query = "TRUNCATE TABLE RPE_APOLLO.NAMELY_FIVETRAN.INCREMENTAL_UPDATE_PROFILE_GROUP";
      var truncateTable1Stmt = snowflake.createStatement({sqlText: truncateTable1Query});
      truncateTable1Stmt.execute();

      return "Data copied from INCREMENTAL_UPDATE_PROFILE_GROUP to PROFILE_GROUP and TABLE1 truncated.";
    } else {
      return "INCREMENTAL_UPDATE_PROFILE_GROUP is empty. No data copied or truncated.";
    }
  } else {
    return "Error checking INCREMENTAL_UPDATE_PROFILE_GROUP data.";
  }
} catch (err) {
  return "Error: " + err.message;
}
';


CREATE OR REPLACE PROCEDURE RPE_APOLLO.NAMELY_FIVETRAN.UPDATENAMELYGROUP()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '

    function executeSqlStatement(sqlText) {
        try {
            var stmt = snowflake.createStatement({ sqlText: sqlText });
            var rs = stmt.execute();
            return "Success";
        } catch (err) {
            return "Error: " + err.message;
        }
    }
    
    
    var update_namely_group = `
        MERGE INTO RPE_APOLLO.NAMELY_FIVETRAN.GROUP_DATA As t
        USING RPE_APOLLO.NAMELY_FIVETRAN.NAMELY_GROUP_TO_INSERT AS s
        ON t.ID = s.ID
        WHEN MATCHED THEN
            UPDATE SET
				t.ID = s.ID,
				t._FIVETRAN_DELETED = s._FIVETRAN_DELETED,
				t.ADDRESS_COUNTRY_ID = s.ADDRESS_COUNTRY_ID,
				t.GROUP_TYPE_ID = s.GROUP_TYPE_ID,
				t._FIVETRAN_SYNCED = s._FIVETRAN_SYNCED,
				t.ADDRESS_COUNTRY = s.ADDRESS_COUNTRY,
				t.COUNT = s.COUNT,
				t.ADDRESS_STATE = s.ADDRESS_STATE,
				t.ADDRESS_STATE_ID = s.ADDRESS_STATE_ID,
				t.TITLE = s.TITLE,
				t.TYPE = s.TYPE,
				t.IS_TEAM = s.IS_TEAM,
				t.ADDRESS_CITY = s.ADDRESS_CITY,
				t.ADDRESS_ADDRESS_2 = s.ADDRESS_ADDRESS_2,
				t.ADDRESS_ADDRESS_1 = s.ADDRESS_ADDRESS_1,
				t.ADDRESS_ZIP = s.ADDRESS_ZIP
		WHEN NOT MATCHED THEN
			INSERT (ID, _FIVETRAN_DELETED, ADDRESS_COUNTRY_ID, GROUP_TYPE_ID, _FIVETRAN_SYNCED, ADDRESS_COUNTRY, COUNT, ADDRESS_STATE, ADDRESS_STATE_ID, TITLE, TYPE, IS_TEAM, ADDRESS_CITY, ADDRESS_ADDRESS_2, ADDRESS_ADDRESS_1, ADDRESS_ZIP)
			VALUES (s.ID, s._FIVETRAN_DELETED, s.ADDRESS_COUNTRY_ID, s.GROUP_TYPE_ID, s._FIVETRAN_SYNCED, s.ADDRESS_COUNTRY, s.COUNT, s.ADDRESS_STATE, s.ADDRESS_STATE_ID, s.TITLE, s.TYPE, s.IS_TEAM, s.ADDRESS_CITY, s.ADDRESS_ADDRESS_2, s.ADDRESS_ADDRESS_1, s.ADDRESS_ZIP);`;

    var remove_deleted_group_from_primary_table = `DELETE FROM RPE_APOLLO.NAMELY_FIVETRAN.GROUP_DATA WHERE ID NOT IN (SELECT ID FROM RPE_APOLLO.NAMELY_FIVETRAN.NAMELY_GROUP_TO_INSERT);`;
    
    var remove_historic_data_from_incremental_table = `DELETE FROM RPE_APOLLO.NAMELY_FIVETRAN.INCREMENTAL_UPDATE_GROUP WHERE _fivetran_synced < (SELECT MIN(_fivetran_synced) FROM RPE_APOLLO.NAMELY_FIVETRAN.NAMELY_GROUP_TO_INSERT WHERE rn = 1);`;
    
        
    // Execute the SQL statements
    var result1 = executeSqlStatement(update_namely_group);
    var result2 = executeSqlStatement(remove_deleted_group_from_primary_table);
    var result3 = executeSqlStatement(remove_historic_data_from_incremental_table);

    return "update_namely_group: " + result1 + "\\n" + "remove_deleted_group_from_primary_table: " + result2 + "\\n" + "remove_historic_data_from_incremental_table: " + result3;
';