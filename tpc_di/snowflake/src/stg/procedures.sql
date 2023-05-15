CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_ACCOUNT_SP(scale float,batches float,wait float)
  returns string
  language javascript
  as
  $$
  var tpcdi_scale = SCALE
  // Load incremental files	
  var batch_counter = 3;
  while (batch_counter <= BATCHES)
  {
    var incrm_stmt = snowflake.createStatement(
      {sqlText: "COPY INTO TPCDI_STG.PUBLIC.ACCOUNT_STG FROM @TPCDI_FILES/tmp/tpcdi/sf=" + tpcdi_scale + "/Batch" + batch_counter + "/Account FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') ON_ERROR = SKIP_FILE"}
      );
    incrm_stmt.execute();
    // insert wait here
    stmt = snowflake.createStatement({sqlText:"call system$wait(" + WAIT + ", 'SECONDS')"});
    rs = stmt.execute();	
    batch_counter++
  }
  // Suspend Load Task
  var task_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_ACCOUNT_I_" + tpcdi_scale + "_TSK SUSPEND"});
  task_stmt.execute();
  return "All account files have been loaded.";
  $$
;

CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_CASHTRANSACTION_H_SP(scale float)
	returns float
	language javascript
	as
	$$
	var tpcdi_scale = SCALE
	// Load historical file
	var hist_stmt = snowflake.createStatement(
		{sqlText: "COPY INTO TPCDI_STG.PUBLIC.CASHTRANSACTION_STG(CT_CA_ID,CT_DTS,CT_AMT,CT_NAME) FROM (SELECT $1 $3 	,$2 $4 	,$3 $5 	,$4 $6 FROM @TPCDI_FILES/tmp/tpcdi/sf=" + tpcdi_scale + "/Batch1/CashTransaction) FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') ON_ERROR = SKIP_FILE"}
		);
	hist_stmt.execute();
	// Stop task
	var stoptask_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_CASHTRANSACTION_H_" + tpcdi_scale + "_TSK SUSPEND"});
	stoptask_stmt.execute();
	return "All historical cashtransaction files have been loaded.";
	$$
;

CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_CASHTRANSACTION_I_SP(scale float,batches float,wait float)
	returns float
	language javascript
	as
	$$
	var tpcdi_scale = SCALE
	// Load incremental files	
	var batch_counter = 1;
	while (batch_counter <= BATCHES)	
	{
		var incrm_stmt = snowflake.createStatement(
			{sqlText: "COPY INTO TPCDI_STG.PUBLIC.CASHTRANSACTION_STG FROM @TPCDI_FILES/tmp/tpcdi/sf=" + tpcdi_scale + "/Batch" + batch_counter + "/CashTransaction FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') ON_ERROR = SKIP_FILE"}
			);
		incrm_stmt.execute();
		// insert wait here
		stmt = snowflake.createStatement({sqlText:"call system$wait(" + WAIT + ", 'SECONDS')"});
		rs = stmt.execute();	
		batch_counter++
	}
	// Suspend Load Task
	var task_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_CASHTRANSACTION_I_" + tpcdi_scale + "_TSK SUSPEND"});
	task_stmt.execute();
	return "All incremental cashtransaction files have been loaded.";
	$$
;

CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_CASHTRANSACTION_SP(files float,wait float)
	returns float
	language javascript
	as
	$$
	// Load historical file
	var hist_stmt = snowflake.createStatement(
		{sqlText: "INSERT INTO TPCDI_STG.PUBLIC.CASHTRANSACTION_STG SELECT 	 TO_CHAR(NULL) $1 	,ROW_NUMBER() OVER (ORDER BY 1) $2 	,$1 $3 	,$2 $4 	,$3 $5 	,$4 $6 FROM @TPCDI_FILES/load/cash_transaction/CashTransaction01.txt (FILE_FORMAT => 'TXT_PIPE')"}
		);
	hist_stmt.execute();
	// Insert wait here
	stmt = snowflake.createStatement({sqlText:"call system$wait(" + WAIT + ", 'SECONDS')"});
	rs = stmt.execute();
	// Load incremental files	
	var file_counter = 2;
	while (file_counter <= FILES)
	{
		var incrm_stmt = snowflake.createStatement(
			{sqlText: "COPY INTO TPCDI_STG.PUBLIC.CASHTRANSACTION_STG FROM @TPCDI_FILES/load/cash_transaction/ FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') PATTERN='.*0" + file_counter + ".txt' ON_ERROR = SKIP_FILE"}
			);
		incrm_stmt.execute();
		// insert wait here
		stmt = snowflake.createStatement({sqlText:"call system$wait(" + WAIT + ", 'SECONDS')"});
		rs = stmt.execute();	
		file_counter++
	}
	return "All cashtransaction files have been loaded.";
	$$
;

CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_CUSTOMER_MGMT_SP(scale float)
  returns string
  language javascript
  as
  $$
  var tpcdi_scale = SCALE
  // Load CUSTOMER_MGMT_STG
  stmt = snowflake.createStatement(
      {sqlText: "COPY INTO TPCDI_STG.PUBLIC.CUSTOMER_MGMT_STG FROM @TPCDI_FILES/tmp/tpcdi/sf=" + tpcdi_scale + "/Batch1/CustomerMgmt FILE_FORMAT = (FORMAT_NAME = 'XML') ON_ERROR=CONTINUE"}
    );
  rs = stmt.execute();
  rs.next();
  // Stop task
  var stoptask_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_CUSTOMER_MGMT_" + tpcdi_scale + "_TSK SUSPEND"});
  stoptask_stmt.execute();
  return "All customer_mgmt files have been loaded.";
  $$
;
CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_CUSTOMER_SP(scale float,batches float,wait float)
  returns string
  language javascript
  as
  $$
  var tpcdi_scale = SCALE
  // Load incremental files	
  var batch_counter = 1;
  while (batch_counter <= BATCHES)
  {
    var incrm_stmt = snowflake.createStatement(
      {sqlText: "COPY INTO TPCDI_STG.PUBLIC.CUSTOMER_STG FROM @TPCDI_FILES/tmp/tpcdi/sf=" + tpcdi_scale + "/Batch" + batch_counter + "/Customer FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') ON_ERROR = SKIP_FILE"}
      );
    incrm_stmt.execute();
    // insert wait here
    stmt = snowflake.createStatement({sqlText:"call system$wait(" + WAIT + ", 'SECONDS')"});
    rs = stmt.execute();	
    batch_counter++
  }
  // Suspend Load Task
  var task_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_CUSTOMER_I_" + tpcdi_scale + "_TSK SUSPEND"});
  task_stmt.execute();
  return "All customer files have been loaded.";
  $$
;
CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_DAILYMARKET_H_SP(scale float)
	returns float
	language javascript
	as
	$$
	var tpcdi_scale = SCALE
	// Load historical file
	var hist_stmt = snowflake.createStatement(
		{sqlText: "COPY INTO TPCDI_STG.PUBLIC.DAILYMARKET_STG(DM_DATE,DM_S_SYMB,DM_CLOSE,DM_HIGH,DM_LOW,DM_VOL) FROM (SELECT $1 $3	,$2 $4	,$3 $5	,$4 $6	,$5 $7	,$6 $8 FROM @TPCDI_FILES/tmp/tpcdi/sf=" + tpcdi_scale + "/Batch1/DailyMarket) FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') ON_ERROR = SKIP_FILE"}
		);
	hist_stmt.execute();
	// Stop task
	var stoptask_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_DAILYMARKET_H_" + tpcdi_scale + "_TSK SUSPEND"});
	stoptask_stmt.execute();
	return "All historical dailymarket files have been loaded.";
	$$
;
CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_DAILYMARKET_I_SP(scale float,batches float,wait float)
	returns float
	language javascript
	as
	$$
	var tpcdi_scale = SCALE
	// Load incremental files	
	var batch_counter = 1;
	while (batch_counter <= BATCHES)	
	{
		var incrm_stmt = snowflake.createStatement(
			{sqlText: "COPY INTO TPCDI_STG.PUBLIC.DAILYMARKET_STG FROM @TPCDI_FILES/tmp/tpcdi/sf=" + tpcdi_scale + "/Batch" + batch_counter + "/DailyMarket FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') ON_ERROR = SKIP_FILE"}
			);
		incrm_stmt.execute();
		// insert wait here
		stmt = snowflake.createStatement({sqlText:"call system$wait(" + WAIT + ", 'SECONDS')"});
		rs = stmt.execute();	
		batch_counter++
	}
	// Suspend Load Task
	var task_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_DAILYMARKET_I_" + tpcdi_scale + "_TSK SUSPEND"});
	task_stmt.execute();
	return "All incremental dailymarket files have been loaded.";
	$$
;
CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_DAILYMARKET_SP(files float,wait float)
	returns float
	language javascript
	as
	$$
	// Load historical file
	var hist_stmt = snowflake.createStatement(
		{sqlText: "INSERT INTO TPCDI_STG.PUBLIC.DAILYMARKET_STG SELECT	 TO_CHAR(NULL) $1	,ROW_NUMBER() OVER (ORDER BY 1) $2	,$1 $3	,$2 $4	,$3 $5	,$4 $6	,$5 $7	,$6 $8 FROM @TPCDI_FILES/load/daily_market/DailyMarket01.txt (FILE_FORMAT => 'TXT_PIPE')"}
		);
	hist_stmt.execute();
	// Insert wait here
	stmt = snowflake.createStatement({sqlText:"call system$wait(" + WAIT + ", 'SECONDS')"});
	rs = stmt.execute();
	// Load incremental files	
	var file_counter = 2;
	while (file_counter <= FILES)
	{
		var incrm_stmt = snowflake.createStatement(
			{sqlText: "COPY INTO TPCDI_STG.PUBLIC.DAILYMARKET_STG FROM @TPCDI_FILES/load/daily_market/ FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') PATTERN='.*0" + file_counter + ".txt' ON_ERROR = SKIP_FILE"}
			);
		incrm_stmt.execute();
		// insert wait here
		stmt = snowflake.createStatement({sqlText:"call system$wait(" + WAIT + ", 'SECONDS')"});
		rs = stmt.execute();	
		file_counter++
	}
	return "All dailymarket files have been loaded.";
	$$
;
CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_DATE_SP(scale float)
  returns string
  language javascript
  as
  $$
  var tpcdi_scale = SCALE
  // Load DATE_STG
  stmt = snowflake.createStatement(
      {sqlText: "COPY INTO TPCDI_STG.PUBLIC.DATE_STG FROM @TPCDI_FILES/tmp/tpcdi/sf=" + tpcdi_scale + "/Batch1/Date.txt FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') ON_ERROR = SKIP_FILE"}
    );
  rs = stmt.execute();
  rs.next();
  // Stop task
  var stoptask_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_DATE_" + tpcdi_scale + "_TSK SUSPEND"});
  stoptask_stmt.execute();
  return "All date files have been loaded.";
  $$
;

CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_FINWIRE_SP(scale float)
  returns string
  language javascript
  as
  $$
   // Load incremental files
  var tpcdi_scale = SCALE;
  var batch_counter = 1;
  var BATCHES = 3;
  var WAIT = 60;
  while (batch_counter <= BATCHES)	{
		// Load All FINWIRE files
	 var incrm_stmt = snowflake.createStatement(
      		{sqlText: "COPY INTO TPCDI_STG.PUBLIC.FINWIRE_STG FROM ( SELECT SUBSTR($1, 0, 15) PTS, SUBSTR($1, 16, 3) REC_TYPE, SUBSTR($1, 19, 60) COMPANY_NAME, SUBSTR($1, 79, 10) CIK, IFF( SUBSTR($1, 16, 3) = ' CMP', SUBSTR($1, 89, 4), SUBSTR($1, 40, 4) ) STATUS, SUBSTR($1, 93, 2) INDUSTRY_ID, SUBSTR($1, 95, 4) SP_RATING, SUBSTR($1, 99, 8) FOUNDING_DATE, SUBSTR($1, 107, 80) ADDR_LINE1, SUBSTR($1, 187, 80) ADDR_LINE2, SUBSTR($1, 267, 12) POSTAL_CODE, SUBSTR($1, 279, 25) CITY, SUBSTR($1, 304, 20) STATE_PROVINCE, SUBSTR($1, 324, 24) COUNTRY, SUBSTR($1, 348, 46) CEO_NAME, SUBSTR($1, 394, 150) DESCRIPTION, SUBSTR($1, 19, 4) YEAR, SUBSTR($1, 23, 1) QUARTER, SUBSTR($1, 24, 8) QTR_START_DATE, SUBSTR($1, 32, 8) POSTING_DATE, SUBSTR($1, 40, 17) REVENUE, SUBSTR($1, 57, 17) EARNINGS, SUBSTR($1, 74, 12) EPS, SUBSTR($1, 86, 12) DILUTED_EPS, SUBSTR($1, 98, 12) MARGIN, SUBSTR($1, 110, 17) INVENTORY, SUBSTR($1, 127, 17) ASSETS, SUBSTR($1, 144, 17) LIABILITIES, IFF( SUBSTR($1, 16, 3) = 'FIN', SUBSTR($1, 161, 13), SUBSTR($1, 120, 13) ) SH_OUT, SUBSTR($1, 174, 13) DILUTED_SH_OUT, IFF( SUBSTR($1, 16, 3) = 'FIN', SUBSTR($1, 187, 60), SUBSTR($1, 161, 60) ) CO_NAME_OR_CIK, SUBSTR($1, 19, 15) SYMBOL, SUBSTR($1, 34, 6) ISSUE_TYPE, SUBSTR($1, 44, 70) NAME, SUBSTR($1, 114, 6) EX_ID, SUBSTR($1, 133, 8) FIRST_TRADE_DATE, SUBSTR($1, 141, 8) FIRST_TRADE_EXCHG, SUBSTR($1, 149, 12) DIVIDEND FROM @TPCDI_FILES/tmp/tpcdi/sf=" + tpcdi_scale + "/Batch" + batch_counter + "/FINWIRE) FILE_FORMAT = (FORMAT_NAME = 'TXT_FIXED_WIDTH')  PATTERN = '^(?:(?!csv).)*$' ON_ERROR = SKIP_FILE"}
   	 );
	 incrm_stmt.execute();
	 // insert wait here
    stmt = snowflake.createStatement({sqlText:"call system$wait(" + WAIT + ", 'SECONDS')"});
    rs = stmt.execute();	
    batch_counter++
	}

  return "All FINWIRE files have been loaded.";
  $$
;

CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_HOLDING_HISTORY_H_SP(scale float)
	returns float
	language javascript
	as
	$$
	var tpcdi_scale = SCALE
	// Load historical file
	var hist_stmt = snowflake.createStatement(
		{sqlText: "COPY INTO TPCDI_STG.PUBLIC.HOLDINGHISTORY_STG(HH_H_T_ID,HH_T_ID,HH_BEFORE_QTY,HH_AFTER_QTY) FROM (SELECT $1 $3 	,$2 $4 	,$3 $5 	,$4 $6 FROM @TPCDI_FILES/tmp/tpcdi/sf=" + tpcdi_scale + "/Batch1/HoldingHistory) FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') ON_ERROR = SKIP_FILE"}
		);
	hist_stmt.execute();
	// Stop task
	var stoptask_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_HOLDING_HISTORY_H_" + tpcdi_scale + "_TSK SUSPEND"});
	stoptask_stmt.execute();
	return "All historical holding history files have been loaded.";
	$$
;
CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_HOLDING_HISTORY_I_SP(scale float,batches float,wait float)
	returns float
	language javascript
	as
	$$
	var tpcdi_scale = SCALE
	// Load incremental files	
	var batch_counter = 1;
	while (batch_counter <= BATCHES)	
	{
		var incrm_stmt = snowflake.createStatement(
			{sqlText: "COPY INTO TPCDI_STG.PUBLIC.HOLDINGHISTORY_STG FROM @TPCDI_FILES/tmp/tpcdi/sf=" + tpcdi_scale + "/Batch" + batch_counter + "/HoldingHistory FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') ON_ERROR = SKIP_FILE"}
			);
		incrm_stmt.execute();
		// insert wait here
		stmt = snowflake.createStatement({sqlText:"call system$wait(" + WAIT + ", 'SECONDS')"});
		rs = stmt.execute();	
		batch_counter++
	}
	// Suspend Load Task
	var task_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_HOLDING_HISTORY_I_" + tpcdi_scale + "_TSK SUSPEND"});
	task_stmt.execute();
	return "All incremental holding history files have been loaded.";
	$$
;
CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_HOLDING_HISTORY_SP(files float,wait float)
	returns float
	language javascript
	as
	$$
	// Load historical file
	var hist_stmt = snowflake.createStatement(
		{sqlText: "INSERT INTO TPCDI_STG.PUBLIC.HOLDINGHISTORY_STG SELECT 	 TO_CHAR(NULL) $1 	,ROW_NUMBER() OVER (ORDER BY 1) $2 	,$1 $3 	,$2 $4 	,$3 $5 	,$4 $6 FROM @TPCDI_FILES/load/holding_history/HoldingHistory01.txt (FILE_FORMAT => 'TXT_PIPE')"}
		);
	hist_stmt.execute();
	// Insert wait here
	stmt = snowflake.createStatement({sqlText:"call system$wait(" + WAIT + ", 'SECONDS')"});
	rs = stmt.execute();
	// Load incremental files	
	var file_counter = 2;
	while (file_counter <= FILES)
	{
		var incrm_stmt = snowflake.createStatement(
			{sqlText: "COPY INTO TPCDI_STG.PUBLIC.HOLDINGHISTORY_STG FROM @TPCDI_FILES/load/holding_history/ FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') PATTERN='.*0" + file_counter + ".txt' ON_ERROR = SKIP_FILE"}
			);
		incrm_stmt.execute();
		// insert wait here
		stmt = snowflake.createStatement({sqlText:"call system$wait(" + WAIT + ", 'SECONDS')"});
		rs = stmt.execute();	
		file_counter++
	}
	return "All holding history files have been loaded.";
	$$
;
CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_HR_SP(scale float)
  returns string
  language javascript
  as
  $$
  var tpcdi_scale = SCALE
  // Load HR_STG
  stmt = snowflake.createStatement(
      {sqlText: "COPY INTO TPCDI_STG.PUBLIC.HR_STG FROM @TPCDI_FILES/tmp/tpcdi/sf=" + tpcdi_scale + "/Batch1/HR FILE_FORMAT = (FORMAT_NAME = 'TXT_CSV') ON_ERROR = SKIP_FILE"}
    );
  rs = stmt.execute();
  rs.next();
  // Stop task
  var stoptask_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_HR_" + tpcdi_scale + "_TSK SUSPEND"});
  stoptask_stmt.execute();
  return "All HR files have been loaded.";
  $$
;
CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_INDUSTRY_SP(scale float)
  returns string
  language javascript
  as
  $$
  var tpcdi_scale = SCALE
  // Load INDUSTRY_STG
  stmt = snowflake.createStatement(
      {sqlText: "COPY INTO TPCDI_STG.PUBLIC.INDUSTRY_STG FROM @TPCDI_FILES/tmp/tpcdi/sf=" + tpcdi_scale + "/Batch1/Industry.txt FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') ON_ERROR = SKIP_FILE"}
    );
  rs = stmt.execute();
  rs.next();
  // Stop task
  var stoptask_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_INDUSTRY_" + tpcdi_scale + "_TSK SUSPEND"});
  stoptask_stmt.execute();
  return "All industry files have been loaded.";
  $$
;
CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_PROSPECT_H_SP(scale float)
	returns float
	language javascript
	as
	$$
	var tpcdi_scale = SCALE
	// Load historical file
	var hist_stmt = snowflake.createStatement(
		{sqlText: "CALL TPCDI_STG.PUBLIC.LOAD_PROSPECT_SP(" + tpcdi_scale + ",1,1,5)"}
		);
	hist_stmt.execute();
	// Stop task
	var stoptask_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_PROSPECT_H_" + tpcdi_scale + "_TSK SUSPEND"});
	stoptask_stmt.execute();
	return "All historical prospect files have been loaded.";
	$$
;
CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_PROSPECT_I_SP(scale float)
	returns float
	language javascript
	as
	$$
	var tpcdi_scale = SCALE
	// Load historical file
	var hist_stmt = snowflake.createStatement(
		{sqlText: "CALL TPCDI_STG.PUBLIC.LOAD_PROSPECT_SP(" + tpcdi_scale + ",3,2,60)"}
		);
	hist_stmt.execute();
	// Stop task
	var stoptask_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_PROSPECT_I_" + tpcdi_scale + "_TSK SUSPEND"});
	stoptask_stmt.execute();
	return "All incremental prospect files have been loaded.";
	$$
;
CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_PROSPECT_SP(scale float,files float,position float,wait float)
  returns string
  language javascript
  as
  $$
  // Load historical and incremental files	
  var tpcdi_scale = SCALE
  var file_counter = POSITION;
  while (file_counter <= FILES)
  {
    var incrm_stmt = snowflake.createStatement(
      {sqlText: "COPY INTO TPCDI_STG.PUBLIC.PROSPECT_STG FROM @TPCDI_FILES/tmp/tpcdi/sf=" + tpcdi_scale + "/Batch" + file_counter + "/Prospect FILE_FORMAT = (FORMAT_NAME = 'TXT_CSV') ON_ERROR = SKIP_FILE"}
      );
    incrm_stmt.execute();
    // insert wait here
    stmt = snowflake.createStatement({sqlText:"call system$wait(" + WAIT + ", 'SECONDS')"});
    rs = stmt.execute();	
    file_counter++
  }
  return "All prospect files have been loaded.";
  $$
;
CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_REFERENCE_FILES_SP()
  returns string
  language javascript
  as
  $$
  // Load Reference Files
  // Load DATE_STG
  stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_STG.PUBLIC.LOAD_DATE_SP()"}
    );
  rs = stmt.execute();
  // Load TIME_STG
  stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_STG.PUBLIC.LOAD_TIME_SP()"}
    );
  rs = stmt.execute();
  // Load TRADETYPE_STG
  stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_STG.PUBLIC.LOAD_TRADETYPE_SP()"}
    );
  rs = stmt.execute();
  // Load STATUSTYPE_STG
  stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_STG.PUBLIC.LOAD_STATUSTYPE_SP()"}
    );
  rs = stmt.execute();
  // Load TAXRATE_STG
  stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_STG.PUBLIC.LOAD_TAXRATE_SP()"}
    );
  rs = stmt.execute();
  // Load INDUSTRY_STG
  stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_STG.PUBLIC.LOAD_INDUSTRY_SP()"}
    );
  rs = stmt.execute();
  // Load HR_STG
  stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_STG.PUBLIC.LOAD_HR_SP()"}
    );
  rs = stmt.execute();
  rs.next();
  return "All reference files have been loaded.";
  $$
;
CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_STATUSTYPE_SP(scale float)
  returns string
  language javascript
  as
  $$
  var tpcdi_scale = SCALE
  // Load STATUSTYPE_STG
  stmt = snowflake.createStatement(
      {sqlText: "COPY INTO TPCDI_STG.PUBLIC.STATUSTYPE_STG FROM @TPCDI_FILES/tmp/tpcdi/sf=" + tpcdi_scale + "/Batch1/StatusType.txt FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') ON_ERROR = SKIP_FILE"}
    );
  rs = stmt.execute();
  rs.next();
  // Stop task
  var stoptask_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_STATUSTYPE_" + tpcdi_scale + "_TSK SUSPEND"});
  stoptask_stmt.execute();
  return "All statustype files have been loaded.";
  $$
;
CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_TAXRATE_SP(scale float)
  returns string
  language javascript
  as
  $$
  var tpcdi_scale = SCALE
  // Load TAXRATE_STG
  stmt = snowflake.createStatement(
      {sqlText: "COPY INTO TPCDI_STG.PUBLIC.TAXRATE_STG FROM @TPCDI_FILES/tmp/tpcdi/sf=" + tpcdi_scale + "/Batch1/TaxRate.txt FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') ON_ERROR = SKIP_FILE"}
    );
  rs = stmt.execute();
  rs.next();
  // Stop task
  var stoptask_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_TAXRATE_" + tpcdi_scale + "_TSK SUSPEND"});
  stoptask_stmt.execute();
  return "All taxrate files have been loaded.";
  $$
;
CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_TIME_SP(scale float)
  returns string
  language javascript
  as
  $$
  var tpcdi_scale = SCALE
  // Load TIME_STG
  stmt = snowflake.createStatement(
      {sqlText: "COPY INTO TPCDI_STG.PUBLIC.TIME_STG FROM @TPCDI_FILES/tmp/tpcdi/sf=" + tpcdi_scale + "/Batch1/Time.txt FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') ON_ERROR = SKIP_FILE"}
    );
  rs = stmt.execute();
  rs.next();
  // Stop task
  var stoptask_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_TIME_" + tpcdi_scale + "_TSK SUSPEND"});
  stoptask_stmt.execute();
  return "All time files have been loaded.";
  $$
;
CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_TRADETYPE_SP(scale float)
  returns string
  language javascript
  as
  $$
  var tpcdi_scale = SCALE
  // Load TRADETYPE_STG
  stmt = snowflake.createStatement(
      {sqlText: "COPY INTO TPCDI_STG.PUBLIC.TRADETYPE_STG FROM @TPCDI_FILES/tmp/tpcdi/sf=" + tpcdi_scale + "/Batch1/TradeType.txt FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') ON_ERROR = SKIP_FILE"}
    );
  rs = stmt.execute();
  // Stop task
  var stoptask_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_TRADETYPE_" + tpcdi_scale + "_TSK SUSPEND"});
  stoptask_stmt.execute();
  return "All tradetype files have been loaded.";
  $$
;
CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_TRADE_H_SP(scale float)
	returns float
	language javascript
	as
	$$
	var tpcdi_scale = SCALE
	// Load historical file
	var hist_stmt = snowflake.createStatement(
		{sqlText: "COPY INTO TPCDI_STG.PUBLIC.TRADE_STG(T_ID,T_DTS,T_ST_ID,T_TT_ID,T_IS_CASH,T_S_SYMB,T_QTY,T_BID_PRICE,T_CA_ID,T_EXEC_NAME,T_TRADE_PRICE,T_CHRG,T_COMM,T_TAX) FROM (SELECT $1 $3 	,$2 $4 	,$3 $5 	,$4 $6 	,$5 $7 	,$6 $8 	,$7 $9 	,$8 $10 	,$9 $11 	,$10 $12 	,$11 $13 	,$12 $14 	,$13 $15 	,$14 $16 FROM @TPCDI_FILES) FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') PATTERN='.*//tpcdi/sf=" + tpcdi_scale + "/Batch1/Trade[^HT].*' ON_ERROR = SKIP_FILE"}
		);
	hist_stmt.execute();
	// Load trade history
	var hist2_stmt = snowflake.createStatement(
		{sqlText: "COPY INTO TPCDI_STG.PUBLIC.TRADEHISTORY_STG FROM @TPCDI_FILES/tmp/tpcdi/sf=" + tpcdi_scale + "/Batch1/TradeHistory FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') ON_ERROR = SKIP_FILE"}
		);
	hist2_stmt.execute();	
	// Stop task
	var stoptask_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_TRADE_H_" + tpcdi_scale + "_TSK SUSPEND"});
	stoptask_stmt.execute();
	return "All historical trade and tradehistory files have been loaded.";
	$$
;
CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_TRADE_I_SP(scale float,batches float,wait float)
	returns float
	language javascript
	as
	$$
	var tpcdi_scale = SCALE
	// Load incremental files	
	var batch_counter = 1;
	while (batch_counter <= BATCHES)	
	{
		var incrm_stmt = snowflake.createStatement(
			{sqlText: "COPY INTO TPCDI_STG.PUBLIC.TRADE_STG FROM @TPCDI_FILES/tmp/tpcdi/sf=" + tpcdi_scale + "/Batch" + batch_counter + "/Trade FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') ON_ERROR = SKIP_FILE"}
			);
		incrm_stmt.execute();
		// insert wait here
		stmt = snowflake.createStatement({sqlText:"call system$wait(" + WAIT + ", 'SECONDS')"});
		rs = stmt.execute();	
		batch_counter++
	}
	// Suspend Load Task
	var task_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_TRADE_I_" + tpcdi_scale + "_TSK SUSPEND"});
	task_stmt.execute();
	return "All incremental trade files have been loaded.";
	$$
;
CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_TRADE_SP(files float,wait float)
	returns float
	language javascript
	as
	$$
	// Load historical file
	var hist_stmt = snowflake.createStatement(
		{sqlText: "INSERT INTO TPCDI_STG.PUBLIC.TRADE_STG SELECT 	 TO_CHAR(NULL) $1 	,ROW_NUMBER() OVER (ORDER BY 1) $2 	,$1 $3 	,$2 $4 	,$3 $5 	,$4 $6 	,$5 $7 	,$6 $8 	,$7 $9 	,$8 $10 	,$9 $11 	,$10 $12 	,$11 $13 	,$12 $14 	,$13 $15 	,$14 $16 FROM @TPCDI_FILES/load/trade/Trade01.txt (FILE_FORMAT => 'TXT_PIPE')"}
		);
	hist_stmt.execute();
	// Load trade history
	var hist2_stmt = snowflake.createStatement(
		{sqlText: "COPY INTO TPCDI_STG.PUBLIC.TRADEHISTORY_STG 		FROM @TPCDI_FILES/load/trade_history/TradeHistory01.txt 		FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') ON_ERROR = SKIP_FILE"}
		);
	hist2_stmt.execute();	
	// Insert wait here
	stmt = snowflake.createStatement({sqlText:"call system$wait(" + WAIT + ", 'SECONDS')"});
	rs = stmt.execute();
	// Load incremental files	
	var file_counter = 2;
	while (file_counter <= FILES)
	{
		var incrm_stmt = snowflake.createStatement(
			{sqlText: "COPY INTO TPCDI_STG.PUBLIC.TRADE_STG FROM @TPCDI_FILES/load/trade/ FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') PATTERN='.*0" + file_counter + ".txt' ON_ERROR = SKIP_FILE"}
			);
		incrm_stmt.execute();
		// insert wait here
		stmt = snowflake.createStatement({sqlText:"call system$wait(" + WAIT + ", 'SECONDS')"});
		rs = stmt.execute();	
		file_counter++
	}
	return "All trade and tradehistory files have been loaded.";
	$$
;
CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_WATCH_HISTORY_H_SP(scale float)
	returns float
	language javascript
	as
	$$
	var tpcdi_scale = SCALE
	// Load historical file
	var hist_stmt = snowflake.createStatement(
		{sqlText: "COPY INTO TPCDI_STG.PUBLIC.WATCH_HISTORY_STG(W_C_ID,W_S_SYMB,W_DTS,W_ACTION) FROM (SELECT $1 $3 	,$2 $4 	,$3 $5 	,$4 $6 FROM @TPCDI_FILES/tmp/tpcdi/sf=" + tpcdi_scale + "/Batch1/WatchHistory) FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') ON_ERROR = SKIP_FILE"}
		);
	hist_stmt.execute();
	// Stop task
	var stoptask_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_WATCH_HISTORY_H_" + tpcdi_scale + "_TSK SUSPEND"});
	stoptask_stmt.execute();
	return "All historical watch_history files have been loaded.";
	$$
;
CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_WATCH_HISTORY_I_SP(scale float,batches float,wait float)
	returns float
	language javascript
	as
	$$
	var tpcdi_scale = SCALE
	// Load incremental files	
	var batch_counter = 1;
	while (batch_counter <= BATCHES)	
	{
		var incrm_stmt = snowflake.createStatement(
			{sqlText: "COPY INTO TPCDI_STG.PUBLIC.WATCH_HISTORY_STG FROM @TPCDI_FILES/tmp/tpcdi/sf=" + tpcdi_scale + "/Batch" + batch_counter + "/WatchHistory FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') ON_ERROR = SKIP_FILE"}
			);
		incrm_stmt.execute();
		// insert wait here
		stmt = snowflake.createStatement({sqlText:"call system$wait(" + WAIT + ", 'SECONDS')"});
		rs = stmt.execute();	
		batch_counter++
	}
	// Suspend Load Task
	var task_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_WATCH_HISTORY_I_" + tpcdi_scale + "_TSK SUSPEND"});
	task_stmt.execute();
	return "All incremental watch_history files have been loaded.";
	$$
;
CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_WATCH_HISTORY_SP(files float,wait float)
	returns float
	language javascript
	as
	$$
	// Load historical file
	var hist_stmt = snowflake.createStatement(
		{sqlText: "INSERT INTO TPCDI_STG.PUBLIC.WATCH_HISTORY_STG SELECT 	 TO_CHAR(NULL) $1 	,ROW_NUMBER() OVER (ORDER BY 1) $2 	,$1 $3 	,$2 $4 	,$3 $5 	,$4 $6 FROM @TPCDI_FILES/load/watch_history/WatchHistory01.txt (FILE_FORMAT => 'TXT_PIPE')"}
		);
	hist_stmt.execute();
	// Insert wait here
	stmt = snowflake.createStatement({sqlText:"call system$wait(" + WAIT + ", 'SECONDS')"});
	rs = stmt.execute();	
	var file_counter = 2;
	while (file_counter <= FILES)
	{
		var incrm_stmt = snowflake.createStatement(
			{sqlText: "COPY INTO TPCDI_STG.PUBLIC.WATCH_HISTORY_STG FROM @TPCDI_FILES/load/watch_history/ FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') PATTERN='.*0" + file_counter + ".txt' ON_ERROR = SKIP_FILE"}
			);
		incrm_stmt.execute();
		// insert wait here
		stmt = snowflake.createStatement({sqlText:"call system$wait(" + WAIT + ", 'SECONDS')"});
		rs = stmt.execute();	
		file_counter++
	}
	return "All watch_history files have been loaded.";
	$$
;

CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.START_LOAD_HISTORICAL_TASKS_SP(scale float)
  returns string
  language javascript
  as
  $$
  // Purpose: resume all tasks associated with loading historical files into the tpcdi_stg database
  // tpcdi_scale is an input variable that represents the TPC-DI data size; 5,10,100,1000 are possible values
  var tpcdi_scale = SCALE
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_STG.PUBLIC.LOAD_DATE_" + tpcdi_scale + "_TSK"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_STG.PUBLIC.LOAD_TIME_" + tpcdi_scale + "_TSK"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_STG.PUBLIC.LOAD_TRADETYPE_" + tpcdi_scale + "_TSK"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_STG.PUBLIC.LOAD_STATUSTYPE_" + tpcdi_scale + "_TSK"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_STG.PUBLIC.LOAD_TAXRATE_" + tpcdi_scale + "_TSK"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_STG.PUBLIC.LOAD_INDUSTRY_" + tpcdi_scale + "_TSK"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_STG.PUBLIC.LOAD_HR_" + tpcdi_scale + "_TSK"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_STG.PUBLIC.LOAD_CUSTOMER_MGMT_" + tpcdi_scale + "_TSK"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_STG.PUBLIC.LOAD_FINWIRE_" + tpcdi_scale + "_TSK"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText:"call system$wait(10, 'SECONDS')"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_STG.PUBLIC.LOAD_CASHTRANSACTION_H_" + tpcdi_scale + "_TSK"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_STG.PUBLIC.LOAD_DAILYMARKET_H_" + tpcdi_scale + "_TSK"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_STG.PUBLIC.LOAD_HOLDING_HISTORY_H_" + tpcdi_scale + "_TSK"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_STG.PUBLIC.LOAD_TRADE_H_" + tpcdi_scale + "_TSK"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_STG.PUBLIC.LOAD_WATCH_HISTORY_H_" + tpcdi_scale + "_TSK"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_STG.PUBLIC.LOAD_PROSPECT_H_" + tpcdi_scale + "_TSK "});
  stmt.execute();
  return "All historical load tasks started.";
  $$
;

CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.START_LOAD_INCREMENTAL_TASKS_SP(scale float)
  returns string
  language javascript
  as
  $$
  // Purpose:  all tasks associated with loading historical files into the tpcdi_stg database
  // tpcdi_scale is an input variable that represents the TPC-DI data size; 5,10,100,1000 are possible values
  var tpcdi_scale = SCALE
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_STG.PUBLIC.LOAD_CUSTOMER_I_" + tpcdi_scale + "_TSK "});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_STG.PUBLIC.LOAD_ACCOUNT_I_" + tpcdi_scale + "_TSK "});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_STG.PUBLIC.LOAD_CASHTRANSACTION_I_" + tpcdi_scale + "_TSK "});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_STG.PUBLIC.LOAD_DAILYMARKET_I_" + tpcdi_scale + "_TSK "});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_STG.PUBLIC.LOAD_HOLDING_HISTORY_I_" + tpcdi_scale + "_TSK "});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_STG.PUBLIC.LOAD_TRADE_I_" + tpcdi_scale + "_TSK "});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_STG.PUBLIC.LOAD_WATCH_HISTORY_I_" + tpcdi_scale + "_TSK "});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_STG.PUBLIC.LOAD_PROSPECT_I_" + tpcdi_scale + "_TSK "});
  stmt.execute();
  return "All incremental load tasks started.";
  $$
;