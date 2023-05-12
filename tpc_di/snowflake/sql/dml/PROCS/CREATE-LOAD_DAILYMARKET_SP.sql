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
			{sqlText: "COPY INTO TPCDI_STG.PUBLIC.DAILYMARKET_STG FROM @TPCDI_FILES/load/daily_market/ FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') PATTERN='.*0" + file_counter + ".txt'"}
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
