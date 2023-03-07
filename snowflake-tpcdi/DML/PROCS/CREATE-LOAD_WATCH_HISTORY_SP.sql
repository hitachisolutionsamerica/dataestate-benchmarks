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
			{sqlText: "COPY INTO TPCDI_STG.PUBLIC.WATCH_HISTORY_STG FROM @TPCDI_FILES/load/watch_history/ FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') PATTERN='.*0" + file_counter + ".txt'"}
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
