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
		{sqlText: "COPY INTO TPCDI_STG.PUBLIC.TRADEHISTORY_STG 		FROM @TPCDI_FILES/load/trade_history/TradeHistory01.txt 		FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE')"}
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
			{sqlText: "COPY INTO TPCDI_STG.PUBLIC.TRADE_STG FROM @TPCDI_FILES/load/trade/ FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') PATTERN='.*0" + file_counter + ".txt'"}
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
