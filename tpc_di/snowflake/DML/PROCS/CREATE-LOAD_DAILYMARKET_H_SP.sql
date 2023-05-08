CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_DAILYMARKET_H_SP(scale float)
	returns float
	language javascript
	as
	$$
	var tpcdi_scale = SCALE
	// Load historical file
	var hist_stmt = snowflake.createStatement(
		{sqlText: "COPY INTO TPCDI_STG.PUBLIC.DAILYMARKET_STG(DM_DATE,DM_S_SYMB,DM_CLOSE,DM_HIGH,DM_LOW,DM_VOL) FROM (SELECT $1 $3	,$2 $4	,$3 $5	,$4 $6	,$5 $7	,$6 $8 FROM @TPCDI_FILES/tpcdi-" + tpcdi_scale + "/Batch1/DailyMarket) FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE')"}
		);
	hist_stmt.execute();
	// Stop task
	var stoptask_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_DAILYMARKET_H_" + tpcdi_scale + "_TSK SUSPEND"});
	stoptask_stmt.execute();
	return "All historical dailymarket files have been loaded.";
	$$
;
