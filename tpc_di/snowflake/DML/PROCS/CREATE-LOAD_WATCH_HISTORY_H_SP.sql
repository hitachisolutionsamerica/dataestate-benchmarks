CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_WATCH_HISTORY_H_SP(scale float)
	returns float
	language javascript
	as
	$$
	var tpcdi_scale = SCALE
	// Load historical file
	var hist_stmt = snowflake.createStatement(
		{sqlText: "COPY INTO TPCDI_STG.PUBLIC.WATCH_HISTORY_STG(W_C_ID,W_S_SYMB,W_DTS,W_ACTION) FROM (SELECT $1 $3 	,$2 $4 	,$3 $5 	,$4 $6 FROM @TPCDI_FILES/tpcdi-" + tpcdi_scale + "/Batch1/WatchHistory) FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE')"}
		);
	hist_stmt.execute();
	// Stop task
	var stoptask_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_WATCH_HISTORY_H_" + tpcdi_scale + "_TSK SUSPEND"});
	stoptask_stmt.execute();
	return "All historical watch_history files have been loaded.";
	$$
;
