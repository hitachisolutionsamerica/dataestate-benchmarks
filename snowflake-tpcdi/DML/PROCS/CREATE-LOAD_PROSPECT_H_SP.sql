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
