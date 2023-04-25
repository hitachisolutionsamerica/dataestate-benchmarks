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
