CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_CASHTRANSACTION_I_SP(scale float,batches float,wait float)
	returns float
	language javascript
	as
	$$
	var tpcdi_scale = SCALE
	// Load incremental files	
	var batch_counter = 2;
	while (batch_counter <= BATCHES)	
	{
		var incrm_stmt = snowflake.createStatement(
			{sqlText: "COPY INTO TPCDI_STG.PUBLIC.CASHTRANSACTION_STG FROM @TPCDI_FILES/tpcdi-" + tpcdi_scale + "/Batch" + batch_counter + "/CashTransaction FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE')"}
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

