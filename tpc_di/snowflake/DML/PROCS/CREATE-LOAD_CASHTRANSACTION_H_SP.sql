CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_CASHTRANSACTION_H_SP(scale float)
	returns float
	language javascript
	as
	$$
	var tpcdi_scale = SCALE
	// Load historical file
	var hist_stmt = snowflake.createStatement(
		{sqlText: "COPY INTO TPCDI_STG.PUBLIC.CASHTRANSACTION_STG(CT_CA_ID,CT_DTS,CT_AMT,CT_NAME) FROM (SELECT $1 $3 	,$2 $4 	,$3 $5 	,$4 $6 FROM @TPCDI_FILES/tpcdi-" + tpcdi_scale + "/Batch1/CashTransaction) FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE')"}
		);
	hist_stmt.execute();
	// Stop task
	var stoptask_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_CASHTRANSACTION_H_" + tpcdi_scale + "_TSK SUSPEND"});
	stoptask_stmt.execute();
	return "All historical cashtransaction files have been loaded.";
	$$
;

