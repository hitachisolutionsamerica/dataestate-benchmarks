CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_TRADE_H_SP(scale float)
	returns float
	language javascript
	as
	$$
	var tpcdi_scale = SCALE
	// Load historical file
	var hist_stmt = snowflake.createStatement(
		{sqlText: "COPY INTO TPCDI_STG.PUBLIC.TRADE_STG(T_ID,T_DTS,T_ST_ID,T_TT_ID,T_IS_CASH,T_S_SYMB,T_QTY,T_BID_PRICE,T_CA_ID,T_EXEC_NAME,T_TRADE_PRICE,T_CHRG,T_COMM,T_TAX) FROM (SELECT $1 $3 	,$2 $4 	,$3 $5 	,$4 $6 	,$5 $7 	,$6 $8 	,$7 $9 	,$8 $10 	,$9 $11 	,$10 $12 	,$11 $13 	,$12 $14 	,$13 $15 	,$14 $16 FROM @TPCDI_FILES) FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') PATTERN='.*/tpcdi-" + tpcdi_scale + "/Batch1/Trade[^HT].*'"}
		);
	hist_stmt.execute();
	// Load trade history
	var hist2_stmt = snowflake.createStatement(
		{sqlText: "COPY INTO TPCDI_STG.PUBLIC.TRADEHISTORY_STG FROM @TPCDI_FILES/tpcdi-" + tpcdi_scale + "/Batch1/TradeHistory FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE')"}
		);
	hist2_stmt.execute();	
	// Stop task
	var stoptask_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_TRADE_H_" + tpcdi_scale + "_TSK SUSPEND"});
	stoptask_stmt.execute();
	return "All historical trade and tradehistory files have been loaded.";
	$$
;
