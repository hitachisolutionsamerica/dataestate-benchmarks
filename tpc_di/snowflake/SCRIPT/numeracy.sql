-- Batch query from audit
with query as (select 'BATCH '||batch_id::string batch, sum(inserted_rows) as "Inserted Rows", sum(updated_rows) "Updated Rows" from tpcdi_wh.public.audit where BATCH_ID = 1 group by batch_id)
select * from query unpivot(records for dml_type in ("Inserted Rows", "Updated Rows"));

-- Current batch
select coalesce(max(BATCH_ID),0) as current_batch from tpcdi_wh.public.ctrl_batch;

-- Active tasks
select count(distinct name) as active_task_count
from table(information_schema.task_history(scheduled_time_range_start=>dateadd('min',-1,current_timestamp())));
	
-- Rows loaded all schemas 
SELECT 
	COALESCE(SUM(table_row_counts."ROW_COUNT" ), 0) AS "table_row_counts.row_count"
FROM PUBLIC.TABLE_ROW_COUNTS  AS table_row_counts;

-- Small tables
SELECT 
	table_row_counts."TABLE_NAME"  AS "table_row_counts.table_name",
	COALESCE(SUM(table_row_counts."ROW_COUNT" ), 0) AS "table_row_counts.row_count"
FROM TPCDI_WH.INFORMATION_SCHEMA.TABLES  AS table_row_counts
WHERE (((table_row_counts."TABLE_CATALOG") = 'TPCDI_WH')) AND ((table_row_counts."TABLE_NAME"  IN ('DIM_ACCOUNT', 'DIM_BROKER', 'DIM_COMPANY', 'DIM_CUSTOMER', 'DIM_DATE', 'DIM_INDUSTRY', 'DIM_SECURITY', 'DIM_STATUS_TYPE', 'DIM_TAX_RATE', 'DIM_TRADE_TYPE', 'DIM_TIME')))
GROUP BY 1
ORDER BY 1 ;

-- Large TABLES
SELECT 
	table_row_counts."TABLE_NAME"  AS "table_row_counts.table_name",
	COALESCE(SUM(table_row_counts."ROW_COUNT" ), 0) AS "table_row_counts.row_count"
FROM TPCDI_WH.INFORMATION_SCHEMA.TABLES  AS table_row_counts
WHERE (((table_row_counts."TABLE_CATALOG") = 'TPCDI_WH')) AND ((table_row_counts."TABLE_NAME"  IN ('DIM_FINANCIAL', 'DIM_TRADE', 'FACT_CASH_BALANCES', 'FACT_HOLDINGS', 'FACT_MARKET_HISTORY', 'FACT_PROSPECT', 'FACT_WATCHES')))
GROUP BY 1
ORDER BY 1 

-- Snapshot
SELECT 
	load_snapshot."SNAPSHOT_TIME" AS "load_snapshot.snapshot_time",
	COALESCE(SUM(load_snapshot."ROW_COUNT" ), 0) AS "load_snapshot.row_count"
FROM PUBLIC.LOAD_SNAPSHOT  AS load_snapshot
WHERE 
	((load_snapshot."DATABASE") = 'TPCDI_WH')
GROUP BY 1
ORDER BY 1;

-- WH Rows Inserted
select coalesce(sum(INSERTED_ROWS),0) from TPCDI_WH.PUBLIC.AUDIT;

-- WH Rows Updated 
select coalesce(sum(UPDATED_ROWS),0) from TPCDI_WH.PUBLIC.AUDIT;

-- Audit log
select * from TPCDI_WH.PUBLIC.AUDIT order by LOG_TIME desc;