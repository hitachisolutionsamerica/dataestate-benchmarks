# snowflake-tpcdi
Code to mimic TPC-DI benchmark using Snowflake

Link to docs: [https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-di_v1.1.0.pdf]

Download tools: [https://www.tpc.org/tpc_documents_current_versions/download_programs/tools-download-request5.asp?bm_type=TPC-DI&bm_vers=1.1.0&mode=CURRENT-ONLY]

paper: [http://www.vldb.org/pvldb/vol7/p1367-poess.pdf]


Install Java 8
Do not install Java 17 or 19, will not work.  Complains of commons cli

snowpark directory has some scripts to copy the files to the stage in snowflake

plan is to build out snowpark code for the etl.  

Full warning, running this in snowflake will cause the warehouses to boot and stay on, regardless if it's processed the data. There is a bug in one of the procedures, and I accidentally left this thing on for a week and I came back to a 8k bill. Be careful. 
