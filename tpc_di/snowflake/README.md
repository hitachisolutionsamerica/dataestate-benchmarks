# TPC-DI Benchmarks on Snowflake Warehouses
This code is an implementation of the TPC-DI Benchmark that is commonly run a s benchamrk of warehouse performance. You can find more information in the following link : [TPC-DI](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-di_v1.1.0.pdf)

# Requirements
This benchmark requires the following to be available as pre-requisitis
1. Bash Shell
2. TPC-DI data outputed into a stage that is accessible by Snowflake. Please follow data generation instructions from :
   1. Tool : [TPC-DI Java Tool](https://www.tpc.org/tpc_documents_current_versions/download_programs/tools-download-request5.asp?bm_type=TPC-DI&bm_vers=1.1.0&mode=CURRENT-ONLY)
   2. Whitepaper : [Introducing TPC-DI](http://www.vldb.org/pvldb/vol7/p1367-poess.pdf)
3. Snowflake account

# Setup
1. Run `./main.sh` in your bash shell. Follow instructions to set the correct scale factor
2. Two sql scripts will be generated from (1). Run `00_setup.sql` on Snowflake to setup the TCP-DI benchmark
3. Create a snowflake external stage that points to a TPC-DI dataset as TPCDI_STG.PUBLIC.@TPCDI_FILES.
   1. Ensure that your TPC-DI files are located in a `/tmp/tcpdi` prefix.
      1. Example : `TPCDI_STG.PUBLIC.@TPCDI_FILES/tmp/tpcdi/sf=100/batch1/...`

# Executing the Benchmark
1. Run `01_execute.sql` to start the TPC-DI benchmark

# Troubleshooting
1. Error : Failure to run `01_execute.sql` due to missing stage
   1. This is a common issue due to not setting up the Snowflake [external stage properly](#setup)