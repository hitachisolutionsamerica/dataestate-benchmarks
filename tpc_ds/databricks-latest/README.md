# Presenting and Executing TPC-DS (for customers) on Databricks


### Before you begin to think about running TPC-DS

Customers look at standard benchmarks, such as TPC-DS, to determine if
it\'s a worthy system. Share the 100TB [TPC-DS benchmark](https://databricks.com/blog/2021/11/02/databricks-sets-official-data-warehousing-performance-record.html)
with your customer to convey the message that Databricks is a pretty
efficient and scalable SQL engine. After sharing the 100TB TPC-DS
benchmark, and your customer still wants to run the TPC-DS benchmark on
Databricks themselves, this document describes how you can do it.

### Introduction

There are 3 steps: create the dataset, run all benchmark queries and
interpret the results.

#### Step 1 - Creating the Dataset

Use the [data generator](https://e2-demo-west.cloud.databricks.com/?o=2556758628403379#notebook/3533331031122525/command/3533331031122526) to generate TPC-DS, with delta z-ordering and data management best practices

1.  Export the notebook and import it in the customer env and then follow the notebook's instructions.

2.  Benchmark with 10GB scale or above. 1GB scale factor can be used to verify that notebook works.

#### Step 2 - Running the benchmark

##### **2.a Compute Resource Setup for running the benchmarks**

Non-serverless DBSQL with scaling set to 1 (i.e. min=max=1) and use
"reliability optimized" is the desired setup. Follow the table below for
sizing. If you have to use serverless SQL, or a cluster size larger than
Large, confirm that the cluster has acquired all the workers by doing
the following:

1.  Execute a simple query such as "Select count(\*) from tpcds_sf10_delta.store_sales".

2.  Go to query history, and click on the query. On the upper right-hand corner, click the three dots and "open in Spark UI".

3.  Click on Executors on the Spark UI to confirm the number of workers.

##### **DB SQL sizing to TPC-DS scale factors**

<table data-number-column="false"><colgroup><col style="width: 225.67px;"><col style="width: 225.67px;"><col style="width: 225.67px;"></colgroup><tbody><tr><th rowspan="1" colspan="1" colorname="" class="ak-renderer-tableHeader-sortable-column__wrapper" data-colwidth="226.67" aria-sort="none"><div class="ak-renderer-tableHeader-sortable-column"><p data-renderer-start-pos="1752"><strong data-renderer-mark="true">Scale factor (GB)</strong></p><figure class="ak-renderer-tableHeader-sorting-icon__wrapper ak-renderer-tableHeader-sorting-icon__no-order"><div role="presentation"><div class="ak-renderer-tableHeader-sorting-icon css-qrn7se" role="button" tabindex="0" aria-label="sort column" aria-disabled="false"><div class="sorting-icon-svg__no_order ak-renderer-tableHeader-sorting-icon-inactive css-37qivc"><div class="css-1h55k8m"></div></div></div></div></figure></div></th><th rowspan="1" colspan="1" colorname="" class="ak-renderer-tableHeader-sortable-column__wrapper" data-colwidth="226.67" aria-sort="none"><div class="ak-renderer-tableHeader-sortable-column"><p data-renderer-start-pos="1773"><strong data-renderer-mark="true"><span id="15581c9b-5776-44a0-99e2-7f3dcf9fb202" data-renderer-mark="true" data-mark-type="annotation" data-mark-annotation-type="inlineComment" data-id="15581c9b-5776-44a0-99e2-7f3dcf9fb202">DB SQL size</span></strong></p><figure class="ak-renderer-tableHeader-sorting-icon__wrapper ak-renderer-tableHeader-sorting-icon__no-order"><div role="presentation"><div class="ak-renderer-tableHeader-sorting-icon css-qrn7se" role="button" tabindex="0" aria-label="sort column" aria-disabled="false"><div class="sorting-icon-svg__no_order ak-renderer-tableHeader-sorting-icon-inactive css-37qivc"><div class="css-1h55k8m"></div></div></div></div></figure></div></th><th rowspan="1" colspan="1" colorname="" class="ak-renderer-tableHeader-sortable-column__wrapper" data-colwidth="226.67" aria-sort="none"><div class="ak-renderer-tableHeader-sortable-column"><p data-renderer-start-pos="1788"><strong data-renderer-mark="true">Notes</strong></p><figure class="ak-renderer-tableHeader-sorting-icon__wrapper ak-renderer-tableHeader-sorting-icon__no-order"><div role="presentation"><div class="ak-renderer-tableHeader-sorting-icon css-qrn7se" role="button" tabindex="0" aria-label="sort column" aria-disabled="false"><div class="sorting-icon-svg__no_order ak-renderer-tableHeader-sorting-icon-inactive css-37qivc"><div class="css-1h55k8m"></div></div></div></div></figure></div></th></tr><tr><td rowspan="1" colspan="1" colorname="" data-colwidth="226.67"><p data-renderer-start-pos="1799">10 to 1,000 (1TB)</p></td><td rowspan="1" colspan="1" colorname="" data-colwidth="226.67"><p data-renderer-start-pos="1820">Medium</p></td><td rowspan="1" colspan="1" colorname="" data-colwidth="226.67"><p data-renderer-start-pos="1830"><a data-testid="link-with-safety" href="https://docs.google.com/presentation/d/1n8oILILhQrK46YBEGeM7PK9UyW_kcssPIkKE7wl7aqs/edit#slide=id.g14082412f71_0_36" title="https://docs.google.com/presentation/d/1n8oILILhQrK46YBEGeM7PK9UyW_kcssPIkKE7wl7aqs/edit#slide=id.g14082412f71_0_36" data-renderer-mark="true" class="css-tgpl01">Results 10 to 300</a>; scale factor t and above 1000 (1TB), use concurrency of 1. Below 1000 scale factor, use concurrency of 8</p></td></tr><tr><td rowspan="1" colspan="1" colorname="" data-colwidth="226.67"><p data-renderer-start-pos="1959"> 3,000 (3TB)</p></td><td rowspan="1" colspan="1" colorname="" data-colwidth="226.67"><p data-renderer-start-pos="1975">Large</p></td><td rowspan="1" colspan="1" colorname="" data-colwidth="226.67"><p data-renderer-start-pos="1984">&nbsp;</p></td></tr><tr><td rowspan="1" colspan="1" colorname="" data-colwidth="226.67"><p data-renderer-start-pos="1990">10,000 (10TB)</p></td><td rowspan="1" colspan="1" colorname="" data-colwidth="226.67"><p data-renderer-start-pos="2007">X-Large</p></td><td rowspan="1" colspan="1" colorname="" data-colwidth="226.67"><p data-renderer-start-pos="2018">&nbsp;</p></td></tr><tr><td rowspan="1" colspan="1" colorname="" data-colwidth="226.67"><p data-renderer-start-pos="2024">30,000 (30TB)</p></td><td rowspan="1" colspan="1" colorname="" data-colwidth="226.67"><p data-renderer-start-pos="2041"><span id="10ef4dda-2a57-4204-a40c-92a200f02f80" data-renderer-mark="true" data-mark-type="annotation" data-mark-annotation-type="inlineComment" data-id="10ef4dda-2a57-4204-a40c-92a200f02f80">2 or 3 X-Large</span></p></td><td rowspan="1" colspan="1" colorname="" data-colwidth="226.67"><p data-renderer-start-pos="2059">&nbsp;</p></td></tr><tr><td rowspan="1" colspan="1" colorname="" data-colwidth="226.67"><p data-renderer-start-pos="2065">100,000 (100TB)</p></td><td rowspan="1" colspan="1" colorname="" data-colwidth="226.67"><p data-renderer-start-pos="2084">3 or 4 X-Large</p></td><td rowspan="1" colspan="1" colorname="" data-colwidth="226.67"><p data-renderer-start-pos="2102">Should avoid 100TB due to costs and long running times.  <a data-testid="link-with-safety" href="https://databricks.com/blog/2021/11/02/databricks-sets-official-data-warehousing-performance-record.html" title="https://databricks.com/blog/2021/11/02/databricks-sets-official-data-warehousing-performance-record.html" data-renderer-mark="true" class="css-tgpl01">Refer to the audited result</a>. </p></td></tr></tbody></table>

-   When not using DB SQL, it is recommended to replicate the hardware spec of DBSQL for all clouds.

#### **2.b Executing the queries**

**Query Execution Script**

Use the multi-threaded Java based [query runner](https://drive.google.com/file/d/1YeF6jfpLyd_lJ8t25UFJ5e5pD2Ovrbd5/view?usp=sharing), along with all 99 TPC-DS queries, to run the benchmark. You will need Java 18 (or above) installed on the customer's machine. Run it from a VM within the same cloud region if possible. Running from a local laptop might be \~5% slower due to network latency.

Here's how to run it. You need four input parameters:

1.  uri - The SQL Endpoint URI (and you can access it). Use jdbc:databricks, not jdbc:simba.

2.  token - Personal Access Token (and you have READ permission the TPC-DS database specified)

3.  concurrency - How many threads/streams of TPC-DS queries are running in parallel. 8 is the recommended number.

4.  schema - The database/schema that contains the TPC-DS dataset

Syntax: `./run.sh \"\<URI\>\" \<token\> \<concurrency\> \<schema\>`

Example:
```
./run.sh "jdbc:databricks://e2-demo-west.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/endpoints/29c44a462d0d7d82;\" YOUR_DATABRICKS_TOKEN 1 tpcds_sf1_delta_nopartitions
```

#### **Execution Procedure**

1.  Star the cluster and run it once. Discard the result, or mark it as cold. It's expected to be slower than a warm run.

2.  Run 3 more iterations. This is the warm run and takes the average of the ***Query Throughput*** of the 3. The stddev of the 3 runs should be no more than 8%.

#### Step 3 - Interpreting the Results

It's important to note that the most critical aspect is ***Query Throughput*** (i.e. query per minute or seconds, or hours). It measures how efficient the system is. That's what the ./run.sh from the above reports.

To interpret the Total Cost Ownership (TCO) of the workload, open the [Cost Per Workload Calculator sheet](https://docs.google.com/spreadsheets/d/1ud7Ob2WY7q1wXKAdm2w8Hqr4Z9WbeYNiDcCk_7_5xUs/edit#gid=0). The `./run.sh` also reports the total latency, and you will use it for the "Benchmark Run Duration" in the sheet.

Individual query latency under concurrency run might vary significantly due to system resource usage. Therefore, it's not a good measure to compare against other systems in terms of system performance.
