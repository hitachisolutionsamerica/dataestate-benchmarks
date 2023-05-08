# Databricks notebook source
# MAGIC %md
# MAGIC #TPCx-AI Data Generation
# MAGIC 
# MAGIC This notebook will call the TPCx-AI data generation scripts and convert the data to delta tables for use in the benchmarking.
# MAGIC 
# MAGIC **Setup**:
# MAGIC * You have access to the [TPCx-AI code](https://www.tpc.org/tpc_documents_current_versions/download_programs/tools-download-request5.asp?bm_type=TPCX-AI&bm_vers=1.0.2&mode=CURRENT-ONLY).
# MAGIC * Upload the zip file to DBFS and unzip it.
# MAGIC * Set the path to the root folder in the widget above. Note the path should start with dbfs:/... The bash scripts below will reference the fuse mount as needed.
# MAGIC * The data size controls what data scale factor to generate. 1 for 1 GB, 10 for 10 GB, etc.

# COMMAND ----------

dbutils.widgets.text("tpcxai_path", "")
dbutils.widgets.dropdown("data_size", "1", [str(10**n) for n in range(4)])

tpcxai_path = dbutils.widgets.get("tpcxai_path")
tpcxai_size = dbutils.widgets.get("data_size")
os.environ["TPCXAI_PATH"] = tpcxai_path.replace("dbfs:/", "/dbfs/")
os.environ["TPCXAI_SIZE"] = tpcxai_size

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Generation

# COMMAND ----------

# MAGIC %sh
# MAGIC # Copy TPCx-AI scripts to the driver node. Exclude any already existing data to be fast.
# MAGIC echo "Copying TPCx-AI to Driver"
# MAGIC rsync -a --exclude '1_GB' --exclude '10_GB' --exclude '100_GB' --exclude '1000_GB' ${TPCXAI_PATH} /databricks/tpcx-ai/

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /databricks/tpcx-ai/
# MAGIC source setenv.sh
# MAGIC echo -e "\nYES\n" | ./bin/tpcxai.sh -c config/default.yaml -uc 2 5 8 9 -sf ${TPCXAI_SIZE} --phase DATA_GENERATION 

# COMMAND ----------

# MAGIC %sh
# MAGIC # Copy data back, but use a separate folder for each data size.
# MAGIC rsync -a /databricks/tpcx-ai/output/ ${TPCXAI_PATH}/${TPCXAI_SIZE}_GB/output/

# COMMAND ----------

# MAGIC %md
# MAGIC ###Convert CSV to Delta

# COMMAND ----------

def convert_csv(filepath, sep, optimize=True):
    print(f"Loading file {filepath}")
    data = spark.read.csv(filepath, sep=sep, inferSchema=True, header=True, quote='"')

    outpath = filepath.replace("output/raw_data", "processed").replace(".csv", "")
    print(f"Saving table to {outpath}")
    data.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        outpath
    )

    if optimize:
        spark.sql(f'OPTIMIZE "{outpath}"')


def make_sequence(filepath):
    print(f"Creating sequence from {filepath}")
    files = spark.sparkContext.binaryFiles(filepath)

    outpath = filepath.replace("output/raw_data", f"processed")
    outpath = "/".join(outpath.split("/")[:-2]) + ".seq"

    if os.path.exists(outpath):
        dbutils.fs.rm(outpath, True)

    print(f"Saving sequence to {outpath}")
    files.saveAsSequenceFile(outpath)

# COMMAND ----------

csv_files = [
    "CONVERSATION_AUDIO.csv",
    "marketplace.csv",
    "CUSTOMER_IMAGES_META.csv",
    "order.csv",
    "lineitem.csv",
    "product.csv",
]

# Annoyingly the CSVs for use case 2 and 5 use a "|" separator and 8 and 9 use ","
seps = ["|"] * 2 + [","] * 4

seq_pattern = ["CONVERSATION_AUDIO/wavs/*.wav", "CUSTOMER_IMAGES/*/*.png"]

for stage in ["training", "serving"]:
    for csv_file, sep in zip(csv_files, seps):
        filepath = f"{tpcxai_path}/{tpcxai_size}_GB/output/raw_data/{stage}/{csv_file}"
        convert_csv(filepath=filepath, sep=sep)

    for seq in seq_pattern:
        make_sequence(f"{tpcxai_path}/{tpcxai_size}_GB/output/raw_data/{stage}/{seq}")
