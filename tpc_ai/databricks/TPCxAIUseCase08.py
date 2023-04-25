# Databricks notebook source
# MAGIC %md
# MAGIC # TPCx-AI Use Case 08
# MAGIC Classifying the type of a customer trip from purchase history.
# MAGIC 
# MAGIC **Setup**:
# MAGIC * You've already executed the TPCxAIDataGeneration notebook with the desired data size.

# COMMAND ----------

dbutils.widgets.text("tpcxai_path", "")
dbutils.widgets.dropdown("data_size", "1", [str(10**n) for n in range(4)])

tpcxai_path = dbutils.widgets.get("tpcxai_path")
tpcxai_size = dbutils.widgets.get("data_size")

# COMMAND ----------

import argparse
import os
import timeit

from itertools import chain

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    date_format,
    min,
    abs,
    sum,
    count,
    lit,
    create_map,
)
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler
from sparkdl.xgboost import XgboostClassifier

# COMMAND ----------

department_columns = [
    "FINANCIAL SERVICES",
    "SHOES",
    "PERSONAL CARE",
    "PAINT AND ACCESSORIES",
    "DSD GROCERY",
    "MEAT - FRESH & FROZEN",
    "DAIRY",
    "PETS AND SUPPLIES",
    "HOUSEHOLD CHEMICALS/SUPP",
    "IMPULSE MERCHANDISE",
    "PRODUCE",
    "CANDY, TOBACCO, COOKIES",
    "GROCERY DRY GOODS",
    "BOYS WEAR",
    "FABRICS AND CRAFTS",
    "JEWELRY AND SUNGLASSES",
    "MENS WEAR",
    "ACCESSORIES",
    "HOME MANAGEMENT",
    "FROZEN FOODS",
    "SERVICE DELI",
    "INFANT CONSUMABLE HARDLINES",
    "PRE PACKED DELI",
    "COOK AND DINE",
    "PHARMACY OTC",
    "LADIESWEAR",
    "COMM BREAD",
    "BAKERY",
    "HOUSEHOLD PAPER GOODS",
    "CELEBRATION",
    "HARDWARE",
    "BEAUTY",
    "AUTOMOTIVE",
    "BOOKS AND MAGAZINES",
    "SEAFOOD",
    "OFFICE SUPPLIES",
    "LAWN AND GARDEN",
    "SHEER HOSIERY",
    "WIRELESS",
    "BEDDING",
    "BATH AND SHOWER",
    "HORTICULTURE AND ACCESS",
    "HOME DECOR",
    "TOYS",
    "INFANT APPAREL",
    "LADIES SOCKS",
    "PLUS AND MATERNITY",
    "ELECTRONICS",
    "GIRLS WEAR, 4-6X  AND 7-14",
    "BRAS & SHAPEWEAR",
    "LIQUOR,WINE,BEER",
    "SLEEPWEAR/FOUNDATIONS",
    "CAMERAS AND SUPPLIES",
    "SPORTING GOODS",
    "PLAYERS AND ELECTRONICS",
    "PHARMACY RX",
    "MENSWEAR",
    "OPTICAL - FRAMES",
    "SWIMWEAR/OUTERWEAR",
    "OTHER DEPARTMENTS",
    "MEDIA AND GAMING",
    "FURNITURE",
    "OPTICAL - LENSES",
    "SEASONAL",
    "LARGE HOUSEHOLD GOODS",
    "1-HR PHOTO",
    "CONCEPT STORES",
    "HEALTH AND BEAUTY AIDS",
]

weekday_columns = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]

featureColumns = ["scan_count", "scan_count_abs"] + weekday_columns + department_columns

label_column = "trip_type"

# deleted label 14, since only 4 samples existed in the sample data set
label_range = [
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    12,
    15,
    18,
    19,
    20,
    21,
    22,
    23,
    24,
    25,
    26,
    27,
    28,
    29,
    30,
    31,
    32,
    33,
    34,
    35,
    36,
    37,
    38,
    39,
    40,
    41,
    42,
    43,
    44,
    999,
]
sorted_labels = sorted(label_range, key=str)
label_to_index = {k: v for v, k in enumerate(sorted_labels)}

# COMMAND ----------

def load_data(session: SparkSession, num_proc, path, stage):
    order_data = session.read.format("delta").load(f"{path}/{stage}/order")
    lineitem_data = session.read.format("delta").load(f"{path}/{stage}/lineitem")
    product_data = session.read.format("delta").load(f"{path}/{stage}/product")

    data = order_data.join(
        lineitem_data, order_data.o_order_id == lineitem_data.li_order_id, how="inner"
    )
    data = data.join(
        product_data, data.li_product_id == product_data.p_product_id, how="inner"
    )

    if label_column in data.columns:
        data = data.select("o_order_id", "date", "department", "quantity", "trip_type")
    else:
        data = data.select("o_order_id", "date", "department", "quantity")

    return data.repartition(num_proc).cache()


def pre_process(session: SparkSession, raw_data: DataFrame):
    has_labels = label_column in raw_data.columns

    raw_data = raw_data.withColumn("scan_count", col("quantity")).withColumn(
        "weekday", date_format(col("date"), "EEEE")
    )

    if has_labels:
        features_scan_count = raw_data.groupby("o_order_id").agg(
            sum("scan_count").alias("scan_count"),
            sum(abs("scan_count")).alias("scan_count_abs"),
            min("weekday").alias("weekday"),
            min("trip_type").alias("trip_type"),
        )
    else:
        features_scan_count = raw_data.groupby("o_order_id").agg(
            sum("scan_count").alias("scan_count"),
            sum(abs("scan_count")).alias("scan_count_abs"),
            min("weekday").alias("weekday"),
        )

    weekdays = (
        raw_data.groupby("o_order_id")
        .pivot("weekday")
        .agg(count(col("scan_count") > 0))
        .fillna(0.0)
    )

    missing_weekdays = (
        set(weekday_columns) - set(["o_order_id"]) - set(weekdays.columns)
    )
    for c in missing_weekdays:
        weekdays = weekdays.withColumn(c, lit(0.0))

    departments = (
        raw_data.groupby("o_order_id").pivot("department").agg(sum("scan_count"))
    )

    missing_cols = (
        set(department_columns) - set(["o_order_id"]) - set(departments.columns)
    )
    for c in missing_cols:
        departments = departments.withColumn(c, lit(0.0))

    final_data = (
        features_scan_count.drop("weekday")
        .join(weekdays, "o_order_id")
        .join(departments, "o_order_id")
        .fillna(0.0)
    )

    if label_column in final_data.columns:
        encode_label = create_map([lit(x) for x in chain(*label_to_index.items())])

        # remove tiny classes
        final_data = final_data.where(col("trip_type") != 14)
        final_data = final_data.withColumn("trip_type", encode_label[col("trip_type")])
        return final_data.select(featureColumns + ["trip_type"])
    else:
        return final_data.select(featureColumns)


def train(defaultParallelism, estimators, data):
    vec_assembler = VectorAssembler(inputCols=featureColumns, outputCol="features")

    params = {
        "n_estimators": estimators,
        "objective": "multi:softprob",
        "tree_method": "hist",
        "random_state": 42,
        "num_workers": defaultParallelism,
        "featuresCol": "features",
        "labelCol": label_column,
        "missing": 0,
    }

    xgboost = XgboostClassifier(**params)
    pipeline = Pipeline(stages=[vec_assembler, xgboost])
    model = pipeline.fit(data)
    return model


def serve(model, data):
    return model.transform(data)

# COMMAND ----------

num_processes = 16
num_rounds = 100

base_path = os.path.join(tpcxai_path, f"{tpcxai_size}_GB")
data_path = os.path.join(base_path, "processed")

work_dir = f"{base_path}/output/output/uc08"
model_file = f"{work_dir}/uc08.spark.model"

# COMMAND ----------

dbutils.fs.rm(work_dir, True)

# COMMAND ----------

# DBTITLE 1,Load Training Data
start = timeit.default_timer()
raw_data = load_data(spark, num_processes, data_path, "training")
end = timeit.default_timer()
load_time = end - start
print("load time:\t", load_time)

# COMMAND ----------

display(raw_data)

# COMMAND ----------

# DBTITLE 1,Process Data
start = timeit.default_timer()
data = pre_process(spark, raw_data)
end = timeit.default_timer()
load_time = end - start
print("pre-process time:\t", load_time)

# COMMAND ----------

display(data)

# COMMAND ----------

# DBTITLE 1,Training
start = timeit.default_timer()
model = train(num_processes, num_rounds, data)
end = timeit.default_timer()
train_time = end - start
print("train time:\t", train_time)

# COMMAND ----------

# DBTITLE 1,Save Model
model.write().overwrite().save(model_file)

# COMMAND ----------

# DBTITLE 1,Serving
raw_data = load_data(spark, num_processes, data_path, "serving")
data = pre_process(spark, raw_data)
model = PipelineModel.load(model_file)

# COMMAND ----------

start = timeit.default_timer()
predictions = serve(model, data)
end = timeit.default_timer()
serve_time = end - start
print("serve time:\t", serve_time)
predictions.select("prediction").write.csv(
    f"{work_dir}/predictions.csv", "overwrite", sep="|", header=True
)
