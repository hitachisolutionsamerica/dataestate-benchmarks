# coding: utf-8

# In[1]:

"""
Setup:
You've already executed the TPCxAIDataGeneration notebook with the desired data size.

PySpark with the snowflake spark connector in addition to the required ML libraries.
"""

tpcxai_size = 1  # Replace with the data scale factor.
tpcxai_path = ""  # Replace with the root directory of the TPCx-AI code

# Fill with connection details to snowflake environment
sf_connection = {
    "sfUrl": "",
    "sfUser": "",
    "sfPassword": "",
    "sfDatabase": "",
    "sfSchema": "",
    "sfWarehouse": "",
}

# In[2]:

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

# In[3]:

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

featureColumns = ["SCAN_COUNT", "SCAN_COUNT_ABS"] + weekday_columns + department_columns

label_column = "TRIP_TYPE"

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

# In[4]:


def load_data(session: SparkSession, connection, num_proc, path, stage):
    order_data = (
        spark.read.format("snowflake")
        .options(**connection)
        .option("dbtable", f"{stage}_ORDER")
        .load()
    )
    lineitem_data = (
        spark.read.format("snowflake")
        .options(**connection)
        .option("dbtable", f"{stage}_LINEITEM")
        .load()
    )
    product_data = (
        spark.read.format("snowflake")
        .options(**connection)
        .option("dbtable", f"{stage}_PRODUCT")
        .load()
    )

    data = order_data.join(
        lineitem_data, order_data.O_ORDER_ID == lineitem_data.LI_ORDER_ID, how="inner"
    )
    data = data.join(
        product_data, data.LI_PRODUCT_ID == product_data.P_PRODUCT_ID, how="inner"
    )

    if label_column in data.columns:
        data = data.select("O_ORDER_ID", "DATE", "DEPARTMENT", "QUANTITY", "TRIP_TYPE")
    else:
        data = data.select("O_ORDER_ID", "DATE", "DEPARTMENT", "QUANTITY")

    return data.repartition(num_proc).cache()


def pre_process(session: SparkSession, raw_data: DataFrame):
    has_labels = label_column in raw_data.columns

    raw_data = raw_data.withColumn("SCAN_COUNT", col("QUANTITY")).withColumn(
        "WEEKDAY", date_format(col("DATE"), "EEEE")
    )

    if has_labels:
        features_scan_count = raw_data.groupby("O_ORDER_ID").agg(
            sum("SCAN_COUNT").alias("SCAN_COUNT"),
            sum(abs("SCAN_COUNT")).alias("SCAN_COUNT_ABS"),
            min("WEEKDAY").alias("WEEKDAY"),
            min("TRIP_TYPE").alias("TRIP_TYPE"),
        )
    else:
        features_scan_count = raw_data.groupby("o_order_id").agg(
            sum("SCAN_COUNT").alias("SCAN_COUNT"),
            sum(abs("SCAN_COUNT")).alias("SCAN_COUNT_ABS"),
            min("WEEKDAY").alias("WEEKDAY"),
        )

    weekdays = (
        raw_data.groupby("O_ORDER_ID")
        .pivot("WEEKDAY")
        .agg(count(col("SCAN_COUNT") > 0))
        .fillna(0.0)
    )

    missing_weekdays = (
        set(weekday_columns) - set(["O_ORDER_ID"]) - set(weekdays.columns)
    )
    for c in missing_weekdays:
        weekdays = weekdays.withColumn(c, lit(0.0))

    departments = (
        raw_data.groupby("O_ORDER_ID").pivot("DEPARTMENT").agg(sum("SCAN_COUNT"))
    )

    missing_cols = (
        set(department_columns) - set(["O_ORDER_ID"]) - set(departments.columns)
    )
    for c in missing_cols:
        departments = departments.withColumn(c, lit(0.0))

    final_data = (
        features_scan_count.drop("WEEKDAY")
        .join(weekdays, "O_ORDER_ID")
        .join(departments, "O_ORDER_ID")
        .fillna(0.0)
    )

    if label_column in final_data.columns:
        encode_label = create_map([lit(x) for x in chain(*label_to_index.items())])

        # remove tiny classes
        final_data = final_data.where(col("TRIP_TYPE") != 14)
        final_data = final_data.withColumn("TRIP_TYPE", encode_label[col("TRIP_TYPE")])
        return final_data.select(featureColumns + ["TRIP_TYPE"])
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


# In[5]:

num_processes = 16
num_rounds = 100

base_path = os.path.join(tpcxai_path, f"{tpcxai_size}_GB")
data_path = os.path.join(base_path, "processed")

work_dir = f"{base_path}/output/output/uc08"
model_file = f"{work_dir}/uc08.spark.model"

# In[6]:

# DBTITLE 1,Load Training Data
start = timeit.default_timer()
raw_data = load_data(spark, sf_connection, num_processes, data_path, "training")
end = timeit.default_timer()
load_time = end - start
print("load time:\t", load_time)

# In[7]:

raw_data.show()

# In[8]:

# DBTITLE 1,Process Data
start = timeit.default_timer()
data = pre_process(spark, raw_data)
end = timeit.default_timer()
load_time = end - start
print("pre-process time:\t", load_time)

# In[9]:

data.show()

# In[10]:

# DBTITLE 1,Training
start = timeit.default_timer()
model = train(num_processes, num_rounds, data)
end = timeit.default_timer()
train_time = end - start
print("train time:\t", train_time)

# In[11]:

# DBTITLE 1,Save Model
model.write().overwrite().save(model_file)

# In[12]:

# DBTITLE 1,Serving
raw_data = load_data(spark, sf_connection, num_processes, data_path, "serving")
data = pre_process(spark, raw_data)
model = PipelineModel.load(model_file)

# In[13]:

start = timeit.default_timer()
predictions = serve(model, data)
end = timeit.default_timer()
serve_time = end - start
print("serve time:\t", serve_time)
predictions.select("prediction").write.csv(
    f"{work_dir}/predictions.csv", "overwrite", sep="|", header=True
)
