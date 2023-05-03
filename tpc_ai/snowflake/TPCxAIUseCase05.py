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

#
# Copyright (C) 2021 Transaction Processing Performance Council (TPC) and/or its contributors.
# This file is part of a software package distributed by the TPC
# The contents of this file have been developed by the TPC, and/or have been licensed to the TPC under one or more contributor
# license agreements.
# This file is subject to the terms and conditions outlined in the End-User
# License Agreement (EULA) which can be found in this distribution (EULA.txt) and is available at the following URL:
# http://www.tpc.org/TPC_Documents_Current_Versions/txt/EULA.txt
# Unless required by applicable law or agreed to in writing, this software is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied, and the user bears the entire risk as to quality
# and performance as well as the entire cost of service or repair in case of defect. See the EULA for more details.
#


#
# Copyright 2019 Intel Corporation.
# This software and the related documents are Intel copyrighted materials, and your use of them
# is governed by the express license under which they were provided to you ("License"). Unless the
# License provides otherwise, you may not use, modify, copy, publish, distribute, disclose or
# transmit this software or the related documents without Intel's prior written permission.
#
# This software and the related documents are provided as is, with no express or implied warranties,
# other than those that are expressly stated in the License.
#
#

# In[3]:

import argparse
import os
import timeit
from typing import List, Dict
from pyspark import SparkContext
import numpy as np

import math
import builtins as pybtin
import horovod.spark.keras as hvd
import joblib

from horovod.spark.common.estimator import HorovodModel
from horovod.spark.common.store import HDFSStore

from pyspark.ml.feature import RegexTokenizer
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, udf, explode, when
from pyspark.sql.types import ArrayType, IntegerType

from tensorflow.keras import Sequential
from tensorflow.keras.layers import Embedding, Dense, GRU
from tensorflow.keras.losses import mean_squared_error, mean_squared_logarithmic_error
from tensorflow.keras.optimizers import Adam

SEQUENCE_LEN = 200
EMBEDDING_DIM = 300
BATCH_SIZE = int(4096 / 8)

# In[4]:


def load(session: SparkSession, num_proc, connection, path, stage) -> DataFrame:
    # read csv w/o quote interpretation
    # data = session.read.csv(path, sep='|', inferSchema=True, header=True, quote="")
    # return data.repartition(session.sparkContext.defaultParallelism)
    data = (
        spark.read.format("snowflake")
        .options(**connection)
        .option("dbtable", f"{stage}_marketplace")
        .load()
    )
    return data.repartition(num_proc).cache()


def build_vocabulary(texts: DataFrame) -> List[str]:
    tokens = texts.select(explode(texts.WORDS))
    vocab = tokens.distinct().collect()
    vocab = list(map(lambda r: r[0], vocab))
    return sorted(vocab)


def build_vocabulary_map(vocabulary: List[str]) -> Dict[str, int]:
    vocabulary_map = dict()
    for i in range(len(vocabulary)):
        vocabulary_map[vocabulary[i]] = i
    return vocabulary_map


def pre_process(
    data: DataFrame, sc: SparkContext, vocabulary: List[str] = None
) -> (DataFrame, List[str]):
    # map and clean headers and descriptions from beginning/end quotation
    if "PRICE" in data.columns:
        text_data = data.rdd.map(
            lambda row: (row["PRICE"], row["DESCRIPTION"][1:-1], row["ID"])
        ).toDF(["PRICE", "DESCRIPTION", "ID"])
    else:
        text_data = data.rdd.map(
            lambda row: (row["DESCRIPTION"][1:-1], row["ID"])
        ).toDF(["DESCRIPTION", "ID"])

    tokenizer = (
        RegexTokenizer(pattern="\\W").setInputCol("DESCRIPTION").setOutputCol("WORDS")
    )
    tokens = tokenizer.transform(text_data)
    if vocabulary is None:
        vocabulary = build_vocabulary(tokens)
    vocabulary_map = build_vocabulary_map(vocabulary)
    broadcast_vocabulary_map = sc.broadcast(vocabulary_map)

    def text_to_sequence(text, padding=SEQUENCE_LEN):
        sequence = []
        length = 0

        for word in text:
            try:
                idx = broadcast_vocabulary_map.value[word] + 1
            except KeyError:
                idx = 1
            sequence.append(idx)
            length += 1

        if padding is not None:
            missing = padding - length
            if missing > 0:
                # pre-pad
                return [0] * missing + sequence
            elif missing < 0:
                # truncate first
                return sequence[abs(missing) :]
            else:
                # do nothing
                return sequence

    text_to_sequence_udf = udf(
        lambda w: text_to_sequence(w, SEQUENCE_LEN), ArrayType(IntegerType())
    )
    sequences = tokens.withColumn("SEQUENCE", text_to_sequence_udf(tokens.WORDS))
    sequences = sequences.withColumnRenamed("SEQUENCE", "X").withColumnRenamed(
        "PRICE", "Y"
    )
    training_data = sequences
    return training_data, vocabulary


def make_bi_lstm(tokenizer_len):
    rnn_model = Sequential()
    rnn_model.add(Embedding(tokenizer_len, EMBEDDING_DIM, input_length=SEQUENCE_LEN))
    rnn_model.add(GRU(16))
    rnn_model.add(Dense(128))
    rnn_model.add(Dense(64))
    rnn_model.add(Dense(1, activation="linear"))
    return rnn_model


def train(
    defaultParallelism,
    architecture,
    batch_size,
    epochs,
    loss,
    data,
    work_dir="/tmp",
    current_user="",
    learning_rate=None,
):
    num_samples = data.count()
    num_processes = math.floor(
        min(
            defaultParallelism,
            num_samples * 0.8 / batch_size,
            num_samples * 0.2 / batch_size,
        )
    )
    num_processes = pybtin.max(num_processes, 1)
    lr = 0.001 / num_processes if not learning_rate else learning_rate
    opt = Adam(learning_rate=lr)

    store = HDFSStore(work_dir, user=current_user)
    estimator = hvd.KerasEstimator(
        num_proc=num_processes,
        model=architecture,
        loss=loss,
        optimizer=opt,
        store=store,
        batch_size=batch_size,
        epochs=epochs,
        shuffle_buffer_size=2,
        feature_cols=["X"],
        label_cols=["Y"],
        verbose=1,
    )

    data = data.repartition(num_processes)
    model: HorovodModel = estimator.fit(data)
    return model


def serve(model, data):
    predictions: DataFrame = model.transform(data)
    return predictions.withColumn(
        "PRICE", when(col("Y__OUTPUT") < 0, 0).otherwise(col("Y__OUTPUT"))
    )


# In[5]:

epochs = 15
batch_size = 512
loss = mean_squared_error
learning_rate = None
num_processes = 32

current_user = spark.sparkContext.sparkUser()

base_path = os.path.join(tpcxai_path, f"{tpcxai_size}_GB")
data_path = os.path.join(base_path, "processed")

work_dir = f"{base_path}/output/output/uc05"
model_file = f"{work_dir}/uc05.spark.model"
dictionary_url = f"{work_dir}/uc05.dict"

# In[6]:

# DBTITLE 1,Load Training Data
start = timeit.default_timer()
data = load(spark, num_processes, sf_connection, data_path, "training")
end = timeit.default_timer()
load_time = end - start
print("load time:\t", load_time)

# In[7]:

data.show()

# In[8]:

# DBTITLE 1,Process Data
start = timeit.default_timer()
training_data, vocabulary = pre_process(data, spark.sparkContext)
end = timeit.default_timer()
pre_process_time = end - start
print("pre-process time:\t", pre_process_time)

# In[9]:

training_data.show()

# In[10]:

# DBTITLE 1,Training
start = timeit.default_timer()

tok_len = len(vocabulary) + 1
architecture = make_bi_lstm(tok_len)
model = train(
    num_processes,
    architecture,
    batch_size,
    epochs,
    loss,
    training_data,
    work_dir,
    current_user,
    learning_rate,
)

end = timeit.default_timer()
train_time = end - start
print("train time:\t", train_time)

# In[11]:

# DBTITLE 1,Save Model
model.write().overwrite().save(model_file)
dict_dir = os.path.dirname(dictionary_url)
os.makedirs(dict_dir, exist_ok=True)
joblib.dump(vocabulary, dictionary_url)

# In[12]:

# DBTITLE 1,Serving
data = load(spark, num_processes, sf_connection, data_path, "serving")
vocabulary = joblib.load(dictionary_url)
model = hvd.KerasModel.load(model_file)

# In[13]:

start = timeit.default_timer()
(serving_data, _) = pre_process(data, spark.sparkContext, vocabulary)
end = timeit.default_timer()
pre_process_time = end - start
print("pre-process time:\t", pre_process_time)

# In[14]:

start = timeit.default_timer()
price_suggestions = serve(model, serving_data)
end = timeit.default_timer()
serve_time = end - start
print("serve time:\t", serve_time)
price_suggestions.select("ID", "PRICE").write.csv(
    f"{work_dir}/predictions.csv", "overwrite", sep="|", header=True
)
