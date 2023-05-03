# coding: utf-8

# In[1]:

"""
Setup:
You've already executed the TPCxAIDataGeneration notebook with the desired data size.

This notebook depends on sndfile library installed with, for example, "sudo apt-get install libsndfile1 -y".

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
# Copyright 2021 Intel Corporation.
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
import io
import math
import os
import re
import string
import subprocess  # nosec subprocess is needed here to execute the sox tool.
import sys
import timeit
from pathlib import Path
from typing import Dict, Tuple, Union

import librosa
import numpy as np
import pyarrow
import scipy.io.wavfile as wav
import tensorflow as tf
import builtins as pybtin

# Spark
from horovod.spark.common.estimator import HorovodModel
from horovod.spark.common.store import HDFSStore

# Horovod
from horovod.spark.keras import KerasEstimator, KerasModel
from pyspark import RDD
from pyspark.ml.linalg import Vectors, VectorUDT, Vector, DenseVector
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import udf, lit, max, rand, when
from pyspark.sql.types import StringType, StructType, StructField, IntegerType

# Keras
from tensorflow.keras.backend import ctc_batch_cost, expand_dims, squeeze, ctc_decode
from tensorflow.keras.layers import (
    Input,
    Masking,
    TimeDistributed,
    Dense,
    ReLU,
    Dropout,
    Bidirectional,
    LSTM,
    Lambda,
    ZeroPadding2D,
    Conv2D,
)
from tensorflow.keras.models import Model
from tensorflow.keras.optimizers import Adam
from tqdm import tqdm

# In[4]:

# GLOBALE DEFINITIONS
INPUT_SEPARATOR = "|"
OUTPUT_SEPARATOR = "|"
ALPHABET = " '" + string.ascii_lowercase
ALPHABET_DICT = {k: v for v, k in enumerate(ALPHABET)}

PAUSE_IN_MS = 20
SAMPLE_RATE = 16000
WINDOW_SIZE = 32
WINDOW_STRIDE = 20
N_MFCC = 26
N_HIDDEN = 1024
DROPOUT_RATE = 0.005
CONTEXT = 9
MAX_RELU = 20
MAX_SEQUENCE_LENGTH = 100  # max. number of symbols in a transcript

# DEFAULTS
BATCH_SIZE_DEFAULT = 32
EPOCHS_DEFAULT = 5

AUDIO_SCHEMA = StructType(
    [StructField("rate", IntegerType()), StructField("data", VectorUDT())]
)

FEATURES_SCHEMA = StructType(
    [StructField("length", IntegerType()), StructField("data", VectorUDT())]
)

# In[5]:


def resample_audio(audio, original_sample_rate, desired_sample_rate):
    if original_sample_rate == desired_sample_rate:
        return original_sample_rate, audio
    cmd = f"sox - --type raw --bits 16 --channels 1 --rate {desired_sample_rate} --encoding signed-integer --endian little --compression 0.0 --no-dither - "
    f = io.BytesIO()
    wav.write(f, audio[0], audio[1])
    result = subprocess.run(cmd.split(), input=f.read(), stdout=subprocess.PIPE)
    if result.returncode == 0:
        return desired_sample_rate, np.frombuffer(result.stdout, dtype=np.int16)
    else:
        print(result.stdout, file=sys.stdout)
        print(result.stderr, file=sys.stderr)
        return 0, ""


def decode_sequence(sequence, alphabet: Dict):
    def lookup(k):
        try:
            return alphabet[k]
        except KeyError:
            return "_"

    decoded_sequence = list(map(lookup, sequence))
    return "".join(decoded_sequence)


def load_data(
    session: SparkSession, num_proc, connection, basepath, path, stage
) -> DataFrame:
    # meta_data = session.read.csv(path, sep=INPUT_SEPARATOR, inferSchema=True, header=True)
    meta_data = (
        spark.read.format("snowflake")
        .options(**connection)
        .option("dbtable", f"{stage}_CONVERSATION_AUDIO")
        .load()
    )
    meta_data = meta_data.repartition(num_proc)

    # TODO: Can this be stored in snowpark?
    audio_data_seq: RDD = session.sparkContext.sequenceFile(
        f"{path}/{stage}/CONVERSATION_AUDIO.seq"
    )
    audio_data = session.createDataFrame(
        audio_data_seq, ["WAV_FILENAME", "AUDIO_BYTES"]
    )

    # Meta data has relative paths, but we need absolute
    @udf(returnType=StringType())
    def make_path(f):
        return f"{base_path}/output/raw_data/{stage}/{f}"

    meta_data = meta_data.withColumn("WAV_FILENAME", make_path("WAV_FILENAME"))

    audio_data = audio_data.repartition(num_proc)
    data = meta_data.join(audio_data, "WAV_FILENAME")

    def wav_read(audio_bytes):
        audio = wav.read(io.BytesIO(audio_bytes))
        return audio[0], Vectors.dense(audio[1])

    wav_read_udf = udf(wav_read, AUDIO_SCHEMA)

    data = data.withColumn("AUDIO", wav_read_udf(data.AUDIO_BYTES))
    if "TRANSCRIPT" in data.columns:
        return data.select("WAV_FILENAME", "TRANSCRIPT", "AUDIO").cache()
    else:
        return data.select("WAV_FILENAME", "AUDIO").cache()


def clean_data(data: DataFrame) -> DataFrame:
    if "TRANSCRIPT" not in data.columns:
        return data
    # remove samples with no transcript
    pattern = re.compile(f"[^{ALPHABET}]")

    def normalize_transcript(trans):
        try:
            return pattern.sub("", trans.lower())
        except AttributeError:
            return pattern.sub("", str(trans).lower())

    normalize_transcript_udf = udf(normalize_transcript, StringType())

    data = data.withColumn("TRANSCRIPT_NORM", normalize_transcript_udf(data.TRANSCRIPT))
    data = data.filter(data.TRANSCRIPT_NORM.isNotNull())
    data = data.filter(data.TRANSCRIPT_NORM != "")
    return data.cache()


def preprocess_data(data: DataFrame, max_timesteps=None) -> Tuple[DataFrame, int]:
    # resampling
    resample_audio_udf = udf(resample_audio, AUDIO_SCHEMA)
    data = data.withColumn(
        "AUDIO", resample_audio_udf(data.AUDIO.data, data.AUDIO.rate, lit(SAMPLE_RATE))
    )
    # possibly add silence to each utterance

    # LABELS
    if "TRANSCRIPT" in data.columns:

        def text_to_seq(text):
            seq = []
            for c in text:
                seq.append(ALPHABET_DICT[c])
            seq = np.asarray(seq)
            return Vectors.dense(seq)

        def vector_length(vector):
            return vector.size

        def ctc_length(vector):
            repeated_chars = 0
            last_char = None
            for elem in vector:
                if last_char == elem:
                    repeated_chars += 1
                last_char = elem
            return vector.size + repeated_chars

        vector_length_udf = udf(vector_length, IntegerType())
        ctc_length_udf = udf(ctc_length, IntegerType())

        text_to_seq_udf = udf(text_to_seq, VectorUDT())
        data = data.withColumn(
            "TRANSCRIPT_SEQ_RAW", text_to_seq_udf(data.TRANSCRIPT_NORM)
        )
        data = data.withColumn("LABELS_LEN", vector_length_udf(data.TRANSCRIPT_SEQ_RAW))
        data = data.withColumn(
            "CTC_LABELS_LEN", ctc_length_udf(data.TRANSCRIPT_SEQ_RAW)
        )
        # filter all training samples where transcript has more than MAX_SEQUENCE_LENGTH symbols
        data = data[
            (data["LABELS_LEN"] <= MAX_SEQUENCE_LENGTH) & (data["LABELS_LEN"] > 0)
        ]

    # FEATURES
    # calculate the mel spectograms windows_size 32 * 16, window_stride 20 * 16
    def to_mfcc(
        audio: Vector, sample_rate, win_size=WINDOW_SIZE, win_stride=WINDOW_STRIDE
    ):
        # convert to float32 (-1.0 to 1.0)
        dtype = np.int16
        min_value = np.iinfo(dtype).min
        max_value = np.iinfo(dtype).max
        factor = 1 / np.max(np.abs([min_value, max_value]))
        y = audio.toArray()
        y = y * factor
        sr = sample_rate
        spectograms = librosa.feature.melspectrogram(
            y, sr, n_fft=win_size * sr // 1000, hop_length=win_stride * sr // 1000
        )
        mfccs = librosa.feature.mfcc(
            S=spectograms, sr=SAMPLE_RATE, n_mfcc=N_MFCC
        ).transpose()
        # flatten the matrix of shape (timesteps, n_mfcc) to (timesteps * n_mfcc)
        return mfccs.shape[0], Vectors.dense(mfccs.flatten())

    # get the mfcc's (20 coeefficients) for the mel spectograms
    to_melspectograms_udf = udf(to_mfcc, FEATURES_SCHEMA)
    data = data.withColumn(
        "FEATURES_RAW", to_melspectograms_udf(data.AUDIO.data, data.AUDIO.rate)
    )

    # padding transcripts and features
    data = data.cache()
    if not max_timesteps:
        max_timesteps = data.agg(max(data.FEATURES_RAW.length)).collect()[0][0]

    def pad_features(features, max_len):
        zeros = np.zeros(max_len * N_MFCC)
        # copy everything
        if features.length < max_len:
            zeros[: features.length * N_MFCC] = features.data
        # copy only a slice
        else:
            zeros = features.data[: max_len * N_MFCC]

        return Vectors.dense(zeros)

    pad_features_udf = udf(pad_features, VectorUDT())
    data = data.withColumn(
        "FEATURES", pad_features_udf(data.FEATURES_RAW, lit(max_timesteps))
    )

    data = data.withColumn(
        "FEATURES_LENGTH",
        when(
            data.FEATURES_RAW.length <= max_timesteps, data.FEATURES_RAW.length
        ).otherwise(max_timesteps),
    )

    if "TRANSCRIPT" in data.columns:

        def pad_sequence(sequence: Vector, max_len):
            seq_len = len(sequence)
            return Vectors.sparse(max_len, range(seq_len), sequence)

        pad_sequence_udf = udf(pad_sequence, VectorUDT())
        data = data.withColumn(
            "TRANSCRIPT_SEQ",
            pad_sequence_udf(data.TRANSCRIPT_SEQ_RAW, lit(MAX_SEQUENCE_LENGTH)),
        )
        data = data[data.FEATURES_RAW.length > data.CTC_LABELS_LEN]
        return (
            data.select(
                data.WAV_FILENAME,
                data.FEATURES,
                data.FEATURES_LENGTH,
                data.TRANSCRIPT_SEQ,
                data.LABELS_LEN,
            ).cache(),
            max_timesteps,
        )
    else:
        return (
            data.select(data.WAV_FILENAME, data.FEATURES, data.FEATURES_LENGTH).cache(),
            max_timesteps,
        )


def make_model_func(max_timesteps, n_hidden=N_HIDDEN, with_convolution=True):
    x = Input((max_timesteps, N_MFCC), name="X")
    y_true = Input((MAX_SEQUENCE_LENGTH,), name="y")
    seq_lengths = Input((1,), name="sequence_lengths")
    time_steps = Input((1,), name="time_steps")

    masking = Masking(mask_value=0)(x)

    if with_convolution:
        conv_layer = Lambda(lambda val: expand_dims(val, axis=-1))(masking)
        conv_layer = ZeroPadding2D(padding=(CONTEXT, 0))(conv_layer)
        conv_layer = Conv2D(filters=n_hidden, kernel_size=(2 * CONTEXT + 1, N_MFCC))(
            conv_layer
        )
        conv_layer = Lambda(squeeze, arguments=dict(axis=2))(conv_layer)
        conv_layer = ReLU(max_value=20)(conv_layer)
        conv_layer = Dropout(DROPOUT_RATE)(conv_layer)

        layer_1 = TimeDistributed(Dense(n_hidden))(conv_layer)
    else:
        layer_1 = TimeDistributed(Dense(n_hidden))(masking)

    layer_1 = ReLU(max_value=MAX_RELU)(layer_1)
    layer_1 = Dropout(DROPOUT_RATE)(layer_1)

    layer_2 = TimeDistributed(Dense(n_hidden))(layer_1)
    layer_2 = ReLU(max_value=MAX_RELU)(layer_2)
    layer_2 = Dropout(DROPOUT_RATE)(layer_2)

    lstm = Bidirectional(LSTM(n_hidden, return_sequences=True), merge_mode="sum")(
        layer_2
    )
    softmax = TimeDistributed(
        Dense(len(ALPHABET) + 1, activation="softmax"), name="prediction_softmax"
    )(lstm)

    def myloss_layer(args):
        y_true, y_pred, time_steps, label_lengths = args
        return ctc_batch_cost(y_true, y_pred, time_steps, label_lengths)

    ctc_loss_layer = Lambda(myloss_layer, output_shape=(1,), name="ctc")(
        [y_true, softmax, time_steps, seq_lengths]
    )

    model = Model(inputs=[x, y_true, time_steps, seq_lengths], outputs=ctc_loss_layer)

    return model


def make_serving_model(training_model: Union[Model, KerasModel]) -> (KerasModel, int):
    _model = training_model.getModel()
    input_layer = _model.get_layer(name="X")
    max_timesteps = input_layer.input_shape[0][1]
    output_layer = _model.get_layer(name="prediction_softmax")
    predict_model = Model([input_layer.input], output_layer.output)
    meta_data = {
        "FEATURES": {
            "spark_data_type": DenseVector,
            "is_sparse_vector_only": False,
            "shape": None,
            "intermediate_format": "array",
            "max_size": None,
        },
        "TRANSCRIPT": {
            "spark_data_type": DenseVector,
            "is_sparse_vector_only": False,
            "shape": None,
            "intermediate_format": "array",
            "max_size": None,
        },
    }
    horovod_model = KerasModel(
        model=predict_model,
        feature_columns=["FEATURES"],
        label_columns=["TRANSCRIPT"],
        _metadata=meta_data,
        _floatx="float32",
    )
    return horovod_model, max_timesteps


def ctc_dummy(y_true, y_pred):
    mean = tf.reduce_mean(y_pred)
    return mean


def train(
    data: DataFrame,
    max_timesteps,
    batch_size,
    epochs,
    work_dir,
    defaultParallelism,
    current_user="",
    learning_rate=None,
) -> HorovodModel:
    model = make_model_func(with_convolution=False, max_timesteps=max_timesteps)
    model.summary(line_length=80)

    store = HDFSStore(work_dir, user=current_user)
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
    optimizer = Adam(learning_rate=lr, beta_1=0.9, beta_2=0.999, epsilon=1e-8)

    keras_estimator = KerasEstimator(
        model=model,
        num_proc=num_processes,
        store=store,
        loss=ctc_dummy,
        optimizer=optimizer,
        validation="VALIDATION",
        batch_size=batch_size,
        epochs=epochs,
        feature_cols=["FEATURES", "TRANSCRIPT_SEQ", "FEATURES_LENGTH", "LABELS_LEN"],
        label_cols=["TRANSCRIPT_SEQ"],
        custom_objects={"ctc_batch_cost": ctc_batch_cost, "ctc_dummy": ctc_dummy},
    )
    data = data.repartition(num_processes)
    model = keras_estimator.fit(data)
    return model


def serve(model: KerasModel, x: DataFrame):
    pred = model.transform(x)
    pred = pred.withColumn(
        "PREDICTION", decode("TRANSCRIPT__OUTPUT", "FEATURES_LENGTH")
    )
    return pred


@udf(returnType=StringType())
def decode(vector, input_length):
    probs = vector.toArray().reshape(-1, len(ALPHABET) + 1)
    decoded_seq = ctc_decode([probs], [input_length], greedy=False)[0][0].numpy()
    inv_alphabet = {v: k for k, v in ALPHABET_DICT.items()}
    return decode_sequence(decoded_seq[0], inv_alphabet)


# In[6]:

tqdm.pandas()

# In[7]:

batch_size = 32
epochs = 25
learning_rate = None
num_processes = 16

current_user = spark.sparkContext.sparkUser()

base_path = os.path.join(tpcxai_path, f"{tpcxai_size}_GB")
data_path = os.path.join(base_path, "processed")

work_dir = f"{base_path}/output/output/uc02"
preprocessed_data_path = f"{work_dir}/tmp/preprocessed.parquet"
model_file_name = f"{work_dir}/uc02.python.model"

# In[8]:

# DBTITLE 1,Load Training Data
start = timeit.default_timer()
raw_data = load_data(
    spark, num_processes, sf_connection, base_path, data_path, "training"
)
end = timeit.default_timer()
load_time = end - start
print("load time:\t", load_time)

# In[9]:

raw_data.show()

# In[10]:

# DBTITLE 1,Process Data
start = timeit.default_timer()
cleaned_data = clean_data(raw_data)
preprocessed_data, max_timesteps = preprocess_data(cleaned_data)

# mark: train / validation split (80/20)
preprocessed_data = preprocessed_data.withColumn(
    "VALIDATION", when(rand(seed=0xC0FFE) <= 0.2, True).otherwise(False)
)

end = timeit.default_timer()
pre_process_time = end - start
print("pre-process time:\t", pre_process_time)

# In[11]:

preprocessed_data.show()

# In[12]:

# DBTITLE 1,Training
start = timeit.default_timer()
preprocessed_data.write.mode("overwrite").parquet(preprocessed_data_path)
training_data = spark.read.parquet(preprocessed_data_path)
model = train(
    training_data,
    max_timesteps,
    batch_size,
    epochs,
    work_dir,
    num_processes,
    current_user,
    learning_rate,
)
end = timeit.default_timer()
train_time = end - start
print("train time:\t", train_time)

# In[13]:

# DBTITLE 1,Save Model
model.write().overwrite().save(model_file_name)

# In[14]:

# DBTITLE 1,Serving
model = KerasModel.load(model_file_name)
serving_model, max_timesteps = make_serving_model(model)
raw_data = load_data(
    spark, num_processes, sf_connection, base_path, data_path, "serving"
)

# In[15]:

start = timeit.default_timer()
cleaned_data = clean_data(raw_data)
preprocessed_data, _ = preprocess_data(cleaned_data, max_timesteps=max_timesteps)
end = timeit.default_timer()
pre_process_time = end - start
print("pre-process time:\t", pre_process_time)

# In[16]:

start = timeit.default_timer()
prediction = serve(serving_model, preprocessed_data)
end = timeit.default_timer()
serve_time = end - start
print("serve time:\t", serve_time)
prediction.select("WAV_FILENAME", "PREDICTION").withColumnRenamed(
    "PREDICTION", "TRANSCRIPT"
).write.mode("overwrite").option("quoteAll", True).csv(
    f"{work_dir}/predictions.csv", header=True, sep="|"
)
