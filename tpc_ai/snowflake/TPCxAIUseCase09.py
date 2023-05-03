# coding: utf-8

# In[1]:

"""
Setup:
You've already executed the TPCxAIDataGeneration notebook with the desired data size.

PyPi packages tensorflow-addons, opencv-python, dlib installable with pip.

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
import math
import timeit
from pathlib import Path

import horovod.spark.keras as hvd
from horovod.spark.common.store import HDFSStore
from horovod.spark.keras import KerasEstimator

from pyspark import SparkFiles
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer, IndexToString, OneHotEncoder
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, lit
from pyspark.sql.types import (
    IntegerType,
    BinaryType,
    StringType,
    StructType,
    _create_row,
    StructField,
)

import tensorflow as tf
from tensorflow.keras import optimizers
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.models import Sequential
from tensorflow.keras.optimizers import Adadelta
from tensorflow_addons.losses import TripletHardLoss

from tensorflow.keras.layers import Input, concatenate
from tensorflow.keras.layers import Lambda, Flatten, Dense
from tensorflow.keras.layers import MaxPooling2D, AveragePooling2D
from tensorflow.keras.layers import Conv2D, ZeroPadding2D, Activation
from tensorflow.keras.layers import BatchNormalization
from tensorflow.keras.models import Model
from tensorflow.keras import backend as K

import cv2
import dlib
import numpy as np
from numpy import genfromtxt

# In[4]:

INPUT_SEPARATOR = ","
IMG_SCHEMA = StructType(
    [
        StructField("origin", StringType()),
        StructField("height", IntegerType()),
        StructField("width", IntegerType()),
        StructField("nChannels", IntegerType()),
        StructField("mode", IntegerType()),
        StructField("data", BinaryType()),
    ]
)
BATCH_SIZE_DEFAULT = 64
EPOCHS_EMBEDDING_DEFAULT = 15
EPOCHS_CLASSIFIER_DEFAULT = 10000

TEMPLATE = np.float32(
    [
        (0.0792396913815, 0.339223741112),
        (0.0829219487236, 0.456955367943),
        (0.0967927109165, 0.575648016728),
        (0.122141515615, 0.691921601066),
        (0.168687863544, 0.800341263616),
        (0.239789390707, 0.895732504778),
        (0.325662452515, 0.977068762493),
        (0.422318282013, 1.04329000149),
        (0.531777802068, 1.06080371126),
        (0.641296298053, 1.03981924107),
        (0.738105872266, 0.972268833998),
        (0.824444363295, 0.889624082279),
        (0.894792677532, 0.792494155836),
        (0.939395486253, 0.681546643421),
        (0.96111933829, 0.562238253072),
        (0.970579841181, 0.441758925744),
        (0.971193274221, 0.322118743967),
        (0.163846223133, 0.249151738053),
        (0.21780354657, 0.204255863861),
        (0.291299351124, 0.192367318323),
        (0.367460241458, 0.203582210627),
        (0.4392945113, 0.233135599851),
        (0.586445962425, 0.228141644834),
        (0.660152671635, 0.195923841854),
        (0.737466449096, 0.182360984545),
        (0.813236546239, 0.192828009114),
        (0.8707571886, 0.235293377042),
        (0.51534533827, 0.31863546193),
        (0.516221448289, 0.396200446263),
        (0.517118861835, 0.473797687758),
        (0.51816430343, 0.553157797772),
        (0.433701156035, 0.604054457668),
        (0.475501237769, 0.62076344024),
        (0.520712933176, 0.634268222208),
        (0.565874114041, 0.618796581487),
        (0.607054002672, 0.60157671656),
        (0.252418718401, 0.331052263829),
        (0.298663015648, 0.302646354002),
        (0.355749724218, 0.303020650651),
        (0.403718978315, 0.33867711083),
        (0.352507175597, 0.349987615384),
        (0.296791759886, 0.350478978225),
        (0.631326076346, 0.334136672344),
        (0.679073381078, 0.29645404267),
        (0.73597236153, 0.294721285802),
        (0.782865376271, 0.321305281656),
        (0.740312274764, 0.341849376713),
        (0.68499850091, 0.343734332172),
        (0.353167761422, 0.746189164237),
        (0.414587777921, 0.719053835073),
        (0.477677654595, 0.706835892494),
        (0.522732900812, 0.717092275768),
        (0.569832064287, 0.705414478982),
        (0.635195811927, 0.71565572516),
        (0.69951672331, 0.739419187253),
        (0.639447159575, 0.805236879972),
        (0.576410514055, 0.835436670169),
        (0.525398405766, 0.841706377792),
        (0.47641545769, 0.837505914975),
        (0.41379548902, 0.810045601727),
        (0.380084785646, 0.749979603086),
        (0.477955996282, 0.74513234612),
        (0.523389793327, 0.748924302636),
        (0.571057789237, 0.74332894691),
        (0.672409137852, 0.744177032192),
        (0.572539621444, 0.776609286626),
        (0.5240106503, 0.783370783245),
        (0.477561227414, 0.778476346951),
    ]
)

TPL_MIN, TPL_MAX = np.min(TEMPLATE, axis=0), np.max(TEMPLATE, axis=0)
MINMAX_TEMPLATE = (TEMPLATE - TPL_MIN) / (TPL_MAX - TPL_MIN)

_FLOATX = "float32"

weights = [
    "conv1",
    "bn1",
    "conv2",
    "bn2",
    "conv3",
    "bn3",
    "inception_3a_1x1_conv",
    "inception_3a_1x1_bn",
    "inception_3a_pool_conv",
    "inception_3a_pool_bn",
    "inception_3a_5x5_conv1",
    "inception_3a_5x5_conv2",
    "inception_3a_5x5_bn1",
    "inception_3a_5x5_bn2",
    "inception_3a_3x3_conv1",
    "inception_3a_3x3_conv2",
    "inception_3a_3x3_bn1",
    "inception_3a_3x3_bn2",
    "inception_3b_3x3_conv1",
    "inception_3b_3x3_conv2",
    "inception_3b_3x3_bn1",
    "inception_3b_3x3_bn2",
    "inception_3b_5x5_conv1",
    "inception_3b_5x5_conv2",
    "inception_3b_5x5_bn1",
    "inception_3b_5x5_bn2",
    "inception_3b_pool_conv",
    "inception_3b_pool_bn",
    "inception_3b_1x1_conv",
    "inception_3b_1x1_bn",
    "inception_3c_3x3_conv1",
    "inception_3c_3x3_conv2",
    "inception_3c_3x3_bn1",
    "inception_3c_3x3_bn2",
    "inception_3c_5x5_conv1",
    "inception_3c_5x5_conv2",
    "inception_3c_5x5_bn1",
    "inception_3c_5x5_bn2",
    "inception_4a_3x3_conv1",
    "inception_4a_3x3_conv2",
    "inception_4a_3x3_bn1",
    "inception_4a_3x3_bn2",
    "inception_4a_5x5_conv1",
    "inception_4a_5x5_conv2",
    "inception_4a_5x5_bn1",
    "inception_4a_5x5_bn2",
    "inception_4a_pool_conv",
    "inception_4a_pool_bn",
    "inception_4a_1x1_conv",
    "inception_4a_1x1_bn",
    "inception_4e_3x3_conv1",
    "inception_4e_3x3_conv2",
    "inception_4e_3x3_bn1",
    "inception_4e_3x3_bn2",
    "inception_4e_5x5_conv1",
    "inception_4e_5x5_conv2",
    "inception_4e_5x5_bn1",
    "inception_4e_5x5_bn2",
    "inception_5a_3x3_conv1",
    "inception_5a_3x3_conv2",
    "inception_5a_3x3_bn1",
    "inception_5a_3x3_bn2",
    "inception_5a_pool_conv",
    "inception_5a_pool_bn",
    "inception_5a_1x1_conv",
    "inception_5a_1x1_bn",
    "inception_5b_3x3_conv1",
    "inception_5b_3x3_conv2",
    "inception_5b_3x3_bn1",
    "inception_5b_3x3_bn2",
    "inception_5b_pool_conv",
    "inception_5b_pool_bn",
    "inception_5b_1x1_conv",
    "inception_5b_1x1_bn",
    "dense_layer",
]

conv_shape = {
    "conv1": [64, 3, 7, 7],
    "conv2": [64, 64, 1, 1],
    "conv3": [192, 64, 3, 3],
    "inception_3a_1x1_conv": [64, 192, 1, 1],
    "inception_3a_pool_conv": [32, 192, 1, 1],
    "inception_3a_5x5_conv1": [16, 192, 1, 1],
    "inception_3a_5x5_conv2": [32, 16, 5, 5],
    "inception_3a_3x3_conv1": [96, 192, 1, 1],
    "inception_3a_3x3_conv2": [128, 96, 3, 3],
    "inception_3b_3x3_conv1": [96, 256, 1, 1],
    "inception_3b_3x3_conv2": [128, 96, 3, 3],
    "inception_3b_5x5_conv1": [32, 256, 1, 1],
    "inception_3b_5x5_conv2": [64, 32, 5, 5],
    "inception_3b_pool_conv": [64, 256, 1, 1],
    "inception_3b_1x1_conv": [64, 256, 1, 1],
    "inception_3c_3x3_conv1": [128, 320, 1, 1],
    "inception_3c_3x3_conv2": [256, 128, 3, 3],
    "inception_3c_5x5_conv1": [32, 320, 1, 1],
    "inception_3c_5x5_conv2": [64, 32, 5, 5],
    "inception_4a_3x3_conv1": [96, 640, 1, 1],
    "inception_4a_3x3_conv2": [192, 96, 3, 3],
    "inception_4a_5x5_conv1": [
        32,
        640,
        1,
        1,
    ],
    "inception_4a_5x5_conv2": [64, 32, 5, 5],
    "inception_4a_pool_conv": [128, 640, 1, 1],
    "inception_4a_1x1_conv": [256, 640, 1, 1],
    "inception_4e_3x3_conv1": [160, 640, 1, 1],
    "inception_4e_3x3_conv2": [256, 160, 3, 3],
    "inception_4e_5x5_conv1": [64, 640, 1, 1],
    "inception_4e_5x5_conv2": [128, 64, 5, 5],
    "inception_5a_3x3_conv1": [96, 1024, 1, 1],
    "inception_5a_3x3_conv2": [384, 96, 3, 3],
    "inception_5a_pool_conv": [96, 1024, 1, 1],
    "inception_5a_1x1_conv": [256, 1024, 1, 1],
    "inception_5b_3x3_conv1": [96, 736, 1, 1],
    "inception_5b_3x3_conv2": [384, 96, 3, 3],
    "inception_5b_pool_conv": [96, 736, 1, 1],
    "inception_5b_1x1_conv": [256, 736, 1, 1],
}

# In[5]:


class AlignDlib:
    """
    Use `dlib's landmark estimation <http://blog.dlib.net/2014/08/real-time-face-pose-estimation.html>`_ to align faces.

    The alignment preprocess faces for input into a neural network.
    Faces are resized to the same size (such as 96x96) and transformed
    to make landmarks (such as the eyes and nose) appear at the same
    location on every image.

    Normalized landmarks:

    .. image:: ../images/dlib-landmark-mean.png
    """

    #: Landmark indices.
    INNER_EYES_AND_BOTTOM_LIP = [39, 42, 57]
    OUTER_EYES_AND_NOSE = [36, 45, 33]

    def __init__(self, facePredictor):
        """
        Instantiate an 'AlignDlib' object.

        :param facePredictor: The path to dlib's
        :type facePredictor: str
        """
        assert facePredictor is not None

        self.detector = dlib.get_frontal_face_detector()
        self.predictor = dlib.shape_predictor(facePredictor)

    def getAllFaceBoundingBoxes(self, rgbImg):
        """
        Find all face bounding boxes in an image.

        :param rgbImg: RGB image to process. Shape: (height, width, 3)
        :type rgbImg: numpy.ndarray
        :return: All face bounding boxes in an image.
        :rtype: dlib.rectangles
        """
        assert rgbImg is not None

        try:
            return self.detector(rgbImg, 1)
        except Exception as e:
            print("Warning: {}".format(e))
            # In rare cases, exceptions are thrown.
            return []

    def getLargestFaceBoundingBox(self, rgbImg, skipMulti=False):
        """
        Find the largest face bounding box in an image.

        :param rgbImg: RGB image to process. Shape: (height, width, 3)
        :type rgbImg: numpy.ndarray
        :param skipMulti: Skip image if more than one face detected.
        :type skipMulti: bool
        :return: The largest face bounding box in an image, or None.
        :rtype: dlib.rectangle
        """
        assert rgbImg is not None

        faces = self.getAllFaceBoundingBoxes(rgbImg)
        if (not skipMulti and len(faces) > 0) or len(faces) == 1:
            return max(faces, key=lambda rect: rect.width() * rect.height())
        else:
            return None

    def findLandmarks(self, rgbImg, bb):
        """
        Find the landmarks of a face.

        :param rgbImg: RGB image to process. Shape: (height, width, 3)
        :type rgbImg: numpy.ndarray
        :param bb: Bounding box around the face to find landmarks for.
        :type bb: dlib.rectangle
        :return: Detected landmark locations.
        :rtype: list of (x,y) tuples
        """
        assert rgbImg is not None
        assert bb is not None

        points = self.predictor(rgbImg, bb)
        return list(map(lambda p: (p.x, p.y), points.parts()))

    def align(
        self,
        imgDim,
        rgbImg,
        bb=None,
        landmarks=None,
        landmarkIndices=INNER_EYES_AND_BOTTOM_LIP,
        skipMulti=False,
    ):
        r"""align(imgDim, rgbImg, bb=None, landmarks=None, landmarkIndices=INNER_EYES_AND_BOTTOM_LIP)

        Transform and align a face in an image.

        :param imgDim: The edge length in pixels of the square the image is resized to.
        :type imgDim: int
        :param rgbImg: RGB image to process. Shape: (height, width, 3)
        :type rgbImg: numpy.ndarray
        :param bb: Bounding box around the face to align. \
                   Defaults to the largest face.
        :type bb: dlib.rectangle
        :param landmarks: Detected landmark locations. \
                          Landmarks found on `bb` if not provided.
        :type landmarks: list of (x,y) tuples
        :param landmarkIndices: The indices to transform to.
        :type landmarkIndices: list of ints
        :param skipMulti: Skip image if more than one face detected.
        :type skipMulti: bool
        :return: The aligned RGB image. Shape: (imgDim, imgDim, 3)
        :rtype: numpy.ndarray
        """
        assert imgDim is not None
        assert rgbImg is not None
        assert landmarkIndices is not None

        if bb is None:
            bb = self.getLargestFaceBoundingBox(rgbImg, skipMulti)
            if bb is None:
                return

        if landmarks is None:
            landmarks = self.findLandmarks(rgbImg, bb)

        npLandmarks = np.float32(landmarks)
        npLandmarkIndices = np.array(landmarkIndices)

        H = cv2.getAffineTransform(
            npLandmarks[npLandmarkIndices], imgDim * MINMAX_TEMPLATE[npLandmarkIndices]
        )
        thumbnail = cv2.warpAffine(rgbImg, H, (imgDim, imgDim))

        return thumbnail


# In[6]:


def create_model():
    # flat_input = Input(shape=(96 * 96 * 3))
    # my_input = Reshape(target_shape=(96, 96, 3))(flat_input)
    myInput = Input(shape=(96, 96, 3))

    x = ZeroPadding2D(padding=(3, 3), input_shape=(96, 96, 3))(myInput)
    x = Conv2D(64, (7, 7), strides=(2, 2), name="conv1")(x)
    x = BatchNormalization(axis=3, epsilon=0.00001, name="bn1")(x)
    x = Activation("relu")(x)
    x = ZeroPadding2D(padding=(1, 1))(x)
    x = MaxPooling2D(pool_size=3, strides=2)(x)
    x = Lambda(LRN2D, name="lrn_1")(x)
    x = Conv2D(64, (1, 1), name="conv2")(x)
    x = BatchNormalization(axis=3, epsilon=0.00001, name="bn2")(x)
    x = Activation("relu")(x)
    x = ZeroPadding2D(padding=(1, 1))(x)
    x = Conv2D(192, (3, 3), name="conv3")(x)
    x = BatchNormalization(axis=3, epsilon=0.00001, name="bn3")(x)
    x = Activation("relu")(x)
    x = Lambda(LRN2D, name="lrn_2")(x)
    x = ZeroPadding2D(padding=(1, 1))(x)
    x = MaxPooling2D(pool_size=3, strides=2)(x)

    # Inception3a
    inception_3a_3x3 = Conv2D(96, (1, 1), name="inception_3a_3x3_conv1")(x)
    inception_3a_3x3 = BatchNormalization(
        axis=3, epsilon=0.00001, name="inception_3a_3x3_bn1"
    )(inception_3a_3x3)
    inception_3a_3x3 = Activation("relu")(inception_3a_3x3)
    inception_3a_3x3 = ZeroPadding2D(padding=(1, 1))(inception_3a_3x3)
    inception_3a_3x3 = Conv2D(128, (3, 3), name="inception_3a_3x3_conv2")(
        inception_3a_3x3
    )
    inception_3a_3x3 = BatchNormalization(
        axis=3, epsilon=0.00001, name="inception_3a_3x3_bn2"
    )(inception_3a_3x3)
    inception_3a_3x3 = Activation("relu")(inception_3a_3x3)

    inception_3a_5x5 = Conv2D(16, (1, 1), name="inception_3a_5x5_conv1")(x)
    inception_3a_5x5 = BatchNormalization(
        axis=3, epsilon=0.00001, name="inception_3a_5x5_bn1"
    )(inception_3a_5x5)
    inception_3a_5x5 = Activation("relu")(inception_3a_5x5)
    inception_3a_5x5 = ZeroPadding2D(padding=(2, 2))(inception_3a_5x5)
    inception_3a_5x5 = Conv2D(32, (5, 5), name="inception_3a_5x5_conv2")(
        inception_3a_5x5
    )
    inception_3a_5x5 = BatchNormalization(
        axis=3, epsilon=0.00001, name="inception_3a_5x5_bn2"
    )(inception_3a_5x5)
    inception_3a_5x5 = Activation("relu")(inception_3a_5x5)

    inception_3a_pool = MaxPooling2D(pool_size=3, strides=2)(x)
    inception_3a_pool = Conv2D(32, (1, 1), name="inception_3a_pool_conv")(
        inception_3a_pool
    )
    inception_3a_pool = BatchNormalization(
        axis=3, epsilon=0.00001, name="inception_3a_pool_bn"
    )(inception_3a_pool)
    inception_3a_pool = Activation("relu")(inception_3a_pool)
    inception_3a_pool = ZeroPadding2D(padding=((3, 4), (3, 4)))(inception_3a_pool)

    inception_3a_1x1 = Conv2D(64, (1, 1), name="inception_3a_1x1_conv")(x)
    inception_3a_1x1 = BatchNormalization(
        axis=3, epsilon=0.00001, name="inception_3a_1x1_bn"
    )(inception_3a_1x1)
    inception_3a_1x1 = Activation("relu")(inception_3a_1x1)

    inception_3a = concatenate(
        [inception_3a_3x3, inception_3a_5x5, inception_3a_pool, inception_3a_1x1],
        axis=3,
    )

    # Inception3b
    inception_3b_3x3 = Conv2D(96, (1, 1), name="inception_3b_3x3_conv1")(inception_3a)
    inception_3b_3x3 = BatchNormalization(
        axis=3, epsilon=0.00001, name="inception_3b_3x3_bn1"
    )(inception_3b_3x3)
    inception_3b_3x3 = Activation("relu")(inception_3b_3x3)
    inception_3b_3x3 = ZeroPadding2D(padding=(1, 1))(inception_3b_3x3)
    inception_3b_3x3 = Conv2D(128, (3, 3), name="inception_3b_3x3_conv2")(
        inception_3b_3x3
    )
    inception_3b_3x3 = BatchNormalization(
        axis=3, epsilon=0.00001, name="inception_3b_3x3_bn2"
    )(inception_3b_3x3)
    inception_3b_3x3 = Activation("relu")(inception_3b_3x3)

    inception_3b_5x5 = Conv2D(32, (1, 1), name="inception_3b_5x5_conv1")(inception_3a)
    inception_3b_5x5 = BatchNormalization(
        axis=3, epsilon=0.00001, name="inception_3b_5x5_bn1"
    )(inception_3b_5x5)
    inception_3b_5x5 = Activation("relu")(inception_3b_5x5)
    inception_3b_5x5 = ZeroPadding2D(padding=(2, 2))(inception_3b_5x5)
    inception_3b_5x5 = Conv2D(64, (5, 5), name="inception_3b_5x5_conv2")(
        inception_3b_5x5
    )
    inception_3b_5x5 = BatchNormalization(
        axis=3, epsilon=0.00001, name="inception_3b_5x5_bn2"
    )(inception_3b_5x5)
    inception_3b_5x5 = Activation("relu")(inception_3b_5x5)

    inception_3b_pool = AveragePooling2D(pool_size=(3, 3), strides=(3, 3))(inception_3a)
    inception_3b_pool = Conv2D(64, (1, 1), name="inception_3b_pool_conv")(
        inception_3b_pool
    )
    inception_3b_pool = BatchNormalization(
        axis=3, epsilon=0.00001, name="inception_3b_pool_bn"
    )(inception_3b_pool)
    inception_3b_pool = Activation("relu")(inception_3b_pool)
    inception_3b_pool = ZeroPadding2D(padding=(4, 4))(inception_3b_pool)

    inception_3b_1x1 = Conv2D(64, (1, 1), name="inception_3b_1x1_conv")(inception_3a)
    inception_3b_1x1 = BatchNormalization(
        axis=3, epsilon=0.00001, name="inception_3b_1x1_bn"
    )(inception_3b_1x1)
    inception_3b_1x1 = Activation("relu")(inception_3b_1x1)

    inception_3b = concatenate(
        [inception_3b_3x3, inception_3b_5x5, inception_3b_pool, inception_3b_1x1],
        axis=3,
    )

    # Inception3c
    inception_3c_3x3 = conv2d_bn(
        inception_3b,
        layer="inception_3c_3x3",
        cv1_out=128,
        cv1_filter=(1, 1),
        cv2_out=256,
        cv2_filter=(3, 3),
        cv2_strides=(2, 2),
        padding=(1, 1),
    )

    inception_3c_5x5 = conv2d_bn(
        inception_3b,
        layer="inception_3c_5x5",
        cv1_out=32,
        cv1_filter=(1, 1),
        cv2_out=64,
        cv2_filter=(5, 5),
        cv2_strides=(2, 2),
        padding=(2, 2),
    )

    inception_3c_pool = MaxPooling2D(pool_size=3, strides=2)(inception_3b)
    inception_3c_pool = ZeroPadding2D(padding=((0, 1), (0, 1)))(inception_3c_pool)

    inception_3c = concatenate(
        [inception_3c_3x3, inception_3c_5x5, inception_3c_pool], axis=3
    )

    # inception 4a
    inception_4a_3x3 = conv2d_bn(
        inception_3c,
        layer="inception_4a_3x3",
        cv1_out=96,
        cv1_filter=(1, 1),
        cv2_out=192,
        cv2_filter=(3, 3),
        cv2_strides=(1, 1),
        padding=(1, 1),
    )
    inception_4a_5x5 = conv2d_bn(
        inception_3c,
        layer="inception_4a_5x5",
        cv1_out=32,
        cv1_filter=(1, 1),
        cv2_out=64,
        cv2_filter=(5, 5),
        cv2_strides=(1, 1),
        padding=(2, 2),
    )

    inception_4a_pool = AveragePooling2D(pool_size=(3, 3), strides=(3, 3))(inception_3c)
    inception_4a_pool = conv2d_bn(
        inception_4a_pool,
        layer="inception_4a_pool",
        cv1_out=128,
        cv1_filter=(1, 1),
        padding=(2, 2),
    )
    inception_4a_1x1 = conv2d_bn(
        inception_3c, layer="inception_4a_1x1", cv1_out=256, cv1_filter=(1, 1)
    )
    inception_4a = concatenate(
        [inception_4a_3x3, inception_4a_5x5, inception_4a_pool, inception_4a_1x1],
        axis=3,
    )

    # inception4e
    inception_4e_3x3 = conv2d_bn(
        inception_4a,
        layer="inception_4e_3x3",
        cv1_out=160,
        cv1_filter=(1, 1),
        cv2_out=256,
        cv2_filter=(3, 3),
        cv2_strides=(2, 2),
        padding=(1, 1),
    )
    inception_4e_5x5 = conv2d_bn(
        inception_4a,
        layer="inception_4e_5x5",
        cv1_out=64,
        cv1_filter=(1, 1),
        cv2_out=128,
        cv2_filter=(5, 5),
        cv2_strides=(2, 2),
        padding=(2, 2),
    )
    inception_4e_pool = MaxPooling2D(pool_size=3, strides=2)(inception_4a)
    inception_4e_pool = ZeroPadding2D(padding=((0, 1), (0, 1)))(inception_4e_pool)

    inception_4e = concatenate(
        [inception_4e_3x3, inception_4e_5x5, inception_4e_pool], axis=3
    )

    # inception5a
    inception_5a_3x3 = conv2d_bn(
        inception_4e,
        layer="inception_5a_3x3",
        cv1_out=96,
        cv1_filter=(1, 1),
        cv2_out=384,
        cv2_filter=(3, 3),
        cv2_strides=(1, 1),
        padding=(1, 1),
    )

    inception_5a_pool = AveragePooling2D(pool_size=(3, 3), strides=(3, 3))(inception_4e)
    inception_5a_pool = conv2d_bn(
        inception_5a_pool,
        layer="inception_5a_pool",
        cv1_out=96,
        cv1_filter=(1, 1),
        padding=(1, 1),
    )
    inception_5a_1x1 = conv2d_bn(
        inception_4e, layer="inception_5a_1x1", cv1_out=256, cv1_filter=(1, 1)
    )

    inception_5a = concatenate(
        [inception_5a_3x3, inception_5a_pool, inception_5a_1x1], axis=3
    )

    # inception_5b
    inception_5b_3x3 = conv2d_bn(
        inception_5a,
        layer="inception_5b_3x3",
        cv1_out=96,
        cv1_filter=(1, 1),
        cv2_out=384,
        cv2_filter=(3, 3),
        cv2_strides=(1, 1),
        padding=(1, 1),
    )
    inception_5b_pool = MaxPooling2D(pool_size=3, strides=2)(inception_5a)
    inception_5b_pool = conv2d_bn(
        inception_5b_pool, layer="inception_5b_pool", cv1_out=96, cv1_filter=(1, 1)
    )
    inception_5b_pool = ZeroPadding2D(padding=(1, 1))(inception_5b_pool)

    inception_5b_1x1 = conv2d_bn(
        inception_5a, layer="inception_5b_1x1", cv1_out=256, cv1_filter=(1, 1)
    )
    inception_5b = concatenate(
        [inception_5b_3x3, inception_5b_pool, inception_5b_1x1], axis=3
    )

    av_pool = AveragePooling2D(pool_size=(3, 3), strides=(1, 1))(inception_5b)
    reshape_layer = Flatten()(av_pool)
    dense_layer = Dense(128, name="dense_layer")(reshape_layer)
    norm_layer = Lambda(lambda x: K.l2_normalize(x, axis=1), name="norm_layer")(
        dense_layer
    )

    return Model(inputs=[myInput], outputs=norm_layer)


# In[7]:


def shape(x):
    return x.get_shape()


def square(x):
    return tf.square(x)


def concatenate(tensors, axis=-1):
    if axis < 0:
        axis = axis % len(tensors[0].get_shape())
    return tf.concat(tensors, axis)


def LRN2D(x):
    import tensorflow as tf

    return tf.nn.lrn(x, alpha=1e-4, beta=0.75)


def conv2d_bn(
    x,
    layer=None,
    cv1_out=None,
    cv1_filter=(1, 1),
    cv1_strides=(1, 1),
    cv2_out=None,
    cv2_filter=(3, 3),
    cv2_strides=(1, 1),
    padding=None,
):
    num = "" if cv2_out is None else "1"
    tensor = Conv2D(
        cv1_out, cv1_filter, strides=cv1_strides, name=layer + "_conv" + num
    )(x)
    tensor = BatchNormalization(axis=3, epsilon=0.00001, name=layer + "_bn" + num)(
        tensor
    )
    tensor = Activation("relu")(tensor)
    if padding is None:
        return tensor
    tensor = ZeroPadding2D(padding=padding)(tensor)
    if cv2_out is None:
        return tensor
    tensor = Conv2D(
        cv2_out, cv2_filter, strides=cv2_strides, name=layer + "_conv" + "2"
    )(tensor)
    tensor = BatchNormalization(axis=3, epsilon=0.00001, name=layer + "_bn" + "2")(
        tensor
    )
    tensor = Activation("relu")(tensor)
    return tensor


def load_weights():
    weightsDir = "./weights"
    fileNames = filter(lambda f: not f.startswith("."), os.listdir(weightsDir))
    paths = {}
    weights_dict = {}

    for n in fileNames:
        paths[n.replace(".csv", "")] = weightsDir + "/" + n

    for name in weights:
        if "conv" in name:
            conv_w = genfromtxt(paths[name + "_w"], delimiter=",", dtype=None)
            conv_w = np.reshape(conv_w, conv_shape[name])
            conv_w = np.transpose(conv_w, (2, 3, 1, 0))
            conv_b = genfromtxt(paths[name + "_b"], delimiter=",", dtype=None)
            weights_dict[name] = [conv_w, conv_b]
        elif "bn" in name:
            bn_w = genfromtxt(paths[name + "_w"], delimiter=",", dtype=None)
            bn_b = genfromtxt(paths[name + "_b"], delimiter=",", dtype=None)
            bn_m = genfromtxt(paths[name + "_m"], delimiter=",", dtype=None)
            bn_v = genfromtxt(paths[name + "_v"], delimiter=",", dtype=None)
            weights_dict[name] = [bn_w, bn_b, bn_m, bn_v]
        elif "dense" in name:
            dense_w = genfromtxt(weightsDir + "/dense_w.csv", delimiter=",", dtype=None)
            dense_w = np.reshape(dense_w, (128, 736))
            dense_w = np.transpose(dense_w, (1, 0))
            dense_b = genfromtxt(weightsDir + "/dense_b.csv", delimiter=",", dtype=None)
            weights_dict[name] = [dense_w, dense_b]

    return weights_dict


def align(data, width, height, origin):
    fields = ["origin", "height", "width", "nChannels", "mode", "data"]
    array = np.asarray(data, dtype=np.uint8).reshape((width, height, 3))
    res_path = f"{tpcxai_path}/workload/spark/pyspark/workload-pyspark/resources/uc09/shape_predictor_5_face_landmarks.dat"
    alignment = AlignDlib(str(res_path))
    box = alignment.getLargestFaceBoundingBox(array)
    if not box:
        zero = bytearray(96 * 96 * 3)
        return _create_row(fields, [origin, 96, 96, 3, 16, zero])
    landmarks = alignment.findLandmarks(array, box)
    new_landmarks = 68 * [(0, 0)]
    new_landmarks[33] = landmarks[4]
    new_landmarks[36] = landmarks[2]
    new_landmarks[45] = landmarks[0]

    image_aligned = alignment.align(
        96,
        array,
        box,
        landmarks=new_landmarks,
        landmarkIndices=AlignDlib.OUTER_EYES_AND_NOSE,
    )

    length = np.prod(image_aligned.shape)
    data = bytearray(
        image_aligned.astype(dtype=np.int8)[:, :, (2, 1, 0)].reshape(length)
    )
    return _create_row(fields, [origin, 96, 96, 3, 16, data])


def get_identity(image):
    identity = os.path.dirname(image.origin).split("/")[
        -1
    ]  # get the last directory from the path
    return identity


def image_to_array(image):
    emb_array = np.asarray(image[5]).reshape((96, 96, 3))
    emb = emb_array.reshape((96 * 96 * 3))
    emb = emb / 255
    return Vectors.dense(emb)


def empty_vector(shape=128):
    vals = np.zeros(shape)
    return Vectors.dense(vals)


def load_data(
    session: SparkSession, num_proc, connection, base_path, path, stage
) -> DataFrame:
    # meta_data = session.read.csv(path, sep=INPUT_SEPARATOR, inferSchema=True, header=True)
    meta_data = (
        spark.read.format("snowflake")
        .options(**connection)
        .option("dbtable", f"{stage}_CUSTOMER_IMAGES_META")
        .load()
    )
    meta_data = meta_data.repartition(num_proc)

    # TODO: Can this be stored in snowpark?
    img_data_seq = session.sparkContext.sequenceFile(
        f"{path}/{stage}/CUSTOMER_IMAGES.seq"
    )
    img_data = session.createDataFrame(img_data_seq, ["IMG_FILENAME", "IMG_BYTES"])

    @udf(returnType=StringType())
    def make_path(f):
        return f"{base_path}/output/raw_data/{stage}/{f}.png"

    meta_data = meta_data.withColumn("FILEPATH", make_path("IMG_FILENAME"))

    img_data = img_data.repartition(num_proc)

    data = meta_data.join(img_data, [meta_data.FILEPATH == img_data.IMG_FILENAME])

    @udf(returnType=StringType())
    def path_to_identity(img_path):
        return os.path.dirname(img_path).split("/")[-1]

    @udf(returnType=IMG_SCHEMA)
    def img_read(img_path, img_bytes):
        img = cv2.imdecode(np.frombuffer(img_bytes, np.uint8), cv2.IMREAD_COLOR)
        width = img.shape[0]
        height = img.shape[1]
        n_channels = 3
        mode = 16
        data = bytearray(img)
        return img_path, height, width, n_channels, mode, data

    data = data.withColumn("IDENTITY", path_to_identity(data.FILEPATH))
    data = data.withColumn("IMAGE", img_read(data.FILEPATH, data.IMG_BYTES))
    if "IDENTITY" in data.columns:
        return data.select("IDENTITY", "SAMPLE", "IMAGE").cache()
    else:
        return data.select("SAMPE", "IMAGE").cache()


def pre_process(images: DataFrame):
    align_udf = udf(
        align,
        StructType(
            [
                StructField("origin", StringType(), True),
                StructField("height", IntegerType(), True),
                StructField("width", IntegerType(), True),
                StructField("nChannels", IntegerType(), True),
                StructField("mode", IntegerType(), True),
                StructField("data", BinaryType(), True),
            ]
        ),
    )

    image_to_array_udf = udf(image_to_array, VectorUDT())
    empty_vector_udf = udf(empty_vector, VectorUDT())

    images_aligned = images.withColumn(
        "IMAGE_ALIGNED",
        align_udf(
            images.IMAGE.data,
            images.IMAGE.width,
            images.IMAGE.height,
            images.IMAGE.origin,
        ),
    )

    features = images_aligned.withColumn(
        "FEATURES", image_to_array_udf(images_aligned.IMAGE_ALIGNED)
    )
    features = features.withColumn("EMBEDDING", empty_vector_udf())

    return features.select("IDENTITY", "SAMPLE", "FEATURES", "EMBEDDING")


def train_classifier_svm(data):
    indexer = StringIndexer(
        inputCol="IDENTITY", outputCol="LABEL", handleInvalid="keep"
    )
    log_reg = LogisticRegression(featuresCol="EMBEDDING", labelCol="LABEL")
    pipeline = Pipeline(stages=[indexer, log_reg])
    model = pipeline.fit(data)
    return model


def train_classifier(
    data,
    work_dir,
    defaultParallelism,
    current_user,
    epochs,
    batch_size,
    current_user="",
    learning_rate=None,
):
    indexer = StringIndexer(
        inputCol="IDENTITY", outputCol="LABEL", handleInvalid="keep"
    )
    onehot = OneHotEncoderEstimator(
        inputCols=["LABEL"],
        outputCols=["LABEL_ENC"],
        dropLast=False,
        handleInvalid="keep",
    )
    pipeline = Pipeline(stages=[indexer, onehot])
    encoder = pipeline.fit(data)

    data_enc = encoder.transform(data)

    # prepare data
    num_classes = encoder.stages[1].categorySizes[0]
    num_samples = data_enc.count()

    # calculate number of processes
    num_processes = math.floor(
        min(
            defaultParallelism,
            num_samples * 0.8 / batch_size,
            num_samples * 0.2 / batch_size,
        )
    )
    num_processes = max(num_processes, 1)

    # create keras model that is equivalent of a SVM
    model = Sequential()
    model.add(Dense(128, input_shape=(128,)))
    # one neuron per identity and one additional for unknown identities
    model.add(Dense(num_classes + 1, kernel_regularizer="l2", activation="linear"))
    model.summary(line_length=120)
    lr = 0.1 / num_processes if not learning_rate else learning_rate
    opt = Adadelta(learning_rate=lr)
    early_stop = EarlyStopping(
        monitor="loss", patience=10, verbose=1, restore_best_weights=True
    )

    # create Horovod store object for auxiliary files
    store = HDFSStore(work_dir, user=current_user)
    # wrap Keras model with Horovod KerasEstimator
    estimator = KerasEstimator(
        model=model,
        feature_cols=["EMBEDDING"],
        label_cols=["LABEL_ENC"],
        optimizer=opt,
        loss="categorical_hinge",
        num_proc=num_processes,
        epochs=epochs,
        batch_size=batch_size,
        shuffle_buffer_size=4 * 1024,
        callbacks=[early_stop],
        store=store,
        train_steps_per_epoch=num_samples // batch_size,
    )
    data_enc = data_enc.repartition(num_processes)
    trained_model = estimator.fit(data_enc)
    return trained_model, encoder


def serve(model, encoder, data, novsm):
    predictions: DataFrame = model.transform(data)

    if novsm:

        def argmax_(v):
            return int(np.argmax(v))

        argmax_udf = udf(argmax_, IntegerType())
        # get the numerical label from probabilities of labels
        predictions = predictions.withColumn(
            "PREDICTION", argmax_udf("LABEL_ENC__OUTPUT")
        )
        # convert numerical labels back into the original string representation
        indexer = encoder.stages[0]
    else:
        indexer = model.stages[0]

    labels = indexer.labels
    converter = IndexToString(
        inputCol="prediction", outputCol="PREDICTED_IDENTITY"
    ).setLabels(labels)
    converted = converter.transform(predictions)
    return converted.select("SAMPLE", "PREDICTED_IDENTITY").withColumnRenamed(
        "PREDICTED_IDENTITY", "IDENTITY"
    )


def my_loss(y_true, y_pred):
    loss = TripletHardLoss(0.2)
    loss_value = loss(y_true[:, 0], y_pred)
    return loss_value


# In[8]:

tf.compat.v1.disable_eager_execution()

# In[9]:

learning_rate = None
batch_size = 64
num_processes = 15
epochs_embedding = 15
epochs_classifier = 10000
nosvm = False

current_user = spark.sparkContext.sparkUser()

base_path = os.path.join(tpcxai_path, f"{tpcxai_size}_GB")
data_path = os.path.join(base_path, "processed")

work_dir = f"{base_path}/output/output/uc09"

# In[10]:

# DBTITLE 1,Load Training Data
start = timeit.default_timer()
images = load_data(
    spark, num_processes, sf_connection, base_path, data_path, "training"
)
end = timeit.default_timer()
load_time = end - start
print("load time:\t", load_time)

# In[11]:

images.show()

# In[12]:

# DBTITLE 1,Process Data
start = timeit.default_timer()
preprocessed_data = pre_process(images)
end = timeit.default_timer()
pre_process_time = end - start
print("pre-process time:\t", pre_process_time)

# In[13]:

preprocessed_data.show()

# In[14]:

# DBTITLE 1,Training
start = timeit.default_timer()
# store = Store.create(work_dir)
embedding_model = create_model()
lr = learning_rate if learning_rate else 0.000001
opt = optimizers.Adam(learning_rate=lr)
weights_path = f"{tpcxai_path}/workload/spark/pyspark/workload-pyspark/resources/uc09/nn4.small2.v1.h5"
embedding_model.load_weights(weights_path)
indexer = StringIndexer(
    inputCol="IDENTITY", outputCol="IDENTITY_INT", handleInvalid="keep"
)
training_data = indexer.fit(preprocessed_data).transform(preprocessed_data)


# create vector (same lengths as output from embedding, i.e. 128) to prevent reshaping issues with horovod
# horovod assumes that trainung out and labels have the same dimensions, but for triplet loss this is not true
@udf(returnType=VectorUDT())
def make_vec(lbl):
    vec = np.empty([128])
    vec[0] = lbl
    return Vectors.dense(vec)


training_data = training_data.withColumn("IDENTITY_INT_VEC", make_vec("IDENTITY_INT"))
training_data_path = f"{work_dir}/tmp/preprocessed.parquet"
training_data.write.mode("overwrite").parquet(training_data_path)

store = HDFSStore(work_dir, user=current_user)

training_data = spark.read.parquet(training_data_path)

estimator = hvd.KerasEstimator(
    num_proc=num_processes,
    model=embedding_model,
    optimizer=opt,
    epochs=epochs_embedding,
    feature_cols=["FEATURES"],
    batch_size=batch_size,
    label_cols=["IDENTITY_INT_VEC"],
    store=store,
    loss=my_loss,
    custom_objects={"TripletHardLoss": TripletHardLoss, "my_loss": my_loss, "K": K},
    shuffle_buffer_size=2,
)

training_data = training_data.repartition(num_processes)
training_data = training_data.drop("EMBEDDING")
embedding = estimator.fit(training_data).setOutputCols(["EMBEDDING"])

embedded_data = embedding.transform(training_data)
embedded_data_path = f"{work_dir}/tmp/embedded.parquet"
embedding.write().overwrite().save(f"{work_dir}/embedding")

if nosvm:
    model, encoder = train_classifier(
        embedded_data,
        work_dir,
        num_processes,
        current_user,
        epochs=epochs_classifier,
        batch_size=batch_size,
        current_user=current_user,
    )
    encoder.write().overwrite().save(f"{work_dir}/model.enc")
else:
    embedded_data.write.mode("overwrite").parquet(embedded_data_path)
    embedded_data = spark.read.parquet(embedded_data_path).repartition(num_processes)
    model = train_classifier_svm(embedded_data)
end = timeit.default_timer()
train_time = end - start
print("train time:\t", train_time)

# In[15]:

# DBTITLE 1,Save Model
model.write().overwrite().save(f"{work_dir}/model")

# In[16]:

# DBTITLE 1,Serving
images = load_data(spark, num_processes, sf_connection, base_path, data_path, "serving")
preprocessed_data = pre_process(images)

# In[17]:

start = timeit.default_timer()
embedding = hvd.KerasModel.load(f"{work_dir}/embedding")
if nosvm:
    model = hvd.KerasModel.load(f"{work_dir}/model")
    encoder = PipelineModel.load(f"{work_dir}/model.enc")
else:
    model = PipelineModel.load(f"{work_dir}/model")
    encoder = None
embedded_data = embedding.transform(preprocessed_data.drop("embedding"))
predictions: DataFrame = serve(model, encoder, embedded_data, nosvm)
end = timeit.default_timer()
serve_time = end - start
print("serve time:\t", serve_time)
predictions.select("sample", "identity").write.mode("overwrite").csv(
    f"{work_dir}/predictions.csv", header=True
)
