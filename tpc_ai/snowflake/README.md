# [Overview](#overview)

## [Table of Contents](#toc)
   * [Overview](#overview)
   * [Prerequisites](#prerequisites)
   * [Code](#code)
      * [Data Generator](#data-generator)
      * [Notebooks](#notebooks)

This code is meant to run the TPC-AI benchmark using Snowflake for a given scale factor using four of the official use cases.  The code is based on the [TPC-AI specification](https://www.tpc.org/tpcx-ai/default5.asp).
The use cases are as follows:

- Use Case 2
- Use Case 5
- Use Case 8
- Use Case 9

The code is not meant to be a perfect implementation of the specification, but rather a good enough implementation of the specification to be used for unofficial benchmarking purposes.

# [Prerequisites](#prerequisites)

The code assumes that you have a Snowflake workspace and that you also have a 3rd party (managed or unmanaged!) Spark-cluster set up with the following libraries installed:

- [Tensorflow](https://www.tensorflow.org/)
- [Keras](https://keras.io/)
- [Pandas](https://pandas.pydata.org/)
- [Numpy](https://numpy.org/)
- [Scikit-Learn](https://scikit-learn.org/stable/)
- [Horovod](https://horovod.readthedocs.io/en/stable/)



UNDER CONSTRUCTION