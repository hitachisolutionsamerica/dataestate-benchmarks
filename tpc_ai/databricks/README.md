# [Overview](#overview)

## [Table of Contents](#toc)
   * [Overview](#overview)
   * [Prerequisites](#prerequisites)
   * [Code](#code)
      * [Data Generator](#data-generator)
      * [Notebooks](#notebooks)

This code is meant to run the TPC-AI benchmark using Databricks for a given scale factor using four of the official use cases.  The code is based on the [TPC-AI specification](https://www.tpc.org/tpcx-ai/default5.asp).
The use cases are as follows:

- Use Case 2
- Use Case 5
- Use Case 8
- Use Case 9

The code is not meant to be a perfect implementation of the specification, but rather a good enough implementation of the specification to be used for unofficial benchmarking purposes.

# [Prerequisites](#prerequisites)

The code assumes that you have a Databricks workspace and that you have a cluster set up with the following libraries installed:

- [Tensorflow](https://www.tensorflow.org/)
- [Keras](https://keras.io/)
- [Pandas](https://pandas.pydata.org/)
- [Numpy](https://numpy.org/)
- [Scikit-Learn](https://scikit-learn.org/stable/)

The code also assumes that you have a database with the tables from the TPC-AI specification loaded into it.

# [Code](#code)

The code is divided into four notebooks, one for each use case. The notebooks require the use of the TPC-AI Data generator to create the data which is then loaded into DBFS as raw files.  The data generator can be found in TPCxAIDataGeneration.py. For each, 

## [Data Generator](#data-generator)
Within the TPCxAIDataGeneration.py file, you may specify the scale factor and location of the TPC_AI data in DBFS.


## [Notebooks](#notebooks)
Within each notebook, you may specify the scale factor and location of the TPC_AI data in DBFS. As a rule of thumb, each notebook will load the raw data from DBFS, save it into delta tables, transform and preprocess the tables into a suitable training data format, train a model, save the model, then serve the model, all the while timing the speed of each step.



UNDER CONSTRUCTION

