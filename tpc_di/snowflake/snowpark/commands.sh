CONDA_SUBDIR=osx-64 conda create -n snowparks_tpcdi python=3.8 numpy pandas --override-channels -c https://repo.anaconda.com/pkgs/snowflake
conda activate snowparks_tpcdi
conda install snowflake-snowpark-python
conda install snowflake-snowpark-python pandas
