#!/bin/bash

###########################################
# This script generates a main.sql script #
# which executes the TPC-DI Benchmark     #
###########################################

function main {

    echo "-- Main Execution Procedure for TPC_DI Benchmark" > "00_setup.sql"

    # valid scale factors
    scale_factors=(5 10 100 1000 5000)

    if [[ " ${scale_factors[*]} " =~ " $1 " ]]; then
        scale=$1
    else
        help
        exit
    fi;
    genSetupPlan
    genExecutionPlan ${scale=100}
    instructions ${scale=100}

}

function genSetupPlan {

    # setup the sequence of stages to process
    stages_ddl=("stg" "ods" "wh")

    # setup the sequence of ddl creation for each medallion stage
    sequence_ddl=("tables" "views" "procedures" "tasks")

    # setup the warehouse for TPC_DI test
    cat src/warehouse.sql >> "00_setup.sql"

    # concat to create a large sql script
    for a in ${stages_ddl[@]}; do
        for b in ${sequence_ddl[@]}; do
            cat src/$a/$b.sql >> "00_setup.sql"
        done;
    done;

    # generate the needed formats
    cat src/formats.sql >> "00_setup.sql"
}

function genExecutionPlan {

    scale=$1
    # create the executionplan
    cat << EOF > "01_execute.sql"
-- Execute TPC-DI Benchmark with Scale Factor : $scale

-- Swith to use the TPC_DI warehouse
USE WAREHOUSE TPCDI_GENERAL;

-- Execute the TPC DI Benchmark
CALL TPCDI_WH.PUBLIC.RUN_ALL_SP(100);

EOF
}

function instructions {
    echo ""
    echo "Finished compiling TPC-DI benchmarks with scale factor : $1"
    echo "Run 00_setup.sql in Snowflake first to setup the TPC_DI databases."
    echo "Create a stage in TPCDI_STG.PUBLIC.@TPCDI_FILES that stores the TPC DI files in a /tmp/tpcdi."
    echo "Run 01_execute.sql to execute the Snowflake TPC DI Benchmark."
    echo ""
}

function help {
    echo ""
    echo "Usage:  ./main.sh [scale_factor, values that are valid at 5, 10, 100, 1000, 5000]"
    echo ""
}

# call the main execution function
main $@;