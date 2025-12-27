#!/bin/bash

# Function to run experiments with given parameters
# Usage: run_experiment <config_parameters>
run_experiment() {
    local params="$*"
    echo "Running experiment with params: $params"
    python run.py $params
}

# Run YCSB benchmark for disaggregated memory DB
config="exp_profile/tpcc.json"
output_dir="outputs/tpcc_general"
output_dir_tput="$output_dir/throughput"
mkdir -p $output_dir
mkdir -p $output_dir_tput
rm outputs/stats.csv

## transport config
twosided_config="$config TRANSPORT=TWO_SIDED COLLECT_LATENCY=false"
onesided_config="$config TRANSPORT=ONE_SIDED COLLECT_LATENCY=false BATCH=0"

## workload config
num_wh="4 16 64"

## compute thread config
threads="1 4 8 16 32 40"

## cc schemes
cc_one="NO_WAIT WAIT_DIE"
cc_two="NO_WAIT WAIT_DIE WOUND_WAIT"

for wh in $num_wh; do
    echo "Starting workload with $wh warehouses"

    for thd in $threads; do
        echo "Starting with $thd client threads"

        for cc in $cc_one; do
            param="$onesided_config NUM_WH=$wh NUM_CLIENT_THREADS=$thd CC_ALG=$cc"
            run_experiment "$param"
        done

        for cc in $cc_two; do
            param="$twosided_config NUM_WH=$wh NUM_CLIENT_THREADS=$thd CC_ALG=$cc BATCH=1"
            run_experiment "$param"
        done
    done
    mv outputs/stats.csv $output_dir_tput/${wh}.csv
done