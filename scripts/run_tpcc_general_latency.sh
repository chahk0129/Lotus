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
output_dir_latency="$output_dir/latency"
mkdir -p $output_dir
mkdir -p $output_dir_latency
rm outputs/stats.csv

## transport config
twosided_config="$config RUN_TIME=1 TRANSPORT=TWO_SIDED COLLECT_LATENCY=true"
onesided_config="$config RUN_TIME=1 TRANSPORT=ONE_SIDED COLLECT_LATENCY=true BATCH=0"

## workload config
num_wh="4 16 64"

## cc schemes
cc_one="NO_WAIT WAIT_DIE"
cc_two="NO_WAIT WAIT_DIE WOUND_WAIT"

for wh in $num_wh; do
    echo "Starting workload with $wh warehouses"

    for cc in $cc_one; do
        param="$onesided_config NUM_WH=$wh CC_ALG=$cc"
        run_experiment "$param"
        mv build/cdf.txt $output_dir_latency/${wh}_${cc}_onesided.txt
    done

    for cc in $cc_two; do
        param="$twosided_config NUM_WH=$wh CC_ALG=$cc BATCH=1"
        run_experiment "$param"
        mv build/cdf.txt $output_dir_latency/${wh}_${cc}_twosided.txt
    done
done
rm outputs/stats.csv